/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/

/*
        This is a part of TempTable implementation concerned with the query
   execution low-level mechanisms
*/

#include "common/assert.h"
#include "common/data_format.h"
#include "core/engine.h"
#include "core/pack_guardian.h"
#include "core/sorter_wrapper.h"
#include "core/temp_table.h"
#include "core/transaction.h"
#include "exporter/data_exporter.h"
#include "system/fet.h"
#include "system/io_parameters.h"
#include "system/rc_system.h"
#include "system/txt_utils.h"
#include "types/value_parser4txt.h"
#include "util/thread_pool.h"
#include "vc/expr_column.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {

bool TempTable::OrderByAndMaterialize(std::vector<SortDescriptor> &ord, int64_t limit, int64_t offset,
                                      ResultSender *sender)  // Sort MultiIndex using some (existing) attributes
                                                             // in some tables
{
  // "limit=10; offset=20" means that the first 10 positions of sorted table
  // will contain objects 21...30.
  MEASURE_FET("TempTable::OrderBy(...)");

  thd_proc_info(m_conn_->Thd(), "order by");
  DEBUG_ASSERT(limit >= 0 && offset >= 0);
  num_of_obj_ = limit;

  if ((int)ord.size() == 0 || filter_.mind->NumOfTuples() < 2 || limit == 0) {
    ord.clear();
    return false;
  }

  int task_num{1};
  int total_limit = limit + offset;
  DimensionVector all_dims(filter_.mind->NumOfDimensions());
  all_dims.SetAll();

  int one_dim = {-1};
  int no_dims = all_dims.NoDimsUsed();

  if (no_dims == 1) {
    for (int i = 0; i < filter_.mind->NumOfDimensions(); i++) {
      if (all_dims[i]) {
        if (filter_.mind->GetFilter(i))
          one_dim = i;  // exactly one filter (non-join or join with forgotten dims)
        break;
      }
    }
  }

  int packs_no = (int)((filter_.mind->OrigSize(one_dim) + ((1 << filter_.mind->ValueOfPower()) - 1)) >>
                       filter_.mind->ValueOfPower());
  // Fixme: single thread control logic based on the following assumption:
  // 1. Single thread is enough for cases with pack num less than 20
  //   A rough statistic is it takes about 1 secs handle 20 packs - Intel(R)
  //   Xeon(R) CPU E5-2430 0 @ 2.20GHz
  // 2. Cannot support multi-dimension(join) case as MIIndex rewind so far does
  // not support it.
  if (tianmu_sysvar_orderby_speedup && packs_no > 20 && no_dims == 1) {
    task_num = 8;
    // recheck the up threashold for each SortLimit sub-sortedtable
    if (((packs_no - 1) * ((1 << filter_.mind->ValueOfPower()) - 1)) / task_num < (limit + offset)) {
      task_num = 1;
      TIANMU_LOG(LogCtl_Level::INFO, "Beyond uplimit of limit sort, switch to single thread logic. ");
    }

    total_limit = task_num * (limit + offset);
  }

  // Prepare sorter
  std::vector<vcolumn::VirtualColumn *> vc_for_prefetching;
  SorterWrapper sorted_table(*(filter_.mind), total_limit);
  // Fixme: make task_num configurable or auto assigned.
  SorterWrapper subsorted_table[8] = {
      SorterWrapper(*(filter_.mind), limit + offset), SorterWrapper(*(filter_.mind), limit + offset),
      SorterWrapper(*(filter_.mind), limit + offset), SorterWrapper(*(filter_.mind), limit + offset),
      SorterWrapper(*(filter_.mind), limit + offset), SorterWrapper(*(filter_.mind), limit + offset),
      SorterWrapper(*(filter_.mind), limit + offset), SorterWrapper(*(filter_.mind), limit + offset)};

  int sort_order{0};
  for (auto &j : attrs_) {
    if (j->alias_ != nullptr) {
      vcolumn::VirtualColumn *vc = j->term_.vc;
      DEBUG_ASSERT(vc);

      sort_order = 0;
      for (uint i = 0; i < ord.size(); i++) {
        if (ord[i].vc == vc) {
          sort_order = (ord[i].dir == 0 ? (i + 1) : -(i + 1));
          ord[i].vc = nullptr;  // annotate this entry as already added
        }
      }

      sorted_table.AddSortedColumn(vc, sort_order, true);

      if (task_num != 1) {
        for (int i = 0; i < task_num; i++) subsorted_table[i].AddSortedColumn(vc, sort_order, true);
      }

      vc_for_prefetching.push_back(vc);
    }
  }

  // find all columns not added yet (i.e. not visible in output)
  for (uint i = 0; i < ord.size(); i++) {
    if (ord[i].vc != nullptr) {
      sort_order = (ord[i].dir == 0 ? (i + 1) : -(i + 1));
      if (task_num != 1) {
        for (int j = 0; j < task_num; j++)
          // v_sw[j].AddSortedColumn(ord[i].vc, sort_order, true);
          subsorted_table[j].AddSortedColumn(ord[i].vc, sort_order, true);
      }

      sorted_table.AddSortedColumn(ord[i].vc, sort_order, false);
      vc_for_prefetching.push_back(ord[i].vc);
    }
  }

  if (task_num == 1)
    sorted_table.InitSorter(*(filter_.mind), true);
  else
    sorted_table.InitSorter(*(filter_.mind), false);

  if (sorted_table.GetSorter() && std::strcmp(sorted_table.GetSorter()->Name(), "Heap Sort") != 0) {
    TIANMU_LOG(LogCtl_Level::DEBUG, "Multi-thread order by is not supported for %s table.",
               sorted_table.GetSorter()->Name());
    task_num = 1;
  }

  // Put data
  std::vector<PackOrderer> po(filter_.mind->NumOfDimensions());
  if (task_num == 1) {
    sorted_table.SortRoughly(po);
  }

  MIIterator it(filter_.mind, all_dims, po);
  int64_t local_row{0};
  bool continue_now{true};

  ord.clear();
  if (task_num == 1) {
    while (it.IsValid() && continue_now) {
      if (m_conn_->Killed())
        throw common::KilledException();

      if (it.PackrowStarted()) {
        if (sorted_table.InitPackrow(it)) {
          local_row += it.GetPackSizeLeft();
          it.NextPackrow();
          continue;
        }
      }

      continue_now = sorted_table.PutValues(it);  // return false if a limit is already reached (min. values only)
      ++it;

      local_row++;
      if (local_row % (1024 * 1024 * 10) == 0)
        rc_control_.lock(m_conn_->GetThreadID())
            << "Preparing values to sort (" << int(local_row / double(filter_.mind->NumOfTuples()) * 100) << "% done)."
            << system::unlock;
    }
  } else {
    int mod = packs_no % task_num;
    int num = packs_no / task_num;

    std::vector<MultiIndex> mis;
    mis.reserve(task_num);
    std::vector<MIIterator> taskIterator;
    taskIterator.reserve(task_num);

    for (int i = 0; i < task_num; ++i) {
      int pstart = ((i == 0) ? 0 : mod + i * num);
      int pend = mod + (i + 1) * num - 1;
      TIANMU_LOG(LogCtl_Level::INFO, "create new MIIterator: start pack %d, endpack %d", pstart, pend);

      auto &mi = mis.emplace_back(*filter_.mind, true);
      auto &mii = taskIterator.emplace_back(&mi, all_dims, po);

      mii.SetTaskNum(task_num);
      mii.SetTaskId(i);
      mii.SetNoPacksToGo(pend);
      mii.RewindToPack(pstart);
    }

    TIANMU_LOG(LogCtl_Level::DEBUG, "table statistic  no_dim %d, packs_no %d \n", one_dim, packs_no);
    // Repeat the same logic to prepare the new sort tables
    // Note: Don't RoughSort them as it would impact initPack logic
    // and some rows would be skipped from adding in the sort table

    for (int i = 0; i < task_num; i++) subsorted_table[i].InitSorter(*(filter_.mind), false);

    utils::result_set<size_t> res;
    for (int i = 0; i < task_num; i++)
      res.insert(ha_rcengine_->query_thread_pool.add_task(&TempTable::TaskPutValueInST, this, &taskIterator[i],
                                                          current_txn_, &subsorted_table[i]));
    if (filter_.mind->m_conn->Killed())
      throw common::KilledException("Query killed by user");

    for (int i = 0; i < task_num; ++i) {
      local_row += res.get(i);
      continue_now = sorted_table.PutValues(subsorted_table[i]);
    }
  }

  TIANMU_LOG(LogCtl_Level::DEBUG,
             "SortTable preparation done. row num %ld, offset %ld, limit %ld, "
             "task_num %d",
             local_row, offset, limit, task_num);

  // Create output
  for (uint i = 0; i < NumOfAttrs(); i++) {
    if (attrs_[i]->alias_ != nullptr) {
      if (sender)
        attrs_[i]->CreateBuffer(
            num_of_obj_ > tianmu_sysvar_result_sender_rows ? tianmu_sysvar_result_sender_rows : num_of_obj_, m_conn_,
            num_of_obj_ > tianmu_sysvar_result_sender_rows);
      else
        attrs_[i]->CreateBuffer(num_of_obj_, m_conn_);
    }
  }

  int64_t global_row{0};
  int64_t val64{0};
  int64_t offset_done{0};
  int64_t produced_rows{0};
  bool null_value{false};
  bool valid{true};

  local_row = 0;
  types::BString val_s;

  do {  // outer loop - through streaming buffers (if sender != nullptr)
    do {
      valid = sorted_table.FetchNextRow();
      if (valid && global_row >= offset) {
        int col = 0;
        if (m_conn_->Killed())
          throw common::KilledException();

        for (auto &attr : attrs_) {
          if (attr->alias_ != nullptr) {
            switch (attr->TypeName()) {
              case common::ColumnType::STRING:
              case common::ColumnType::VARCHAR:
              case common::ColumnType::BIN:
              case common::ColumnType::BYTE:
              case common::ColumnType::VARBYTE:
              case common::ColumnType::LONGTEXT:
                val_s = sorted_table.GetValueT(col);
                attr->SetValueString(local_row, val_s);
                break;
              default:
                val64 = sorted_table.GetValue64(col, null_value);  // works also for constants
                if (null_value)
                  attr->SetValueInt64(local_row, common::NULL_VALUE_64);
                else
                  attr->SetValueInt64(local_row, val64);
                break;
            }
            col++;
          }
        }

        local_row++;
        ++produced_rows;

        if ((global_row - offset + 1) % (1024 * 1024 * 10) == 0)
          rc_control_.lock(m_conn_->GetThreadID())
              << "Retrieving sorted rows (" << int((global_row - offset) / double(limit - offset) * 100) << "% done)."
              << system::unlock;
      } else if (valid)
        ++offset_done;

      global_row++;
    } while (valid && global_row < limit + offset &&
             !(sender && local_row >= tianmu_sysvar_result_sender_rows));  // a limit for
                                                                           // streaming buffer
    // Note: what about SetNumOfMaterialized()? Only no_obj is set now.
    if (sender) {
      TempTable::RecordIterator iter = begin();
      for (int i = 0; i < local_row; i++) {
        sender->Send(iter);
        ++iter;
      }
      // TIANMU_LOG(LogCtl_Level::DEBUG, "Put sort output - %d rows in sender", local_row);
      local_row = 0;
    }

  } while (valid && global_row < limit + offset);

  rc_control_.lock(m_conn_->GetThreadID()) << "Sorted end, rows retrieved." << system::unlock;

  // TIANMU_LOG(LogCtl_Level::INFO, "OrderByAndMaterialize complete global_row %d, limit %d,
  // offset %d", global_row, limit, offset);
  return true;
}

void TempTable::FillMaterializedBuffers(int64_t local_limit, int64_t local_offset, ResultSender *sender,
                                        bool pagewise) {
  if (filter_.mind->ZeroTuples())
    return;

  if (sender) {
    SendResult(local_limit, local_offset, *sender, pagewise);
    return;
  }

  num_of_obj_ = local_limit;
  uint page_size = CalculatePageSize();

  if (pagewise)
    // the number of rows to be sent at once
    page_size = std::min(page_size, tianmu_sysvar_result_sender_rows);

  if (num_of_materialized_ == 0) {
    // Column statistics
    if (m_conn_->DisplayAttrStats()) {
      for (uint j = 0; j < NumOfAttrs(); j++) {
        if (attrs_[j]->term_.vc)
          attrs_[j]->term_.vc->DisplayAttrStats();
      }

      m_conn_->SetDisplayAttrStats(false);  // already displayed
    }
  }

  bool has_intresting_columns{false};
  for (auto &attr : attrs_) { /* materialize dependent tables */
    if (attr->ShouldOutput()) {
      has_intresting_columns = true;
      break;
    }
  }

  if (!has_intresting_columns)
    return;

  MIIterator it(filter_.mind, filter_.mind->ValueOfPower());
  if (pagewise && local_offset < num_of_materialized_)
    local_offset = num_of_materialized_;  // continue filling

  if (local_offset > 0)
    it.Skip(local_offset);

  if (!it.IsValid())
    return;

  int64_t row = local_offset;
  std::vector<char> skip_parafilloutput;
  std::set<int> set_vcid;

  for (auto &attr : attrs_) {  // check if there is duplicated columns, mark skip flag for yes
    char skip = 0;

    if (attr->NeedFill()) {
      auto search = set_vcid.find(attr->term_.vc_id);
      if (search != set_vcid.end()) {  // found duplicated column, skip parallel filling output
        skip = 1;
      } else {
        set_vcid.insert(attr->term_.vc_id);
      }
    }

    skip_parafilloutput.push_back(skip);
  }

  if (attrs_.size() == 0)
    return;

  // Semantics of variables:
  // row		- a row number in orig. tables
  // no_obj	- a number of rows to be actually sent (offset already omitted)
  // start_row, page_end - in terms of orig. tables
  while (it.IsValid() && row < num_of_obj_ + local_offset) { /* go thru all rows */
    bool outer_iterator_updated = false;
    MIIterator page_start(it);
    int64_t start_row = row;
    int64_t page_end = (((row - local_offset) / page_size) + 1) * page_size + local_offset;
    // where the current TempTable buffer ends, in terms of multiindex rows
    // (integer division)
    if (page_end > num_of_obj_ + local_offset)
      page_end = num_of_obj_ + local_offset;

    for (auto attr : attrs_) attr->CreateBuffer(page_end - start_row, m_conn_, pagewise);

    auto &attr = attrs_[0];
    if (attr->NeedFill()) {
      MIIterator i(page_start);
      auto cnt = attr->FillValues(i, start_row, page_end - start_row);
      num_of_materialized_ += cnt;

      if (!outer_iterator_updated) {
        it.swap(i); /* update global iterator - once */
        row = start_row + cnt;
        outer_iterator_updated = true;
      }
    }

    utils::result_set<void> res;
    for (uint i = 1; i < attrs_.size(); i++) {
      if (!skip_parafilloutput[i]) {
        res.insert(ha_rcengine_->query_thread_pool.add_task(&TempTable::FillbufferTask, this, attrs_[i], current_txn_,
                                                            &page_start, start_row, page_end));
      }
    }

    res.get_all_with_except();

    for (uint i = 1; i < attrs_.size(); i++)
      if (skip_parafilloutput[i])
        FillbufferTask(attrs_[i], current_txn_, &page_start, start_row, page_end);

    if (lazy_)
      break;
  }
}

void TempTable::SendResult(int64_t limit, int64_t offset, ResultSender &sender, bool pagewise) {
  num_of_obj_ = limit;

  if (num_of_materialized_ == 0) {
    //////// Column statistics ////////////////////////
    if (m_conn_->DisplayAttrStats()) {
      for (uint j = 0; j < NumOfAttrs(); j++) {
        if (attrs_[j]->term_.vc)
          attrs_[j]->term_.vc->DisplayAttrStats();
      }

      m_conn_->SetDisplayAttrStats(false);  // already displayed
    }
  }

  bool has_intresting_columns = false;
  for (auto &attr : attrs_) { /* materialize dependent tables */
    if (attr->ShouldOutput()) {
      has_intresting_columns = true;
      break;
    }
  }

  if (!has_intresting_columns)
    return;

  MIIterator it(filter_.mind, filter_.mind->ValueOfPower());
  if (pagewise && offset < num_of_materialized_)
    offset = num_of_materialized_;  // continue filling

  if (offset > 0)
    it.Skip(offset);

  int row = 0;
  bool first_row_for_vc = true;
  while (it.IsValid() && row < num_of_obj_) {
    if (it.PackrowStarted() || first_row_for_vc) {
      for (auto &attr : attrs_) attr->term_.vc->LockSourcePacks(it);

      first_row_for_vc = false;
    }

    std::vector<std::unique_ptr<types::RCDataType>> record;
    for (uint att = 0; att < NumOfDisplaybleAttrs(); ++att) {
      Attr *col = GetDisplayableAttrP(att);
      common::ColumnType ct = col->TypeName();

      auto vc = col->term_.vc;
      if (ct == common::ColumnType::INT || ct == common::ColumnType::MEDIUMINT || ct == common::ColumnType::SMALLINT ||
          ct == common::ColumnType::BYTEINT || ct == common::ColumnType::NUM || ct == common::ColumnType::BIGINT) {
        auto data_ptr = new types::RCNum();

        if (vc->IsNull(it))
          data_ptr->SetToNull();
        else
          data_ptr->Assign(vc->GetValueInt64(it), col->Type().GetScale(), false, ct);

        record.emplace_back(data_ptr);
      } else if (ATI::IsRealType(ct)) {
        auto data_ptr = new types::RCNum();
        if (vc->IsNull(it))
          data_ptr->SetToNull();
        else
          data_ptr->Assign(vc->GetValueDouble(it));

        record.emplace_back(data_ptr);
      } else if (ATI::IsDateTimeType(ct)) {
        auto data_ptr = new types::RCDateTime();
        if (vc->IsNull(it))
          data_ptr->SetToNull();
        else
          data_ptr->Assign(vc->GetValueInt64(it), ct);

        record.emplace_back(data_ptr);
      } else {
        ASSERT(ATI::IsStringType(ct), "not all possible attr_types checked");
        auto data_ptr = new types::BString();
        if (vc->IsNull(it))
          data_ptr->SetToNull();
        else
          vc->GetNotNullValueString(*data_ptr, it);

        record.emplace_back(data_ptr);
      }
    }

    sender.SendRow(record, this);
    row++;
    ++it;
    if (lazy_)
      break;
  }

  for (auto &attr : attrs_) {
    attr->term_.vc->UnlockSourcePacks();
  }
}

std::vector<AttributeTypeInfo> TempTable::GetATIs(bool orig) {
  std::vector<AttributeTypeInfo> deas;
  for (uint i = 0; i < NumOfAttrs(); i++) {
    if (!IsDisplayAttr(i))
      continue;
    deas.emplace_back(attrs_[i]->TypeName(), attrs_[i]->Type().NotNull(),
                      orig ? attrs_[i]->orig_precision_ : attrs_[i]->Type().GetPrecision(),
                      attrs_[i]->Type().GetScale(), false, attrs_[i]->Type().GetCollation());
  }

  return deas;
}

#define STRING_LENGTH_THRESHOLD 512
void TempTable::VerifyAttrsSizes()  // verifies attr[i].field_size basing on the
                                    // current multiindex contents
{
  for (auto attr : attrs_) {
    if (ATI::IsStringType(attr->TypeName())) {
      // reduce string size when column defined too large to reduce allocated
      // temp memory
      if (attr->term_.vc->MaxStringSize() < STRING_LENGTH_THRESHOLD) {
        attr->OverrideStringSize(attr->term_.vc->MaxStringSize());
      } else {
        int max_length = attr->term_.vc->MaxStringSize();
// TODO, the code has some bug, max_length in some case wil be negative, see #671
// comment the optimization code for temp solution;
#if 0
        vcolumn::VirtualColumn *vc = attrs[i]->term.vc;
        if (dynamic_cast<vcolumn::ExpressionColumn *>(vc)) {
          auto &var_map = dynamic_cast<vcolumn::ExpressionColumn *>(vc)->GetVarMap();
          for (auto &it : var_map) {
            PhysicalColumn *column = it.GetTabPtr()->GetColumn(it.col_ndx);
            ColumnType ct = column->Type();
            uint precision = ct.GetPrecision();
            if (precision >= STRING_LENGTH_THRESHOLD) {
              uint actual_size = column->MaxStringSize() * ct.GetCollation().collation->mbmaxlen;
              if (actual_size < precision)
                max_length += (actual_size - precision);
            }
          }
        }
#endif
        attr->OverrideStringSize(max_length);
      }
    }
  }  // for
}

void TempTable::FillbufferTask(Attr *attr, Transaction *ci, MIIterator *page_start, int64_t start_row,
                               int64_t page_end) {
  // save TLS for mysql function
  common::SetMySQLTHD(m_conn_->Thd());
  current_txn_ = ci;

  if (attr->NeedFill()) {
    MIIterator i(*page_start);
    attr->FillValues(i, start_row, page_end - start_row);
  }
}

size_t TempTable::TaskPutValueInST(MIIterator *it, Transaction *ci, SorterWrapper *st) {
  size_t local_row = 0;
  bool continue_now = true;
  current_txn_ = ci;
  while (it->IsValid() && continue_now) {
    if (m_conn_->Killed())
      throw common::KilledException();
    if (it->PackrowStarted()) {
      if (st->InitPackrow(*it)) {
        local_row += it->GetPackSizeLeft();
        it->NextPackrow();

        TIANMU_LOG(LogCtl_Level::DEBUG, "skip this pack it: %p", it);
        continue;
      }
    }
    continue_now = st->PutValues(*it);  // return false if a limit is already reached (min. values only)
    ++(*it);

    local_row++;
    if (local_row % 10000000 == 0)
      rc_control_.lock(m_conn_->GetThreadID())
          << "Preparing values to sort (" << int(local_row / double(filter_.mind->NumOfTuples()) * 100) << "% done)."
          << system::unlock;
  }
  return local_row;
}

}  // namespace core
}  // namespace Tianmu
