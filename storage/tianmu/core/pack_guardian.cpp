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

#include "pack_guardian.h"

#include "core/just_a_table.h"
#include "core/mi_iterator.h"
#include "core/rc_attr.h"
#include "vc/virtual_column.h"

namespace Tianmu {
namespace core {
void VCPackGuardian::Initialize(int no_th) {
  UnlockAll();
  last_pack_.clear();

  guardian_threads_ = no_th;

  int no_dims = -1;
  for (auto const &iter : my_vc_.GetVarMap())
    if (iter.dim > no_dims)
      no_dims = iter.dim;  // find the maximal number of dimension used
  no_dims++;
  if (no_dims > 0) {  // else constant
    last_pack_.reserve(no_dims);
    for (int i = 0; i < no_dims; ++i) last_pack_.emplace_back(guardian_threads_, common::NULL_VALUE_32);
  }
  initialized_ = true;
}

/*
 * ResizeLastPack handles last_pack_ overflow under multi-threads group by
 */
void VCPackGuardian::ResizeLastPack(int taskNum) {
  if (!initialized_)
    return;

  for (auto &v : last_pack_) v.resize(taskNum, common::NULL_VALUE_32);

  guardian_threads_ = taskNum;
}

void VCPackGuardian::LockPackrow(const MIIterator &mit) {
  int threadId = mit.GetTaskId();
  int taskNum = mit.GetTaskNum();
  {
    std::scoped_lock g(mx_thread_);
    if (!initialized_) {
      Initialize(taskNum);
    }
    if (initialized_ && (taskNum > guardian_threads_)) {
      // recheck to make sure last_pack_ is not overflow
      ResizeLastPack(taskNum);
    }
  }
  for (auto iter = my_vc_.GetVarMap().cbegin(); iter != my_vc_.GetVarMap().cend(); iter++) {
    int cur_dim = iter->dim;
    if (last_pack_[cur_dim][threadId] != mit.GetCurPackrow(cur_dim)) {
      JustATable *tab = iter->GetTabPtr().get();
      if (last_pack_[cur_dim][threadId] != common::NULL_VALUE_32)
        tab->UnlockPackFromUse(iter->col_ndx, last_pack_[cur_dim][threadId]);
      try {
        tab->LockPackForUse(iter->col_ndx, mit.GetCurPackrow(cur_dim));
      } catch (...) {
        // unlock packs which are partially locked for this packrow
        auto it = my_vc_.GetVarMap().begin();
        for (; it != iter; ++it) {
          int cur_dim = it->dim;
          if (last_pack_[cur_dim][threadId] != mit.GetCurPackrow(cur_dim) &&
              last_pack_[cur_dim][threadId] != common::NULL_VALUE_32)
            it->GetTabPtr()->UnlockPackFromUse(it->col_ndx, mit.GetCurPackrow(cur_dim));
        }

        for (++iter; iter != my_vc_.GetVarMap().end(); ++iter) {
          int cur_dim = iter->dim;
          if (last_pack_[cur_dim][threadId] != mit.GetCurPackrow(cur_dim) &&
              last_pack_[cur_dim][threadId] != common::NULL_VALUE_32)
            iter->GetTabPtr()->UnlockPackFromUse(iter->col_ndx, last_pack_[cur_dim][threadId]);
        }

        for (auto const &iter : my_vc_.GetVarMap()) last_pack_[iter.dim][threadId] = common::NULL_VALUE_32;
        throw;
      }
    }
  }
  for (auto const &iter : my_vc_.GetVarMap())
    last_pack_[iter.dim][threadId] = mit.GetCurPackrow(iter.dim);  // must be in a separate loop, otherwise
                                                                   // for "a + b" will not lock b
}

void VCPackGuardian::UnlockAll() {
  if (!initialized_)
    return;
  for (auto const &iter : my_vc_.GetVarMap()) {
    for (int i = 0; i < guardian_threads_; ++i)
      if (last_pack_[iter.dim][i] != common::NULL_VALUE_32 && iter.GetTabPtr())
        iter.GetTabPtr()->UnlockPackFromUse(iter.col_ndx, last_pack_[iter.dim][i]);
  }
  for (auto const &iter : my_vc_.GetVarMap()) {
    for (int i = 0; i < guardian_threads_; ++i)
      last_pack_[iter.dim][i] = common::NULL_VALUE_32;  // must be in a separate loop, otherwise
                                                        // for "a + b" will not unlock b
  }
}
}  // namespace core
}  // namespace Tianmu
