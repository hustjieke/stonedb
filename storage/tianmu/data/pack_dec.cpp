/* Copyright (c) 2023 StoneAtom, Inc. All rights reserved.
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
#include "pack_dec.h"

#include <algorithm>

#include "zlib.h"

#include "compress/bit_stream_compressor.h"
#include "compress/num_compressor.h"
#include "compress/part_dict.h"
#include "compress/text_compressor.h"
#include "core/value.h"
#include "loader/value_cache.h"
#include "lz4.h"
#include "mm/mm_guard.h"
#include "system/stream.h"
#include "system/tianmu_file.h"
#include "system/txt_utils.h"
#include "util/bin_tools.h"
#include "util/tools.h"
#include "vc/column_share.h"

namespace Tianmu {
namespace core {
const uint kDeleteOrNullBitmap = 2;

PackDec::PackDec(DPN *dpn, PackCoordinate pc, ColumnShare *col_share) : Pack(dpn, pc, col_share) {
  auto t = col_share->ColType().GetTypeName();
  DEBUG_ASSERT(ATI::IsDecimalType(t));

  data_.len_mode = sizeof(uint8_t);

  try {
    data_.index = (char **)alloc(sizeof(char *) * (1 << col_share->pss), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    data_.lens = alloc((data_.len_mode * (1 << col_share->pss)), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    std::memset(data_.lens, 0, data_.len_mode * (1 << col_share->pss));

    if (!dpn_->NullOnly()) {
      system::TianmuFile f;
      f.OpenReadOnly(col_share->DataFile());
      f.Seek(dpn_->dataAddress, SEEK_SET);
      LoadDataFromFile(&f);
    }
  } catch (...) {
    Destroy();
    throw;
  }
}

PackDec::PackDec(const PackDec &aps, const PackCoordinate &pc) : Pack(aps, pc) {
  try {
    data_.len_mode = aps.data_.len_mode;
    data_.lens = alloc((data_.len_mode * (1 << col_share_->pss)), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
    std::memset(data_.lens, 0, data_.len_mode * (1 << col_share_->pss));
    data_.index =
        reinterpret_cast<char **>(alloc(sizeof(char *) * (1 << col_share_->pss), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED));

    data_.sum_len = aps.data_.sum_len;
    data_.v.push_back(
        {reinterpret_cast<char *>(alloc(data_.sum_len + 1, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED)), data_.sum_len, 0});

    for (uint i = 0; i < aps.dpn_->numOfRecords; i++) {
      if (aps.IsNull(i)) {
        SetPtr(i, nullptr);
        continue;
      }
      auto value = aps.GetValueBinary(i);
      auto size = value.size();
      if (size != 0) {
        SetPtrSize(i, Put(value.GetDataBytesPointer(), size), size);
      } else {
        SetPtrSize(i, nullptr, 0);
      }
    }
  } catch (...) {
    Destroy();
    throw;
  }
}

std::unique_ptr<Pack> PackDec::Clone(const PackCoordinate &pc) const {
  return std::unique_ptr<PackDec>(new PackDec(*this, pc));
}

void PackDec::LoadDataFromFile(system::Stream *f) {
  FunctionExecutor fe([this]() { Lock(); }, [this]() { Unlock(); });

  if (IsModeNoCompression()) {
    LoadUncompressed(f);
  } else if (col_share_->ColType().GetFmt() == common::PackFmt::TRIE) {
    LoadCompressedTrie(f);
  } else {
    LoadCompressed(f);
  }
}

void PackDec::Destroy() {
  if (pack_str_state_ == PackDecState::kPackArray) {
    for (auto &it : data_.v) {
      dealloc(it.ptr);
    }
  } else {
    marisa_trie_.clear();
    compressed_data_.reset(nullptr);
  }
  dealloc(data_.index);
  data_.index = nullptr;
  dealloc(data_.lens);
  data_.lens = nullptr;
  Instance()->AssertNoLeak(this);
}

size_t PackDec::CalculateMaxLen() const {
  return *std::max_element(data_.lens8, data_.lens8 + dpn_->numOfRecords);
}

void PackDec::TransformIntoArray() {
  if (pack_str_state_ == PackDecState::kPackArray)
    return;
  data_.lens = alloc((data_.len_mode * (1 << col_share_->pss)), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED);
  std::memset(data_.lens, 0, data_.len_mode * (1 << col_share_->pss));
  data_.index =
      reinterpret_cast<char **>(alloc(sizeof(char *) * (1 << col_share_->pss), mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED));

  data_.v.push_back(
      {reinterpret_cast<char *>(alloc(data_.sum_len + 1, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED)), data_.sum_len, 0});

  for (uint i = 0; i < dpn_->numOfRecords; i++) {
    if (IsNull(i)) {
      SetPtr(i, nullptr);
      continue;
    }
    auto value = GetValueBinary(i);
    auto size = value.size();
    if (size != 0) {
      SetPtrSize(i, Put(value.GetDataBytesPointer(), size), size);
    } else {
      SetPtrSize(i, nullptr, 0);
    }
  }
  pack_str_state_ = PackDecState::kPackArray;
}

void PackDec::UpdateValue(size_t locationInPack, const Value &v) {
  if (IsDeleted(locationInPack))  // The deleted value should not be updated.
    return;
  TransformIntoArray();
  dpn_->synced = false;

  if (IsNull(locationInPack)) {
    // In the delta layer, there may be situations where null
    // values are updated with null values, so when encountering this situation, it is directly returned
    if (!v.HasValue())
      return;

    // update null to non-null

    // first non-null value?
    if (dpn_->NullOnly()) {
      dpn_->max_i = -1;
    }

    // ASSERT(v.HasValue(), col_share_->DataFile() + " locationInPack: " + std::to_string(locationInPack));
    UnsetNull(locationInPack);
    dpn_->numOfNulls--;
    auto &str = v.GetString();
    if (str.size() == 0) {
      // we don't need to copy any data_
      SetPtrSize(locationInPack, nullptr, 0);
      return;
    }
    SetPtrSize(locationInPack, Put(str.data(), str.size()), str.size());
    data_.sum_len += str.size();
  } else {
    // update an original non-null value

    if (!v.HasValue()) {
      // update non-null to null
      data_.sum_len -= GetValueBinary(locationInPack).size();
      // note that we do not reclaim any space. The buffers will
      // will be compacted when saving to disk
      SetPtrSize(locationInPack, nullptr, 0);
      SetNull(locationInPack);
      dpn_->numOfNulls++;
    } else {
      // update non-null to another nonull
      auto vsize = GetValueBinary(locationInPack).size();
      ASSERT(data_.sum_len >= vsize);
      data_.sum_len -= vsize;
      auto &str = v.GetString();
      if (str.size() <= vsize) {
        // rclog << lock << "     JUST overwrite the original data_ " <<
        // system::unlock;
        std::memcpy(GetPtr(locationInPack), str.data(), str.size());
        SetSize(locationInPack, str.size());
      } else {
        SetPtrSize(locationInPack, Put(str.data(), str.size()), str.size());
      }
      data_.sum_len += str.size();
    }
  }

  dpn_->maxlen = CalculateMaxLen();
}

void PackDec::DeleteByRow(size_t locationInPack) {
  if (IsDeleted(locationInPack))
    return;
  TransformIntoArray();
  dpn_->synced = false;
  if (!IsNull(locationInPack)) {
    data_.sum_len -= GetValueBinary(locationInPack).size();
    // note that we do not reclaim any space. The buffers will
    // will be compacted when saving to disk
    SetPtrSize(locationInPack, nullptr, 0);
    SetNull(locationInPack);
    dpn_->numOfNulls++;
  }
  SetDeleted(locationInPack);
  dpn_->numOfDeleted++;
  dpn_->maxlen = CalculateMaxLen();
}

void PackDec::LoadValues(const loader::ValueCache *vc) {
  dpn_->synced = false;
  auto sz = vc->SumarizedSize();
  data_.v.push_back({(char *)alloc(sz, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), sz, 0});

  auto total = vc->NumOfValues();

  TransformIntoArray();

  for (uint i = 0; i < total; i++) {
    if (vc->IsNull(i) && col_share_->ColType().IsNullable()) {
      SetNull(dpn_->numOfRecords);
      SetPtrSize(dpn_->numOfRecords, nullptr, 0);
      dpn_->numOfRecords++;
      dpn_->numOfNulls++;
      if (vc->IsDelete(i)) {
        SetDeleted(dpn_->numOfRecords - 1);
        dpn_->numOfDeleted++;
      }
      continue;
    }
    if (vc->IsDelete(i)) {
      SetNull(dpn_->numOfRecords);
      SetPtrSize(dpn_->numOfRecords, nullptr, 0);
      dpn_->numOfNulls++;
      SetDeleted(dpn_->numOfRecords);
      dpn_->numOfDeleted++;
      dpn_->numOfRecords++;
      continue;
    }

    char const *v = 0;
    uint size = 0;
    if (vc->NotNull(i)) {
      v = vc->GetDataBytesPointer(i);
      size = vc->Size(i);
    }
    AppendValue(v, size);
  }

  // update min/max/maxlen in DPN, if there is non-null values loaded, diff with pack_str.
  if (vc->NumOfValues() > vc->NumOfNulls()) {
    // TODO(): maybe we can try another to calculate max/min/sum, used uint128 directly?
    // Not a good idea, uint128 can not store signed values, if use int128, ocupy 32 bytes not 16
    types::BString min_s;
    ConvertVecToBinChar(dpn->min_s, min_s);
    types::BString max_s;
    ConvertVecToBinChar(dpn->max_s, max_s);
    types::BString sum_s;
    ConvertVecToBinChar(dpn->sum_s, sum_s);

    uint maxlen;
    vc->CalcDecStats(min_s, max_s, sum_s, maxlen, s->ColType().GetCollation());

    bool sign;
    std::vector<char> arr;

    arr.clear();
    ConvertBStringToBinChar(min_s, arr, sign);
    CopyToDPN(arr, sign, dpn->min_s, arr.size() > MAX_DPN_S ? MAX_DPN_S : arr.size());

    arr.clear();
    ConvertBStringToBinChar(max_s, arr, sign);
    CopyToDPN(arr, sign, dpn->max_s, arr.size() > MAX_DPN_S ? MAX_DPN_S : arr.size());

    arr.clear();
    ConvertBStringToBinChar(sum_s, arr, sign);
    CopyToDPN(arr, sign, dpn->sum_s, arr.size() > MAX_DPN_S ? MAX_DPN_S : arr.size());
  }
}

int PackDec::GetCompressBufferSize(size_t size) {
  int compress_len = 0;
  if (col_share_->ColType().GetFmt() == common::PackFmt::LZ4) {
    compress_len = LZ4_COMPRESSBOUND(size);
  } else if (col_share_->ColType().GetFmt() == common::PackFmt::ZLIB) {
    compress_len = compressBound(size);
  } else {
    compress_len = size;
  }
  // 10 - reserve for header
  return compress_len + 10;
}

std::pair<PackDec::UniquePtr, size_t> PackDec::Compress() {
  uint comp_null_buf_size = 0;
  mm::MMGuard<uchar> comp_null_buf;
  if (dpn_->numOfNulls > 0) {
    if (CompressedBitMap(comp_null_buf, comp_null_buf_size, nulls_ptr_, dpn_->numOfNulls)) {
      SetModeNullsCompressed();
    } else {
      ResetModeNullsCompressed();
    }
  }
  uint comp_delete_buf_size = 0;
  mm::MMGuard<uchar> comp_delete_buf;
  if (dpn_->numOfDeleted > 0) {
    if (CompressedBitMap(comp_delete_buf, comp_delete_buf_size, deletes_ptr_, dpn_->numOfDeleted)) {
      SetModeDeletesCompressed();
    } else {
      ResetModeDeletesCompressed();
    }
  }

  mm::MMGuard<uint> nc_buffer((uint *)alloc((1 << col_share_->pss) * sizeof(uint32_t), mm::BLOCK_TYPE::BLOCK_TEMPORARY),
                              *this);

  int onn = 0;
  uint maxv = 0;
  uint cv = 0;
  for (uint o = 0; o < dpn_->numOfRecords; o++) {
    if (!IsNull(o)) {
      cv = GetSize(o);
      *(nc_buffer.get() + onn++) = cv;
      if (cv > maxv)
        maxv = cv;
    }
  }

  size_t comp_len_buf_size;
  mm::MMGuard<uint> comp_len_buf;

  if (maxv != 0) {
    comp_len_buf_size = onn * sizeof(uint) + 28;
    comp_len_buf = mm::MMGuard<uint>(
        reinterpret_cast<uint *>(alloc(comp_len_buf_size / 4 * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
    uint tmp_comp_len_buf_size = comp_len_buf_size - 8;
    compress::NumCompressor<uint> nc;
    CprsErr res = nc.Compress(reinterpret_cast<char *>(comp_len_buf.get() + sizeof(ushort)), tmp_comp_len_buf_size,
                              nc_buffer.get(), onn, maxv);
    if (res != CprsErr::CPRS_SUCCESS) {
      throw common::InternalException("Compression of lengths of values failed for column " +
                                      std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                      std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " error " +
                                      std::to_string(static_cast<int>(res)));
    }
    comp_len_buf_size = tmp_comp_len_buf_size + 8;
  } else {
    comp_len_buf_size = 8;
    comp_len_buf = mm::MMGuard<uint>(
        reinterpret_cast<uint *>(alloc(sizeof(uint) * kDeleteOrNullBitmap, mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
  }

  *comp_len_buf.get() = comp_len_buf_size;
  *(comp_len_buf.get() + 1) = maxv;

  compress::TextCompressor tc;
  int zlo = 0;
  for (uint obj = 0; obj < dpn_->numOfRecords; obj++)
    if (!IsNull(obj) && GetSize(obj) == 0)
      zlo++;

  auto dlen = GetCompressBufferSize(data_.sum_len);

  mm::MMGuard<char> comp_buf(reinterpret_cast<char *>(alloc(dlen, mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);

  if (data_.sum_len) {
    int objs = (dpn_->numOfRecords - dpn_->numOfNulls) - zlo;

    mm::MMGuard<char *> tmp_index(
        reinterpret_cast<char **>(alloc(objs * sizeof(char *), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);
    mm::MMGuard<uint> tmp_len(reinterpret_cast<uint *>(alloc(objs * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY)),
                              *this);

    int nid = 0;
    uint packlen = 0;
    for (int id = 0; id < (int)dpn_->numOfRecords; id++) {
      if (!IsNull(id) && GetSize(id) != 0) {
        tmp_index[nid] = GetPtr(id);
        tmp_len[nid++] = GetSize(id);
        packlen += GetSize(id);
      }
    }

    CprsErr res = tc.Compress(comp_buf.get(), dlen, tmp_index.get(), tmp_len.get(), objs, packlen,
                              static_cast<int>(col_share_->ColType().GetFmt()));
    if (res != CprsErr::CPRS_SUCCESS) {
      std::stringstream msg_buf;
      msg_buf << "Compression of string values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
              << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
      throw common::InternalException(msg_buf.str());
    }

  } else {
    dlen = 0;
  }

  size_t comp_buf_size = comp_null_buf_size > 0 ? sizeof(ushort) + comp_null_buf_size : 0;
  comp_buf_size += comp_delete_buf_size > 0 ? sizeof(ushort) + comp_delete_buf_size : 0;
  comp_buf_size += comp_len_buf_size + sizeof(uint32_t) * kDeleteOrNullBitmap + dlen;

  UniquePtr compressed_buf = alloc_ptr(comp_buf_size, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  uchar *p = reinterpret_cast<uchar *>(compressed_buf.get());

  if (dpn_->numOfNulls > 0) {
    *(reinterpret_cast<ushort *>(p)) = (ushort)comp_null_buf_size;
    p += sizeof(ushort);
    std::memcpy(p, comp_null_buf.get(), comp_null_buf_size);
    p += comp_null_buf_size;
  }

  if (dpn_->numOfDeleted > 0) {
    *(reinterpret_cast<ushort *>(p)) = (ushort)comp_delete_buf_size;
    p += sizeof(ushort);
    std::memcpy(p, comp_delete_buf.get(), comp_delete_buf_size);
    p += comp_delete_buf_size;
  }

  if (comp_len_buf_size)
    std::memcpy(p, comp_len_buf.get(), comp_len_buf_size);

  p += comp_len_buf_size;

  *(reinterpret_cast<uint32_t *>(p)) = dlen;
  p += sizeof(uint32_t);
  *(reinterpret_cast<uint32_t *>(p)) = data_.sum_len;
  p += sizeof(uint32_t);
  if (dlen)
    std::memcpy(p, comp_buf.get(), dlen);

  SetModeDataCompressed();

  return std::make_pair(std::move(compressed_buf), comp_buf_size);
}

void PackDec::CompressTrie() {
  DEBUG_ASSERT(pack_str_state_ == PackDecState::kPackArray);
  marisa::Keyset keyset;
  std::size_t sum_len = 0;
  for (uint row = 0; row < dpn_->numOfRecords; row++) {
    if (!IsNull(row)) {
      keyset.push_back(GetPtr(row), GetSize(row));
      sum_len += GetSize(row);
    }
  }
  marisa_trie_.clear();
  marisa_trie_.build(keyset);
  auto bufsz = marisa_trie_.io_size() + (dpn_->numOfRecords * sizeof(unsigned short)) + 8;
  dpn_->dataLength = bufsz;
  std::ostringstream oss;
  oss << marisa_trie_;
  compressed_data_ = alloc_ptr(bufsz, mm::BLOCK_TYPE::BLOCK_TEMPORARY);
  char *buf_ptr = static_cast<char *>(compressed_data_.get());
  std::memcpy(buf_ptr, oss.str().data(), oss.str().length());
  buf_ptr += oss.str().length();
  auto sumlenptr = reinterpret_cast<std::uint64_t *>(buf_ptr);
  *sumlenptr = sum_len;
  buf_ptr += 8;
  unsigned short *ids = reinterpret_cast<unsigned short *>(buf_ptr);
  for (uint row = 0, idx = 0; row < dpn_->numOfRecords; row++) {
    if (IsNull(row)) {
      ids[row] = 0xffff;
    } else {
      auto id = keyset[idx].id();
      DEBUG_ASSERT(id < (dpn_->numOfRecords - dpn_->numOfNulls));
      ids[row] = id;
      idx++;
    }
  }
  ids_array_ = ids;

  SetModeDataCompressed();
  for (auto &it : data_.v) {
    dealloc(it.ptr);
  }
  data_.v.clear();
  pack_str_state_ = PackDecState::kPackTrie;
}

void PackDec::Save() {
  UniquePtr compressed_buf;
  if (!ShouldNotCompress()) {
    if (data_.sum_len > common::MAX_CMPR_SIZE) {
      TIANMU_LOG(LogCtl_Level::WARN,
                 "pack (%d-%d-%d) size %ld exceeds supported compression "
                 "size, will not be compressed!",
                 pc_table(GetCoordinate().co.pack), pc_column(GetCoordinate().co.pack), pc_dp(GetCoordinate().co.pack),
                 data_.sum_len);
      SetModeNoCompression();
      dpn_->dataLength = bitmap_size_ * kDeleteOrNullBitmap + data_.sum_len + (data_.len_mode * (1 << col_share_->pss));
    } else if (col_share_->ColType().GetFmt() == common::PackFmt::TRIE) {
      CompressTrie();
    } else {
      auto res = Compress();
      dpn_->dataLength = res.second;
      compressed_buf = std::move(res.first);
    }
  } else {
    SetModeNoCompression();
    dpn_->dataLength = bitmap_size_ * kDeleteOrNullBitmap + data_.sum_len + (data_.len_mode * (1 << col_share_->pss));
  }
  col_share_->alloc_seg(dpn_);
  system::TianmuFile f;
  f.OpenCreate(col_share_->DataFile());
  f.Seek(dpn_->dataAddress, SEEK_SET);
  if (IsModeCompressionApplied()) {
    if (pack_str_state_ == PackDecState::kPackTrie) {
      f.WriteExact(compressed_data_.get(), dpn_->dataLength);
    } else {
      f.WriteExact(compressed_buf.get(), dpn_->dataLength);
    }
  } else {
    SaveUncompressed(&f);
  }

  ASSERT(f.Tell() == off_t(dpn_->dataAddress + dpn_->dataLength),
         std::to_string(dpn_->dataAddress) + ":" + std::to_string(dpn_->dataLength) + "/" + std::to_string(f.Tell()));
  dpn_->synced = true;
}

void PackDec::SaveUncompressed(system::Stream *f) {
  f->WriteExact(nulls_ptr_.get(), bitmap_size_);
  f->WriteExact(deletes_ptr_.get(), bitmap_size_);
  f->WriteExact(data_.lens, (data_.len_mode * (1 << col_share_->pss)));
  if (data_.v.empty())
    return;

  std::unique_ptr<char[]> buff(new char[data_.sum_len]);
  char *ptr = buff.get();
  for (uint i = 0; i < dpn_->numOfRecords; i++) {
    if (!IsNull(i)) {
      std::memcpy(ptr, data_.index[i], GetSize(i));
      ptr += GetSize(i);
    }
  }
  ASSERT(ptr == buff.get() + data_.sum_len,
         "lengh sum: " + std::to_string(data_.sum_len) + ", copied " + std::to_string(ptr - buff.get()));
  f->WriteExact(buff.get(), data_.sum_len);
}

void PackDec::LoadCompressed(system::Stream *f) {
  ASSERT(IsModeCompressionApplied());

  auto compressed_buf = alloc_ptr(dpn_->dataLength + 1, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  f->ReadExact(compressed_buf.get(), dpn_->dataLength);

  dpn_->synced = true;

  // uncompress the data_
  mm::MMGuard<char *> tmp_index(
      reinterpret_cast<char **>(alloc(dpn_->numOfRecords * sizeof(char *), mm::BLOCK_TYPE::BLOCK_TEMPORARY)), *this);

  char *cur_buf = reinterpret_cast<char *>(compressed_buf.get());

  uint null_buf_size = 0;
  if (dpn_->numOfNulls > 0) {
    null_buf_size = (*reinterpret_cast<ushort *>(cur_buf));
    if (!IsModeNullsCompressed())  // flat null encoding
      std::memcpy(nulls_ptr_.get(), cur_buf + 2, null_buf_size);
    else {
      compress::BitstreamCompressor bsc;
      CprsErr res = bsc.Decompress(reinterpret_cast<char *>(nulls_ptr_.get()), null_buf_size, cur_buf + sizeof(ushort),
                                   dpn_->numOfRecords, dpn_->numOfNulls);
      if (res != CprsErr::CPRS_SUCCESS) {
        throw common::DatabaseException("Decompression of nulls failed for column " +
                                        std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                        std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " (error " +
                                        std::to_string(static_cast<int>(res)) + ").");
      }
    }
    cur_buf += (null_buf_size + sizeof(ushort));
  }

  if (dpn_->numOfDeleted > 0) {
    uint delete_buf_size = 0;
    delete_buf_size = (*reinterpret_cast<ushort *>(cur_buf));
    if (delete_buf_size > bitmap_size_)
      throw common::DatabaseException("Unexpected bytes found in data pack.");
    if (!IsModeDeletesCompressed())  // no deletes compression
      std::memcpy(deletes_ptr_.get(), reinterpret_cast<char *>(cur_buf) + sizeof(ushort), delete_buf_size);
    else {
      compress::BitstreamCompressor bsc;
      CprsErr res = bsc.Decompress(reinterpret_cast<char *>(deletes_ptr_.get()), delete_buf_size,
                                   reinterpret_cast<char *>(cur_buf) + 2, dpn_->numOfRecords, dpn_->numOfDeleted);
      if (res != CprsErr::CPRS_SUCCESS) {
        throw common::DatabaseException("Decompression of deletes failed for column " +
                                        std::to_string(pc_column(GetCoordinate().co.pack) + 1) + ", pack " +
                                        std::to_string(pc_dp(GetCoordinate().co.pack) + 1) + " (error " +
                                        std::to_string(static_cast<int>(res)) + ").");
      }
    }
    cur_buf += delete_buf_size + sizeof(ushort);
  }

  auto comp_len_buf_size = *reinterpret_cast<uint32_t *>(cur_buf);
  auto maxv = *reinterpret_cast<uint32_t *>(cur_buf + sizeof(uint32_t));

  if (maxv != 0) {
    compress::NumCompressor<uint> nc;
    mm::MMGuard<uint> cn_ptr((uint *)alloc((1 << col_share_->pss) * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY),
                             *this);
    CprsErr res = nc.Decompress(cn_ptr.get(), reinterpret_cast<char *>(cur_buf + 8), comp_len_buf_size - 8,
                                dpn_->numOfRecords - dpn_->numOfNulls, maxv);
    if (res != CprsErr::CPRS_SUCCESS) {
      std::stringstream msg_buf;
      msg_buf << "Decompression of lengths of std::string values failed for column "
              << (pc_column(GetCoordinate().co.pack) + 1) << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1)
              << " (error " << static_cast<int>(res) << ").";
      throw common::DatabaseException(msg_buf.str());
    }

    int oid = 0;
    for (uint o = 0; o < dpn_->numOfRecords; o++)
      if (!IsNull(int(o)))
        SetSize(o, (uint)cn_ptr[oid++]);
  } else {
    for (uint o = 0; o < dpn_->numOfRecords; o++)
      if (!IsNull(int(o)))
        SetSize(o, 0);
  }
  cur_buf += comp_len_buf_size;

  auto dlen = *reinterpret_cast<uint32_t *>(cur_buf);
  cur_buf += sizeof(dlen);
  data_.sum_len = *reinterpret_cast<uint32_t *>(cur_buf);
  cur_buf += sizeof(uint32_t);

  ASSERT(cur_buf + dlen == dpn_->dataLength + reinterpret_cast<char *>(compressed_buf.get()),
         std::to_string(data_.sum_len) + "/" + std::to_string(dpn_->dataLength) + "/" + std::to_string(dlen));

  int zlo = 0;
  for (uint obj = 0; obj < dpn_->numOfRecords; obj++)
    if (!IsNull(obj) && GetSize(obj) == 0)
      zlo++;
  int objs = dpn_->numOfRecords - dpn_->numOfNulls - zlo;

  if (objs) {
    mm::MMGuard<uint> tmp_len((uint *)alloc(objs * sizeof(uint), mm::BLOCK_TYPE::BLOCK_TEMPORARY), *this);
    for (uint tmp_id = 0, id = 0; id < dpn_->numOfRecords; id++)
      if (!IsNull(id) && GetSize(id) != 0)
        tmp_len[tmp_id++] = GetSize(id);

    if (dlen) {
      data_.v.push_back(
          {reinterpret_cast<char *>(alloc(data_.sum_len, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED)), data_.sum_len, 0});
      compress::TextCompressor tc;
      CprsErr res =
          tc.Decompress(data_.v.front().ptr, data_.sum_len, cur_buf, dlen, tmp_index.get(), tmp_len.get(), objs);
      if (res != CprsErr::CPRS_SUCCESS) {
        std::stringstream msg_buf;
        msg_buf << "Decompression of std::string values failed for column " << (pc_column(GetCoordinate().co.pack) + 1)
                << ", pack " << (pc_dp(GetCoordinate().co.pack) + 1) << " (error " << static_cast<int>(res) << ").";
        throw common::DatabaseException(msg_buf.str());
      }
    }
  }

  for (uint tmp_id = 0, id = 0; id < dpn_->numOfRecords; id++) {
    if (!IsNull(id) && GetSize(id) != 0)
      SetPtr(id, static_cast<char *>(tmp_index[tmp_id++]));
    else {
      SetSize(id, 0);
      SetPtr(id, 0);
    }
  }
}

void PackDec::LoadCompressedTrie(system::Stream *f) {
  ASSERT(IsModeCompressionApplied());

  compressed_data_ = alloc_ptr(dpn_->dataLength + 1, mm::BLOCK_TYPE::BLOCK_COMPRESSED);
  f->ReadExact(compressed_data_.get(), dpn_->dataLength);
  auto trie_length = dpn_->dataLength - (dpn_->numOfRecords * sizeof(unsigned short)) - 8;
  marisa_trie_.map(compressed_data_.get(), trie_length);
  dpn_->synced = true;
  char *buf_ptr = reinterpret_cast<char *>(compressed_data_.get());
  data_.sum_len = *(std::uint64_t *)(buf_ptr + trie_length);
  ids_array_ = reinterpret_cast<unsigned short *>(buf_ptr + trie_length + 8);
  if (dpn_->numOfNulls > 0) {
    for (uint row = 0; row < dpn_->numOfRecords; row++) {
      if (ids_array_[row] == 0xffff)
        SetNull(row);
    }
  }
  pack_str_state_ = PackDecState::kPackTrie;
}

types::BString PackDec::GetStringValueTrie(int locationInPack) const {
  marisa::Agent agent;
  std::size_t keyid = ids_array_[locationInPack];
  agent.set_query(keyid);
  marisa_trie_.reverse_lookup(agent);
  return types::BString(agent.key().ptr(), agent.key().length(), 1);
}

types::BString PackDec::GetValueBinary(uint32_t locationInPack) const {
  if (IsNull(locationInPack))
    return types::BString();
  assert(locationInPack <= dpn_->numOfRecords);

  if (pack_str_state_ == PackDecState::kPackTrie)
    return GetStringValueTrie(locationInPack);

  size_t str_size;
  str_size = data_.lens8[locationInPack];

  if (str_size == 0)
    return ZERO_LENGTH_STRING;
  return types::BString(data_.index[locationInPack], str_size);
}

void PackDec::LoadUncompressed(system::Stream *f) {
  auto sz = dpn_->dataLength;
  f->ReadExact(nulls_ptr_.get(), bitmap_size_);
  f->ReadExact(deletes_ptr_.get(), bitmap_size_);
  sz -= (bitmap_size_ * kDeleteOrNullBitmap);
  f->ReadExact(data_.lens, (data_.len_mode * (1 << col_share_->pss)));
  sz -= (data_.len_mode * (1 << col_share_->pss));

  data_.v.push_back({reinterpret_cast<char *>(alloc(sz + 1, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED)), sz, 0});
  f->ReadExact(data_.v.back().ptr, sz);
  data_.v.back().pos = sz;
  data_.sum_len = 0;
  for (uint i = 0; i < dpn_->numOfRecords; i++) {
    if (!IsNull(i) && GetSize(i) != 0) {
      SetPtr(i, data_.v.front().ptr + data_.sum_len);
      data_.sum_len += GetSize(i);
    } else {
      SetPtrSize(i, nullptr, 0);
    }
  }
  ASSERT(data_.sum_len == sz, "bad pack! " + std::to_string(data_.sum_len) + "/" + std::to_string(sz));
}

bool PackDec::Lookup(const types::BString &pattern, uint16_t &id) {
  marisa::Agent agent;
  agent.set_query(pattern.GetDataBytesPointer(), pattern.size());
  if (!marisa_trie_.lookup(agent)) {
    return false;
  }
  id = agent.key().id();
  return true;
}

// check begin
void PackDec::CopyToDPN(std::vector<char> &src, bool sign, char* dest, int len) {
  if (src.size() > 0) {
    for (int i = 0; i < len; i++, dest++) {
      *dest = src[i];
    }
    for (int j = len; j < MAX_DPN_S; j++, dest++) {
      *dest = 0;
    }
  }
  // TODO(gry): set a signed flag in dpn.h
  if (sign) dest[MAX_DPN_S-1] |= 0x80;
}

// TODO(gry): Try to use mysql decimal to str funciton maybe better?
void PackDec::ConvertVecToBinChar(const char* arr, types::BString& str) {
  bool sign = false;
  char buf[MAX_DPN_S] = {0};
  std::memcpy(buf, arr, MAX_DPN_S);
  if (buf[MAX_DPN_S-1] & 0x80) sign = true;
  buf[MAX_DPN_S-1] &= 0x7F;

  std::vector<uint8_t> res;
  for (int i=0; i<MAX_DPN_S; i++) {
    res.push_back(static_cast<uint8_t>(buf[i]));
  }
  ConvertBinCharToBString(res, sign, str);
}

// TODO(gry): Try to use mysql decimal to str funciton maybe better.
void PackDec::ConvertBStringToBinChar(types::BString &str, std::vector<char> &res, bool &sign) {
  sign = false;
  int128 v(std::string(str.begin(), str.size()));
  if (v < 0) {
    sign = true;
    v = -v;
  }
  for (int128 p = v; p > 0; p >>=8) {
    int128 temp = p & 0xFF;
    uint8_t r = temp.convert_to<uint8_t>();
    res.push_back(static_cast<char>(r));
  }
}

void PackDec::ConvertBinCharToBString(std::vector<uint8_t> &res, bool sign, types::BString &str) {
  int128 v = 0;
  std::for_each(res.rbegin(), res.rend(), [&](const uint8_t item){
    v |= item;
    v <<= 8;
  });
  v >>= 8;

  if (sign) v = -v;
  std::string ss = v.convert_to<std::string>();
  str = types::BString(ss.c_str(), ss.length(), true);
}

int128 PackDec::ConvertVecToInt128(const char* arr) {
  types::BString str;
  ConvertVecToBinChar(arr, str);
  return int128(std::string(str.begin(), str.size()));
}

void PackDec::WriteInt128ToVec(int128 v, char* arr) {
  std::string vs = v.convert_to<std::string>();
  types::BString str(vs.c_str(), vs.size());
  std::vector<char> vec;
  bool sign;
  ConvertBStringToBinChar(str, vec, sign);
  std::memset(arr, 0, MAX_DPN_S);
  for (size_t i=0; i<vec.size(); i++) {
    arr[i] = vec[i];
  }
  if (sign) arr[MAX_DPN_S-1] |= 0x80;
}
// check end

}  // namespace core
}  // namespace Tianmu
