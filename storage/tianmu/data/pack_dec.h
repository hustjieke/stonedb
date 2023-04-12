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

#ifndef TIANMU_CORE_PACK_DEC_H_
#define TIANMU_CORE_PACK_DEC_H_
#pragma once

#include <unordered_set>

#include "data/pack.h"
#include "marisa.h"

namespace Tianmu {

namespace loader {
class ValueCache;
}

namespace core {

class PackDec final : public Pack {
 public:
  PackDec(DPN *dpn, PackCoordinate pc, ColumnShare *s);
  ~PackDec() {
    DestructionLock();
    Destroy();
  }

  // overrides
  std::unique_ptr<Pack> Clone(const PackCoordinate &pc) const override;
  void LoadDataFromFile(system::Stream *fcurfile) override;
  void UpdateValue(size_t locationInPack, const Value &v) override;
  void DeleteByRow(size_t locationInPack) override;
  void Save() override;

  types::BString GetValueBinary(int locationInPack) const override;

  void LoadValues(const loader::ValueCache *vc);
  bool IsTrie() const { return pack_dec_state_ == PackDecState::kPackTrie; }
  bool Lookup(const types::BString &pattern, uint16_t &id);

  // TODO(gry): used by string like, no need in decimal.
  // bool LikePrefix(const types::BString &pattern, std::size_t prefixlen, std::unordered_set<uint16_t> &ids);
  // TODO(gry): used by string between, decimal has between function like other integer.
  // bool IsNotMatched(int row, uint16_t &id);
  // bool IsNotMatched(int row, const std::unordered_set<uint16_t> &ids);

  static void ConvertBStringToBinChar(types::BString &str, std::vector<char> &res, bool &sign);
  static void ConvertBinCharToBString(std::vector<uint8_t> &res, bool sign, types::BString &str);

  static int128 ConvertVecToInt128(const char* arr);
  static void WriteInt128ToVec(int128 v, char* arr);

  void CopyToDPN(std::vector<char> &src, bool sign, char* dest, int len);

 protected:
  std::pair<UniquePtr, size_t> Compress() override;
  void CompressTrie();
  void Destroy() override;

 private:
  PackDec() = delete;
  PackDec(const PackDec &, const PackCoordinate &pc);

  void Prepare(int no_nulls);
  void AppendValue(const char *value, uint size) {
    if (size == 0) {
      SetPtrSize(dpn_->numOfRecords, nullptr, 0);
    } else {
      SetPtrSize(dpn_->numOfRecords, Put(value, size), size);
      data_.sum_len += size;
    }
    dpn_->numOfRecords++;
  }

  // high bit is sign
  static const int MAX_DPN_S = 16;

  // Covert readable dec to binary char
  static void ConvertVecToBinChar(const char* arr, types::BString& str);

  size_t CalculateMaxLen() const;
  types::BString GetStringValueTrie(int locationInPack) const;
  size_t GetSize(int locationInPack) {
      return data_.lens8[locationInPack];
  }

  void SetSize(int locationInPack, uint size) {
    data_.lens8[locationInPack] = (uint8_t)size;
  }
  // TODO(gry): how to deal?
  // void SetMinS(const types::BString &s); 
  // void SetMaxS(const types::BString &s);

  // Make sure this is larger than the max length of CHAR/TEXT field of mysql.
  static const size_t DEFAULT_BUF_SIZE = 64_KB;

  struct buf {
    char *ptr;
    const size_t len;
    size_t pos;

    size_t capacity() const { return len - pos; }
    void *put(const void *src, size_t length) {
      ASSERT(length <= capacity());
      auto ret = std::memcpy(ptr + pos, src, length);
      pos += length;
      return ret;
    }
  };

  char *GetPtr(int locationInPack) const { return data_.index[locationInPack]; }
  void SetPtr(int locationInPack, void *addr) { data_.index[locationInPack] = reinterpret_cast<char *>(addr); }
  void SetPtrSize(int locationInPack, void *addr, uint size) {
    SetPtr(locationInPack, addr);
    SetSize(locationInPack, size);
  }

  enum class PackDecState {
    kPackArray = 0,
    kPackTrie,
  };

  PackDecState pack_dec_state_ = PackDecState::kPackArray;
  marisa::Trie marisa_trie_;
  UniquePtr compressed_data_;
  uint16_t *ids_array_;
  struct {
    std::vector<buf> v;
    size_t sum_len;
    char **index;
    union {
      void *lens;
      uint8_t *lens8;
    };
    uint8_t len_mode;
  } data{};

  void *Put(const void *src, size_t length) {
    if (data_.v.empty() || length > data_.v.back().capacity()) {
      auto sz = length * 2;
      data_.v.push_back({reinterpret_cast<char *>(alloc(sz, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED)), sz, 0});
    }
    return data_.v.back().put(src, length);
  }

  void SaveUncompressed(system::Stream *fcurfile);
  void LoadUncompressed(system::Stream *fcurfile);
  void LoadCompressed(system::Stream *fcurfile);
  void LoadCompressedTrie(system::Stream *fcurfile);
  void TransformIntoArray();

  int GetCompressBufferSize(size_t size);
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PACK_DEC_H_
