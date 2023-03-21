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

#ifndef TIANMU_DATA_TYPE_DECIMAL_H_
#define TIANMU_DATA_TYPE_DECIMAL_H_
#pragma once

#include "types/tianmu_data_types.h"
#include "types/extended_data_types.h"

namespace Tianmu {
namespace types {

class BString;

class DataTypeDecimal : public ValueBasic<DataTypeDecimal> {
  friend class ValueParserForText;
  friend class Engine;

 public:
  DataTypeDecimal(common::ColumnType attrt = common::ColumnType::NUM);  // TODO: need it anymore?
  // TODO: reserved to construct from binary/string
  DataTypeDecimal(BString value, short scale = -1, short prec = -1, common::ColumnType attrt = common::ColumnType::UNK);
  DataTypeDecimal(int128 value, short scale = -1, short prec = -1, common::ColumnType attrt = common::ColumnType::UNK);
  DataTypeDecimal(const DataTypeDecimal &);
  ~DataTypeDecimal();

  // assign from BString & int64, make sure if we still need them
  DataTypeDecimal &Assign(BString value, short scale = -1, short prec = -1,
                          common::ColumnType attrt = common::ColumnType::UNK);
  DataTypeDecimal &Assign(int64_t value, short scale = -1, short prec = -1,
                          common::ColumnType attrt = common::ColumnType::UNK);

  // TODO need any more? or use my_decimal_t to parse from string to decimal
  static common::ErrorCode Parse(const BString &rcs, DataTypeDecimal &dec, ushort precision, ushort scale,
                                 common::ColumnType at = common::ColumnType::UNK);

  static common::ErrorCode ParseReal(const BString &, DataTypeDecimal &dec,
                                     common::ColumnType at = common::ColumnType::UNK);

  DataTypeDecimal &operator=(const DataTypeDecimal &dec);
  DataTypeDecimal &operator=(const TianmuDataType &tianmu_dt) override;
  DataTypeDecimal &operator=(const TianmuValueObject &tvo);

  common::ColumnType Type() const override;  // TODO: need it?

  // compare with decimal & other types
  bool operator==(const TianmuDataType &tianmu_dt) const override;
  bool operator<(const TianmuDataType &tianmu_dt) const override;
  bool operator>(const TianmuDataType &tianmu_dt) const override;
  bool operator>=(const TianmuDataType &tianmu_dt) const override;
  bool operator<=(const TianmuDataType &tianmu_dt) const override;
  bool operator!=(const TianmuDataType &tianmu_dt) const override;

  DataTypeDecimal &operator-=(const DataTypeDecimal &dec);
  DataTypeDecimal &operator+=(const DataTypeDecimal &dec);
  DataTypeDecimal &operator*=(const DataTypeDecimal &dec);
  DataTypeDecimal &operator/=(const DataTypeDecimal &dec);

  DataTypeDecimal operator-(const DataTypeDecimal &dec) const;
  DataTypeDecimal operator+(const DataTypeDecimal &dec) const;
  DataTypeDecimal operator*(const DataTypeDecimal &dec) const;
  DataTypeDecimal operator/(const DataTypeDecimal &dec) const;

  bool IsReal() const { return false; }  // TODO need anymore?
  bool IsInt() const;                    // considering int128 ??

  DataTypeDecimal ToInt() const;  // TODO ?? not TianmuNum or int128? why decimal
  BString ToBString() const override;
  DataTypeDecimal ToDecimal(
      int scale = -1) const;  // If canBePromoted, maybe usedful in the future when support from int32~int256.
  BString ToReal() const;     // TODO ??? return val not TianmuNum?

  operator int128() const { return GetIntPart(); }
  operator double() const;
  operator float() const { return (float)(double)*this; }  // TODO: c++ style

  inline short Precision() const { return precision_; }
  inline short Scale() const { return scale_; }
  inline char *GetDataBytesPointer() const override { return (char *)&value_; }  // TODO: c++ style

  inline int128 GetIntPart() const { return value_ / Uint128PowOfTen(scale_); }
  inline int128 GetFracPart() const { return value_ % Uint128PowOfTen(scale_); }
  inline int128 GetValueInt() const { return value_; }
  // int128 ValueInt() const { return value_; } repeat with above? no longer used.

  short GetDecStrLen() const;
  short GetDecIntLen() const;
  inline short GetDecFractLen() const;

  uint GetHashCode() const override;
  void Negate();

  static constexpr int MAX_DEC_PRECISION = 38;  // support max 32 or 38?, sth. wrong? change it to private?

 private:
  int compare(const DataTypeDecimal &dec) const;
  int compare(const TianmuDateTime &dt_n) const;

 private:
  int128 value_;
  ushort precision_;  // means 'precision' actually
  ushort scale_;      // means 'scale' actually
  // In TianmuValueObject will use it, ugly! Check if use NUM or a new DECIMAL instead
  common::ColumnType attr_type_ = common::ColumnType::NUM;  // also check if we need it in the future.

 public:
  const static ValueTypeEnum value_type_ = ValueTypeEnum::DECIMAL_TYPE;  // TODO: not numeric ?
};                                                                      // class DataTypeDecimal

}  // namespace types
}  // namespace Tianmu

#endif  // TIANMU_DATA_TYPE_DECIMAL_H_
