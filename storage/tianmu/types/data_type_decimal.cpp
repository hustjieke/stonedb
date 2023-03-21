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

#include "data_type_decimal.h"

#include "common/assert.h"
#include "core/tools.h"
#include "system/txt_utils.h"
#include "types/tianmu_data_types.h"
#include "types/tianmu_num.h"
#include "types/value_parser4txt.h"

#include <cmath>

namespace Tianmu {
namespace types {

DataTypeDecimal::DataTypeDecimal(common::ColumnType attrt) : value_(0), precision_(0), scale_(0), attr_type_(attrt) {}

DataTypeDecimal::DataTypeDecimal(BString value, short precision, short scale, common::ColumnType attrt)
    : value_(value.ToString()), precision_(precision), scale_(scale), attr_type_(attrt) {
  null_ = (NULL_VALUE_128 == value_) ? true : false;
}

DataTypeDecimal::DataTypeDecimal(int128 value, short precision, short scale, common::ColumnType attrt)
    : value_(value), precision_(precision), scale_(scale), attr_type_(attrt) {
  null_ = (NULL_VALUE_128 == value_) ? true : false;
}

DataTypeDecimal::DataTypeDecimal(const DataTypeDecimal &dec)
    : ValueBasic<DataTypeDecimal>(dec),
      value_(dec.value_),
      precision_(dec.precision_),
      scale_(dec.scale_),
      attr_type_(dec.attr_type_) {
  null_ = dec.null_;
}

DataTypeDecimal::~DataTypeDecimal() {}

DataTypeDecimal &DataTypeDecimal::Assign(BString value, short precision, short scale, common::ColumnType attrt) {
  int128 t(value.ToString());
  this->value_ = t;
  this->precision_ = precision;
  this->scale_ = scale;
  this->attr_type_ = attrt;

  if (scale <= -1)
    scale_ = 0;  // TODO: should throw error like CK?
  null_ = (NULL_VALUE_128 == value_ ? true : false);

  return *this;
}

DataTypeDecimal &DataTypeDecimal::Assign(int64_t value, short scale, short precision, common::ColumnType attrt) {
  int128 t =
      value;  // TODO: should we follow the way boost docs? assign directly is no problem for narrow data, not for wide.
  this->value_ = t;
  this->precision_ = precision;
  this->scale_ = scale;
  this->attr_type_ = attrt;

  if (scale <= -1)
    scale_ = 0;
  null_ = (NULL_VALUE_128 == value_ ? true : false);
  return *this;
}

// TODO make sure if we should support these parse funcs.
common::ErrorCode DataTypeDecimal::Parse([[maybe_unused]] const BString &bs, [[maybe_unused]] DataTypeDecimal &dec,
	[[maybe_unused]] ushort precision, [[maybe_unused]] ushort scale, [[maybe_unused]] common::ColumnType at) {
  // parse decimal from text
  // return ValueParserForText::ParseDecimal(bs, dec, precision, scale); // TODO wait to impl, we put it to phase 3
  return common::ErrorCode::SUCCESS;
}

common::ErrorCode DataTypeDecimal::ParseReal([[maybe_unused]] const BString &bs, [[maybe_unused]] DataTypeDecimal &dec,
	[[maybe_unused]] common::ColumnType at) {
  // return ValueParserForText::ParseRealDecimal(bs, dec, at); // TODO wait to impl, we put it to phase 3
  return common::ErrorCode::SUCCESS;
}

// DataTypeDecimal::ParseNum() {}

DataTypeDecimal &DataTypeDecimal::operator=(const DataTypeDecimal &dec) {
  this->value_ = dec.value_;
  this->precision_ = dec.precision_;
  this->scale_ = dec.scale_;
  this->null_ = dec.null_;
  this->attr_type_ = dec.attr_type_;
  return *this;
}

DataTypeDecimal &DataTypeDecimal::operator=(const TianmuValueObject &tvo) {
  const TianmuDataType &tianmu_dt = *(tvo.Get());  // TODO: should we check it nullptr?
  *this = tianmu_dt;

  return *this;  // return (*this = tianmu_dt);
}

DataTypeDecimal &DataTypeDecimal::operator=(const TianmuDataType &tianmu_dt) {
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    TianmuNum &dec = (TianmuNum &)tianmu_dt;
    TianmuNum dec1 = dec.ToDecimal();
    this->Assign(dec1.ValueInt(), dec1.Scale(), TianmuNum::MAX_DEC_PRECISION, common::ColumnType::NUM);
  } else if (tianmu_dt.GetValueType() == ValueTypeEnum::DECIMAL_TYPE) {
    *this = (DataTypeDecimal &)tianmu_dt;
  } else {
    DataTypeDecimal dec1;
    if (common::IsError(DataTypeDecimal::Parse(tianmu_dt.ToBString(), dec1, precision_, scale_, this->attr_type_))) {
      *this = dec1;
    } else {
      TIANMU_ERROR("Unsupported assign operation!");
      null_ = true;
    }
  }
  return *this;
}

common::ColumnType DataTypeDecimal::Type() const { return attr_type_; }

bool DataTypeDecimal::IsInt() const { return (0 == (value_ % Uint128PowOfTen(scale_))) ? true : false; }

DataTypeDecimal DataTypeDecimal::ToInt() const {  // TODO: cpy from TianmuNum, but make me confused!
  int128 t = GetIntPart();
  return DataTypeDecimal(t, scale_, precision_, common::ColumnType::NUM);
}

BString DataTypeDecimal::ToReal() const {
  int128 v = GetIntPart();
  int128 f = GetFracPart();
  std::string real = v.convert_to<std::string>() + "." + f.convert_to<std::string>();
  return BString(real.c_str(), real.length(), true);
}

DataTypeDecimal DataTypeDecimal::ToDecimal(int scale) const {
  int128 tmpv = 0;  // keep same order with TianmuNum, we may do refactor in the future.
  short tmpp = 0;
  int sign = 1;

  tmpv = this->value_;
  tmpp = this->scale_;
  if (scale != -1) {
    if (tmpp > scale)
      tmpv /= (int64_t)Uint64PowOfTen(tmpp - scale);
    else
      tmpv *= (int64_t)Uint64PowOfTen(scale - tmpp);
    tmpp = scale;
  }
  return DataTypeDecimal(tmpv * sign, 0, tmpp, common::ColumnType::NUM);
}

static char *Text(int64_t value, char buf[], int scale) {  // TODO: check if int64_t ---> int128
  bool sign = true;
  if (value < 0) {
    sign = false;
    value *= -1;
  }
  longlong2str(value, buf, 10);
  int l = (int)std::strlen(buf);
  std::memset(buf + l + 1, ' ', 21 - l);
  int pos = 21;
  int i = 0;
  for (i = l; i >= 0; i--) {
    if (scale != 0 && pos + scale == 20) {
      buf[pos--] = '.';
      i++;
    } else {
      buf[pos--] = buf[i];
      buf[i] = ' ';
    }
  }

  if (scale >= l) {
    buf[20 - scale] = '.';
    buf[20 - scale - 1] = '0';
    i = 20 - scale + 1;
    while (buf[i] == ' ') buf[i++] = '0';
  }
  pos = 0;
  while (buf[pos] == ' ') pos++;
  if (!sign)
    buf[--pos] = '-';
  return buf + pos;
}

BString DataTypeDecimal::ToBString() const {
  if (!IsNull()) {
    std::string val = value_.convert_to<std::string>();  // use boost's built in function
    return BString(val.c_str(), val.length(), true);
  }
  return BString();
}

bool DataTypeDecimal::operator==(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (ValueTypeEnum::NUMERIC_TYPE == tianmu_dt.GetValueType())
    return (0 == compare((DataTypeDecimal &)tianmu_dt));
  if (ValueTypeEnum::DATE_TIME_TYPE == tianmu_dt.GetValueType())
    return (0 == compare((TianmuDateTime &)tianmu_dt));
  if (ValueTypeEnum::STRING_TYPE == tianmu_dt.GetValueType())
    return (tianmu_dt == this->ToBString());

  TIANMU_ERROR("Bad cast inside DataTypeDecimal");
  return false;
}

bool DataTypeDecimal::operator!=(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (ValueTypeEnum::NUMERIC_TYPE == tianmu_dt.GetValueType())
    return (0 != compare((DataTypeDecimal &)tianmu_dt));
  if (ValueTypeEnum::DATE_TIME_TYPE == tianmu_dt.GetValueType())
    return (0 != compare((TianmuDateTime &)tianmu_dt));
  if (ValueTypeEnum::STRING_TYPE == tianmu_dt.GetValueType())
    return (tianmu_dt != this->ToBString());

  TIANMU_ERROR("Bad cast inside DataTypeDecimal");
  return false;
}

bool DataTypeDecimal::operator<(const TianmuDataType &tianmu_dt) const {
  if (IsNull() || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((DataTypeDecimal &)tianmu_dt) < 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) < 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() < tianmu_dt);
  TIANMU_ERROR("Bad cast inside DataTypeDecimal");
  return false;
}

bool DataTypeDecimal::operator>(const TianmuDataType &tianmu_dt) const {
  if (IsNull() || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE)
    return (compare((DataTypeDecimal &)tianmu_dt) > 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) > 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() > tianmu_dt);
  TIANMU_ERROR("Bad cast inside DataTypeDecimal");
  return false;
}

bool DataTypeDecimal::operator<=(const TianmuDataType &tianmu_dt) const {
  if (IsNull() || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DECIMAL_TYPE)
    return (compare((DataTypeDecimal &)tianmu_dt) <= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    DataTypeDecimal dec;
    dec = (TianmuNum &)tianmu_dt;
    return (compare(dec) <= 0);
  }
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) <= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() <= tianmu_dt);
  TIANMU_ERROR("Bad cast inside DataTypeDecimal");
  return false;
}

bool DataTypeDecimal::operator>=(const TianmuDataType &tianmu_dt) const {
  if (null_ || tianmu_dt.IsNull())
    return false;
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DECIMAL_TYPE)
    return (compare((DataTypeDecimal &)tianmu_dt) >= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    DataTypeDecimal dec;
    dec = (TianmuNum &)tianmu_dt;
    return (compare(dec) >= 0);
  }
  if (tianmu_dt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE)
    return (compare((TianmuDateTime &)tianmu_dt) >= 0);
  if (tianmu_dt.GetValueType() == ValueTypeEnum::STRING_TYPE)
    return (this->ToBString() >= tianmu_dt);
  TIANMU_ERROR("Bad cast inside DataTypeDecimal");
  return false;
}

// TODO: functions next, where to check overflow? TianmuNum does not check.
DataTypeDecimal &DataTypeDecimal::operator-=(const DataTypeDecimal &dec) {
  DEBUG_ASSERT(!null_);
  if (IsNull() || dec.IsNull())
    return *this;
  if (scale_ < dec.scale_) {
    value_ = (value_ * Uint128PowOfTen(dec.scale_ - scale_)) - dec.value_;
    scale_ = dec.scale_;
  } else {
    value_ -= (dec.value_ * Uint128PowOfTen(scale_ - dec.scale_));
  }
  return *this;
}

DataTypeDecimal &DataTypeDecimal::operator+=(const DataTypeDecimal &dec) {
  DEBUG_ASSERT(!null_);
  if (IsNull() || dec.IsNull())
    return *this;
  if (scale_ < dec.scale_) {
    value_ = ((value_ * Uint128PowOfTen(dec.scale_ - scale_)) + dec.value_);
    scale_ = dec.scale_;
  } else {
    value_ += (dec.value_ * Uint128PowOfTen(scale_ - dec.scale_));
  }
  return *this;
}

DataTypeDecimal &DataTypeDecimal::operator*=(const DataTypeDecimal &dec) {
  DEBUG_ASSERT(!null_);
  if (IsNull() || dec.IsNull())
    return *this;
  value_ *= dec.value_;
  scale_ += dec.scale_;
  return *this;
}

DataTypeDecimal &DataTypeDecimal::operator/=(const DataTypeDecimal &dec) {
  DEBUG_ASSERT(!null_);
  if (IsNull() || dec.IsNull())
    return *this;
  if (scale_ < dec.scale_) {
    value_ = (value_ * Uint128PowOfTen(dec.scale_ - scale_)) / dec.value_;
    scale_ = dec.scale_;
  } else {
    value_ /= (dec.value_ * Uint128PowOfTen(scale_ - dec.scale_));
  }
  return *this;
}

DataTypeDecimal DataTypeDecimal::operator-(const DataTypeDecimal &dec) const {
  DataTypeDecimal res(*this);
  return res -= dec;
}

DataTypeDecimal DataTypeDecimal::operator+(const DataTypeDecimal &dec) const {
  DataTypeDecimal res(*this);
  return res += dec;
}

DataTypeDecimal DataTypeDecimal::operator*(const DataTypeDecimal &dec) const {
  DataTypeDecimal res(*this);
  return res *= dec;
}

DataTypeDecimal DataTypeDecimal::operator/(const DataTypeDecimal &dec) const {
  DataTypeDecimal res(*this);
  return res /= dec;
}

uint DataTypeDecimal::GetHashCode() const { return uint(GetIntPart() * 1040021); }

int DataTypeDecimal::compare(const DataTypeDecimal &dec) const {
  if (IsNull() || dec.IsNull())
    return false;
  if (scale_ != dec.scale_) {
    if (value_ < 0 && dec.value_ >= 0)
      return -1;
    if (value_ >= 0 && dec.value_ < 0)
      return 1;
    if (scale_ < dec.scale_) {
      int128 power_of_ten = Uint128PowOfTen(dec.scale_ - scale_);
      int128 tmpv = dec.value_ / power_of_ten;
      if (value_ > tmpv)
        return 1;
      if (value_ < tmpv || dec.value_ % power_of_ten > 0)
        return -1;
      if (dec.value_ % power_of_ten < 0)
        return 1;
      return 0;
    } else {
      int128 power_of_ten = Uint128PowOfTen(scale_ - dec.scale_);
      int128 tmpv = value_ / power_of_ten;
      if (tmpv < dec.value_)
        return -1;
      if (tmpv > dec.value_ || value_ % power_of_ten > 0)
        return 1;
      if (value_ % power_of_ten < 0)
        return -1;
      return 0;
    }
  } else
    return (value_ > dec.value_ ? 1 : (value_ == dec.value_ ? 0 : -1));
}

int DataTypeDecimal::compare(const TianmuDateTime &tianmu_dt) const {
  int64_t tmp;
  tianmu_dt.ToInt64(tmp);
  return int(GetIntPart() - tmp);
}

short DataTypeDecimal::GetDecStrLen() const {
  if (IsNull())
    return 0;
  short res = scale_;
  int128 tmpi = value_ / Uint128PowOfTen(scale_);
  while (tmpi != 0) {
    tmpi /= 10;
    res++;
  }
  return res;
}

short DataTypeDecimal::GetDecIntLen() const {
  if (IsNull())
    return 0;
  short res = 0;
  int128 tmpi = 0;
  tmpi = GetIntPart();
  while (tmpi != 0) {
    tmpi /= 10;
    res++;
  }
  return res;
}

inline short DataTypeDecimal::GetDecFractLen() const {
  if (IsNull())
    return 0;
  return scale_;
}

void DataTypeDecimal::Negate() {
  if (IsNull())
    return;
  value_ *= -1;
}

}  // namespace types
}  // namespace Tianmu
