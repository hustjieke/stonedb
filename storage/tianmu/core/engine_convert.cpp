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

#include <ctime>

#include "common/assert.h"
#include "core/engine.h"
#include "core/transaction.h"

namespace Tianmu {
namespace core {
// gry: tianmu item 转 mysql Field, 这个在 SendResults 的时候被调用.
bool Engine::ConvertToField(Field *field, types::TianmuDataType &tianmu_item, std::vector<uchar> *blob_buf) {
  if (tianmu_item.IsNull()) {
    std::memset(field->ptr, 0, 2);
    field->set_null();
    return true;
  }

  field->set_notnull();

  switch (field->type()) {
    case MYSQL_TYPE_VARCHAR: {
      DEBUG_ASSERT(dynamic_cast<types::BString *>(&tianmu_item));
      types::BString &str_val = (types::BString &)tianmu_item;
      if (str_val.size() > field->field_length)
        throw common::DatabaseException("Incorrect field size: " + std::to_string(str_val.size()));
      if (field->field_length <= 255)
        str_val.PutVarchar((char *&)field->ptr, 1, false);
      else if (field->field_length <= SHORT_MAX)
        str_val.PutVarchar((char *&)field->ptr, 2, false);
      break;
    }
    case MYSQL_TYPE_STRING:
      if (dynamic_cast<types::BString *>(&tianmu_item)) {
        ((types::BString &)tianmu_item).PutString((char *&)field->ptr, (ushort)field->field_length, false);
      } else {
        tianmu_item.ToBString().PutString((char *&)field->ptr, (ushort)field->field_length, false);
      }
      break;
    case MYSQL_TYPE_BLOB: {
      DEBUG_ASSERT(dynamic_cast<types::BString *>(&tianmu_item));
      Field_blob *blob = (Field_blob *)field;
      if (blob_buf == nullptr) {
        blob->set_ptr(((types::BString &)tianmu_item).len_, (uchar *)((types::BString &)tianmu_item).val_);
        blob->copy();
      } else {
        blob->store(((types::BString &)tianmu_item).val_, ((types::BString &)tianmu_item).len_, &my_charset_bin);
        uchar *src, *tgt;

        uint packlength = blob->pack_length_no_ptr();
        uint length = blob->get_length(blob->ptr);
        std::memcpy(&src, blob->ptr + packlength, sizeof(char *));
        if (src) {
          blob_buf->resize(length);
          tgt = &((*blob_buf)[0]);
          memmove(tgt, src, length);
          std::memcpy(blob->ptr + packlength, &tgt, sizeof(char *));
        }
      }
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL: {
      my_decimal md;
      if (tianmu_item.Type() == common::ColumnType::REAL) {
        double2decimal((double)((types::TianmuNum &)(tianmu_item)), &md);
      } else {
        int is_null;
        Engine::Convert(is_null, &md, tianmu_item);
      }
      decimal_round(&md, &md, ((Field_new_decimal *)field)->decimals(), HALF_UP);
      decimal2bin(&md, (uchar *)field->ptr, ((Field_new_decimal *)field)->precision,
                  ((Field_new_decimal *)field)->decimals());
      break;
    }
    default:
      switch (tianmu_item.Type()) {
        case common::ColumnType::BYTEINT:
        case common::ColumnType::SMALLINT:
        case common::ColumnType::MEDIUMINT:
        case common::ColumnType::INT:
        case common::ColumnType::BIGINT:
        case common::ColumnType::REAL:
        case common::ColumnType::FLOAT:
        case common::ColumnType::NUM: // gry: decimal ?? 这部分逻辑有问题把，上面已经有decimal了。
        case common::ColumnType::BIT:
          switch (field->type()) {
            case MYSQL_TYPE_TINY:
              *reinterpret_cast<char *>(field->ptr) = (char)(int64_t)((types::TianmuNum &)(tianmu_item));
              break;
            case MYSQL_TYPE_SHORT:
              *reinterpret_cast<short *>(field->ptr) = (short)(int64_t)((types::TianmuNum &)(tianmu_item));
              break;
            case MYSQL_TYPE_INT24:
              int3store((char *)field->ptr, (int)(int64_t)((types::TianmuNum &)(tianmu_item)));
              break;
            case MYSQL_TYPE_LONG: // gry: 32 位系统是 4 字节解释吧，所以 int 转换
              *reinterpret_cast<int *>(field->ptr) = (int)(int64_t)((types::TianmuNum &)(tianmu_item));
              break;
            case MYSQL_TYPE_LONGLONG:
              *reinterpret_cast<int64_t *>(field->ptr) = (int64_t)((types::TianmuNum &)(tianmu_item));
              break;
            case MYSQL_TYPE_BIT:  // mysql bit(1~64), here is (1~63, 1 precision lose)
              // Bit type used ** type_conversion_status Field_bit::store(longlong nr, bool unsigned_val) ** to store
              // longlong val. Storing of values to field->ptr in high byte first order to get better compression(that
              // means for integer values, stored in big-endian bytes order. We should not use
              // **reinterpret_cast<int64_t *>(field->ptr) ** to store directly, otherwise the field->ptr will be
              // little-endian bytes order and will get wrong value when used Field_bit::val_int() to get bit value back
              // params: "true" is unsigned flag, but it's not used in bit field->store() function.
              field->store((int64_t)((types::TianmuNum &)(tianmu_item)), true);
              break;
            case MYSQL_TYPE_FLOAT:
              *reinterpret_cast<float *>(field->ptr) = (float)((types::TianmuNum &)(tianmu_item));
              break;
            case MYSQL_TYPE_DOUBLE:
              *reinterpret_cast<double *>(field->ptr) = (double)((types::TianmuNum &)(tianmu_item));
              break;
            default:
              DEBUG_ASSERT(!"No data types conversion available!");
              break;
          }
          break;
        case common::ColumnType::STRING:
          switch (field->type()) {
            case MYSQL_TYPE_VARCHAR: {
              types::BString &str_val = (types::BString &)tianmu_item;
              if (str_val.size() > field->field_length)
                throw common::DatabaseException("Incorrect field size " + std::to_string(str_val.size()));
              if (field->field_length <= 255) {
                str_val.PutVarchar((char *&)field->ptr, 1, false);
              } else if (field->field_length <= SHORT_MAX) {  // PACK_SIZE - 1
                str_val.PutVarchar((char *&)field->ptr, 2, false);
              }
              break;
            }
            case MYSQL_TYPE_STRING:
              ((types::BString &)tianmu_item).PutString((char *&)field->ptr, (ushort)field->field_length, false);
              break;
            case MYSQL_TYPE_BLOB: {
              Field_blob *blob = (Field_blob *)field;
              if (blob_buf == nullptr) {
                blob->set_ptr(((types::BString &)tianmu_item).len_, (uchar *)((types::BString &)tianmu_item).val_);
                blob->copy();
              } else {
                blob->store(((types::BString &)tianmu_item).val_, ((types::BString &)tianmu_item).len_,
                            &my_charset_bin);
                uchar *src, *tgt;

                uint packlength = blob->pack_length_no_ptr();
                uint length = blob->get_length(blob->ptr);
                std::memcpy(&src, blob->ptr + packlength, sizeof(char *));
                if (src) {
                  blob_buf->resize(length);
                  tgt = &((*blob_buf)[0]);
                  memmove(tgt, src, length);
                  std::memcpy(blob->ptr + packlength, &tgt, sizeof(char *));
                }
              }
              break;
            }
            case MYSQL_TYPE_DATE: {
              char tmp[10];
              char *tmpptr = tmp;
              ((types::BString &)tianmu_item).PutString(tmpptr, ushort(sizeof(tmp)), false);
              ((Field_newdate *)field)->store(tmp, sizeof(tmp), nullptr);
              break;
            }
            case MYSQL_TYPE_TIME: {
              char tmp[10];
              char *tmpptr = tmp;
              ((types::BString &)tianmu_item).PutString(tmpptr, ushort(sizeof(tmp)), false);
              ((Field_time *)field)->store(tmp, sizeof(tmp), nullptr);
              break;
            }
            case MYSQL_TYPE_DATETIME: {
              char tmp[19];
              char *tmpptr = tmp;
              ((types::BString &)tianmu_item).PutString(tmpptr, ushort(sizeof(tmp)), false);
              ((Field_datetime *)field)->store(tmp, sizeof(tmp), nullptr);
              break;
            }
            default:
              ((types::BString &)tianmu_item).PutString((char *&)field->ptr, (ushort)field->field_length, false);
              break;
          }

          break;
        case common::ColumnType::YEAR: {
          ASSERT(field->type() == MYSQL_TYPE_YEAR);
          auto tianmu_dt = dynamic_cast<types::TianmuDateTime *>(&tianmu_item);
          MYSQL_TIME my_time = {};
          tianmu_dt->Store(&my_time, MYSQL_TIMESTAMP_DATE);
          field->store_time(&my_time);
          break;
        }
        case common::ColumnType::DATE: {
          if (field->type() == MYSQL_TYPE_DATE || field->type() == MYSQL_TYPE_NEWDATE) {
            auto tianmu_dt = dynamic_cast<types::TianmuDateTime *>(&tianmu_item);
            MYSQL_TIME my_time = {};
            tianmu_dt->Store(&my_time, MYSQL_TIMESTAMP_DATE);
            field->store_time(&my_time);
          }
          break;
        }

          /*
          Datetime representations:
           - Packed:  In-memory representation
           - Binary:  On-disk representation
           - Structure inside MySQL: MYSQL_TIME

               Type        before MySQL 5.6.4        Storage as of MySQL 5.6.4
               ---------------------------------------------------------------
               YEAR        1 byte,little endian      Unchanged
               DATE        3 bytes,little endian     Unchanged
               TIME        3 bytes,little endian     3 bytes +
          fractional-seconds storage, big endian TIMESTAMP   4 bytes,little
          endian     4 bytes + fractional-seconds storage, big endian DATETIME
          8 bytes,little endian     5 bytes + fractional-seconds storage, big
          endian

          DATETIME encoding for nonfractional part:
          -----------------------------------------

              1 bit  sign           (1= non-negative, 0= negative)
             17 bits year*13+month  (year 0-9999, month 0-12)
              5 bits day            (0-31)
              5 bits hour           (0-23)
              6 bits minute         (0-59)
              6 bits second         (0-59)
             ---------------------------
             40 bits = 5 bytes
             The sign bit is always 1

          */

        case common::ColumnType::TIME: {
          ASSERT(field->type() == MYSQL_TYPE_TIME);
          auto tianmu_dt = dynamic_cast<types::TianmuDateTime *>(&tianmu_item);
          MYSQL_TIME my_time = {};
          tianmu_dt->Store(&my_time, MYSQL_TIMESTAMP_TIME);
          field->store_time(&my_time);
          break;
        }
        case common::ColumnType::DATETIME: {
          ASSERT(field->type() == MYSQL_TYPE_DATETIME);
          auto tianmu_dt = dynamic_cast<types::TianmuDateTime *>(&tianmu_item);
          MYSQL_TIME my_time = {};
          tianmu_dt->Store(&my_time, MYSQL_TIMESTAMP_DATETIME);
          field->store_time(&my_time);
          break;
        }
        case common::ColumnType::TIMESTAMP: {
          auto tianmu_dt = dynamic_cast<types::TianmuDateTime *>(&tianmu_item);
          MYSQL_TIME my_time = {};
          types::TianmuDateTime::AdjustTimezone(*tianmu_dt);
          tianmu_dt->Store(&my_time, MYSQL_TIMESTAMP_DATETIME);
          field->store_time(&my_time);
          break;
        }
        default:
          break;
      }
      break;
  }
  return false;
}

#define DIG_PER_DEC1 9
#define DIG_BASE 1000000000
#define ROUND_UP(X) (((X) + DIG_PER_DEC1 - 1) / DIG_PER_DEC1)

// gry: mysql decimal 转 TianmuDataType
int Engine::Convert(int &is_null, my_decimal *value, types::TianmuDataType &tianmu_item, int output_scale) {
  if (tianmu_item.IsNull())
    is_null = 1;
  else {
    if (!Engine::AreConvertible(tianmu_item, MYSQL_TYPE_NEWDECIMAL))
      return false;
    is_null = 0;
    if (tianmu_item.Type() == common::ColumnType::NUM) {
      types::TianmuNum *tianmu_n = (types::TianmuNum *)(&tianmu_item);
      int intg = tianmu_n->GetDecIntLen();
      int frac = tianmu_n->GetDecFractLen();
      int intg1 = ROUND_UP(intg);
      int frac1 = ROUND_UP(frac);
      value->intg = intg;
      value->frac = frac;
      int64_t ip = tianmu_n->GetIntPart();
      int64_t fp = (tianmu_n->ValueInt() % (int64_t)types::Uint64PowOfTen(tianmu_n->Scale()));
      bool special_value_minbigint = false;
      if (uint64_t(ip) == 0x8000000000000000ULL) {
        // a special case, cannot be converted like that
        special_value_minbigint = true;
        ip += 1;  // just for now...
      }
      if (ip < 0) {
        ip *= -1;
        value->sign(true);
        if (fp < 0)
          fp *= -1;
      } else if (ip == 0 && fp < 0) {
        fp *= -1;
        value->sign(true);
      } else
        value->sign(false);

      decimal_digit_t *buf = value->buf + intg1;
      for (int i = intg1; i > 0; i--) {
        *--buf = decimal_digit_t(ip % DIG_BASE);
        if (special_value_minbigint && i == intg1) {
          *buf += 1;  // revert the special case (plus, because now it is
                      // unsigned part)
        }
        ip /= DIG_BASE;
      }
      buf = value->buf + intg1 + (frac1 - 1);
      int64_t tmp(fp);
      int no_digs = 0;
      while (tmp > 0) {
        tmp /= 10;
        no_digs++;
      }
      int tmp_prec = tianmu_n->Scale();

      for (; frac1; frac1--) {
        int digs_to_take = tmp_prec - (frac1 - 1) * DIG_PER_DEC1;
        if (digs_to_take < 0)
          digs_to_take = 0;
        tmp_prec -= digs_to_take;
        int cur_pow = DIG_PER_DEC1 - digs_to_take;
        *buf-- = decimal_digit_t((fp % (int64_t)types::Uint64PowOfTen(digs_to_take)) *
                                 (int64_t)types::Uint64PowOfTen(cur_pow));
        fp /= (int64_t)types::Uint64PowOfTen(digs_to_take);
      }
      int output_scale_1 = (output_scale > 18) ? 18 : output_scale;
      my_decimal_round(0, value, (output_scale_1 == -1) ? frac : output_scale_1, false, value);
      return 1;
    } else if (tianmu_item.Type() == common::ColumnType::REAL || tianmu_item.Type() == common::ColumnType::FLOAT) {
      double2decimal((double)((types::TianmuNum &)(tianmu_item)), (decimal_t *)value);
      return 1;
    } else if (ATI::IsIntegerType(tianmu_item.Type())) {
      longlong2decimal((longlong)((types::TianmuNum &)(tianmu_item)).ValueInt(), (decimal_t *)value);
      return 1;
    }
    return false;
  }
  return 1;
}

// gry: mysql int64_t 转 TianmuDataType, 对应 convert / cast 函数
int Engine::Convert(int &is_null, int64_t &value, types::TianmuDataType &tianmu_item, enum_field_types f_type,
                    bool unsigned_flag) {
  if (tianmu_item.IsNull())
    is_null = 1;
  else {
    is_null = 0;
    if (tianmu_item.Type() == common::ColumnType::NUM || tianmu_item.Type() == common::ColumnType::BIGINT ||
        tianmu_item.Type() == common::ColumnType::BIT) { // gry(bit): 需要测试下怎么才能走到这里，类型转换？
      value = (int64_t)(types::TianmuNum &)tianmu_item;
      switch (f_type) {
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_INT24:
          if (value == common::NULL_VALUE_64)
            value = common::NULL_VALUE_32;
          else if (value == common::PLUS_INF_64)
            value = std::numeric_limits<int>::max();
          else if (value == common::MINUS_INF_64)
            value = TIANMU_INT_MIN;
          break;
        case MYSQL_TYPE_TINY:
          if (value == common::NULL_VALUE_64)
            value = common::NULL_VALUE_C;
          else if (value == common::PLUS_INF_64)
            value = TIANMU_TINYINT_MAX;
          else if (value == common::MINUS_INF_64)
            value = TIANMU_TINYINT_MIN;
          break;
        case MYSQL_TYPE_SHORT:
          if (value == common::NULL_VALUE_64)
            value = common::NULL_VALUE_SH;
          else if (value == common::PLUS_INF_64)
            value = TIANMU_SMALLINT_MAX;
          else if (value == common::MINUS_INF_64)
            value = TIANMU_SMALLINT_MIN;
          break;
        default:
          break;
      }
      return 1;
    } else if (tianmu_item.Type() == common::ColumnType::INT || tianmu_item.Type() == common::ColumnType::MEDIUMINT) {
      value = (unsigned_flag ? (uint)(int64_t) dynamic_cast<types::TianmuNum &>(tianmu_item)
                             : (int)(int64_t) dynamic_cast<types::TianmuNum &>(tianmu_item));
      return 1;
    } else if (tianmu_item.Type() == common::ColumnType::BYTEINT) {
      value = (unsigned_flag ? (uchar)(int64_t) dynamic_cast<types::TianmuNum &>(tianmu_item)
                             : (char)(int64_t) dynamic_cast<types::TianmuNum &>(tianmu_item));
      return 1;
    } else if (tianmu_item.Type() == common::ColumnType::SMALLINT) {
      value = (unsigned_flag ? (ushort)(int64_t) dynamic_cast<types::TianmuNum &>(tianmu_item)
                             : (short)(int64_t) dynamic_cast<types::TianmuNum &>(tianmu_item));
      return 1;
    } else if (tianmu_item.Type() == common::ColumnType::YEAR) {
      value = dynamic_cast<types::TianmuDateTime &>(tianmu_item).Year();
      return 1;
    } else if (tianmu_item.Type() == common::ColumnType::REAL) {
      value = (int64_t)(double)dynamic_cast<types::TianmuNum &>(tianmu_item);
      return 1;
    }
  }
  return 0;
}

// gry: double 转 TianmuDataType
int Engine::Convert(int &is_null, double &value, types::TianmuDataType &tianmu_item) {
  if (tianmu_item.IsNull())
    is_null = 1;
  else {
    if (!Engine::AreConvertible(tianmu_item, MYSQL_TYPE_DOUBLE))
      return 0;
    is_null = 0;
    if (tianmu_item.Type() == common::ColumnType::REAL) {
      value = (double)dynamic_cast<types::TianmuNum &>(tianmu_item);
      return 1;
    } else if (tianmu_item.Type() == common::ColumnType::FLOAT) {
      value = (float)dynamic_cast<types::TianmuNum &>(tianmu_item);
      return 1;
    }
  }
  return 0;
}

// gry: string 转 TianmuDataType
int Engine::Convert(int &is_null, String *value, types::TianmuDataType &tianmu_item, enum_field_types f_type) {
  if (tianmu_item.IsNull())
    is_null = 1;
  else {
    if (!Engine::AreConvertible(tianmu_item, MYSQL_TYPE_STRING))
      return 0;
    is_null = 0;
    if (f_type == MYSQL_TYPE_VARCHAR || f_type == MYSQL_TYPE_VAR_STRING) {
      types::BString str = tianmu_item.ToBString();
      value->set_ascii(str.val_, str.len_);
      value->copy();
    } else if (f_type == MYSQL_TYPE_STRING) {
      types::BString str = tianmu_item.ToBString();
      value->set_ascii(str.val_, str.len_);
      value->copy();
    } else if (f_type == MYSQL_TYPE_NEWDATE || f_type == MYSQL_TYPE_DATE) {
      types::BString str = tianmu_item.ToBString();
      value->set_ascii(str.val_, str.len_);
      value->copy();
    } else if (f_type == MYSQL_TYPE_TIME) {
      types::BString str = tianmu_item.ToBString();
      value->set_ascii(str.val_, str.len_);
      value->copy();
    } else if (f_type == MYSQL_TYPE_DATETIME) {
      types::BString str = tianmu_item.ToBString();
      value->set_ascii(str.val_, str.len_);
      value->copy();
    } else if (f_type == MYSQL_TYPE_TIMESTAMP) {
      if (types::TianmuDateTime *tianmu_dt = dynamic_cast<types::TianmuDateTime *>(&tianmu_item)) {
        if (*tianmu_dt != types::kTianmuTimestampSpec) {
          MYSQL_TIME local_time;
          my_time_t secs = tianmu_sec_since_epoch(tianmu_dt->Year(), tianmu_dt->Month(), tianmu_dt->Day(),
                                                  tianmu_dt->Hour(), tianmu_dt->Minute(), tianmu_dt->Second());
          current_txn_->Thd()->variables.time_zone->gmt_sec_to_TIME(&local_time, secs);
          char buf[32];
          local_time.second_part = tianmu_dt->MicroSecond();
          my_datetime_to_str(&local_time, buf, 0);
          value->set_ascii(buf, 19);
        } else {
          value->set_ascii("0000-00-00 00:00:00", 19);
        }
      } else {
        types::BString str = tianmu_item.ToBString();
        value->set_ascii(str.val_, str.len_);
      }
      value->copy();
    } else if (f_type == MYSQL_TYPE_BLOB || f_type == MYSQL_TYPE_MEDIUM_BLOB) {
      types::BString str = tianmu_item.ToBString();
      value->set_ascii(str.val_, str.len_);
      value->copy();
    }
    return 1;
  }
  return 0;
}

// gry: 判断是否可以转换
bool Engine::AreConvertible(types::TianmuDataType &tianmu_item, enum_field_types my_type,
                            [[maybe_unused]] uint length) {
  /*if(tianmu_item->Type() == Engine::GetCorrespondingType(my_type, length) ||
   tianmu_item->IsNull()) return true;*/
  common::ColumnType tianmu_type = tianmu_item.Type();
  switch (my_type) {
    case MYSQL_TYPE_LONGLONG: // gry(bit): 这里要支持转 bit 么？这里要测试转换函数，这里是转 longlong
      if (tianmu_type == common::ColumnType::INT || tianmu_type == common::ColumnType::MEDIUMINT ||
          tianmu_type == common::ColumnType::BIGINT ||
          (tianmu_type == common::ColumnType::NUM && dynamic_cast<types::TianmuNum &>(tianmu_item).Scale() == 0))
        return true;
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      if (tianmu_type == common::ColumnType::FLOAT || tianmu_type == common::ColumnType::REAL ||
          ATI::IsIntegerType(tianmu_type) || tianmu_type == common::ColumnType::NUM)
        return true;
      break;
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      return (tianmu_type == common::ColumnType::STRING || tianmu_type == common::ColumnType::VARCHAR ||
              tianmu_type == common::ColumnType::BYTE || tianmu_type == common::ColumnType::VARBYTE ||
              tianmu_type == common::ColumnType::LONGTEXT || tianmu_type == common::ColumnType::BIN);
    case MYSQL_TYPE_YEAR:
      return tianmu_type == common::ColumnType::YEAR;
    case MYSQL_TYPE_SHORT:
      return tianmu_type == common::ColumnType::SMALLINT;
    case MYSQL_TYPE_TINY:
      return tianmu_type == common::ColumnType::BYTEINT;
    case MYSQL_TYPE_INT24:
      return tianmu_type == common::ColumnType::MEDIUMINT;
    case MYSQL_TYPE_LONG:
      return tianmu_type == common::ColumnType::INT;
    case MYSQL_TYPE_BIT:
      return tianmu_type == common::ColumnType::BIT;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      return tianmu_type == common::ColumnType::FLOAT || tianmu_type == common::ColumnType::REAL;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
      return (tianmu_type == common::ColumnType::DATETIME || tianmu_type == common::ColumnType::TIMESTAMP);
    case MYSQL_TYPE_TIME:
      return tianmu_type == common::ColumnType::TIME;
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
      return tianmu_type == common::ColumnType::DATE;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
      return true;
    default:
      break;
  }
  return false;
}

// gry: 获得和 MySQL 一致的 tianmu 侧定义的类型, 入参在 binary log 文件定义
common::ColumnType Engine::GetCorrespondingType(const enum_field_types &eft) {
  switch (eft) {
    case MYSQL_TYPE_YEAR:
      return common::ColumnType::YEAR;
    case MYSQL_TYPE_SHORT:
      return common::ColumnType::SMALLINT;
    case MYSQL_TYPE_TINY:
      return common::ColumnType::BYTEINT;
    case MYSQL_TYPE_INT24:
      return common::ColumnType::MEDIUMINT;
    case MYSQL_TYPE_LONG:
      return common::ColumnType::INT;
    case MYSQL_TYPE_LONGLONG:
      return common::ColumnType::BIGINT;
    case MYSQL_TYPE_BIT:
      return common::ColumnType::BIT;
    case MYSQL_TYPE_FLOAT:
      return common::ColumnType::FLOAT;
    case MYSQL_TYPE_DOUBLE:
      return common::ColumnType::REAL;
    case MYSQL_TYPE_TIMESTAMP:
      return common::ColumnType::TIMESTAMP;
    case MYSQL_TYPE_DATETIME:
      return common::ColumnType::DATETIME;
    case MYSQL_TYPE_TIME:
      return common::ColumnType::TIME;
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
      return common::ColumnType::DATE;
    case MYSQL_TYPE_NEWDECIMAL:
      return common::ColumnType::NUM;
    case MYSQL_TYPE_STRING:
      return common::ColumnType::STRING;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_BLOB:
      return common::ColumnType::VARCHAR;
    default:
      return common::ColumnType::UNK;
  }
}

// gry: 从 MySQL Field 获取 tianmu 列类型，获取一致的类型, 重载的函数，这个设计真让人容易疑惑
common::ColumnType Engine::GetCorrespondingType(const Field &field) {
  common::ColumnType typ = GetCorrespondingType(field.type());
  if (!ATI::IsStringType(typ))
    return typ;
  else {
    switch (field.type()) {
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING: {
        if (const Field_str *fstr = dynamic_cast<const Field_string *>(&field)) {
          if (fstr->charset() != &my_charset_bin)
            return common::ColumnType::STRING;
          return common::ColumnType::BYTE;
        } else if (const Field_str *fvstr = dynamic_cast<const Field_varstring *>(&field)) {
          if (fvstr->charset() != &my_charset_bin)
            return common::ColumnType::VARCHAR;
          return common::ColumnType::VARBYTE;
        }
      } break;
      case MYSQL_TYPE_BLOB:
        if (const Field_str *fstr = dynamic_cast<const Field_str *>(&field)) {
          if (const Field_blob *fblo = dynamic_cast<const Field_blob *>(fstr)) {
            if (fblo->charset() != &my_charset_bin) {
              // TINYTEXT, MEDIUMTEXT, TEXT, LONGTEXT
              if (fblo->field_length > 65535)
                return common::ColumnType::LONGTEXT;
              return common::ColumnType::VARCHAR;
            } else {
              switch (field.field_length) {
                case 255:
                case 65535:
                  // TINYBLOB, BLOB
                  return common::ColumnType::VARBYTE;
                case 16777215:
                case (size_t)4294967295UL:
                  // MEDIUMBLOB, LONGBLOB
                  return common::ColumnType::BIN;
              }
            }
          }
        }
        break;
      default:
        return common::ColumnType::UNK;
    }
  }
  return common::ColumnType::UNK;
}

// gry: 从 MySQL Field 信息同步到 tianmu 列属性 attr，获取一致的属性，行和列还是不能混用，不然 field 可以直接用了
AttributeTypeInfo Engine::GetCorrespondingATI(Field &field) {
  common::ColumnType at = GetCorrespondingType(field);

  if (ATI::IsNumericType(at)) {
    DEBUG_ASSERT(dynamic_cast<Field_num *>(&field));
    if (at == common::ColumnType::NUM) { // gry(bit): 这里没问题(需要测试 bit 是否满足，精度获取是否 ok).因为 decimal 精度只能转然后获取，而 bit 直接从 field.field_length 获取
      DEBUG_ASSERT(dynamic_cast<Field_new_decimal *>(&field));
      return AttributeTypeInfo(at, !field.maybe_null(), static_cast<Field_new_decimal &>(field).precision,
                               static_cast<Field_num &>(field).decimals());
    }
    auto unsigned_flag = field.flags & UNSIGNED_FLAG;
    return AttributeTypeInfo(at, !field.maybe_null(), field.field_length, static_cast<Field_num &>(field).decimals(),
                             false, DTCollation(), common::PackFmt::DEFAULT, false, std::string(), unsigned_flag); // gry(bit): 能走还是走一遍确认下 scale 对 bit 影响：static_cast<Field_num &>(field).decimals()
  }
  return AttributeTypeInfo(at, !field.maybe_null(), field.field_length);
}
}  // namespace core
}  // namespace Tianmu
