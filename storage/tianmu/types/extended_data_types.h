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

#ifndef TIANMU_TYPES_EXTENDED_DATA_TYPES_H
#define TIANMU_TYPES_EXTENDED_DATA_TYPES_H

#include "common/mysql_gate.h"
#include "boost/multiprecision/cpp_int.hpp"

namespace Tianmu {

using int128 = boost::multiprecision::int128_t;
using uint128 = boost::multiprecision::uint128_t;

// ref: https://www.boost.org/doc/libs/1_81_0/libs/multiprecision/doc/html/boost_multiprecision/tut/conversions.html
static const int128 operator""_cppi128(const char *s, unsigned long len) { return int128(std::string(s, len)); }

// const int128 PLUS_INF_128 = "0x4ee2d6d415b85acef8100000000"_cppi128; // TODO: support max prec (32, 6) or (38, 0)
// just like clickhouse const int128 MINUS_INF_128 = "-0x4ee2d6d415b85acef8100000000"_cppi128; // -100 000 000 000 000
// 000 000 000 000 000 000 (max prec (32,6)) const int128 NULL_VALUE_128 = "-0x4ee2d6d415b85acef80ffffffff"_cppi128;

const int128 PLUS_INF_128 =
    std::numeric_limits<int128>::max();  // max prec (38, 0) 340282366920938463463374607431768211455
const int128 MINUS_INF_128 = std::numeric_limits<int128>::min();  // -340282366920938463463374607431768211455
const int128 NULL_VALUE_128 = MINUS_INF_128 + 1;                           // -340282366920938463463374607431768211454

static inline uint128 Uint128PowOfTen(short exponent) {
  DEBUG_ASSERT(exponent >= 0 && exponent < 39);
  static const short EXP = 19;
  static uint128 v[] =  {1ULL,
                                  10ULL,
                                  100ULL,
                                  1000ULL,
                                  10000ULL,
                                  100000ULL,
                                  1000000ULL,
                                  10000000ULL,
                                  100000000ULL,
                                  1000000000ULL,
                                  10000000000ULL,
                                  100000000000ULL,
                                  1000000000000ULL,
                                  10000000000000ULL,
                                  100000000000000ULL,
                                  1000000000000000ULL,
                                  10000000000000000ULL,
                                  100000000000000000ULL,
                                  1000000000000000000ULL,
                                  10000000000000000000ULL};

  if (exponent >= 0 && exponent <= EXP)
    return v[exponent];
  else {
    return v[exponent-EXP] * v[EXP];
  }
}

}  // namespace Tianmu

#endif
