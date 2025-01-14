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
#ifndef TIANMU_UTIL_TIMER_H_
#define TIANMU_UTIL_TIMER_H_
#pragma once

#include <chrono>
#include <string>

#include "util/log_ctl.h"

class THD;

namespace Tianmu {
namespace utils {

class Timer {
 public:
  Timer() = default;

  void Print(const std::string &msg) const {
    if (TIANMU_LOGCHECK(LogCtl_Level::DEBUG))
      DoPrint(msg);
  }

 private:
  void DoPrint(const std::string &msg) const;

  std::chrono::high_resolution_clock::time_point start_ = std::chrono::high_resolution_clock::now();
};

class KillTimer {
 public:
  KillTimer(THD *thd, long secs);
  KillTimer() = delete;

  ~KillTimer() {
    if (armed)
      timer_delete(id);
  }

 private:
  timer_t id;
  bool armed = false;
};

}  // namespace utils
}  // namespace Tianmu

#endif  // TIANMU_UTIL_TIMER_H_
