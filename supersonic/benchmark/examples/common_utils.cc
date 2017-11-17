// Copyright 2012 Google Inc.  All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
//
// Implementation of benchmark utils.

#include "supersonic/benchmark/examples/common_utils.h"

#include "supersonic/utils/std_namespace.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/benchmark/manager/benchmark_manager.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/view_printer.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"

namespace supersonic {

void BenchmarkOperation(unique_ptr<Operation> operation,
                        const string& benchmark_name,
                        GraphVisualisationOptions options,
                        rowcount_t max_block_size,
                        bool log_result) {
  string operation_info;
  operation->AppendDebugDescription(&operation_info);
  LOG(INFO) << "Benchmarking: " << operation_info;

  FailureOrOwned<Cursor> cursor_owner = operation->CreateCursor();
  CHECK(cursor_owner.is_success())
      << cursor_owner.exception().PrintStackTrace();

  ViewPrinter printer;

  auto data_wrapper = SetUpBenchmarkForCursor(cursor_owner.move());
  auto benchmarked_cursor = data_wrapper->move_cursor();

  while (true) {
    ResultView view = benchmarked_cursor->Next(max_block_size);
    if (view.is_eos() || !view.has_data()) {
      break;
    }

    if (log_result) {
      LOG(INFO) << ViewPrinter::StreamResultViewAdapter(printer, view);
    }
  }

  CreateGraph(benchmark_name, data_wrapper->node(), options);
}

int32_t EpochDaysFromStringDate(const string& date_string) {
  int32_t days_since_epoch = GetDaysSinceEpoch(date_string.c_str());
  CHECK_GT(days_since_epoch, 0)
      << "Cannot parse date: " << date_string
      << "; it must be in year-month-day format";
  return days_since_epoch;
}


}  // namespace supersonic
