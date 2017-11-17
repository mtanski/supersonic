// Copyright 2010 Google Inc.  All Rights Reserved
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
//
// Overrides of C++ operators, so that they have saner semantics (e.g. signed
// vs unsigned comparison), and are usable as parameter templates.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_OPERATORS_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_OPERATORS_H_

#include <cstddef>

#include <string>
namespace supersonic {using std::string; }
#include <type_traits>

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/template_util.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/endian.h"
#include "supersonic/utils/hash/hash.h"

namespace supersonic {
namespace operators {

// Identical to Cast, but used in a context when the copy is made without
// changing the type.
struct Copy {
  template<typename T>
  T operator()(const T& arg) const { return arg; }
};

// Cast is a by-product of assigning the result to a variable of a different
// type.
struct Cast : public Copy {
};

// Used for a Date to Datetime cast.
struct DateToDatetime {
  int64_t operator()(const int32_t arg) const { return arg * 24LL * 3600000000LL; }
};

struct Negate {
  int32_t operator()(const int32_t arg) const { return -arg; }
  int64_t operator()(const int64_t arg) const { return -arg; }
  float operator()(const float arg) const { return -arg; }
  double operator()(const double arg) const { return -arg; }

  int64_t operator()(const uint32_t arg) const { return -static_cast<int64_t>(arg); }
  int64_t operator()(const uint64_t arg) const { return -static_cast<int64_t>(arg); }
};

struct Plus {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a + b; }
};

struct Minus {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a - b; }
};

struct Multiply {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a * b; }
};

struct Divide {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a / b; }
};

struct Modulus {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a % b; }

  // Specializations for DOUBLE and FLOAT.
  int64_t operator()(const double& a, const double& b) const {
    return static_cast<int64_t>(a) % static_cast<int64_t>(b);
  }
  int64_t operator()(const float& a, const float& b) const {
    return static_cast<int64_t>(a) % static_cast<int64_t>(b);
  }
};

struct IsOdd {
  template<typename T>
  bool operator()(const T& arg) const { return arg % 2; }

  // Specializations for DOUBLE and FLOAT.
  bool operator()(const double& arg) const {
    return static_cast<int64_t>(arg) % 2;
  }
  bool operator()(const float& arg) const {
    return static_cast<int64_t>(arg) % 2;
  }
};

struct IsEven {
  template<typename T>
  bool operator()(const T& arg) const {
    IsOdd is_odd;
    return !is_odd(arg);
  }
};

struct And {
  template<typename T>
  bool operator()(const T& a, const T& b) const { return a && b; }
};

struct Xor {
  bool operator()(bool a, bool b) const {
    // The reason we do it this way is we want to be safe in the case
    // there are values different than 0 and 1 in the input.
    return !a != !b;
  }
};

struct AndNot {
  template<typename T>
  bool operator()(const T& a, const T& b) const { return (!a) && b; }
};

struct Or {
  template<typename T>
  bool operator()(const T& a, const T& b) const { return a || b; }
};

struct Not {
  template<typename T>
  bool operator()(const T& a) const { return !a; }
};

struct BitwiseAnd {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a & b; }
};

struct BitwiseOr {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a | b; }
};

struct BitwiseXor {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a ^ b; }
};

struct BitwiseAndNot {
  template<typename T>
  T operator()(const T& a, const T& b) const { return (~a) & b; }
};

struct BitwiseNot {
  template<typename T>
  T operator()(const T& a) const { return ~a; }
};

struct LeftShift {
  template<typename T1, typename T2>
  T1 operator()(const T1& a, const T2& b) { return a << b; }
};

struct RightShift {
  template<typename T1, typename T2>
  T1 operator()(const T1& a, const T2& b) { return a >> b; }
};

struct Equal {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const { return a == b; }

  // Specializations to resolve signed-vs-unsigned.

  bool operator()(const int32_t& a, const uint32_t&b) const {
    return a >= 0 && static_cast<uint32_t>(a) == b;
  }
  bool operator()(const int32_t& a, const uint64_t&b) const {
    return a >= 0 && static_cast<uint32_t>(a) == b;
  }
  bool operator()(const int64_t& a, const uint32_t&b) const {
    return a >= 0 && static_cast<uint64_t>(a) == b;
  }
  bool operator()(const int64_t& a, const uint64_t&b) const {
    return a >= 0 && static_cast<uint64_t>(a) == b;
  }
  bool operator()(const uint32_t& a, const int32_t&b) const {
    return b >= 0 && a == static_cast<uint32_t>(b);
  }
  bool operator()(const uint32_t& a, const int64_t&b) const {
    return b >= 0 && a == static_cast<uint64_t>(b);
  }
  bool operator()(const uint64_t& a, const int32_t&b) const {
    return b >= 0 && a == static_cast<uint32_t>(b);
  }
  bool operator()(const uint64_t& a, const int64_t&b) const {
    return b >= 0 && a == static_cast<uint64_t>(b);
  }
};

struct NotEqual {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Equal equal;
    return !equal(a, b);
  }
};

struct Less {
  // We want to order DataType values based on their textual representation.
  // However, this would create an inconsistency (loss of transitivity) if we
  // could also compare (for ordering) DataType values with values of other
  // types, so we forbid those mixed type comparisons.
  bool operator()(const DataType& a, const DataType& b) const {
    return DataType_Name(a) < DataType_Name(b);
  }

  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    // Disabling mixed-type DataType vs other-type comparisons. See the comment
    // above.
    static_assert(!std::is_same<T1, DataType>::value &&
                  !std::is_same<T2, DataType>::value,
                  "Less for DataType and other type is disabled");
    return a < b;
  }

  // Specializations to resolve signed-vs-unsigned.

  bool operator()(const int32_t& a, const uint32_t&b) const {
    return a < 0 || static_cast<uint32_t>(a) < b;
  }
  bool operator()(const int32_t& a, const uint64_t&b) const {
    return a < 0 || static_cast<uint32_t>(a) < b;
  }
  bool operator()(const int64_t& a, const uint32_t&b) const {
    return a < 0 || static_cast<uint64_t>(a) < b;
  }
  bool operator()(const int64_t& a, const uint64_t&b) const {
    return a < 0 || static_cast<uint64_t>(a) < b;
  }
  bool operator()(const uint32_t& a, const int32_t&b) const {
    return b >= 0 && a < static_cast<uint32_t>(b);
  }
  bool operator()(const uint32_t& a, const int64_t&b) const {
    return b >= 0 && a < static_cast<uint32_t>(b);
  }
  bool operator()(const uint64_t& a, const int32_t&b) const {
    return b >= 0 && a < static_cast<uint32_t>(b);
  }
  bool operator()(const uint64_t& a, const int64_t&b) const {
    return b >= 0 && a < static_cast<uint64_t>(b);
  }
};

struct Greater {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Less less;
    return less(b, a);
  }
};

struct LessOrEqual {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Less less;
    return !less(b, a);
  }
};

struct GreaterOrEqual {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Less less;
    return !less(a, b);
  }
};

#include "hasher.h"

struct Hash {
  template<typename T>
  size_t operator()(const T& v) const {
    // Handle enums by conversion to the appropriate integral type.
    std::hash<typename std::conditional<
           std::is_enum<T>::value,
           typename std::conditional<
               sizeof(T) <= sizeof(int32_t),
               int32_t,
               int64_t>::type,
           T>::type> hasher;
    return hasher(v);
  }
  size_t operator()(const float& v) const {
    std::hash<int32_t> hasher;
    return hasher(*reinterpret_cast<const int32_t*>(&v));
  }
  size_t operator()(const double& v) const {
    std::hash<int64_t> hasher;
    return hasher(*reinterpret_cast<const int64_t*>(&v));
  }
  size_t operator()(const bool& v) const {
    // Arbitrary relative primes.
    return v ? 23 : 34;
  }
  size_t operator()(const StringPiece& v) const {
    return CityHash64(v.data(), v.size());
  }
};

}  // namespace operators

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_OPERATORS_H_
