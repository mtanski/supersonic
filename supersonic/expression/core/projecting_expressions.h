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
// Expressions describing operations on views - projecting to a subset of
// columns, joining several views, etc.

#ifndef SUPERSONIC_EXPRESSION_CORE_PROJECTING_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_PROJECTING_EXPRESSIONS_H_

#include <cstddef>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

// ----------------------- Input projections -----------------------------------
// Creates an expression that will perform an arbitrary projection on the
// input view, as specified by the projector argument. The ownership of the
// projector is transferred to the returned Expression.
class BufferAllocator;
class TupleSchema;

unique_ptr<const Expression> InputAttributeProjection(
    unique_ptr<const SingleSourceProjector> projector);

// Creates an expression that will return a value of the specified attribute
// in the input view.
// Shortcut for InputAttributeProjection(ProjectNamedAttribute(name)).
inline unique_ptr<const Expression> NamedAttribute(const string& name) {
  return InputAttributeProjection(ProjectNamedAttribute(name));
}

// Creates an expression that will return a value of the attribute at the
// specified position in the input view.
// Shortcut for InputAttributeProjection(ProjectAttributeAt(position)).
inline unique_ptr<const Expression> AttributeAt(const size_t position) {
  return InputAttributeProjection(ProjectAttributeAt(position));
}

// ---------------------- Child expression projections -------------------------
// Takes a single-column-output argument and renames it (without changing the
// data, type or nullability).
// Will return a Failure at binding time if argument has more columns in the
// output than one.
unique_ptr<const Expression> Alias(const string& new_name,
                        unique_ptr<const Expression> const argument);

// Creates an expression that evaluates multiple sources (sub-expressions)
// and projects them into a single (possibly multi-attribute) result.
unique_ptr<const Expression> Projection(
    unique_ptr<const ExpressionList> sources,
    unique_ptr<const MultiSourceProjector> projector);

// Concatenates all sources into a single expression. The sources must have
// non-conflicting schemas.
inline unique_ptr<const Expression> Flat(unique_ptr<const ExpressionList> inputs) {
  auto projector = make_unique<CompoundMultiSourceProjector>();
  for (int i = 0; i < inputs->size(); ++i) {
    projector->add(i, ProjectAllAttributes());
  }
  return Projection(std::move(inputs), std::move(projector));
}

// A convenience class to assemble expressions with multi-attribute results
// out of individual children expressions. Useful as an input to the Compute
// operation (which expects a single expression).
// In comparison to 'Flat' above, gives the user an opportunity to define
// aliases, in order to avoid naming clashes in the schema.
class CompoundExpression : public Expression {
 public:
  // Creates an empty compound expression.
  CompoundExpression()
      : arguments_(new ExpressionList),
        projector_(new CompoundMultiSourceProjector) {}
  virtual ~CompoundExpression() {}

  // Adds a single expression to this compound expression. Copies attribute
  // names from the argument. Returns 'this' for easy chaining.
  CompoundExpression* Add(unique_ptr<const Expression> argument);

  // Adds a single, one-attribute expression to this compound expression.
  // The 'alias' parameter specifies a name of the resulting attribute.
  // If, later during bind, the argument turns out to have attribute count
  // different than 1, a bind error occurs. Returns 'this' for easy chaining.
  CompoundExpression* AddAs(const StringPiece& alias,
                            unique_ptr<const Expression> argument);

  // Adds a single, multi-attribute expression to this compound expression.
  // The 'alias' vector specifies names of the resulting attributes.
  // If, later during bind, the argument turns out to have attribute count
  // different than the number of aliases specified here, a bind error occurs.
  // Returns 'this' for easy chaining.
  CompoundExpression* AddAsMulti(const vector<string>& aliases,
                                 unique_ptr<const Expression> argument);

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const;

  virtual string ToString(bool verbose) const {
    if (verbose) {
      return StrCat(
          projector_->ToString(verbose), ": ", arguments_->ToString(verbose));
    }
    return arguments_->ToString(verbose);
  }

 private:
  std::unique_ptr<ExpressionList> arguments_;
  std::unique_ptr<CompoundMultiSourceProjector> projector_;

  DISALLOW_COPY_AND_ASSIGN(CompoundExpression);
};

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_PROJECTING_EXPRESSIONS_H_
