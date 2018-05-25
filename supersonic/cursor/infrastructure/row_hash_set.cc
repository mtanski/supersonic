// Copyright 2010 Google Inc. All Rights Reserved.
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

#include "supersonic/cursor/infrastructure/row_hash_set.h"

#include "supersonic/utils/std_namespace.h"

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/bits.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

namespace row_hash_set {

// This is row-oriented version of row_hash_set. It has depending on test case
// 20% - 200% better performance compared to row_hash_set with hashtable. It is
// optimized to work fast with huge amount of output rows. It replaced other
// row-oriented version
// The previous column-oriented code is available in older revisions of source
// files in Perforce:

#define CPP_TYPE(type) typename TypeTraits<type>::cpp_type

// Abstract interface of classes that compare particular values (table cells)
// in corresponding columns for equality.
struct ValueComparatorInterface {
  virtual ~ValueComparatorInterface() {}
  virtual bool Equal(rowid_t row_id_a, rowid_t row_id_b) const = 0;
  virtual void set_left_column(const Column* left_column) = 0;
  virtual void set_right_column(const Column* right_column) = 0;
  virtual bool non_colliding_hash_type() = 0;
  virtual bool any_column_nullable() = 0;
};

// A concrete implementation for an arbitrary data type. Columns have to be set
// before Equal call.
template <DataType type>
class ValueComparator : public ValueComparatorInterface {
 public:
  ValueComparator()
      : any_column_nullable_(false),
        left_column_(NULL),
        right_column_(NULL) {}

  bool Equal(rowid_t row_id_a, rowid_t row_id_b) const {
    if (any_column_nullable_) {
      const bool is_null_a = (left_column_->is_null() != NULL) &&
          left_column_->is_null()[row_id_a];
      const bool is_null_b = (right_column_->is_null() != NULL) &&
          right_column_->is_null()[row_id_b];
      if (is_null_a || is_null_b) {
        return is_null_a == is_null_b;
      }
    }
    return comparator_((left_column_->typed_data<type>() + row_id_a),
                       (right_column_->typed_data<type>() + row_id_b));
  }

  void set_left_column(const Column* left_column) {
    left_column_ = left_column;
    update_any_column_nullable();
  }

  void set_right_column(const Column* right_column) {
    right_column_ = right_column;
    update_any_column_nullable();
  }

  bool non_colliding_hash_type() {
    return type == INT64 || type == INT32 || type == UINT64 || type == UINT32
        || type == BOOL;
  }

  bool any_column_nullable() {
    return any_column_nullable_;
  }

 private:
  void update_any_column_nullable() {
    if (left_column_ != NULL && right_column_ != NULL) {
      any_column_nullable_ = (left_column_->is_null() != NULL ||
                              right_column_->is_null() != NULL);
    }
  }
  EqualityWithNullsComparator<type, type, false, false> comparator_;
  bool any_column_nullable_;
  const Column* left_column_;
  const Column* right_column_;
};

// Helper struct used by CreateValueComparator.
struct ValueComparatorFactory {
  template <DataType type>
  unique_ptr<ValueComparatorInterface> operator()() const {
    return make_unique<ValueComparator<type>>();
  }
};

// Instantiates a particular specialization of ValueComparator<type>.
unique_ptr<ValueComparatorInterface> CreateValueComparator(DataType type) {
  return TypeSpecialization<unique_ptr<ValueComparatorInterface>,
                            ValueComparatorFactory>(type);
}

// A compound row equality comparator, implemented with a vector of individual
// value comparators. Views have to be set befor equal call.
class RowComparator {
 public:
  ~RowComparator() = default;

  explicit RowComparator(const TupleSchema& key_schema) :
    left_view_(NULL),
    right_view_(NULL),
    hash_comparison_only_(false) {
    for (int i = 0; i < key_schema.attribute_count(); i++) {
      comparators_.emplace_back(
          CreateValueComparator(key_schema.attribute(i).type()));
    }
    one_column_with_non_colliding_hash_ =
        (comparators_.size() == 1 &&
         comparators_[0]->non_colliding_hash_type());
  }

  // Function equal assumes that hashes of two rows are equal. It performs
  // comparison on every key-column of two given rows.
  bool Equal(int left_pos, int right_pos) const {
    for (const auto& comparator: comparators_) {
      if (!comparator->Equal(left_pos, right_pos))
        return false;
    }
    return true;
  }

  void set_left_view(const View* left_view) {
    left_view_ = left_view;
    for (int i = 0; i < comparators_.size(); ++i) {
      comparators_[i]->set_left_column(&left_view_->column(i));
    }
    hash_comparison_only_ = one_column_with_non_colliding_hash_ &&
        !comparators_[0]->any_column_nullable();
  }

  void set_right_view(const View* right_view) {
    right_view_ = right_view;
    for (int i = 0; i < comparators_.size(); ++i) {
      comparators_[i]->set_right_column(&right_view_->column(i));
    }
    hash_comparison_only_ = one_column_with_non_colliding_hash_ &&
        !comparators_[0]->any_column_nullable();
  }

  // It is true iff there is only one key-column, which has non-colliding hash
  // function. In such case it is enough to compare hashes and not to compare
  // key-values.
  bool hash_comparison_only() {
    return hash_comparison_only_;
  }

 private:
  vector<unique_ptr<ValueComparatorInterface>> comparators_;
  // Pointers to compared views. RowComparator doesn't take its ownership.
  const View* left_view_;
  const View* right_view_;
  bool one_column_with_non_colliding_hash_;
  bool hash_comparison_only_;
};


// A helper function that creates a key selector that selects all attributes
// from schema.
unique_ptr<const BoundSingleSourceProjector> CreateAllAttributeSelector(
    const TupleSchema& schema) {
  std::unique_ptr<const SingleSourceProjector> projector(
      ProjectAllAttributes());
  return SucceedOrDie(projector->Bind(schema));
}

// Project everything if no selector is specified
static
unique_ptr<const BoundSingleSourceProjector>
key_selector_or_default(unique_ptr<const BoundSingleSourceProjector> key_selector, const TupleSchema& schema) {
  if ((bool) key_selector) {
    return key_selector;
  }
  return CreateAllAttributeSelector(schema);
}

// For multiset. See equal_row_groups_.
struct EqualRowGroup {
  rowid_t first;
  rowid_t last;
};

// See equal_row_ids_.
struct EqualRowIdsLink {
  rowid_t group_id;
  rowid_t next;
};

class RowHashSetBase {

 protected:
  // Computes the column of hashes for the 'key' view, given the row count.
  // TODO(user): perhaps make the row_count part of the view?
  static void HashQuery(const View& key, rowcount_t row_count, size_t* hash);
};

// The actual row hash set implementation.
// TODO(user): replace vectors and scoped_arrays with Tables, to close the
// loop on memory management.
class RowHashSetImpl final: public RowHashSetBase {
 public:
  RowHashSetImpl(
      const TupleSchema& block_schema,
      BufferAllocator* const allocator,
      unique_ptr<const BoundSingleSourceProjector> key_selector,
      bool is_multiset,
      const int64_t max_unique_keys_in_result);

  bool ReserveRowCapacity(rowcount_t block_capacity);

  // RowSet variant.
  void FindUnique(
      const View& query, const bool_const_ptr selection_vector,
      FindResult* result) const;

  // RowMultiSet variant.
  void FindMany(
      const View& query, const bool_const_ptr selection_vector,
      FindMultiResult* result) const;

  // RowSet variant.
  size_t InsertUnique(
      const View& query, const bool_const_ptr selection_vector,
      FindResult* result);

  // RowMultiSet variant.
  size_t InsertMany(
      const View& query, const bool_const_ptr selection_vector,
      FindMultiResult* result);

  void Clear();

  void Compact();

  const View& indexed_view() const { return index_.view(); }

 private:
  void FindInternal(
      const View& query, const bool_const_ptr selection_vector,
      rowid_t* result_row_ids) const;

  // Selects key columns from index_ and from queries to Insert.
  std::unique_ptr<const BoundSingleSourceProjector> key_selector_;

  // Contains all inserted rows; Find and Insert match against these rows
  // using last_row_id_ and prev_row_id_.
  Table index_;

  TableRowAppender<DirectRowSourceReader<ViewRowIterator> > index_appender_;

  // View over the index's key columns. Contains keys of all the rows inserted
  // into the index.
  View index_key_;

  // A fixed-size vector of links used to group all Rows with the same key into
  // a linked list. Used only by FindMany / InsertMany which implement
  // RowHashMultiSet functionality. In a group of Rows with identical key, only
  // the first one is inserted in the hashtable; all other Rows are linked from
  // the first one using the links below.
  vector<EqualRowIdsLink> equal_row_ids_;

  // Identifies groups of rows with the same key. Within the group, the items
  // form a linked list described by equal_row_ids_.
  vector<EqualRowGroup> equal_row_groups_;

  // View over key columns of a query to insert.
  View query_key_;

  //  Array for keeping block rows' hashes.
  vector<size_t> hash_;

  // Size of last_row_id_. Should be power of 2. Adjusted by ReserveRowCapacity.
  int last_row_id_size_;

  // Value of last_row_id_[hash_index] denotes position of last row in
  // indexed_block_ having same value of hash_index =
  // (query_hash_[query_row_id] & hash_mask_).
  std::unique_ptr<int[]> last_row_id_;

  // Index of a previous row having the same hash_index = (row.hash &
  // hash_mask_) but a different key. (For multisets, there may be many rows
  // with the same key (and thus hash); only the first instance is actually
  // written to this array).
  std::unique_ptr<int[]> prev_row_id_;

  // Structure used for comparing rows.
  mutable RowComparator comparator_;

  // Placeholder for hash values calculated in one go over entire query to
  // find/insert.
  mutable size_t query_hash_[Cursor::kDefaultRowCount];

  // Bit mask used for calculating last_row_id_ index.
  int hash_mask_;

  const bool is_multiset_;

  const int64_t max_unique_keys_in_result_;

  friend class RowIdSetIterator;
};

class RowHashSetUniqueImpl final : public RowHashSetBase {
 public:
  RowHashSetUniqueImpl(
      const TupleSchema& block_schema,
      BufferAllocator* const allocator,
      unique_ptr<const BoundSingleSourceProjector> key_selector,
      bool is_multiset,
      const int64_t max_unique_keys_in_result)
      : key_selector_(key_selector_or_default(std::move(key_selector), block_schema)),
        index_(block_schema, allocator),
        index_appender_(&index_, true),
        index_key_(key_selector_->result_schema()),
        query_key_(key_selector_->result_schema()),
        comparator_(query_key_.schema()),
        max_unique_keys_in_result_(max_unique_keys_in_result) { }

  void FindUnique(
      const View& query, const bool_const_ptr selection_vector,
      FindResult* result) const;

  // RowSet variant.
  size_t InsertUnique(
      const View& query, const bool_const_ptr selection_vector,
      FindResult* result);

  bool ReserveRowCapacity(rowcount_t block_capacity);

  void Clear();

  void Compact();

  const View& indexed_view() const { return index_.view(); }

 private:
  // Load factor is scaled to 0 -> INT_MAX
  constexpr static int MaxLoadFactor = (int) (0.75f * INT_MAX);

  struct HashTable {
    constexpr static int MinSize = 16;
    constexpr static int SearchDistance = 12;

    int factor_ = 0;

    // Bucket count (plus overhang for linear prob at end)
    int num_buckets_ = 0;

    // Bit mask used for calculating bucket count index.
    int mask_ = 0;

    int values_ = 0;

    unique_ptr<size_t[]> buckets_;
    unique_ptr<int[]> offsets_;

    HashTable(rowcount_t buckets = MinSize) {
      DCHECK(buckets == 0 || Bits::IsPow2(buckets)) << "Not a power of 2";

      if (buckets == 0) {
        return;
      }

      factor_ = buckets;
      num_buckets_ = buckets + SearchDistance;
      mask_ = factor_ - 1;

      buckets_ = make_unique<size_t[]>(num_buckets_);
      offsets_ = make_unique<int[]>(num_buckets_);

      std::fill(offsets_.get(), offsets_.get() + num_buckets_, -1);
    }
  };

 private:
  inline bool ResizeHash(rowcount_t rowcount);
  inline int InsertHash(HashTable& where, size_t hash, int pos);

  // Selects key columns from index_ and from queries to Insert.
  std::unique_ptr<const BoundSingleSourceProjector> key_selector_;

  // Contains all inserted rows; Find and Insert match against these rows
  // using last_row_id_ and prev_row_id_.
  Table index_;

  TableRowAppender<DirectRowSourceReader<ViewRowIterator> > index_appender_;

  // View over the index's key columns. Contains keys of all the rows inserted
  // into the index.
  View index_key_;

  // View over key columns of a query to insert.
  View query_key_;

  HashTable hash_table_;

  // Structure used for comparing rows.
  mutable RowComparator comparator_;

  // Placeholder for hash values calculated in one go over entire query to
  // find/insert.
  mutable size_t query_hash_[Cursor::kDefaultRowCount];

  const int64_t max_unique_keys_in_result_;

  friend class RowIdSetIterator;
};

constexpr int RowHashSetUniqueImpl::HashTable::MinSize;


RowHashSetImpl::RowHashSetImpl(
    const TupleSchema& block_schema,
    BufferAllocator* const allocator,
    unique_ptr<const BoundSingleSourceProjector> key_selector,
    bool is_multiset,
    const int64_t max_unique_keys_in_result)
    : key_selector_(key_selector_or_default(std::move(key_selector), block_schema)),
      index_(block_schema, allocator),
      index_appender_(&index_, true),
      index_key_(key_selector_->result_schema()),
      query_key_(key_selector_->result_schema()),
      last_row_id_size_(0),
      comparator_(query_key_.schema()),
      hash_mask_(0),
      is_multiset_(is_multiset),
      max_unique_keys_in_result_(max_unique_keys_in_result) {
  // FindUnique and FindMany compute hash index % hash_mask_, which results
  // in a non-negative integer, used to index the indirection tables.
  // To make sure that the newly created instance adheres to the implicit
  // invariants, we need to pre-allocate and fill certain dummy values.
  last_row_id_ = make_unique<int[]>(1);
  last_row_id_[0] = -1;
  prev_row_id_ = make_unique<int[]>(1);
}

bool RowHashSetImpl::ReserveRowCapacity(rowcount_t row_count) {
  if (index_.row_capacity() >= row_count) return true;
  if (!index_.ReserveRowCapacity(row_count)) return false;
  key_selector_->Project(index_.view(), &index_key_);
  comparator_.set_right_view(&index_key_);
  hash_.reserve(index_.row_capacity());
  if (is_multiset_) equal_row_ids_.resize(index_.row_capacity());

  // Arbitrary load factor of 75%. (Same is used in Java HashMap).
  while (last_row_id_size_ * 3 < index_.row_capacity() * 4) {
    if (last_row_id_size_ == 0) {
      last_row_id_size_ = 16;
    } else {
      last_row_id_size_ *= 2;
    }
  }
  last_row_id_ = make_unique<int[]>(last_row_id_size_);
  std::fill(last_row_id_.get(), last_row_id_.get() + last_row_id_size_, -1);
  hash_mask_ = last_row_id_size_ - 1;

  prev_row_id_ = make_unique<int[]>(last_row_id_size_);

  // Rebuild the linked lists that depend on hash_index (which changed, as
  // it depends on hash_mask_, that changed).
  if (is_multiset_) {
    for (auto& equal_row_group: equal_row_groups_) {
      rowid_t first = equal_row_group.first;
      int hash_index = (hash_mask_ & hash_[first]);
      prev_row_id_[first] = last_row_id_[hash_index];
      last_row_id_[hash_index] = first;
    }
  } else {
    for (rowid_t i = 0; i < index_.view().row_count(); ++i) {
      int hash_index = (hash_mask_ & hash_[i]);
      prev_row_id_[i] = last_row_id_[hash_index];
      last_row_id_[hash_index] = i;
    }
  }
  return true;
}

void RowHashSetImpl::FindUnique(
    const View& query, const bool_const_ptr selection_vector,
    FindResult* result) const {
  FindInternal(query, selection_vector, result->mutable_row_ids());
}

void RowHashSetImpl::FindMany(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) const {
  result->set_equal_row_ids((equal_row_ids_.empty()) ? NULL :
                            &equal_row_ids_.front());
  // For each query row, the matching row (if any) is also the head of a group
  // of possibly many Rows.
  FindInternal(query, selection_vector, result->mutable_row_ids());
}

void RowHashSetImpl::FindInternal(
    const View& query, const bool_const_ptr selection_vector,
    rowid_t* result_row_ids) const {
  DCHECK(query.schema().EqualByType(key_selector_->result_schema()));
  CHECK_GE(arraysize(query_hash_), query.row_count());
  comparator_.set_left_view(&query);

  HashQuery(query, query.row_count(), query_hash_);
  ViewRowIterator iterator(query);
  while (iterator.next()) {
    const rowid_t query_row_id = iterator.current_row_index();
    rowid_t* const result_row_id = result_row_ids + query_row_id;
    if (selection_vector != NULL && !selection_vector[query_row_id]) {
      *result_row_id = kInvalidRowId;
    } else {
      int hash_index = (hash_mask_ & query_hash_[query_row_id]);
      int index_row_id = last_row_id_[hash_index];
      if (comparator_.hash_comparison_only()) {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id])) {
          index_row_id = prev_row_id_[index_row_id];
        }
      } else {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id]
                || !comparator_.Equal(query_row_id, index_row_id))) {
          index_row_id = prev_row_id_[index_row_id];
        }
      }
      *result_row_id = (index_row_id != -1) ? index_row_id : kInvalidRowId;
    }
  }
}

size_t RowHashSetImpl::InsertUnique(
    const View& query, const bool_const_ptr selection_vector,
    FindResult* result) {
  DCHECK(query.schema().EqualByType(index_.schema()))
      << "Expected: " << index_.schema().GetHumanReadableSpecification()
      << ", is: " << query.schema().GetHumanReadableSpecification();
  // Best-effort; if fails, we may end up adding less rows later.
  ReserveRowCapacity(index_.row_count() + query.row_count());
  CHECK_GE(arraysize(query_hash_), query.row_count());

  key_selector_->Project(query, &query_key_);
  HashQuery(query_key_, query.row_count(), query_hash_);
  comparator_.set_left_view(&query_key_);

  ViewRowIterator iterator(query);
  while (iterator.next()) {
    const int64_t query_row_id = iterator.current_row_index();
    rowid_t* const result_row_id =
        result ? (result->mutable_row_ids() + query_row_id) : NULL;
    if (selection_vector != NULL && !selection_vector[query_row_id]) {
      if (result_row_id)
          *result_row_id = kInvalidRowId;
    } else {
      // TODO(user): The following 12 lines are identical to the piece
      // of code in FindInternal. Needs refactoring (but be careful about a
      // performance regression).
      int hash_index = (hash_mask_ & query_hash_[query_row_id]);
      int index_row_id = last_row_id_[hash_index];

      if (comparator_.hash_comparison_only()) {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id])) {
          index_row_id = prev_row_id_[index_row_id];
        }
      } else {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id]
                || !comparator_.Equal(query_row_id, index_row_id))) {
          index_row_id = prev_row_id_[index_row_id];
        }
      }

      if (index_row_id == -1) {
        if (index_.row_count() <= max_unique_keys_in_result_) {
          index_row_id = index_.row_count();
          if (index_row_id  == index_.row_capacity() ||
              !index_appender_.AppendRow(iterator)) break;
          hash_.push_back(query_hash_[query_row_id]);
          prev_row_id_[index_row_id] = last_row_id_[hash_index];
          last_row_id_[hash_index] = index_row_id;
        } else {
          index_row_id = index_.row_count() - 1;
        }
      }
      if (result_row_id)
        *result_row_id = index_row_id;
    }
  }

  return iterator.current_row_index();
}

size_t RowHashSetImpl::InsertMany(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) {
  DCHECK(query.schema().EqualByType(index_.schema()))
      << "Expected: " << index_.schema().GetHumanReadableSpecification()
      << ", is: " << query.schema().GetHumanReadableSpecification();
  // Best-effort; if fails, we may end up adding less rows later.
  ReserveRowCapacity(index_.row_count() + query.row_count());
  CHECK_GE(arraysize(query_hash_), query.row_count());

  key_selector_->Project(query, &query_key_);
  // We create the hashes for all the rows in the query, we store them in
  // query_hash_.
  HashQuery(query_key_, query.row_count(), query_hash_);
  comparator_.set_left_view(&index_key_);

  if (result)
    result->set_equal_row_ids(&equal_row_ids_.front());
  rowid_t insert_row_id = index_.row_count();
  ViewRowIterator iterator(query);
  while (iterator.next()) {
    const rowid_t query_row_id = iterator.current_row_index();
    rowid_t* const result_row_id =
        (result != NULL) ? (result->mutable_row_ids() + query_row_id) : NULL;
    if (selection_vector != NULL && !selection_vector[query_row_id]) {
      if (result_row_id)
        *result_row_id = kInvalidRowId;
    } else {
      // Copy query row into the index.
      if (query_row_id  == index_.row_capacity() ||
          !index_appender_.AppendRow(iterator)) break;
      hash_.push_back(query_hash_[query_row_id]);
      int hash_index = (hash_mask_ & query_hash_[query_row_id]);
      int index_row_id = last_row_id_[hash_index];

      // We iterate over all rows with the same hash_index (which is a
      // truncation of the hash), looking for a row with the same hash as
      // our row.
      if (comparator_.hash_comparison_only()) {
        // This is the case where hash-comparison is equivalent to a full
        // comparison.
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id])) {
          index_row_id = prev_row_id_[index_row_id];
        }
      } else {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id]
                || !comparator_.Equal(insert_row_id, index_row_id))) {
          index_row_id = prev_row_id_[index_row_id];
        }
      }

      // If we found no row with an equal hash, we start a new group at the end
      // of the list of rows with the same hash_index.
      if (index_row_id == -1) {
        index_row_id = insert_row_id;
        prev_row_id_[index_row_id] = last_row_id_[hash_index];
        last_row_id_[hash_index] = index_row_id;
      }

      // Head of the linked list grouping Rows with the same key as query row.
      EqualRowIdsLink& equal_row_ids = equal_row_ids_[index_row_id];
      if (insert_row_id == index_row_id) {
        // First Row with such key in the index; create a single-element list.
        equal_row_ids.group_id = equal_row_groups_.size();
        equal_row_ids.next = kInvalidRowId;
        equal_row_groups_.push_back(EqualRowGroup());
        EqualRowGroup& equal_row_group = equal_row_groups_.back();
        equal_row_group.first = insert_row_id;
        equal_row_group.last = insert_row_id;
      } else {
        // Another Row with the same key; append to the linked list.
        EqualRowGroup& equal_row_group =
            equal_row_groups_[equal_row_ids.group_id];
        equal_row_ids_[equal_row_group.last].next = insert_row_id;
        equal_row_group.last = insert_row_id;
        equal_row_ids_[insert_row_id].group_id = equal_row_ids.group_id;
        equal_row_ids_[insert_row_id].next = kInvalidRowId;
      }

      if (result_row_id)
        *result_row_id = index_row_id;

      insert_row_id++;
    }
  }
  return iterator.current_row_index();
}

void RowHashSetImpl::Clear() {
  // Be more aggresive in freeing memory. Otherwise clients like
  // BestEffortGroupAggregate may end up with memory_limit->Available() == 0
  // after clearing RowHashSet.
  index_.move_block();
  key_selector_->Project(index_.view(), &index_key_);
  hash_.clear();
  std::fill(last_row_id_.get(), last_row_id_.get() + last_row_id_size_, -1);
  equal_row_groups_.clear();
}

// TODO(user): More internal datastructures could be compacted (last_row_id_ and
// prev_row_id_), but it would require recomputing their content.
void RowHashSetImpl::Compact() {
  index_.Compact();
  // Using the swap trick to trim excess vector capacity.
  vector<size_t>(hash_).swap(hash_);
  vector<EqualRowGroup>(equal_row_groups_).swap(equal_row_groups_);
}

void RowHashSetBase::HashQuery(
    const View& key_columns, rowcount_t row_count, size_t* hash) {
  const TupleSchema& key_schema = key_columns.schema();
  // TODO(user): this should go into the constructor.
  // If somebody uses hash-join to do a cross-join (that is no columns are
  // given as key) we have to fill out the hash table with any single hash,
  // say zero.
  if (key_schema.attribute_count() == 0) {
    memset(hash, '\0', row_count * sizeof(*hash));
  }
  // In the other case the first ColumnHasher will initialize the data for
  // us.
  for (int c = 0; c < key_schema.attribute_count(); ++c) {
    ColumnHasher column_hasher =
        GetColumnHasher(key_schema.attribute(c).type(), c != 0, false);
    const Column& key_column = key_columns.column(c);
    column_hasher(key_column.data(), key_column.is_null(), row_count, hash);
  }
}

void RowHashSetUniqueImpl::Clear() {
  // Be more aggresive in freeing memory. Otherwise clients like
  // BestEffortGroupAggregate may end up with memory_limit->Available() == 0
  // after clearing RowHashSet.
  index_.move_block();
  key_selector_->Project(index_.view(), &index_key_);
  hash_table_ = HashTable(0);
}

void RowHashSetUniqueImpl::Compact() {
  index_.Compact();
}

bool RowHashSetUniqueImpl::ReserveRowCapacity(rowcount_t row_count) {
  // Resize output hashset (if needed)
  if (index_.row_capacity() < row_count) {
    if (!index_.ReserveRowCapacity(row_count)) {
      return false;
    }

    // Reset state after changing (reallocating) index_
    key_selector_->Project(index_.view(), &index_key_);
    comparator_.set_right_view(&index_key_);
  }

  int spill_at = (hash_table_.factor_ * 3) / 4;

  // TODO: Should we resize hash table (pass fill factor)
  if (hash_table_.factor_ == 0 || hash_table_.values_ > spill_at) {
    auto at_least = std::max(HashTable::MinSize, hash_table_.factor_ * 2);
    return ResizeHash(at_least);
  }

  return true;
}

void RowHashSetUniqueImpl::FindUnique(
    const View& query, const bool_const_ptr selection_vector,
    FindResult* result) const {

  DCHECK(query.schema().EqualByType(key_selector_->result_schema()));
  CHECK_GE(arraysize(query_hash_), query.row_count());
  comparator_.set_left_view(&query);

  HashQuery(query, query.row_count(), query_hash_);
  ViewRowIterator iterator(query);

  while (iterator.next()) {
    const rowid_t query_row_id = iterator.current_row_index();
    const auto row_hash = query_hash_[query_row_id];

    auto result_row_id = result->mutable_row_ids() + query_row_id;

    if (selection_vector != NULL && !selection_vector[query_row_id]) {
      *result_row_id = kInvalidRowId;
      continue;
    }

    int hash_index = (hash_table_.mask_ & query_hash_[query_row_id]);

    // Default to Not found
    *result_row_id = kInvalidRowId;

    if (comparator_.hash_comparison_only()) {
      for (int probe_off = 0; probe_off < HashTable::SearchDistance;
           probe_off += 1)
      {
        auto hash_off = hash_index + probe_off;
        auto idx_off = hash_table_.offsets_[hash_off];

        // Empty bucket / less then maximum limit (we can insert here)
        if (idx_off == -1) break;

        // FOUND MATCH
        if (hash_table_.buckets_[hash_off] == row_hash) {
          *result_row_id = idx_off;
          break;
        }
      }
    } else {
      for (int probe_off = 0; probe_off < HashTable::SearchDistance;
           probe_off += 1)
      {
        auto hash_off = hash_index + probe_off;
        auto idx_off = hash_table_.offsets_[hash_off];

        // Empty bucket / less then maximum limit (we can insert here)
        if (idx_off == -1) break;

        // FOUND MATCH
        if (hash_table_.buckets_[hash_off] == row_hash
            && comparator_.Equal(query_row_id, idx_off))
        {
          *result_row_id = idx_off;
          break;
        }
      }
    }
  }
}

int RowHashSetUniqueImpl::InsertHash(RowHashSetUniqueImpl::HashTable& where,
                                     size_t hash, int pos)
{
  auto v = where.mask_ & hash;

  for (int i = v; (i - v) < HashTable::SearchDistance; i += 1) {
    if (where.offsets_[i] == -1) {
      where.buckets_[i] = hash;
      where.offsets_[i] = pos;
      return i;
    }
  }

  return -1;
}

bool RowHashSetUniqueImpl::ResizeHash(rowcount_t rowcount) {
  // TODO: Resize based estimated spill

  if (rowcount < HashTable::MinSize) rowcount = HashTable::MinSize;
  // IGNORE resizes down
  if (rowcount < hash_table_.factor_) return true;

  bool copy_old = (hash_table_.num_buckets_ > 0);

  if (!copy_old) {
    hash_table_ = HashTable(rowcount);
  }

  while (true) {
    HashTable output(rowcount);

    auto old_buckets = hash_table_.buckets_.get();
    auto old_offsets = hash_table_.offsets_.get();

    for (int i = 0; i < hash_table_.num_buckets_; i += 1) {
      if (old_offsets[i] == -1) continue;

      if (InsertHash(output, old_buckets[i], old_offsets[i]) == -1) {
        // TODO: Use different seed
        // TODO: Log / warn
        // Resize because we exhausted our probe for free spot
        rowcount *= 2;
        continue;
      }
    }

    break;
  }

  return true;
}

size_t RowHashSetUniqueImpl::InsertUnique(
    const View& query, const bool_const_ptr selection_vector,
    FindResult* result)
{
  DCHECK(query.schema().EqualByType(index_.schema()))
    << "Expected: " << index_.schema().GetHumanReadableSpecification()
    << ", is: " << query.schema().GetHumanReadableSpecification();

  // Best-effort; if fails, we may end up adding less rows later.
  ReserveRowCapacity(index_.row_count() + query.row_count());
  CHECK_GE(arraysize(query_hash_), query.row_count());

  key_selector_->Project(query, &query_key_);
  HashQuery(query_key_, query.row_count(), query_hash_);
  comparator_.set_left_view(&query_key_);

  ViewRowIterator iterator(query);

  while (iterator.next()) {
  retry_row:

    const int64_t query_row_id = iterator.current_row_index();
    const auto row_hash = query_hash_[query_row_id];

    rowid_t* const result_row_id =
        result ? (result->mutable_row_ids() + query_row_id) : NULL;

    if (selection_vector != NULL && !selection_vector[query_row_id]) {
      if (result_row_id) {
        *result_row_id = kInvalidRowId;
      }
      continue;
    }

    auto set_output_row = [&](int idx_off) {
      if (result_row_id) {
        *result_row_id = idx_off;
      }
    };

    int hash_index = (hash_table_.mask_ & query_hash_[query_row_id]);
    int probe_off = 0;

    if (comparator_.hash_comparison_only()) {
      for (; probe_off < HashTable::SearchDistance; probe_off += 1) {
        auto hash_off = hash_index + probe_off;
        auto idx_off = hash_table_.offsets_[hash_off];

        // Empty bucket / less then maximum limit (we can insert here)
        if (idx_off == -1) break;

        // FOUND MATCH
        if (hash_table_.buckets_[hash_off] == row_hash) {
          set_output_row(idx_off);
          goto next;
        }
      }
    } else {
      for (; probe_off < HashTable::SearchDistance; probe_off += 1) {
        auto hash_off = hash_index + probe_off;
        auto idx_off = hash_table_.offsets_[hash_off];

        // Empty bucket / less then maximum limit (we can insert here)
        if (idx_off == -1) break;

        // FOUND MATCH
        if (hash_table_.buckets_[hash_off] == row_hash
            && comparator_.Equal(query_row_id, idx_off))
        {
          set_output_row(idx_off);
          goto next;
        }
      }
    }

    // LIMIT, maximum number of groupings reached
    if (index_.row_count() > max_unique_keys_in_result_) {
      set_output_row(index_.row_count() - 1);
      continue;
    }

    // RUN TOO LONG, resize and retry this row
    if (probe_off > HashTable::SearchDistance) {
      ResizeHash(hash_table_.factor_ * 2);
      goto retry_row;
    }

    // FOUND SLOT
    {
      auto hash_off = hash_index + probe_off;
      auto idx_off = index_.row_count();
      hash_table_.buckets_[hash_off] = row_hash;
      hash_table_.offsets_[hash_off] = idx_off;

      // EOM when adding new output row
      if (!index_appender_.AppendRow(iterator)) {
        // Return partially done
        break;
      }

      set_output_row(idx_off);
      hash_table_.values_ += 1;
    }

    next:
      ;
  }

  return iterator.current_row_index();
}

void RowIdSetIterator::Next() {
  current_ = equal_row_ids_[current_].next;
}

RowHashSet::RowHashSet(const TupleSchema& block_schema,
                       BufferAllocator* const allocator)
    : impl_(new RowHashSetUniqueImpl(block_schema, allocator, NULL, false,
                               INT64_MAX)) {}

RowHashSet::RowHashSet(
    const TupleSchema& block_schema,
    BufferAllocator* const allocator,
    unique_ptr<const BoundSingleSourceProjector> key_selector)
    : impl_(new RowHashSetUniqueImpl(block_schema, allocator, std::move(key_selector), false,
                               INT64_MAX)) {}

RowHashSet::RowHashSet(const TupleSchema &block_schema,
                       BufferAllocator *const allocator,
                       const int64_t max_unique_keys_in_result)
    : impl_(new RowHashSetUniqueImpl(block_schema, allocator, NULL, false,
                               max_unique_keys_in_result)) {}

RowHashSet::RowHashSet(
    const TupleSchema &block_schema, BufferAllocator *const allocator,
    unique_ptr<const BoundSingleSourceProjector> key_selector,
    const int64_t max_unique_keys_in_result)
    : impl_(new RowHashSetUniqueImpl(block_schema, allocator, std::move(key_selector),
                               false, max_unique_keys_in_result)) {}

RowHashSet::~RowHashSet() {
  delete impl_;
}

bool RowHashSet::ReserveRowCapacity(rowcount_t capacity) {
  return impl_->ReserveRowCapacity(capacity);
}

void RowHashSet::Find(const View& query, FindResult* result) const {
  impl_->FindUnique(query, bool_ptr(NULL), result);
}

void RowHashSet::Find(const View& query, const bool_const_ptr selection_vector,
                  FindResult* result) const {
  impl_->FindUnique(query, selection_vector, result);
}

size_t RowHashSet::Insert(const View& query, FindResult* result) {
  return impl_->InsertUnique(query, bool_ptr(NULL), result);
}

size_t RowHashSet::Insert(const View& query,
                          const bool_const_ptr selection_vector,
                          FindResult* result) {
  return impl_->InsertUnique(query, selection_vector, result);
}

size_t RowHashSet::Insert(const View& query) {
  return impl_->InsertUnique(query, bool_ptr(NULL), NULL);
}

size_t RowHashSet::Insert(const View& query,
                          const bool_const_ptr selection_vector) {
  return impl_->InsertUnique(query, selection_vector, NULL);
}

void RowHashSet::Clear() {
  return impl_->Clear();
}

void RowHashSet::Compact() {
  return impl_->Compact();
}

const View& RowHashSet::indexed_view() const { return impl_->indexed_view(); }

rowcount_t RowHashSet::size() const { return indexed_view().row_count(); }

RowHashMultiSet::RowHashMultiSet(const TupleSchema& block_schema,
                                 BufferAllocator* const allocator)
    : impl_(make_unique<RowHashSetImpl>(block_schema, allocator, nullptr, true,
                               INT64_MAX)) {}

RowHashMultiSet::RowHashMultiSet(const TupleSchema &block_schema,
                                 BufferAllocator *const allocator,
                                 unique_ptr<const BoundSingleSourceProjector> key_selector)
    : impl_(make_unique<RowHashSetImpl>(
          block_schema, allocator, std::move(key_selector), true, INT64_MAX)) {}

RowHashMultiSet::~RowHashMultiSet() { }

bool RowHashMultiSet::ReserveRowCapacity(rowcount_t capacity) {
  return impl_->ReserveRowCapacity(capacity);
}

void RowHashMultiSet::Find(const View& query, FindMultiResult* result) const {
  impl_->FindMany(query, bool_ptr(NULL), result);
}

void RowHashMultiSet::Find(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) const {
  impl_->FindMany(query, selection_vector, result);
}

size_t RowHashMultiSet::Insert(const View& query, FindMultiResult* result) {
  return impl_->InsertMany(query, bool_ptr(NULL), result);
}

size_t RowHashMultiSet::Insert(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) {
  return impl_->InsertMany(query, selection_vector, result);
}

size_t RowHashMultiSet::Insert(const View& query) {
  return impl_->InsertMany(query, bool_ptr(NULL), NULL);
}

size_t RowHashMultiSet::Insert(
    const View& query, const bool_const_ptr selection_vector) {
  return impl_->InsertMany(query, selection_vector, NULL);
}

void RowHashMultiSet::Clear() {
  return impl_->Clear();
}

void RowHashMultiSet::Compact() {
  return impl_->Compact();
}

const View& RowHashMultiSet::indexed_view() const {
  return impl_->indexed_view();
}

rowcount_t RowHashMultiSet::size() const { return indexed_view().row_count(); }

#undef CPP_TYPE


}  // namespace row_hash_set

}  // namespace supersonic
