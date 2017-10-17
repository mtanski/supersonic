// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for splitting strings into substrings.
//
// This file contains functions for splitting strings. The new and recommended
// API for string splitting is the strings::Split() function. The old API is a
// large collection of standalone functions declared at the bottom of this file
// in the global scope. See go/split2 for details about why we built a new
// splitting API.
//
#ifndef STRINGS_SPLIT_H_
#define STRINGS_SPLIT_H_

#include <cstddef>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <unordered_map>
#include <unordered_set>
#include <iterator>
#include "supersonic/utils/std_namespace.h"
#include <map>
using std::map;
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/strings/charset.h"
#include "supersonic/utils/strings/split_internal.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/strings/strip.h"

namespace strings {

//                              The new Split API
//                                  aka Split2
//                              aka strings::Split()
//
// This string splitting API consists of a Split() function in the ::strings
// namespace and a handful of delimiter objects in the ::strings::delimiter
// namespace (more on delimiter objects below). The Split() function always
// takes two arguments: the text to be split and the delimiter on which to split
// the text. An optional third argument may also be given, which is a Predicate
// functor that will be used to filter the results, e.g., to skip empty strings
// (more on predicates below). The Split() function adapts the returned
// collection to the type specified by the caller.
//
// Example 1:
//   // Splits the given string on commas. Returns the results in a
//   // vector of strings.
//   vector<string> v = strings::Split("a,b,c", ",");
//   assert(v.size() == 3);
//
// Example 2:
//   // By default, empty strings are *included* in the output. See the
//   // strings::SkipEmpty predicate below to omit them.
//   vector<string> v = strings::Split("a,b,,c", ",");
//   assert(v.size() == 4);  // "a", "b", "", "c"
//   v = strings::Split("", ",");
//   assert(v.size() == 1);  // v contains a single ""
//
// Example 3:
//   // Splits the string as in the previous example, except that the results
//   // are returned as StringPiece objects. Note that because we are storing
//   // the results within StringPiece objects, we have to ensure that the input
//   // string outlives any results.
//   vector<StringPiece> v = strings::Split("a,b,c", ",");
//   assert(v.size() == 3);
//
// Example 4:
//   // Stores results in a set<string>.
//   set<string> a = strings::Split("a,b,c,a,b,c", ",");
//   assert(a.size() == 3);
//
// Example 5:
//   // Stores results in a map. The map implementation assumes that the input
//   // is provided as a series of key/value pairs. For example, the 0th element
//   // resulting from the split will be stored as a key to the 1st element. If
//   // an odd number of elements are resolved, the last element is paired with
//   // a default-constructed value (e.g., empty string).
//   map<string, string> m = strings::Split("a,b,c", ",");
//   assert(m.size() == 2);
//   assert(m["a"] == "b");
//   assert(m["c"] == "");  // last component value equals ""
//
// Example 6:
//   // Splits on the empty string, which results in each character of the input
//   // string becoming one element in the output collection.
//   vector<string> v = strings::Split("abc", "");
//   assert(v.size() == 3);
//
// Example 7:
//   // Stores first two split strings as the members in an std::pair.
//   std::pair<string, string> p = strings::Split("a,b,c", ",");
//   EXPECT_EQ("a", p.first);
//   EXPECT_EQ("b", p.second);
//   // "c" is omitted because std::pair can hold only two elements.
//
// As illustrated above, the Split() function adapts the returned collection to
// the type specified by the caller. The returned collections may contain
// string, StringPiece, Cord, or any object that has a constructor (explicit or
// not) that takes a single StringPiece argument. This pattern works for all
// standard STL containers including vector, list, deque, set, multiset, map,
// and multimap, non-standard containers including hash_set and hash_map, and
// even std::pair which is not actually a container.
//
// Splitting to std::pair is an interesting case because it can hold only two
// elements and is not a collection type. When splitting to an std::pair the
// first two split strings become the std::pair's .first and .second members
// respectively. The remaining split substrings are discarded. If there are less
// than two split substrings, the empty string is used for the corresponding
// std::pair member.
//
// The strings::Split() function can be used multiple times to perform more
// complicated splitting logic, such as intelligently parsing key-value pairs.
// For example
//
//   // The input string "a=b=c,d=e,f=,g" becomes
//   // { "a" => "b=c", "d" => "e", "f" => "", "g" => "" }
//   map<string, string> m;
//   for (StringPiece sp : strings::Split("a=b=c,d=e,f=,g", ",")) {
//     m.insert(strings::Split(sp, strings::delimiter::Limit("=", 1)));
//   }
//   EXPECT_EQ("b=c", m.find("a")->second);
//   EXPECT_EQ("e", m.find("d")->second);
//   EXPECT_EQ("", m.find("f")->second);
//   EXPECT_EQ("", m.find("g")->second);
//
// The above example stores the results in an std::map. But depending on your
// data requirements, you can just as easily store the results in an
// std::multimap or even a vector<std::pair<>>.
//
//
//                                  Delimiters
//
// The Split() function also takes a second argument that is a delimiter. This
// delimiter is actually an object that defines the boundaries between elements
// in the provided input. If a string (const char*, ::string, or StringPiece) is
// passed in place of an explicit Delimiter object, the argument is implicitly
// converted to a ::strings::delimiter::Literal.
//
// With this split API comes the formal concept of a Delimiter (big D). A
// Delimiter is an object with a Find() function that knows how find the first
// occurrence of itself in a given StringPiece. Models of the Delimiter concept
// represent specific kinds of delimiters, such as single characters,
// substrings, or even regular expressions.
//
// The following Delimiter objects are provided as part of the Split() API:
//
//   - Literal (default)
//   - AnyOf
//   - Limit
//   - FixedLength
//
// In addition to these:
//
//   - //util/regexp/re2:delimiter provides the re2::Delimiter class for
//     splitting strings using regular expressions.
//
//   - //i18n/utf8:strutil provides the UTF8StringUtils::UTF8WhitespaceDelimiter
//     class for splitting strings using whitespace.
//     It also provides UTF8StringUtils::AnyOfUTF8Delimiter class that is
//     similar to AnyOf, except that it uses utf8 characters instead of bytes.
//
// The following are examples of using some provided Delimiter objects:
//
// Example 1:
//   // Because a string literal is converted to a strings::delimiter::Literal,
//   // the following two splits are equivalent.
//   vector<string> v1 = strings::Split("a,b,c", ",");           // (1)
//   using ::strings::delimiter::Literal;
//   vector<string> v2 = strings::Split("a,b,c", Literal(","));  // (2)
//
// Example 2:
//   // Splits on any of the characters specified in the delimiter string.
//   using ::strings::delimiter::AnyOf;
//   vector<string> v = strings::Split("a,b;c-d", AnyOf(",;-"));
//   assert(v.size() == 4);
//
// Example 3:
//   // Uses the Limit meta-delimiter to limit the number of matches a delimiter
//   // can have. In this case, the delimiter of a Literal comma is limited to
//   // to matching at most one time. The last element in the returned
//   // collection will contain all unsplit pieces, which may contain instances
//   // of the delimiter.
//   using ::strings::delimiter::Limit;
//   vector<string> v = strings::Split("a,b,c", Limit(",", 1));
//   assert(v.size() == 2);  // Limited to 1 delimiter; so two elements found
//   assert(v[0] == "a");
//   assert(v[1] == "b,c");
//
// Example 4:
//   // Splits into equal-length substrings.
//   using ::strings::delimiter::FixedLength;
//   vector<string> v = strings::Split("12345", FixedLength(2));
//   assert(v.size() == 3);
//   assert(v[0] == "12");
//   assert(v[1] == "34");
//   assert(v[2] == "5");
//
//                                  Predicates
//
// Predicates can filter the results of a Split() operation by determining
// whether or not a resultant element is included in the result set. A predicate
// may be passed as an *optional* third argument to the Split() function.
//
// Predicates are unary functions (or functors) that take a single StringPiece
// argument and return bool indicating whether the argument should be included
// (true) or excluded (false).
//
// One example where this is useful is when filtering out empty substrings. By
// default, empty substrings may be returned by strings::Split(), which is
// similar to the way split functions work in other programming languages. For
// example:
//
//   // Empty strings *are* included in the returned collection.
//   vector<string> v = strings::Split(",a,,b,", ",");
//   assert(v.size() ==  5);  // v[0] == "", v[1] == "a", v[2] == "", ...
//
// These empty strings can be filtered out of the results by simply passing the
// provided SkipEmpty predicate as the third argument to the Split() function.
// SkipEmpty does not consider a string containing all whitespace to be empty.
// For that behavior use the SkipWhitespace predicate. For example:
//
// Example 1:
//   // Uses SkipEmpty to omit empty strings. Strings containing whitespace are
//   // not empty and are therefore not skipped.
//   using strings::SkipEmpty;
//   vector<string> v = strings::Split(",a, ,b,", ",", SkipEmpty());
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == " ");  // <-- The whitespace makes the string not empty.
//   assert(v[2] == "b");
//
// Example 2:
//   // Uses SkipWhitespace to skip all strings that are either empty or contain
//   // only whitespace.
//   using strings::SkipWhitespace;
//   vector<string> v = strings::Split(",a, ,b,", ",",  SkipWhitespace());
//   assert(v.size() == 2);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//
//
//                     Differences between Split1 and Split2
//
// Split2 is the strings::Split() API described above. Split1 is a name for the
// collection of legacy Split*() functions declared later in this file. Most of
// the Split1 functions follow a set of conventions that don't necessarily match
// the conventions used in Split2. The following are some of the important
// differences between Split1 and Split2:
//
// Split1 -> Split2
// ----------------
// Append -> Assign:
//   The Split1 functions all returned their output collections via a pointer to
//   an out parameter as is typical in Google code. In some cases the comments
//   explicitly stated that results would be *appended* to the output
//   collection. In some cases it was ambiguous whether results were appended.
//   This ambiguity is gone in the Split2 API as results are always assigned to
//   the output collection, never appended.
//
// AnyOf -> Literal:
//   Most Split1 functions treated their delimiter argument as a string of
//   individual byte delimiters. For example, a delimiter of ",;" would split on
//   "," and ";", not the substring ",;". This behavior is equivalent to the
//   Split2 delimiter strings::delimiter::AnyOf, which is *not* the default. By
//   default, strings::Split() splits using strings::delimiter::Literal() which
//   would treat the whole string ",;" as a single delimiter string.
//
// SkipEmpty -> allow empty:
//   Most Split1 functions omitted empty substrings in the results. To keep
//   empty substrings one would have to use an explicitly named
//   Split*AllowEmpty() function. This behavior is reversed in Split2. By
//   default, strings::Split() *allows* empty substrings in the output. To skip
//   them, use the strings::SkipEmpty predicate.
//
// string -> user's choice:
//   Most Split1 functions return collections of string objects. Some return
//   char*, but the type returned is dictated by each Split1 function. With
//   Split2 the caller can choose which string-like object to return. (Note:
//   char* C-strings are not supported in Split2--use StringPiece instead).
//

namespace delimiter {
// A Delimiter object tells the splitter where a string should be broken. Some
// examples are breaking a string wherever a given character or substring is
// found, wherever a regular expression matches, or simply after a fixed length.
// All Delimiter objects must have the following member function:
//
//   StringPiece Find(StringPiece text, size_t pos);
//
// A Delimiter's Find() member function will be passed the input text that is to
// be split and the position to begin searching for the next delimiter in the
// input text. The returned StringPiece should refer to the next occurrence
// (after pos) of the represented delimiter; this returned StringPiece
// represents the next location where the input string should be broken. The
// returned StringPiece may be zero-length if the Delimiter does not represent a
// part of the string (e.g., a fixed-length delimiter). If no delimiter is found
// in the given text, a zero-length StringPiece referring to text.end() should
// be returned (e.g., StringPiece(text.end(), 0)). It is important that the
// returned StringPiece always be within the bounds of input text given as an
// argument--it must not refer to a string that is physically located outside of
// the given string.
//
// The following example is a simple Delimiter object that is created with a
// single char and will look for that char in the text passed to the Find()
// function:
//
//   struct SimpleDelimiter {
//     const char c_;
//     explicit SimpleDelimiter(char c) : c_(c) {}
//     StringPiece Find(StringPiece text, size_t pos) {
//       auto found = text.find(c_, pos);
//       if (found == StringPiece::npos) return StringPiece(text.end(), 0);
//       return StringPiece(text, found, 1);
//     }
//   };

// Represents a literal string delimiter. Examples:
//
//   using ::strings::delimiter::Literal;
//   vector<string> v = strings::Split("a=>b=>c", Literal("=>"));
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c");
//
// The next example uses the empty string as a delimiter.
//
//   using ::strings::delimiter::Literal;
//   vector<string> v = strings::Split("abc", Literal(""));
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c");
//
// This is the *default* delimiter used if a literal string or string-like
// object is used where a Delimiter object is expected. For example, the
// following calls are equivalent.
//
//   vector<string> v = strings::Split("a,b", ",");
//   vector<string> v = strings::Split("a,b", strings::delimiter::Literal(","));
//
class Literal {
 public:
  explicit Literal(StringPiece sp);
  StringPiece Find(StringPiece text, size_t pos) const;

 private:
  const string delimiter_;
};

namespace internal {
// A traits-like metafunction for selecting the default Delimiter object type
// for a particular Delimiter type. The base case simply exposes type Delimiter
// itself as the delimiter's Type. However, there are specializations for
// string-like objects that map them to the Literal delimiter object. This
// allows functions like strings::Split() and strings::delimiter::Limit() to
// accept string-like objects (e.g., ",") as delimiter arguments but they will
// be treated as if a Literal delimiter was given.
template <typename Delimiter>
struct SelectDelimiter {
  typedef Delimiter Type;
};
template <> struct SelectDelimiter<char*>       { typedef Literal Type; };
template <> struct SelectDelimiter<const char*> { typedef Literal Type; };
template <> struct SelectDelimiter<StringPiece> { typedef Literal Type; };
template <> struct SelectDelimiter<std::string> { typedef Literal Type; };
#if defined(HAS_GLOBAL_STRING)
template <> struct SelectDelimiter<string> { typedef Literal Type; };
#endif
}  // namespace internal

// Represents a delimiter that will match any of the given byte-sized
// characters. AnyOf is similar to Literal, except that AnyOf uses
// StringPiece::find_first_of() and Literal uses StringPiece::find(). AnyOf
// examples:
//
//   using ::strings::delimiter::AnyOf;
//   vector<string> v = strings::Split("a,b=c", AnyOf(",="));
//
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c");
//
// If AnyOf is given the empty string, it behaves exactly like Literal and
// matches each individual character in the input string.
//
// Note: The string passed to AnyOf is assumed to be a string of single-byte
// ASCII characters. AnyOf does not work with multi-byte characters.
class AnyOf {
 public:
  explicit AnyOf(StringPiece sp);
  StringPiece Find(StringPiece text, size_t pos) const;

 private:
  const string delimiters_;
};

// A delimiter for splitting into equal-length strings. The length argument to
// the constructor must be greater than 0. This delimiter works with ascii
// string data, but does not work with variable-width encodings, such as UTF-8.
// Examples:
//
//   using ::strings::delimiter::FixedLength;
//   vector<string> v = strings::Split("123456789", FixedLength(3));
//   assert(v.size() == 3);
//   assert(v[0] == "123");
//   assert(v[1] == "456");
//   assert(v[2] == "789");
//
// Note that the string does not have to be a multiple of the fixed split
// length. In such a case, the last substring will be shorter.
//
//   using ::strings::delimiter::FixedLength;
//   vector<string> v = strings::Split("12345", FixedLength(2));
//   assert(v.size() == 3);
//   assert(v[0] == "12");
//   assert(v[1] == "34");
//   assert(v[2] == "5");
//
class FixedLength {
 public:
  explicit FixedLength(int length);
  StringPiece Find(StringPiece text, size_t pos) const;

 private:
  const int length_;
};

// Wraps another delimiter and sets a max number of matches for that delimiter.
// Create LimitImpls using the Limit() function. Example:
//
//   using ::strings::delimiter::Limit;
//   vector<string> v = strings::Split("a,b,c,d", Limit(",", 2));
//
//   assert(v.size() == 3);  // Split on 2 commas, giving a vector with 3 items
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c,d");
//
template <typename Delimiter>
class LimitImpl {
 public:
  LimitImpl(Delimiter delimiter, int limit)
      : delimiter_(delimiter), limit_(limit), count_(0) {}
  StringPiece Find(StringPiece text, size_t pos) {
    if (count_++ == limit_) {
      return StringPiece(text.end(), 0);  // No more matches.
    }
    return delimiter_.Find(text, pos);
  }

 private:
  Delimiter delimiter_;
  const int limit_;
  int count_;
};

// Limit() function to create LimitImpl<> objects.
template <typename Delimiter>
inline LimitImpl<typename internal::SelectDelimiter<Delimiter>::Type>
  Limit(Delimiter delim, int limit) {
  typedef typename internal::SelectDelimiter<Delimiter>::Type DelimiterType;
  return LimitImpl<DelimiterType>(DelimiterType(delim), limit);
}

}  // namespace delimiter

//
// Predicates are functors that return bool indicating whether the given
// StringPiece should be included in the split output. If the predicate returns
// false then the string will be excluded from the output from strings::Split().
//

// Always returns true, indicating that all strings--including empty
// strings--should be included in the split output. This predicate is not
// strictly needed because this is the default behavior of the strings::Split()
// function. But it might be useful at some call sites to make the intent
// explicit.
//
// vector<string> v = Split(" a , ,,b,", ",", AllowEmpty());
// EXPECT_THAT(v, ElementsAre(" a ", " ", "", "b", ""));
struct AllowEmpty {
  bool operator()(StringPiece sp) const {
    return true;
  }
};

// Returns false if the given StringPiece is empty, indicating that the
// strings::Split() API should omit the empty string.
//
// vector<string> v = Split(" a , ,,b,", ",", SkipEmpty());
// EXPECT_THAT(v, ElementsAre(" a ", " ", "b"));
struct SkipEmpty {
  bool operator()(StringPiece sp) const {
    return !sp.empty();
  }
};

// Returns false if the given StringPiece is empty or contains only whitespace,
// indicating that the strings::Split() API should omit the string.
//
// vector<string> v = Split(" a , ,,b,", ",", SkipWhitespace());
// EXPECT_THAT(v, ElementsAre(" a ", "b"));
struct SkipWhitespace {
  bool operator()(StringPiece sp) const {
    StripWhiteSpace(&sp);
    return !sp.empty();
  }
};

// Definitions of the main Split() function. The use of SelectDelimiter<> allows
// these functions to be called with a Delimiter template parameter that is an
// actual Delimiter object (e.g., Literal or AnyOf), OR called with a
// string-like delimiter argument, (e.g., ","), in which case the delimiter used
// will default to strings::delimiter::Literal.
//
// In C++11 mode, ConvertibleToStringPiece extends the lifetime of temporary
// strings passed to it.  In C++98 mode, temporary strings only live to the end
// of the current statement, but this is less of a problem without range-based
// for loops and auto.
template <typename Delimiter>
inline internal::Splitter<
    typename delimiter::internal::SelectDelimiter<Delimiter>::Type>
Split(internal::ConvertibleToStringPiece text, Delimiter d) {
  typedef typename delimiter::internal::SelectDelimiter<Delimiter>::Type
      DelimiterType;
  return internal::Splitter<DelimiterType>(&text, DelimiterType(d));
}

template <typename Delimiter, typename Predicate>
inline internal::Splitter<
    typename delimiter::internal::SelectDelimiter<Delimiter>::Type, Predicate>
Split(internal::ConvertibleToStringPiece text, Delimiter d, Predicate p) {
  typedef typename delimiter::internal::SelectDelimiter<Delimiter>::Type
      DelimiterType;
  return internal::Splitter<DelimiterType, Predicate>(&text, DelimiterType(d),
                                                      p);
}

}  // namespace strings

// ----------------------------------------------------------------------
// Clips a string to a max length. We try to clip on a word boundary if this is
// possible. If the string is clipped, we append an ellipsis.
//
// ***NOTE***
// ClipString counts length with strlen.  If you have non-ASCII strings like
// UTF-8, this is wrong.  If you are displaying the clipped strings to users in
// a frontend, consider using ClipStringOnWordBoundary in
// webserver/util/snippets/rewriteboldtags, which considers the width of the
// string, not just the number of bytes.
//
// TODO(user) Move ClipString back to strutil.  The problem with this is
// that ClipStringHelper is used behind the scenes by SplitStringToLines, but
// probably shouldn't be exposed in the .h files.
// ----------------------------------------------------------------------
void ClipString(string* full_str, int max_len);

// ----------------------------------------------------------------------
// SplitStringToLines() Split a string into lines of maximum length
// 'max_len'. Append the resulting lines to 'result'. Will attempt
// to split on word boundaries.  If 'num_lines'
// is zero it splits up the whole string regardless of length. If
// 'num_lines' is positive, it returns at most num_lines lines, and
// appends a "..." to the end of the last line if the string is too
// long to fit completely into 'num_lines' lines.
// ----------------------------------------------------------------------
void SplitStringToLines(const char* full,
                        int max_len,
                        int num_lines,
                        vector<string>* result);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   std::pair<StringPiece, StringPiece> p =
//       strings::Split("a-b-c", strings::delimiter::Limit("-", 1))
//   // p.first = "a"
//   // p.second = "b-c"
//
//   See the strings::Split() documentation above for more details.
//
// SplitOneStringToken()
//   Returns the first "delim" delimited string from "*source" and modifies
//   *source to point after the delimiter that was found. If no delimiter is
//   found, *source is set to NULL.
//
//   If the start of *source is a delimiter, an empty string is returned.
//   If *source is NULL, an empty string is returned.
//
//   "delim" is treated as a sequence of 1 or more character delimiters. Any one
//   of the characters present in "delim" is considered to be a single
//   delimiter; the delimiter is not "delim" as a whole. For example:
//
//     const char* s = "abc=;de";
//     string r = SplitOneStringToken(&s, ";=");
//     // r = "abc"
//     // s points to ";de"
// ----------------------------------------------------------------------
string SplitOneStringToken(const char** source, const char* delim);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   vector<StringPiece> v = strings::Split(text, delim)
//
//   See the strings::Split() documentation above for more details.
//
// SplitToVector()
//    Split a string into substrings based on the nul-terminated list
//    of bytes at delim (uses strsep) and appends the split
//    strings to 'vec'.  Modifies "full".  If omit empty strings is
//    true, empty strings are omitted from the resulting vector.
// ----------------------------------------------------------------------
void SplitToVector(char* full, const char* delimiters,
                   vector<char*>* vec,
                   bool omit_empty_strings);
void SplitToVector(char* full, const char* delimiters,
                   vector<const char*>* vec,
                   bool omit_empty_strings);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   vector<StringPiece> v = strings::Split(text, delim)
//
//   See the strings::Split() documentation above for more details.
//
// SplitStringPieceToVector
//    Split a StringPiece into sub-StringPieces based on the
//    nul-terminated list of bytes at delim and appends the
//    pieces to 'vec'.  If omit empty strings is true, empty strings
//    are omitted from the resulting vector.
//    Expects the original string (from which 'full' is derived) to exist
//    for the full lifespan of 'vec'.
// ----------------------------------------------------------------------
void SplitStringPieceToVector(StringPiece full,
                              const char* delim,
                              vector<StringPiece>* vec,
                              bool omit_empty_strings);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   vector<string> v = strings::Split(text, delim, strings::SkipEmpty())
//
//   See the strings::Split() documentation above for more details.
//
// SplitStringUsing()
// SplitStringToHashsetUsing()
// SplitStringToSetUsing()
// SplitStringToHashmapUsing()

// Splits a string using one or more byte delimiters, presented as a
// nul-terminated c string. Append the components to 'result'. If there are
// consecutive delimiters, this function skips over all of them: in other words,
// empty components are dropped. If you want to keep empty components, try
// SplitStringAllowEmpty().
//
// NOTE: Do not use this for multi-byte delimiters such as UTF-8 strings. Use
// strings::Split() with strings::delimiter::Literal as the delimiter.
//
// ==> NEW API: Use the new Split API defined above. <==
// Example:
//
//   using strings::SkipEmpty;
//   using strings::Split;
//   using strings::delimiter::AnyOf;
//
//   vector<string> v = Split(full, AnyOf(delimiter), SkipEmpty());
//
// For even better performance, store the result in a vector<StringPiece>
// to avoid string copies.
// ----------------------------------------------------------------------
void SplitStringUsing(const string& full, const char* delimiters,
                      vector<string>* result);
void SplitStringToHashsetUsing(const string& full, const char* delimiters,
                               std::unordered_set<string>* result);
void SplitStringToSetUsing(const string& full, const char* delimiters,
                           std::set<string>* result);
// The even-positioned (0-based) components become the keys for the
// odd-positioned components that follow them. When there is an odd
// number of components, the value for the last key will be unchanged
// if the key was already present in the hash table, or will be the
// empty string if the key is a newly inserted key.
void SplitStringToHashmapUsing(const string& full, const char* delim,
                               std::unordered_map<string, string>* result);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   vector<string> v = strings::Split(text, delim)
//
//   See the strings::Split() documentation above for more details.
//
// SplitStringAllowEmpty()
//
// Split a string using one or more byte delimiters, presented as a
// nul-terminated c string. Append the components to 'result'. If there are
// consecutive delimiters, this function will return corresponding empty
// strings.  If you want to drop the empty strings, try SplitStringUsing().
//
// If "full" is the empty string, yields an empty string as the only value.
//
// ==> NEW API: Use the new Split API defined above. <==
//
//   using strings::Split;
//   using strings::delimiter::AnyOf;
//
//   vector<string> v = Split(full, AnyOf(delimiter));
//
// For even better performance, store the result in a vector<StringPiece> to
// avoid string copies.
// ----------------------------------------------------------------------
void SplitStringAllowEmpty(const string& full, const char* delim,
                           vector<string>* result);

// ----------------------------------------------------------------------
// SplitStringWithEscaping()
// SplitStringWithEscapingAllowEmpty()
// SplitStringWithEscapingToSet()
// SplitStringWithEscapingToHashset()

//   Split the string using the specified delimiters, taking escaping into
//   account. '\' is not allowed as a delimiter.
//
//   Within the string, preserve a delimiter preceded by a backslash as a
//   literal delimiter. In addition, preserve two consecutive backslashes as
//   a single literal backslash. Do not unescape any other backslash-character
//   sequence.
//
//   Eg. 'foo\=bar=baz\\qu\ux' split on '=' becomes ('foo=bar', 'baz\qu\ux')
//
//   All versions other than "AllowEmpty" discard any empty substrings.
// ----------------------------------------------------------------------
void SplitStringWithEscaping(StringPiece full,
                             const strings::CharSet& delimiters,
                             vector<string>* result);
void SplitStringWithEscapingAllowEmpty(StringPiece full,
                                       const strings::CharSet& delimiters,
                                       vector<string>* result);
void SplitStringWithEscapingToSet(StringPiece full,
                                  const strings::CharSet& delimiters,
                                  std::set<string>* result);
void SplitStringWithEscapingToHashset(StringPiece full,
                                      const strings::CharSet& delimiters,
                                      std::unordered_set<string>* result);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   vector<string> v = strings::Split(text, strings::delimiter::Limit("-", N))
//
//   See the strings::Split() documentation above for more details.
//
// SplitStringIntoNPiecesAllowEmpty()
//    Split a string using a nul-terminated list of byte
//    delimiters. Append the components to 'result'.  If there are
//    consecutive delimiters, this function will return corresponding
//    empty strings. The string is split into at most the specified
//    number of pieces greedily. This means that the last piece may
//    possibly be split further. To split into as many pieces as
//    possible, specify 0 as the number of pieces.
//
//    If "full" is the empty string, yields an empty string as the only value.
// ----------------------------------------------------------------------
void SplitStringIntoNPiecesAllowEmpty(const string& full,
                                      const char* delimiters,
                                      int pieces,
                                      vector<string>* result);

// ----------------------------------------------------------------------
// SplitStringAndParse()
// SplitStringAndParseToContainer()
// SplitStringAndParseToList()
//    Split a string using a nul-terminated list of character
//    delimiters.  For each component, parse using the provided
//    parsing function and if successful, append it to 'result'.
//    Return true if and only if all components parse successfully.
//    If there are consecutive delimiters, this function skips over
//    all of them.  This function will correctly handle parsing
//    strings that have embedded \0s.
//
// SplitStringAndParse fills into a vector.
// SplitStringAndParseToContainer fills into any container that implements
//    a single-argument insert function. (i.e. insert(const value_type& x) ).
// SplitStringAndParseToList fills into any container that implements a single-
//    argument push_back function (i.e. push_back(const value_type& x) ), plus
//    value_type& back() and pop_back().
//    NOTE: This implementation relies on parsing in-place into the "back()"
//    reference, so its performance may depend on the efficiency of back().
//
// Example Usage:
//  vector<double> values;
//  CHECK(SplitStringAndParse("1.0,2.0,3.0", ",", &safe_strtod, &values));
//  CHECK_EQ(3, values.size());
//
//  vector<int64> values;
//  CHECK(SplitStringAndParse("1M,2M,3M", ",",
//        &HumanReadableNumBytes::ToInt64, &values));
//  CHECK_EQ(3, values.size());
//
//  set<int64> values;
//  CHECK(SplitStringAndParseToContainer("3,1,1,2", ",",
//        &safe_strto64, &values));
//  CHECK_EQ(4, values.size());
//
//  deque<int64> values;
//  CHECK(SplitStringAndParseToList("3,1,1,2", ",", &safe_strto64, &values));
//  CHECK_EQ(4, values.size());
// ----------------------------------------------------------------------
template <class T>
bool SplitStringAndParse(StringPiece source, StringPiece delim,
                         bool (*parse)(const string& str, T* value),
                         vector<T>* result);
template <class Container>
bool SplitStringAndParseToContainer(
    StringPiece source, StringPiece delim,
    bool (*parse)(const string& str, typename Container::value_type* value),
    Container* result);

template <class List>
bool SplitStringAndParseToList(
    StringPiece source, StringPiece delim,
    bool (*parse)(const string& str, typename List::value_type* value),
    List* result);
// ----------------------------------------------------------------------
// SplitRange()
//    Splits a string of the form "<from>-<to>".  Either or both can be
//    missing.  A raw number (<to>) is interpreted as "<to>-".  Modifies
//    parameters insofar as they're specified by the string.  RETURNS
//    true iff the input is a well-formed range.  If it RETURNS false,
//    from and to remain unchanged.  The range in rangestr should be
//    terminated either by "\0" or by whitespace.
// ----------------------------------------------------------------------
bool SplitRange(const char* rangestr, int* from, int* to);

// ----------------------------------------------------------------------
// LEGACY(jgm): The SplitCSV* functions are no longer recommended.
// Use util::csv::Parser defined in util/csv/Parser.h instead.
//
// Example: To parse a single line:
//   #include "util/csv/parser.h"
//   vector<string> fields = util::csv::ParseLine(line).fields();
//
// Example: To parse an entire file:
//   #include "util/csv/parser.h"
//   for (const util::csv::Record& rec : Parser(source)) {
//     vector<string> fields = rec.fields();
//   }
//
// SplitCSVLineWithDelimiter()
//    CSV lines come in many guises.  There's the Comma Separated Values
//    variety, in which fields are separated by (surprise!) commas.  There's
//    also the tab-separated values variant, in which tabs separate the
//    fields.  This routine handles both.  For both delimiters, whitespace is
//    trimmed from either side of the field value. If the delimiter is ',', we
//    play additional games with quotes.  A field value surrounded by double
//    quotes is allowed to contain commas, which are not treated as field
//    separators.  Within a double-quoted string, a series of two double quotes
//    signals an escaped single double quote. It'll be clearer in the examples.
//    Example:
//     Google , x , "Buchheit, Paul", "string with "" quote in it"
//     -->  [Google], [x], [Buchheit, Paul], [string with " quote in it]
//
// SplitCSVLine()
//    A convenience wrapper around SplitCSVLineWithDelimiter which uses
//    ',' as the delimiter.
//
// ----------------------------------------------------------------------
void SplitCSVLine(char* line, vector<char*>* cols);
void SplitCSVLineWithDelimiter(char* line, char delimiter,
                               vector<char*>* cols);
// SplitCSVLine string wrapper that internally makes a copy of string line.
void SplitCSVLineWithDelimiterForStrings(const string& line, char delimiter,
                                         vector<string>* cols);

// ----------------------------------------------------------------------
// SplitStructuredLine()
//    Splits a line using the given delimiter, and places the columns into
//    'cols'. If the symbol_pair string has an odd number of characters, the
//    last character (which cannot be paired) will be assumed to be both an
//    opening and closing symbol.
//    WARNING : The input string 'line' is destroyed in the process.
//    The function returns 0 if the line was parsed correctly (i.e all the
//    opened braces had their closing braces) otherwise, it returns the position
//    of the error.
//    Example:
//     SplitStructuredLine("item1,item2,{subitem1,subitem2},item4,[5,{6,7}]",
//                         ',',
//                         "{}[]", &output)
//     --> output = { "item1", "item2", "{subitem1,subitem2}", "item4",
//                    "[5,{6,7}]" }
//    Example2: trying to split "item1,[item2,{4,5],5}" will fail and the
//              function will return the position of the problem : ]
//
// ----------------------------------------------------------------------
char* SplitStructuredLine(char* line,
                          char delimiter,
                          const char* symbol_pairs,
                          vector<char*>* cols);

// Similar to the function with the same name above, but splits a StringPiece
// into StringPiece parts. Returns true if successful.
bool SplitStructuredLine(StringPiece line,
                         char delimiter,
                         const char* symbol_pairs,
                         vector<StringPiece>* cols);

// ----------------------------------------------------------------------
// SplitStructuredLineWithEscapes()
//    Like SplitStructuredLine but also allows characters to be escaped.
//
//    WARNING: the escape characters will be replicated in the output
//    columns rather than being consumed, i.e. if {} were the opening and
//    closing symbols, using \{ to quote a curly brace in the middle of
//    an option would pass this unchanged.
//
//    Example:
//     SplitStructuredLineWithEscapes(
//       "\{item1\},it\\em2,{\{subitem1\},sub\\item2},item4\,item5,[5,{6,7}]",
//                     ',',
//                     "{}[]",
//                     &output)
//     --> output = { "\{item1\}", "it\\em2", "{\{subitem1\},sub\\item2}",
//                    "item4\,item5", "[5,{6,7}]" }
//
// ----------------------------------------------------------------------
char* SplitStructuredLineWithEscapes(char* line,
                                     char delimiter,
                                     const char* symbol_pairs,
                                     vector<char*>* cols);

// Similar to the function with the same name above, but splits a StringPiece
// into StringPiece parts. Returns true if successful.
bool SplitStructuredLineWithEscapes(StringPiece line,
                                    char delimiter,
                                    const char* symbol_pairs,
                                    vector<StringPiece>* cols);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   using strings::delimiter::Limit;
//
//   // kv.first is the key; kv.second contains all the values
//   std::pair<string, string> kv = strings::Split(text, Limit(kv_delim, 1));
//   vector<string> values = Split(kv.second, vv_delim);
//
//   See the strings::Split() documentation above for more details.
//
// SplitStringIntoKeyValues()
// Split a line into a key string and a vector of value strings. The line has
// the following format:
//
// <key><kvsep>+<vvsep>*<value1><vvsep>+<value2><vvsep>+<value3>...<vvsep>*
//
// where key and value are strings; */+ means zero/one or more; <kvsep> is
// a delimiter character to separate key and value; and <vvsep> is a delimiter
// character to separate between values. The user can specify a bunch of
// delimiter characters using a string. For example, if the user specifies
// the separator string as "\t ", then either ' ' or '\t' or any combination
// of them wil be treated as separator. For <vvsep>, the user can specify a
// empty string to indicate there is only one value.
//
// Note: this function assumes the input string begins exactly with a
// key. Therefore, if you use whitespaces to separate key and value, you
// should not let whitespace precedes the key in the input. Otherwise, you
// will get an empty string as the key.
//
// A line with no <kvsep> will return an empty string as the key, even if
// <key> is non-empty!
//
// The syntax makes it impossible for a value to be the empty string.
// It is possible for the number of values to be zero.
//
// Returns false if the line has no <kvsep> or if the number of values is
// zero.
//
// ----------------------------------------------------------------------
bool SplitStringIntoKeyValues(const string& line,
                              const string& key_value_delimiters,
                              const string& value_value_delimiters,
                              string* key, vector<string>* values);

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Split() instead, e.g.,:
//   using strings::delimiter::Limit;
//
//   vector<pair<string, string>> pairs;  // or even map<string, string>
//   for (StringPiece sp : strings::Split(line, pair_delim)) {
//     pairs.push_back(strings::Split(sp, Limit(kv_delim, 1)));
//   }
//
//   See the strings::Split() documentation above for more details.
//
// SplitStringIntoKeyValuePairs()
// Split a line into a vector of <key, value> pairs. The line has
// the following format:
//
// <kvpsep>*<key1><kvsep>+<value1><kvpsep>+<key2><kvsep>+<value2>...<kvpsep>*
//
// Where key and value are strings; */+ means zero/one or more. <kvsep> is
// a delimiter character to separate key and value and <kvpsep> is a delimiter
// character to separate key value pairs. The user can specify a bunch of
// delimiter characters using a string.
//
// Note: this function assumes each key-value pair begins exactly with a
// key. Therefore, if you use whitespaces to separate key and value, you
// should not let whitespace precede the key in the pair. Otherwise, you
// will get an empty string as the key.
//
// A pair with no <kvsep> will return empty strings as the key and value,
// even if <key> is non-empty!
//
// Returns false for pairs with no <kvsep> specified and for pairs with
// empty strings as values.
//
// ----------------------------------------------------------------------
bool SplitStringIntoKeyValuePairs(const string& line,
                                  const string& key_value_delimiters,
                                  const string& key_value_pair_delimiters,
                                  vector<std::pair<string, string> >* kv_pairs);


// ----------------------------------------------------------------------
// SplitLeadingDec32Values()
// SplitLeadingDec64Values()
//    A simple parser for space-separated decimal int32/int64 values.
//    Appends parsed integers to the end of the result vector, stopping
//    at the first unparsable spot.  Skips past leading and repeated
//    whitespace (does not consume trailing whitespace), and returns
//    a pointer beyond the last character parsed.
// --------------------------------------------------------------------
const char* SplitLeadingDec32Values(const char* next, vector<int32>* result);
const char* SplitLeadingDec64Values(const char* next, vector<int64>* result);

// ----------------------------------------------------------------------
// SplitOneIntToken()
// SplitOneInt32Token()
// SplitOneUint32Token()
// SplitOneInt64Token()
// SplitOneUint64Token()
// SplitOneDoubleToken()
// SplitOneFloatToken()
//   Parse a single "delim" delimited number from "*source" into "*value".
//   Modify *source to point after the delimiter.
//   If no delimiter is present after the number, set *source to NULL.
//
//   If the start of *source is not an number, return false.
//   If the int is followed by the null character, return true.
//   If the int is not followed by a character from delim, return false.
//   If *source is NULL, return false.
//
//   They cannot handle decimal numbers with leading 0s, since they will be
//   treated as octal.
// ----------------------------------------------------------------------
bool SplitOneIntToken(const char** source, const char* delim,
                      int* value);
bool SplitOneInt32Token(const char** source, const char* delim,
                        int32* value);
bool SplitOneUint32Token(const char** source, const char* delim,
                         uint32* value);
bool SplitOneInt64Token(const char** source, const char* delim,
                        int64* value);
bool SplitOneUint64Token(const char** source, const char* delim,
                         uint64* value);
bool SplitOneDoubleToken(const char** source, const char* delim,
                         double* value);
bool SplitOneFloatToken(const char** source, const char* delim,
                        float* value);

// Some aliases, so that the function names are standardized against the names
// of the reflection setters/getters in proto2. This makes it easier to use
// certain macros with reflection when creating custom text formats for protos.

inline bool SplitOneUInt32Token(const char** source, const char* delim,
                         uint32* value) {
  return SplitOneUint32Token(source, delim, value);
}

inline bool SplitOneUInt64Token(const char** source, const char* delim,
                         uint64* value) {
  return SplitOneUint64Token(source, delim, value);
}

// ----------------------------------------------------------------------
// SplitOneDecimalIntToken()
// SplitOneDecimalInt32Token()
// SplitOneDecimalUint32Token()
// SplitOneDecimalInt64Token()
// SplitOneDecimalUint64Token()
// Parse a single "delim"-delimited number from "*source" into "*value".
// Unlike SplitOneIntToken, etc., this function always interprets
// the numbers as decimal.
bool SplitOneDecimalIntToken(const char** source, const char* delim,
                             int* value);
bool SplitOneDecimalInt32Token(const char** source, const char* delim,
                               int32* value);
bool SplitOneDecimalUint32Token(const char** source, const char* delim,
                                uint32* value);
bool SplitOneDecimalInt64Token(const char** source, const char* delim,
                               int64* value);
bool SplitOneDecimalUint64Token(const char** source, const char* delim,
                                uint64* value);

// ----------------------------------------------------------------------
// SplitOneHexUint32Token()
// SplitOneHexUint64Token()
// Once more, for hexadecimal numbers (unsigned only).
bool SplitOneHexUint32Token(const char** source, const char* delim,
                            uint32* value);
bool SplitOneHexUint64Token(const char** source, const char* delim,
                            uint64* value);


// ###################### TEMPLATE INSTANTIATIONS BELOW #######################

// SplitStringAndParse() -- see description above
template <class T>
bool SplitStringAndParse(StringPiece source, StringPiece delim,
                         bool (*parse)(const string& str, T* value),
                         vector<T>* result) {
  return SplitStringAndParseToList(source, delim, parse, result);
}

namespace strings {
namespace internal {

template <class Container, class InsertPolicy>
bool SplitStringAndParseToInserter(
    StringPiece source, StringPiece delim,
    bool (*parse)(const string& str, typename Container::value_type* value),
    Container* result, InsertPolicy insert_policy) {
  CHECK(NULL != parse);
  CHECK(NULL != result);
  CHECK(NULL != delim.data());
  CHECK_GT(delim.size(), 0);
  bool retval = true;
  vector<StringPiece> pieces = strings::Split(source,
                                              strings::delimiter::AnyOf(delim),
                                              strings::SkipEmpty());
  for (auto & piece: pieces) {
    typename Container::value_type t;
    if (parse(piece.as_string(), &t)) {
      insert_policy(result, t);
    } else {
      retval = false;
    }
  }
  return retval;
}

// Cannot use output iterator here (e.g. std::inserter, std::back_inserter)
// because some callers use non-standard containers that don't have iterators,
// only an insert() or push_back() method.
struct BasicInsertPolicy {
  template <class C, class V>
  void operator()(C* c, const V& v) const { c->insert(v); }
};

struct BackInsertPolicy {
  template <class C, class V>
  void operator()(C* c, const V& v) const { c->push_back(v); }
};

}  // namespace internal
}  // namespace strings

// SplitStringAndParseToContainer() -- see description above
template <class Container>
bool SplitStringAndParseToContainer(
    StringPiece source, StringPiece delim,
    bool (*parse)(const string& str, typename Container::value_type* value),
    Container* result) {
  return strings::internal::SplitStringAndParseToInserter(
      source, delim, parse, result, strings::internal::BasicInsertPolicy());
}

// SplitStringAndParseToList() -- see description above
template <class List>
bool SplitStringAndParseToList(
    StringPiece source, StringPiece delim,
    bool (*parse)(const string& str, typename List::value_type* value),
    List* result) {
  return strings::internal::SplitStringAndParseToInserter(
      source, delim, parse, result, strings::internal::BackInsertPolicy());
}

#endif  // STRINGS_SPLIT_H_
