// Copyright 2008 Google Inc. All Rights Reserved.

#include "supersonic/utils/strings/charset.h"

#include <cstring>
#include "supersonic/utils/strings/stringpiece.h"

namespace strings {

CharSet::CharSet() {
  memset(bits_, 0, sizeof(bits_));
}

CharSet::CharSet(const char* characters) {
  memset(bits_, 0, sizeof(bits_));
  for (; *characters != '\0'; ++characters) {
    Add(*characters);
  }
}

CharSet::CharSet(StringPiece characters) {
  memset(bits_, 0, sizeof(bits_));
  for (char character : characters) {
    Add(character);
  }
}

}  // namespace strings
