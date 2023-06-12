#pragma once
// clang-format off

extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/lib/stringinfo.h"
}
// clang-format on

// TODO(WAN): Hack.
//  Because PostgreSQL tries to be portable, it makes a bunch of global
//  definitions that can make your C++ libraries very sad.
//  We're just going to undefine those.
#undef vsnprintf
#undef snprintf
#undef vsprintf
#undef sprintf
#undef vfprintf
#undef fprintf
#undef vprintf
#undef printf
#undef gettext
#undef dgettext
#undef ngettext
#undef dngettext
// clang-format on

uint64 Db721TableRowCount(const char *filename);
int64 FILESize(FILE *file);
StringInfo ReadFromFile(FILE *file, uint64 offset, uint32 size);

#define JSONMetadataSize 4
