#include <stdio.h>
#include <string>
#include "db721_reader.h"
#include "json.hpp"


// clang-format off
extern "C" {
#include "../../../../src/include/storage/fd.h"
}
// clang-format on

/* Returns the size of the given file handle. */
int64
FILESize(FILE *file)
{
	int64 fileSize = 0;
	int fseekResult = 0;

	errno = 0;
	fseekResult = fseeko(file, 0, SEEK_END);
	if (fseekResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not seek in file: %m")));
	}

	fileSize = ftello(file);
	if (fileSize == -1)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not get position in file: %m")));
	}

	return fileSize;
}

/* Reads the given segment from the given file. */
StringInfo
ReadFromFile(FILE *file, uint64 offset, uint32 size)
{
	int fseekResult = 0;
	int freadResult = 0;
	int fileError = 0;

	StringInfo resultBuffer = makeStringInfo();
	enlargeStringInfo(resultBuffer, size);
	resultBuffer->len = size;
	resultBuffer->data[resultBuffer->len] = '\0';

	if (size == 0)
	{
		return resultBuffer;
	}

	errno = 0;
	fseekResult = fseeko(file, offset, SEEK_SET);
	if (fseekResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not seek in file: %m")));
	}

	freadResult = fread(resultBuffer->data, size, 1, file);
	if (freadResult != 1)
	{
		ereport(ERROR, (errmsg("could not read enough data from file")));
	}

	fileError = ferror(file);
	if (fileError != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not read file: %m")));
	}

	return resultBuffer;
}

/* Db721TableRowCount returns the exact row count of a table using skiplists */
uint64
Db721TableRowCount(const char *filename)
{
  // TODO(WANG): should I release StringInfo?
  FILE *tableFile = nullptr;
  uint64 tableFileSize = 0;
  StringInfo MetadataSizeBuf = nullptr;
  uint32 MetadataSize = 0;
  StringInfo MetadataBuf = nullptr;
  nlohmann::json MetaDataJson;
  std::string columnName;
  uint64 totalRowCount = 0;


  tableFile = AllocateFile(filename, PG_BINARY_R);
  if (tableFile == NULL)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\" for reading: %m", filename)));
  }
  
  tableFileSize = FILESize(tableFile);
  MetadataSizeBuf = ReadFromFile(tableFile, tableFileSize - JSONMetadataSize, JSONMetadataSize);
  memcpy(&MetadataSize, MetadataSizeBuf->data, JSONMetadataSize);
// elog(LOG, "[Db721TableRowCount] MetadataSize: %u", MetadataSize);

  MetadataBuf = ReadFromFile(tableFile, tableFileSize - JSONMetadataSize - MetadataSize, MetadataSize);
  MetaDataJson = nlohmann::json::parse(MetadataBuf->data);
  columnName = MetaDataJson["Columns"].begin().key();
  for (auto block_iter = MetaDataJson["Columns"][columnName]["block_stats"].begin();
       block_iter != MetaDataJson["Columns"][columnName]["block_stats"].end(); block_iter++) {
    totalRowCount += static_cast<uint64>(block_iter.value()["num"]);
  }

  FreeFile(tableFile);
  return totalRowCount;
}

