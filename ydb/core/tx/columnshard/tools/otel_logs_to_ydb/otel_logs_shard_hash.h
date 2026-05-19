#pragma once

#include "otel_logs_owned_row.h"

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>

namespace NColumnShard::NOtelLogsToYdb {

enum class ELogsPkSchema : int {
    PerService = 0,       // (timestamp, cluster, record_id)
    PerProjectHeap = 1,   // (timestamp, service, cluster, record_id)
    Dedicated = 2,        // (timestamp, record_id)
};

/// Partition hash (XXH64 seed 0) — same byte layout as Go `shardhash` for given schema.
ui64 HashPartitionKey(ELogsPkSchema schema, TInstant ts, TStringBuf a, TStringBuf b, TStringBuf c);

/// Maps hash to shard index [0, numShards), same as Go `ShardIndex`.
/// `numShards` must equal table `PARTITION_COUNT` (auto_create_partition_count_* in config).
int ShardIndexFromHash(ui64 h, int numShards);

/// Hash over PARTITION BY HASH columns using the same values as Arrow BulkUpsert (see `TOwnedLogRow`).
ui64 HashOwnedLogRow(ELogsPkSchema schema, const TOwnedLogRow& row);

} // namespace NColumnShard::NOtelLogsToYdb
