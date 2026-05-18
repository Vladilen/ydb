#pragma once

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
int ShardIndexFromHash(ui64 h, int numShards);

} // namespace NColumnShard::NOtelLogsToYdb
