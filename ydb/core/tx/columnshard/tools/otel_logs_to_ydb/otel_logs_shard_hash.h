#pragma once

#include "otel_logs_owned_row.h"

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>

namespace NColumnShard::NOtelLogsToYdb {

enum class ELogsPkSchema : int {
    // Default schemas: no batch_id column (used for ydb_common/dedicated_logs_dir).
    PerService = 0,       // (timestamp, cluster, record_id)
    PerProjectHeap = 1,   // (timestamp, service, cluster, record_id)
    Dedicated = 2,        // (timestamp, record_id)
    // BatchPartitioned schemas: batch_id column present (used for ydb_*_logs_dir_batch_id).
    PerServiceBatchPartitioned = 3,       // (timestamp, cluster, batch_id, record_id)
    PerProjectHeapBatchPartitioned = 4,   // (timestamp, service, cluster, batch_id, record_id)
    DedicatedBatchPartitioned = 5,        // (timestamp, batch_id, record_id)
};

/// Maps hash to shard index [0, numShards), same as Go `ShardIndex`.
/// `numShards` must equal table `PARTITION_COUNT` (auto_create_partition_count_* in config).
int ShardIndexFromHash(ui64 h, int numShards);

/// Hash over PARTITION BY HASH columns using the same values as Arrow BulkUpsert (see `TOwnedLogRow`).
ui64 HashOwnedLogRow(ELogsPkSchema schema, const TOwnedLogRow& row);

} // namespace NColumnShard::NOtelLogsToYdb
