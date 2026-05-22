#pragma once

#include "otel_logs_shard_hash.h"

#include <util/generic/string.h>

#include <util/generic/hash.h>

namespace NColumnShard::NOtelLogsToYdb {

struct TBuck {
    TString Table;
    int Shard = 0;
    ELogsPkSchema Schema = ELogsPkSchema::PerService;

    bool operator==(const TBuck& o) const noexcept {
        return Table == o.Table && Shard == o.Shard && Schema == o.Schema;
    }
};

struct TBuckHash {
    size_t operator()(const TBuck& k) const noexcept {
        return CombineHashes(
            THash<TString>()(k.Table),
            CombineHashes(THash<int>()(k.Shard), THash<int>()(static_cast<int>(k.Schema))));
    }
};

} // namespace NColumnShard::NOtelLogsToYdb
