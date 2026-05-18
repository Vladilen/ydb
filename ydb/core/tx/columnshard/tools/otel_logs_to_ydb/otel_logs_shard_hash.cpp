#include "otel_logs_shard_hash.h"

/* Stack-allocated XXH64_state_t is only visible with this define (xxhash 0.8+). */
#define XXH_STATIC_LINKING_ONLY
#include <contrib/libs/xxhash/xxhash.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

void AppendTs(XXH64_state_t* st, TInstant ts) {
    const i64 us = ts.MicroSeconds();
    ui64 le = static_cast<ui64>(us);
    unsigned char b[8];
    for (size_t i = 0; i < 8; ++i) {
        b[i] = static_cast<unsigned char>((le >> (8 * i)) & 0xff);
    }
    XXH64_update(st, b, sizeof(b));
}

void AppendUtf8(XXH64_state_t* st, TStringBuf s) {
    if (!s.empty()) {
        XXH64_update(st, s.data(), s.size());
    }
}

} // namespace

ui64 HashPartitionKey(ELogsPkSchema schema, TInstant ts, TStringBuf a, TStringBuf b, TStringBuf c) {
    XXH64_state_t st;
    XXH64_reset(&st, 0);
    switch (schema) {
        case ELogsPkSchema::PerService:
            AppendTs(&st, ts);
            AppendUtf8(&st, a);
            AppendUtf8(&st, b);
            break;
        case ELogsPkSchema::PerProjectHeap:
            AppendTs(&st, ts);
            AppendUtf8(&st, a);
            AppendUtf8(&st, b);
            AppendUtf8(&st, c);
            break;
        case ELogsPkSchema::Dedicated:
            AppendTs(&st, ts);
            AppendUtf8(&st, a);
            break;
    }
    return XXH64_digest(&st);
}

int ShardIndexFromHash(ui64 h, int numShards) {
    if (numShards <= 1) {
        return 0;
    }
    const ui64 n = static_cast<ui64>(numShards);
    const ui64 denom = (~ui64(0)) / n;
    if (denom == 0) {
        return 0;
    }
    int idx = static_cast<int>(h / denom);
    if (idx >= numShards) {
        idx = numShards - 1;
    }
    return idx;
}

} // namespace NColumnShard::NOtelLogsToYdb
