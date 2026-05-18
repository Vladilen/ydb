#pragma once

#include <util/generic/string.h>
#include <util/stream/str.h>

#include <array>
#include <atomic>

namespace NColumnShard::NOtelLogsToYdb {

/// Prometheus text exposition (`otel_logs_to_ydb_*`). See METRICS_PARITY.md.
class TPrometheusMetrics {
public:
    void IncIngestAccepted() {
        ++IngestAccepted_;
    }
    void IncIngestRejectedQueueFull() {
        ++IngestRejectedQueueFull_;
    }
    void IncBulkOk() {
        ++BulkOk_;
    }
    void IncBulkFail() {
        ++BulkFail_;
    }
    void IncDdlRuns() {
        ++DdlRuns_;
    }

    void IncLogsBatchesStored() {
        ++LogsBatchesStored_;
    }
    void IncLogsBatchesErrors() {
        ++LogsBatchesErrors_;
    }

    void AddPipelineIn(ui64 logRows, ui64 protoBytes) {
        LogRowsPipelineIn_ += logRows;
        LogBytesPipelineIn_ += protoBytes;
    }
    void AddRefused(ui64 logRows, ui64 protoBytes) {
        LogRowsRefused_ += logRows;
        LogBytesRefused_ += protoBytes;
    }
    void AddYdbWritten(ui64 logRows, ui64 approxBytes) {
        LogRowsYdbWritten_ += logRows;
        LogBytesYdbWritten_ += approxBytes;
    }

    void IncGrpcExportRpcInflight() {
        GrpcExportRpcInflight_.fetch_add(1, std::memory_order_relaxed);
    }
    void DecGrpcExportRpcInflight() {
        GrpcExportRpcInflight_.fetch_sub(1, std::memory_order_relaxed);
    }

    void SetIngestQueueDepth(size_t depth) {
        IngestQueueDepth_.store(static_cast<ui64>(depth), std::memory_order_relaxed);
    }

    /// Arrow encode only (SerializeLogsBulkArrow → wire buffers); same ms buckets as legacy combined histogram.
    void ObserveBulkArrowEncodeMs(ui64 durationMs);
    /// Concurrency slot wait + BulkUpsert + ExtractValueSync until RPC completes; same ms buckets as Go `logs_write_processing_time`.
    void ObserveBulkYdbRpcMs(ui64 durationMs);
    void ObserveBulkUpsertRows(ui64 rowCount);

    void AddGrpcExportRequestBytes(ui64 bytes) {
        GrpcExportRequestBytes_ += bytes;
    }

    TString RenderText() const;

private:
    /// Same bucket boundaries as Go `ydb_supplier` `logs_write_processing_time` (OpenCensus Distribution, ms).
    void RenderHistogramBulkDurationMs(
        TStringStream& ss,
        TStringBuf resourceLabelsInner,
        TStringBuf name,
        TStringBuf help,
        const std::array<ui64, 9>& edgesMs,
        const std::array<std::atomic<ui64>, 9>& buckets,
        const std::atomic<ui64>& infCount,
        const std::atomic<ui64>& sumMs,
        const std::atomic<ui64>& count) const;

    /// Same bucket boundaries as Go `ydb_supplier` `log_rows_per_batch`.
    void RenderHistogramRows(
        TStringStream& ss,
        TStringBuf resourceLabelsInner,
        TStringBuf name,
        TStringBuf help,
        const std::array<ui64, 11>& edges,
        const std::array<std::atomic<ui64>, 11>& buckets,
        const std::atomic<ui64>& infCount,
        const std::atomic<ui64>& sumRows,
        const std::atomic<ui64>& count) const;

    std::atomic<ui64> IngestAccepted_{0};
    std::atomic<ui64> IngestRejectedQueueFull_{0};
    std::atomic<ui64> BulkOk_{0};
    std::atomic<ui64> BulkFail_{0};
    std::atomic<ui64> DdlRuns_{0};

    std::atomic<ui64> LogsBatchesStored_{0};
    std::atomic<ui64> LogsBatchesErrors_{0};

    std::atomic<ui64> LogRowsPipelineIn_{0};
    std::atomic<ui64> LogBytesPipelineIn_{0};
    std::atomic<ui64> LogRowsRefused_{0};
    std::atomic<ui64> LogBytesRefused_{0};
    std::atomic<ui64> LogRowsYdbWritten_{0};
    std::atomic<ui64> LogBytesYdbWritten_{0};

    std::atomic<i64> GrpcExportRpcInflight_{0};
    std::atomic<ui64> IngestQueueDepth_{0};

    std::atomic<ui64> GrpcExportRequestBytes_{0};

    std::array<std::atomic<ui64>, 9> BulkArrowEncodeBuckets_{};
    std::atomic<ui64> BulkArrowEncodeInf_{0};
    std::atomic<ui64> BulkArrowEncodeSumMs_{0};
    std::atomic<ui64> BulkArrowEncodeCount_{0};

    std::array<std::atomic<ui64>, 9> BulkYdbRpcBuckets_{};
    std::atomic<ui64> BulkYdbRpcInf_{0};
    std::atomic<ui64> BulkYdbRpcSumMs_{0};
    std::atomic<ui64> BulkYdbRpcCount_{0};

    std::array<std::atomic<ui64>, 11> BulkRowsBuckets_{};
    std::atomic<ui64> BulkRowsInf_{0};
    std::atomic<ui64> BulkRowsSum_{0};
    std::atomic<ui64> BulkRowsCount_{0};
};

} // namespace NColumnShard::NOtelLogsToYdb
