#include "otel_logs_metrics.h"

#include <util/stream/str.h>
#include <util/string/builder.h>

#include <fstream>

#if defined(__linux__)
#include <unistd.h>
#endif
#include <sys/resource.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

/// Same as Go `processors/ydb-supplier/metrics.go` view.Distribution(50, 100, 250, 500, 750, 1000, 2000, 5000, 10000) — 9 finite upper bounds.
static constexpr std::array<ui64, 9> DurationEdgesMs{50, 100, 250, 500, 750, 1000, 2000, 5000, 10000};

/// Same as Go `view.Distribution(100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200, 100000)`.
static constexpr ui64 RowEdges[] = {100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200, 100000};

/// Cumulative user+system CPU seconds for this process (`getrusage`), same idea as `otelcol_process_cpu_seconds`.
static double ReadSelfCpuSeconds() {
    struct rusage ru {};
    if (getrusage(RUSAGE_SELF, &ru) != 0) {
        return 0.0;
    }
    const double u = static_cast<double>(ru.ru_utime.tv_sec) + static_cast<double>(ru.ru_utime.tv_usec) * 1e-6;
    const double s = static_cast<double>(ru.ru_stime.tv_sec) + static_cast<double>(ru.ru_stime.tv_usec) * 1e-6;
    return u + s;
}

/// Current RSS in bytes. Linux: `/proc/self/statm` resident pages × page size (same idea as `otelcol_process_memory_rss`).
static ui64 ReadSelfRssBytes() {
#if defined(__linux__)
    std::ifstream in("/proc/self/statm");
    if (!in) {
        return 0;
    }
    ui64 sizePages = 0;
    ui64 residentPages = 0;
    in >> sizePages >> residentPages;
    if (!in) {
        return 0;
    }
    const long pz = sysconf(_SC_PAGESIZE);
    if (pz <= 0) {
        return 0;
    }
    return residentPages * static_cast<ui64>(pz);
#else
    return 0;
#endif
}

/// For dashboards / joins with collector-style series (`service_name` only).
static TStringBuf PrometheusResourceLabelsInner() {
    return TStringBuf("service_name=\"otel-logs-to-ydb\"");
}

} // namespace

void TPrometheusMetrics::ObserveBulkArrowEncodeMs(ui64 durationMs) {
    BulkArrowEncodeSumMs_ += durationMs;
    ++BulkArrowEncodeCount_;
    for (size_t i = 0; i < DurationEdgesMs.size(); ++i) {
        if (durationMs <= DurationEdgesMs[i]) {
            ++BulkArrowEncodeBuckets_[i];
        }
    }
    ++BulkArrowEncodeInf_;
}

void TPrometheusMetrics::ObserveBulkYdbRpcMs(ui64 durationMs) {
    BulkYdbRpcSumMs_ += durationMs;
    ++BulkYdbRpcCount_;
    for (size_t i = 0; i < DurationEdgesMs.size(); ++i) {
        if (durationMs <= DurationEdgesMs[i]) {
            ++BulkYdbRpcBuckets_[i];
        }
    }
    ++BulkYdbRpcInf_;
}

void TPrometheusMetrics::ObserveBulkUpsertRows(ui64 rowCount) {
    ++BulkRowsCount_;
    BulkRowsSum_ += rowCount;
    for (size_t i = 0; i < 11; ++i) {
        if (rowCount <= RowEdges[i]) {
            ++BulkRowsBuckets_[i];
        }
    }
    ++BulkRowsInf_;
}

void TPrometheusMetrics::RenderHistogramBulkDurationMs(
    TStringStream& ss,
    TStringBuf resourceLabelsInner,
    TStringBuf name,
    TStringBuf help,
    const std::array<ui64, 9>& edgesMs,
    const std::array<std::atomic<ui64>, 9>& buckets,
    const std::atomic<ui64>& infCount,
    const std::atomic<ui64>& sumMs,
    const std::atomic<ui64>& count) const
{
    ss << "# HELP " << name << " " << help << "\n"
       << "# TYPE " << name << " histogram\n";
    for (size_t i = 0; i < edgesMs.size(); ++i) {
        ss << name << "_bucket{" << resourceLabelsInner << ",le=\"" << edgesMs[i] << "\"} "
           << buckets[i].load(std::memory_order_relaxed) << "\n";
    }
    ss << name << "_bucket{" << resourceLabelsInner << ",le=\"+Inf\"} "
       << infCount.load(std::memory_order_relaxed) << "\n";
    const ui64 sm = sumMs.load(std::memory_order_relaxed);
    const ui64 c = count.load(std::memory_order_relaxed);
    ss << name << "_sum{" << resourceLabelsInner << "} " << (c ? static_cast<double>(sm) : 0.0) << "\n"
       << name << "_count{" << resourceLabelsInner << "} " << c << "\n";
}

void TPrometheusMetrics::RenderHistogramRows(
    TStringStream& ss,
    TStringBuf resourceLabelsInner,
    TStringBuf name,
    TStringBuf help,
    const std::array<ui64, 11>& edges,
    const std::array<std::atomic<ui64>, 11>& buckets,
    const std::atomic<ui64>& infCount,
    const std::atomic<ui64>& sumRows,
    const std::atomic<ui64>& count) const
{
    ss << "# HELP " << name << " " << help << "\n"
       << "# TYPE " << name << " histogram\n";
    for (size_t i = 0; i < edges.size(); ++i) {
        ss << name << "_bucket{" << resourceLabelsInner << ",le=\"" << edges[i] << "\"} "
           << buckets[i].load(std::memory_order_relaxed) << "\n";
    }
    ss << name << "_bucket{" << resourceLabelsInner << ",le=\"+Inf\"} "
       << infCount.load(std::memory_order_relaxed) << "\n"
       << name << "_sum{" << resourceLabelsInner << "} " << sumRows.load(std::memory_order_relaxed) << "\n"
       << name << "_count{" << resourceLabelsInner << "} " << count.load(std::memory_order_relaxed) << "\n";
}

TString TPrometheusMetrics::RenderText() const {
    const TStringBuf rl = PrometheusResourceLabelsInner();
    TStringStream ss;
    ss << "# HELP otel_logs_to_ydb_ingest_accepted_total Accepted OTLP batches enqueued\n"
       << "# TYPE otel_logs_to_ydb_ingest_accepted_total counter\n"
       << "otel_logs_to_ydb_ingest_accepted_total{" << rl << "} " << IngestAccepted_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_rejected_queue_full_total Rejected with RESOURCE_EXHAUSTED\n"
       << "# TYPE otel_logs_to_ydb_ingest_rejected_queue_full_total counter\n"
       << "otel_logs_to_ydb_ingest_rejected_queue_full_total{" << rl << "} " << IngestRejectedQueueFull_.load() << "\n"
       << "# HELP otel_logs_to_ydb_bulk_upsert_ok_total Successful BulkUpsert RPC calls\n"
       << "# TYPE otel_logs_to_ydb_bulk_upsert_ok_total counter\n"
       << "otel_logs_to_ydb_bulk_upsert_ok_total{" << rl << "} " << BulkOk_.load() << "\n"
       << "# HELP otel_logs_to_ydb_bulk_upsert_fail_total Failed BulkUpsert after retries\n"
       << "# TYPE otel_logs_to_ydb_bulk_upsert_fail_total counter\n"
       << "otel_logs_to_ydb_bulk_upsert_fail_total{" << rl << "} " << BulkFail_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ddl_runs_total Auto-DDL ensure runs\n"
       << "# TYPE otel_logs_to_ydb_ddl_runs_total counter\n"
       << "otel_logs_to_ydb_ddl_runs_total{" << rl << "} " << DdlRuns_.load() << "\n"

       << "# HELP otel_logs_to_ydb_logs_batches_stored_total Successful BulkUpsert batches (chunks)\n"
       << "# TYPE otel_logs_to_ydb_logs_batches_stored_total counter\n"
       << "otel_logs_to_ydb_logs_batches_stored_total{" << rl << "} " << LogsBatchesStored_.load() << "\n"
       << "# HELP otel_logs_to_ydb_logs_batches_errors_total Failed BulkUpsert batches after retries\n"
       << "# TYPE otel_logs_to_ydb_logs_batches_errors_total counter\n"
       << "otel_logs_to_ydb_logs_batches_errors_total{" << rl << "} " << LogsBatchesErrors_.load() << "\n"

       << "# HELP otel_logs_to_ydb_log_rows_pipeline_in_total Log records enqueued to internal pipeline\n"
       << "# TYPE otel_logs_to_ydb_log_rows_pipeline_in_total counter\n"
       << "otel_logs_to_ydb_log_rows_pipeline_in_total{" << rl << "} " << LogRowsPipelineIn_.load() << "\n"
       << "# HELP otel_logs_to_ydb_log_bytes_pipeline_in_total OTLP protobuf bytes enqueued (ByteSizeLong)\n"
       << "# TYPE otel_logs_to_ydb_log_bytes_pipeline_in_total counter\n"
       << "otel_logs_to_ydb_log_bytes_pipeline_in_total{" << rl << "} " << LogBytesPipelineIn_.load() << "\n"
       << "# HELP otel_logs_to_ydb_log_rows_refused_total Log records in batches rejected (queue full)\n"
       << "# TYPE otel_logs_to_ydb_log_rows_refused_total counter\n"
       << "otel_logs_to_ydb_log_rows_refused_total{" << rl << "} " << LogRowsRefused_.load() << "\n"
       << "# HELP otel_logs_to_ydb_log_bytes_refused_total Protobuf bytes in batches rejected (queue full)\n"
       << "# TYPE otel_logs_to_ydb_log_bytes_refused_total counter\n"
       << "otel_logs_to_ydb_log_bytes_refused_total{" << rl << "} " << LogBytesRefused_.load() << "\n"
       << "# HELP otel_logs_to_ydb_log_rows_ydb_written_total Log rows successfully written via BulkUpsert\n"
       << "# TYPE otel_logs_to_ydb_log_rows_ydb_written_total counter\n"
       << "otel_logs_to_ydb_log_rows_ydb_written_total{" << rl << "} " << LogRowsYdbWritten_.load() << "\n"
       << "# HELP otel_logs_to_ydb_log_bytes_ydb_written_total Approximate payload bytes written (heuristic)\n"
       << "# TYPE otel_logs_to_ydb_log_bytes_ydb_written_total counter\n"
       << "otel_logs_to_ydb_log_bytes_ydb_written_total{" << rl << "} " << LogBytesYdbWritten_.load() << "\n"

       << "# HELP otel_logs_to_ydb_grpc_export_rpc_inflight Concurrent gRPC Export handlers before response\n"
       << "# TYPE otel_logs_to_ydb_grpc_export_rpc_inflight gauge\n"
       << "otel_logs_to_ydb_grpc_export_rpc_inflight{" << rl << "} " << GrpcExportRpcInflight_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_queue_depth Current depth of internal ingest queue\n"
       << "# TYPE otel_logs_to_ydb_ingest_queue_depth gauge\n"
       << "otel_logs_to_ydb_ingest_queue_depth{" << rl << "} " << IngestQueueDepth_.load() << "\n"

       << "# HELP otel_logs_to_ydb_grpc_export_request_bytes_total ByteSizeLong of each ExportLogsServiceRequest (uncompressed proto)\n"
       << "# TYPE otel_logs_to_ydb_grpc_export_request_bytes_total counter\n"
       << "otel_logs_to_ydb_grpc_export_request_bytes_total{" << rl << "} " << GrpcExportRequestBytes_.load() << "\n"

       << "# HELP otel_logs_to_ydb_process_cpu_seconds Total CPU user and system time in seconds for this process (getrusage; same semantics as otelcol_process_cpu_seconds)\n"
       << "# TYPE otel_logs_to_ydb_process_cpu_seconds counter\n"
       << "otel_logs_to_ydb_process_cpu_seconds{" << rl << "} " << ReadSelfCpuSeconds() << "\n"
       << "# HELP otel_logs_to_ydb_process_memory_rss Resident set size in bytes (Linux /proc/self/statm; 0 on non-Linux; same semantics as otelcol_process_memory_rss)\n"
       << "# TYPE otel_logs_to_ydb_process_memory_rss gauge\n"
       << "otel_logs_to_ydb_process_memory_rss{" << rl << "} " << ReadSelfRssBytes() << "\n";

    std::array<ui64, 9> deMs{};
    for (size_t i = 0; i < DurationEdgesMs.size(); ++i) {
        deMs[i] = DurationEdgesMs[i];
    }
    std::array<ui64, 11> re{};
    for (size_t i = 0; i < 11; ++i) {
        re[i] = RowEdges[i];
    }

    RenderHistogramBulkDurationMs(
        ss,
        rl,
        TStringBuf{"otel_logs_to_ydb_bulk_arrow_encode_duration_milliseconds"},
        TStringBuf{"Arrow IPC encode for logs BulkUpsert payload before send (ms; buckets 50..10000 like ydb_supplier)"},
        deMs,
        BulkArrowEncodeBuckets_,
        BulkArrowEncodeInf_,
        BulkArrowEncodeSumMs_,
        BulkArrowEncodeCount_);

    RenderHistogramBulkDurationMs(
        ss,
        rl,
        TStringBuf{"otel_logs_to_ydb_bulk_ydb_rpc_duration_milliseconds"},
        TStringBuf{
            "Bulk write slot wait + YDB BulkUpsert RPC until response (ms; buckets 50..10000; comparable to Go "
            "logs_write_processing_time)"},
        deMs,
        BulkYdbRpcBuckets_,
        BulkYdbRpcInf_,
        BulkYdbRpcSumMs_,
        BulkYdbRpcCount_);

    RenderHistogramRows(
        ss,
        rl,
        TStringBuf{"otel_logs_to_ydb_bulk_upsert_rows"},
        TStringBuf{"Log rows per BulkUpsert chunk"},
        re,
        BulkRowsBuckets_,
        BulkRowsInf_,
        BulkRowsSum_,
        BulkRowsCount_);

    return ss.Str();
}

} // namespace NColumnShard::NOtelLogsToYdb
