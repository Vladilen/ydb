#include "otel_logs_metrics.h"

#include <util/generic/utility.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

#include <algorithm>
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

/// Same as Go `log_ydb_bulk_payload_bytes` Distribution (powers of two from 1KiB to 512GiB).
static constexpr ui64 PayloadBytesEdges[] = {
    1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304,
    8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912};

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

void TPrometheusMetrics::ObserveBulkUpsertPayloadBytes(ui64 arrowWireBytes) {
    ++BulkPayloadBytesCount_;
    BulkPayloadBytesSum_ += arrowWireBytes;
    for (size_t i = 0; i < 20; ++i) {
        if (arrowWireBytes <= PayloadBytesEdges[i]) {
            ++BulkPayloadBytesBuckets_[i];
        }
    }
    ++BulkPayloadBytesInf_;
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

template <size_t N>
void TPrometheusMetrics::RenderHistogramLe(
    TStringStream& ss,
    TStringBuf resourceLabelsInner,
    TStringBuf name,
    TStringBuf help,
    const std::array<ui64, N>& edges,
    const std::array<std::atomic<ui64>, N>& buckets,
    const std::atomic<ui64>& infCount,
    const std::atomic<ui64>& sumValue,
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
       << name << "_sum{" << resourceLabelsInner << "} " << sumValue.load(std::memory_order_relaxed) << "\n"
       << name << "_count{" << resourceLabelsInner << "} " << count.load(std::memory_order_relaxed) << "\n";
}

template void TPrometheusMetrics::RenderHistogramLe<11>(
    TStringStream&, TStringBuf, TStringBuf, TStringBuf,
    const std::array<ui64, 11>&, const std::array<std::atomic<ui64>, 11>&,
    const std::atomic<ui64>&, const std::atomic<ui64>&, const std::atomic<ui64>&) const;
template void TPrometheusMetrics::RenderHistogramLe<20>(
    TStringStream&, TStringBuf, TStringBuf, TStringBuf,
    const std::array<ui64, 20>&, const std::array<std::atomic<ui64>, 20>&,
    const std::atomic<ui64>&, const std::atomic<ui64>&, const std::atomic<ui64>&) const;

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
       << "# HELP otel_logs_to_ydb_log_bytes_ingest_offer_total OTLP wire bytes offered to ingest queue after routable wire precheck (if enabled), immediately before TryPush; includes batches rejected when queue is full\n"
       << "# TYPE otel_logs_to_ydb_log_bytes_ingest_offer_total counter\n"
       << "otel_logs_to_ydb_log_bytes_ingest_offer_total{" << rl << "} " << LogBytesIngestOffer_.load() << "\n"
       << "# HELP otel_logs_to_ydb_log_bytes_pipeline_in_total OTLP wire bytes counted in ingest worker after successful parse (Buf.Length per dequeued batch)\n"
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
       << "# HELP otel_logs_to_ydb_log_bytes_ydb_written_total Arrow BulkUpsert wire bytes (schema IPC + data IPC per successful chunk)\n"
       << "# TYPE otel_logs_to_ydb_log_bytes_ydb_written_total counter\n"
       << "otel_logs_to_ydb_log_bytes_ydb_written_total{" << rl << "} " << LogBytesYdbWritten_.load() << "\n"

       << "# HELP otel_logs_to_ydb_grpc_export_rpc_inflight Concurrent gRPC Export handlers before response\n"
       << "# TYPE otel_logs_to_ydb_grpc_export_rpc_inflight gauge\n"
       << "otel_logs_to_ydb_grpc_export_rpc_inflight{" << rl << "} " << GrpcExportRpcInflight_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_queue_depth Wire batches waiting for ingest workers (0..capacity)\n"
       << "# TYPE otel_logs_to_ydb_ingest_queue_depth gauge\n"
       << "otel_logs_to_ydb_ingest_queue_depth{" << rl << "} " << IngestQueueDepth_.load() << "\n"
       // Disabled temporarily (suspected interaction with ingest hot path); re-enable in otel_logs_metrics.cpp when stable.
       // << "# HELP otel_logs_to_ydb_ingest_queue_capacity Max ingest queue size (ingest_queue_max)\n"
       // << "# TYPE otel_logs_to_ydb_ingest_queue_capacity gauge\n"
       // << "otel_logs_to_ydb_ingest_queue_capacity{" << rl << "} " << IngestQueueCapacity_.load() << "\n"
       // << "# HELP otel_logs_to_ydb_ingest_workers_total Ingest worker threads (ingest_workers)\n"
       // << "# TYPE otel_logs_to_ydb_ingest_workers_total gauge\n"
       // << "otel_logs_to_ydb_ingest_workers_total{" << rl << "} " << IngestWorkersTotal_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_workers_busy Ingest workers in wire/proto parse (not shard-lock wait; idle only in WaitPop)\n"
       << "# TYPE otel_logs_to_ydb_ingest_workers_busy gauge\n"
       << "otel_logs_to_ydb_ingest_workers_busy{" << rl << "} " << IngestWorkersBusy_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_workers_waiting_shard_lock Ingest workers waiting for TShardBuffer::Mu (append retry)\n"
       << "# TYPE otel_logs_to_ydb_ingest_workers_waiting_shard_lock gauge\n"
       << "otel_logs_to_ydb_ingest_workers_waiting_shard_lock{" << rl << "} " << IngestWorkersWaitingShardLock_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_worker_exceptions_total Uncaught exceptions in ingest WorkerLoop (recovered)\n"
       << "# TYPE otel_logs_to_ydb_ingest_worker_exceptions_total counter\n"
       << "otel_logs_to_ydb_ingest_worker_exceptions_total{" << rl << "} " << IngestWorkerExceptions_.load() << "\n"
       << "# HELP otel_logs_to_ydb_flush_worker_exceptions_total Uncaught exceptions in YDB flush WorkerLoop (recovered)\n"
       << "# TYPE otel_logs_to_ydb_flush_worker_exceptions_total counter\n"
       << "otel_logs_to_ydb_flush_worker_exceptions_total{" << rl << "} " << FlushWorkerExceptions_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_shard_lock_timeouts_total Shard buffer lock try_lock_for timeouts (append retry)\n"
       << "# TYPE otel_logs_to_ydb_ingest_shard_lock_timeouts_total counter\n"
       << "otel_logs_to_ydb_ingest_shard_lock_timeouts_total{" << rl << "} " << IngestShardLockTimeouts_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_shard_lock_give_up_total Parsed batches dropped after shard lock retries exhausted\n"
       << "# TYPE otel_logs_to_ydb_ingest_shard_lock_give_up_total counter\n"
       << "otel_logs_to_ydb_ingest_shard_lock_give_up_total{" << rl << "} " << IngestShardLockGiveUp_.load() << "\n"
       << "# HELP otel_logs_to_ydb_flush_enqueue_timeout_total Flush queue enqueue timed out; rows rolled back to shard buffer\n"
       << "# TYPE otel_logs_to_ydb_flush_enqueue_timeout_total counter\n"
       << "otel_logs_to_ydb_flush_enqueue_timeout_total{" << rl << "} " << FlushEnqueueTimeout_.load() << "\n"
       << "# HELP otel_logs_to_ydb_flush_enqueue_rejected_total Flush queue full; ingest did not block, rows returned to shard buffer\n"
       << "# TYPE otel_logs_to_ydb_flush_enqueue_rejected_total counter\n"
       << "otel_logs_to_ydb_flush_enqueue_rejected_total{" << rl << "} " << FlushEnqueueRejected_.load() << "\n"
       << "# HELP otel_logs_to_ydb_log_rows_flush_queue_dropped_total Log rows dropped because flush queue was full and flush_queue_drop_on_full=true\n"
       << "# TYPE otel_logs_to_ydb_log_rows_flush_queue_dropped_total counter\n"
       << "otel_logs_to_ydb_log_rows_flush_queue_dropped_total{" << rl << "} " << LogRowsFlushQueueDropped_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ingest_stall_detected_total Watchdog: saturated ingest with no pipeline_in progress\n"
       << "# TYPE otel_logs_to_ydb_ingest_stall_detected_total counter\n"
       << "otel_logs_to_ydb_ingest_stall_detected_total{" << rl << "} " << IngestStallDetected_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ydb_flush_queue_depth Shard flush chunks waiting for YDB writer threads\n"
       << "# TYPE otel_logs_to_ydb_ydb_flush_queue_depth gauge\n"
       << "otel_logs_to_ydb_ydb_flush_queue_depth{" << rl << "} " << YdbFlushQueueDepth_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ydb_flush_workers_busy YDB flush worker threads executing BulkUpsert\n"
       << "# TYPE otel_logs_to_ydb_ydb_flush_workers_busy gauge\n"
       << "otel_logs_to_ydb_ydb_flush_workers_busy{" << rl << "} " << YdbFlushWorkersBusy_.load() << "\n"
       << "# HELP otel_logs_to_ydb_ydb_bulk_write_inflight Active YDB BulkUpsert slots (ydb_max_concurrent_bulk)\n"
       << "# TYPE otel_logs_to_ydb_ydb_bulk_write_inflight gauge\n"
       << "otel_logs_to_ydb_ydb_bulk_write_inflight{" << rl << "} " << YdbBulkWriteInflight_.load() << "\n"
       // << "# HELP otel_logs_to_ydb_shard_buffer_log_rows Log rows buffered before shard flush to YDB\n"
       // << "# TYPE otel_logs_to_ydb_shard_buffer_log_rows gauge\n"
       // << "otel_logs_to_ydb_shard_buffer_log_rows{" << rl << "} " << ShardBufferLogRows_.load() << "\n"
       // << "# HELP otel_logs_to_ydb_shard_buffers_active Active (table, shard) shard buffers with pending rows\n"
       // << "# TYPE otel_logs_to_ydb_shard_buffers_active gauge\n"
       // << "otel_logs_to_ydb_shard_buffers_active{" << rl << "} " << ShardBuffersActive_.load() << "\n"

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

    RenderHistogramLe(
        ss,
        rl,
        TStringBuf{"otel_logs_to_ydb_bulk_upsert_rows"},
        TStringBuf{"Log rows per BulkUpsert chunk"},
        re,
        BulkRowsBuckets_,
        BulkRowsInf_,
        BulkRowsSum_,
        BulkRowsCount_);

    std::array<ui64, 20> payloadEdges{};
    for (size_t i = 0; i < 20; ++i) {
        payloadEdges[i] = PayloadBytesEdges[i];
    }
    RenderHistogramLe(
        ss,
        rl,
        TStringBuf{"otel_logs_to_ydb_bulk_upsert_payload_bytes"},
        TStringBuf{"Arrow IPC wire bytes per successful BulkUpsert chunk (schema+data IPC; same as Go log_ydb_bulk_payload_bytes)"},
        payloadEdges,
        BulkPayloadBytesBuckets_,
        BulkPayloadBytesInf_,
        BulkPayloadBytesSum_,
        BulkPayloadBytesCount_);

    return ss.Str();
}

} // namespace NColumnShard::NOtelLogsToYdb
