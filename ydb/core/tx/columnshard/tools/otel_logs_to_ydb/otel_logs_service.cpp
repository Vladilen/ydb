#include "otel_logs_service.h"

#include "health_check_server.h"
#include "otel_logs_ddl.h"
#include "otel_logs_metrics.h"
#include "otel_logs_owned_row.h"
#include "otel_logs_routing.h"
#include "otel_logs_shard_hash.h"
#include "otel_logs_validator.h"
#include "otel_logs_buck.h"
#include "otel_logs_json.h"
#include "otel_logs_wire_ingest.h"
#include "otel_logs_wire_routable.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <arrow/api.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h>
#include <opentelemetry/proto/common/v1/common.pb.h>
#include <opentelemetry/proto/logs/v1/logs.pb.h>
#include <opentelemetry/proto/resource/v1/resource.pb.h>

#include <google/protobuf/arena.h>

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/impl/serialization_traits.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>

#include <util/charset/utf8.h>
#include <util/charset/wide.h>
#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/network/init.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/hex.h>

#include <condition_variable>
#include <chrono>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <utility>

namespace NColumnShard::NOtelLogsToYdb {

using namespace opentelemetry::proto::common::v1;
using namespace opentelemetry::proto::resource::v1;
using namespace opentelemetry::proto::logs::v1;
namespace ologs = opentelemetry::proto::collector::logs::v1;

constexpr TStringBuf AttrProject = "project";
constexpr TStringBuf AttrCluster = "cluster";
constexpr TStringBuf AttrService = "service";
constexpr TStringBuf AttrServiceName = "service.name";
constexpr TStringBuf AttrK8DeploymentName = "k8s.deployment.name";
constexpr TStringBuf AttrK8NamespaceName = "k8s.namespace.name";
constexpr TStringBuf AttrRecordId = "record_id";
constexpr TStringBuf AttrRequestId = "request_id";

bool IsPerProjectLayout(const TServerConfig& cfg) {
    return cfg.TableLayout == "per_project";
}

const THashSet<TStringBuf>& ResourceLabelAttrKeys() {
    static const THashSet<TStringBuf> keys = {
        AttrProject,
        AttrCluster,
        AttrServiceName,
        AttrService,
        TStringBuf("hostname"),
        TStringBuf("host.name"),
        TStringBuf("host"),
    };
    return keys;
}

const THashSet<TStringBuf>& LogLabelAttrKeys() {
    static const THashSet<TStringBuf> keys = {
        AttrRequestId,
        AttrRecordId,
    };
    return keys;
}

TString Utf8Safe(TStringBuf raw) {
    if (IsUtf(raw)) {
        return TString{raw};
    }
    TUtf16String wide = UTF8ToWide<true>(raw.Data(), raw.Size());
    return WideToUTF8(TWtringBuf(wide));
}

TString Utf8Safe(TString raw) {
    if (IsUtf(raw)) {
        return raw;
    }
    TUtf16String wide = UTF8ToWide<true>(raw.data(), raw.size());
    return WideToUTF8(TWtringBuf(wide));
}

void AnyValueToString(const AnyValue& v, TString* out) {
    switch (v.value_case()) {
        case AnyValue::kStringValue:
            *out = TString{v.string_value()};
            return;
        case AnyValue::kBoolValue:
            *out = v.bool_value() ? TString{"true"} : TString{"false"};
            return;
        case AnyValue::kIntValue:
            *out = ToString(v.int_value());
            return;
        case AnyValue::kDoubleValue:
            *out = ToString(v.double_value());
            return;
        case AnyValue::kBytesValue:
            *out = TString{v.bytes_value()};
            return;
        default:
            *out = TString{"<non_scalar>"};
            return;
    }
}

void AnyValueToJsonValue(const AnyValue& v, NJson::TJsonValue* jv);

void KeyValueListToJson(const KeyValueList& list, NJson::TJsonValue* jv) {
    jv->SetType(NJson::JSON_MAP);
    for (const KeyValue& kv : list.values()) {
        NJson::TJsonValue val;
        AnyValueToJsonValue(kv.value(), &val);
        jv->InsertValue(TString{kv.key()}, std::move(val));
    }
}

void ArrayValueToJson(const ArrayValue& arr, NJson::TJsonValue* jv) {
    jv->SetType(NJson::JSON_ARRAY);
    for (const AnyValue& el : arr.values()) {
        NJson::TJsonValue item;
        AnyValueToJsonValue(el, &item);
        jv->AppendValue(std::move(item));
    }
}

void AnyValueToJsonValue(const AnyValue& v, NJson::TJsonValue* jv) {
    switch (v.value_case()) {
        case AnyValue::kStringValue:
            *jv = TString{v.string_value()};
            break;
        case AnyValue::kBoolValue:
            *jv = v.bool_value();
            break;
        case AnyValue::kIntValue:
            *jv = v.int_value();
            break;
        case AnyValue::kDoubleValue:
            *jv = v.double_value();
            break;
        case AnyValue::kBytesValue:
            *jv = TString{v.bytes_value()};
            break;
        case AnyValue::kArrayValue:
            ArrayValueToJson(v.array_value(), jv);
            break;
        case AnyValue::kKvlistValue:
            KeyValueListToJson(v.kvlist_value(), jv);
            break;
        default:
            *jv = TString{};
            break;
    }
}

TString BodyToMessageUtf8(const AnyValue& body) {
    TString raw;
    switch (body.value_case()) {
        case AnyValue::kStringValue:
            raw = TString{body.string_value()};
            break;
        default: {
            NJson::TJsonValue jv;
            AnyValueToJsonValue(body, &jv);
            raw = NJson::WriteJson(&jv, false);
            break;
        }
    }
    return Utf8Safe(std::move(raw));
}

void ParseAttributes(
    const google::protobuf::RepeatedPtrField<KeyValue>& kvs,
    THashMap<TString, TString>* out)
{
    for (const KeyValue& kv : kvs) {
        TString val;
        AnyValueToString(kv.value(), &val);
        val = Utf8Safe(std::move(val));
        if (!val.empty()) {
            (*out)[Utf8Safe(TStringBuf(kv.key()))] = val;
        }
    }
}

struct TProjectService {
    TString Project;
    TString Service;
};

/// Resource fields parsed once per ResourceLogs (shared by all LogRecords in that resource).
struct TResourceRowCtx {
    TString Service;
    TString Cluster;
    THashMap<TString, TString> ResourceLabels;
    THashMap<TString, TString> ResourceMeta;
};

TProjectService ExtractProjectService(const Resource& resource) {
    THashMap<TString, TString> m;
    ParseAttributes(resource.attributes(), &m);
    TProjectService ps;
    if (auto it = m.find(TString{AttrProject}); it != m.end()) {
        ps.Project = it->second;
    }
    const TVector<TStringBuf> serviceKeys = {
        AttrService,
        AttrServiceName,
        AttrK8DeploymentName,
        AttrK8NamespaceName,
    };
    for (TStringBuf key : serviceKeys) {
        if (auto it = m.find(TString{key}); it != m.end() && !it->second.empty()) {
            ps.Service = it->second;
            break;
        }
    }
    return ps;
}

void SplitResourceAttributes(
    const Resource& resource,
    TString* cluster,
    THashMap<TString, TString>* resourceLabels,
    THashMap<TString, TString>* resourceMeta)
{
    for (const KeyValue& kv : resource.attributes()) {
        TString key = Utf8Safe(TStringBuf(kv.key()));
        TString val;
        AnyValueToString(kv.value(), &val);
        val = Utf8Safe(std::move(val));
        if (val.empty()) {
            continue;
        }
        if (key == TString{AttrCluster}) {
            *cluster = val;
            continue;
        }
        if (ResourceLabelAttrKeys().contains(key)) {
            (*resourceLabels)[key] = val;
        } else {
            (*resourceMeta)[key] = val;
        }
    }
}

void SplitLogAttributes(
    const google::protobuf::RepeatedPtrField<KeyValue>& kvs,
    TString* recordId,
    THashMap<TString, TString>* logLabels,
    THashMap<TString, TString>* logMeta)
{
    for (const KeyValue& kv : kvs) {
        TString key = Utf8Safe(TStringBuf(kv.key()));
        TString val;
        AnyValueToString(kv.value(), &val);
        val = Utf8Safe(std::move(val));
        if (val.empty()) {
            continue;
        }
        if (key == TString{AttrRecordId}) {
            *recordId = val;
            continue;
        }
        if (LogLabelAttrKeys().contains(key)) {
            (*logLabels)[key] = val;
        } else {
            (*logMeta)[key] = val;
        }
    }
}

static TResourceRowCtx BuildResourceRowCtx(const Resource& resource, const TProjectService& ps) {
    TResourceRowCtx ctx;
    ctx.Service = ps.Service.empty() ? TString{"_unknown"} : ps.Service;
    ctx.Service = Utf8Safe(std::move(ctx.Service));
    SplitResourceAttributes(resource, &ctx.Cluster, &ctx.ResourceLabels, &ctx.ResourceMeta);
    return ctx;
}

TInstant TimestampFromNanos(ui64 ns) {
    if (!ns) {
        return TInstant::Now();
    }
    const ui64 us = ns / 1000;
    return TInstant::MicroSeconds(us);
}

TString TraceHex(TStringBuf id) {
    if (id.size() == 16 || id.size() == 8) {
        return HexEncode(id.Data(), id.size());
    }
    return {};
}

class TYdbBulkWriteLimiter {
public:
    explicit TYdbBulkWriteLimiter(int maxConcurrent, TPrometheusMetrics* metrics = nullptr)
        : Max_(Max(1, maxConcurrent))
        , M_(metrics)
    {
    }

    class TSlot {
    public:
        explicit TSlot(TYdbBulkWriteLimiter& owner)
            : Owner_(&owner)
        {
            Owner_->Acquire();
        }

        TSlot(const TSlot&) = delete;
        TSlot& operator=(const TSlot&) = delete;

        ~TSlot() {
            if (Owner_) {
                Owner_->Release();
            }
        }

    private:
        TYdbBulkWriteLimiter* Owner_;
    };

private:
    void PublishInflight(int n) {
        if (M_) {
            M_->SetYdbBulkWriteInflight(n);
        }
    }

    void Acquire() {
        int inflight = 0;
        {
            std::unique_lock<std::mutex> lock(Mu_);
            Cv_.wait(lock, [this] { return Active_ < Max_; });
            ++Active_;
            inflight = Active_;
        }
        // Gauge update outside Mu_: relaxed atomic only, cannot deadlock with shard buffers.
        PublishInflight(inflight);
    }

    void Release() {
        int inflight = 0;
        {
            std::unique_lock<std::mutex> lock(Mu_);
            --Active_;
            Y_ASSERT(Active_ >= 0);
            inflight = Active_;
            Cv_.notify_one();
        }
        PublishInflight(inflight);
    }

    std::mutex Mu_;
    std::condition_variable Cv_;
    int Active_ = 0;
    const int Max_;
    TPrometheusMetrics* M_ = nullptr;
};

namespace {

constexpr TStringBuf kExportMethod = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

struct TIngestWirePayload {
    grpc::ByteBuffer Buf;
};

const ologs::ExportLogsServiceRequest* ParseExportWire(
    grpc::ByteBuffer* buf,
    google::protobuf::Arena* arena)
{
    if (!buf || !buf->Valid()) {
        return nullptr;
    }
    auto* req = google::protobuf::Arena::CreateMessage<ologs::ExportLogsServiceRequest>(arena);
    if (!grpc::SerializationTraits<ologs::ExportLogsServiceRequest>::Deserialize(buf, req).ok()) {
        return nullptr;
    }
    return req;
}

bool IssuesContainAsciiInsensitive(TStringBuf haystack, TStringBuf needle) {
    if (needle.empty() || haystack.size() < needle.size()) {
        return false;
    }
    const size_t last = haystack.size() - needle.size();
    for (size_t i = 0; i <= last; ++i) {
        bool match = true;
        for (size_t j = 0; j < needle.size(); ++j) {
            const unsigned char a = static_cast<unsigned char>(haystack[i + j]);
            const unsigned char b = static_cast<unsigned char>(needle[j]);
            const unsigned char al = (a >= 'A' && a <= 'Z') ? static_cast<unsigned char>(a - 'A' + 'a') : a;
            const unsigned char bl = (b >= 'A' && b <= 'Z') ? static_cast<unsigned char>(b - 'A' + 'a') : b;
            if (al != bl) {
                match = false;
                break;
            }
        }
        if (match) {
            return true;
        }
    }
    return false;
}

/// Same triggers as Go `isYdbUnknownTableErr` (case-insensitive `unknown table` + scheme-ish markers).
bool ShouldTryDdlAfterBulkError(TStringBuf issues) {
    return IssuesContainAsciiInsensitive(issues, "unknown table") || issues.Contains("does not exist")
        || issues.Contains("Does not exist") || issues.Contains("SCHEME_ERROR") || issues.Contains("not found");
}

size_t CountExportLogRecords(const ologs::ExportLogsServiceRequest& req) {
    size_t n = 0;
    for (const ResourceLogs& rl : req.resource_logs()) {
        for (const ScopeLogs& sl : rl.scope_logs()) {
            n += static_cast<size_t>(sl.log_records_size());
        }
    }
    return n;
}

/// Same project / route / drop rules as `ProcessExport`.
bool ExportHasRoutableLogRows(const TServerConfig& cfg, const ologs::ExportLogsServiceRequest& request) {
    const bool perProject = IsPerProjectLayout(cfg);
    THashSet<TString> allowed;
    for (const std::string& p : cfg.AllowedProjects) {
        allowed.insert(TString{p.data(), p.size()});
    }
    const bool hasFilter = !allowed.empty();

    for (const ResourceLogs& rl : request.resource_logs()) {
        const TProjectService ps = ExtractProjectService(rl.resource());
        if (hasFilter && !allowed.contains(ps.Project)) {
            continue;
        }
        TString cluster;
        THashMap<TString, TString> rlab;
        THashMap<TString, TString> rmeta;
        SplitResourceAttributes(rl.resource(), &cluster, &rlab, &rmeta);
        const TRoutedTable route = ResolveLogsTable(cfg, ps.Project, ps.Service, cluster, perProject);
        if (route.Drop) {
            continue;
        }
        for (const ScopeLogs& sl : rl.scope_logs()) {
            if (sl.log_records_size() > 0) {
                return true;
            }
        }
    }
    return false;
}

class TIngestQueue {
public:
    explicit TIngestQueue(size_t cap, TPrometheusMetrics* metrics)
        : Cap_(Max<size_t>(size_t(1), cap))
        , M_(metrics)
    {
    }

    bool TryPush(TIngestWirePayload payload) {
        size_t depth = 0;
        {
            std::lock_guard<std::mutex> g(Mu_);
            if (Stopped_) {
                return false;
            }
            if (Q_.size() >= Cap_) {
                return false;
            }
            Q_.push_back(std::move(payload));
            depth = Q_.size();
        }
        if (M_) {
            M_->SetIngestQueueDepth(depth);
        }
        Cv_.notify_one();
        return true;
    }

    void Stop() {
        std::lock_guard<std::mutex> g(Mu_);
        Stopped_ = true;
        Cv_.notify_all();
    }

    bool WaitPop(TIngestWirePayload* out) {
        size_t depth = 0;
        {
            std::unique_lock<std::mutex> lk(Mu_);
            Cv_.wait(lk, [&] { return Stopped_ || !Q_.empty(); });
            if (Q_.empty()) {
                return false;
            }
            *out = std::move(Q_.front());
            Q_.pop_front();
            depth = Q_.size();
        }
        if (M_) {
            M_->SetIngestQueueDepth(depth);
        }
        return true;
    }

private:
    size_t Cap_;
    TPrometheusMetrics* M_ = nullptr;
    std::mutex Mu_;
    std::condition_variable Cv_;
    std::deque<TIngestWirePayload> Q_;
    bool Stopped_ = false;
};

class TIngestWorkerBusyGuard {
public:
    explicit TIngestWorkerBusyGuard(TPrometheusMetrics* m)
        : M_(m)
    {
        if (M_) {
            M_->IncIngestWorkersBusy();
        }
    }

    ~TIngestWorkerBusyGuard() {
        if (M_) {
            M_->DecIngestWorkersBusy();
        }
    }

    TIngestWorkerBusyGuard(const TIngestWorkerBusyGuard&) = delete;
    TIngestWorkerBusyGuard& operator=(const TIngestWorkerBusyGuard&) = delete;

private:
    TPrometheusMetrics* M_ = nullptr;
};

void ParseListenMetrics(const TString& listen, TString* host, ui16* port) {
    TStringBuf buf(listen);
    TStringBuf h;
    TStringBuf p;
    if (buf.TryRSplit(':', h, p)) {
        *host = h.empty() ? TString{} : TString{h};
        *port = FromString<ui16>(TString{p});
    } else if (!buf.empty()) {
        *host = TString{buf};
        *port = 9090;
    } else {
        ythrow yexception() << "metrics listen address is empty";
    }
}

class TPrometheusHttpCallback final : public THttpServer::ICallBack {
public:
    TPrometheusHttpCallback(TString path, std::shared_ptr<const TPrometheusMetrics> metrics)
        : Path_(std::move(path))
        , Metrics_(std::move(metrics))
    {
    }

    class TRequest final : public TRequestReplier {
    public:
        TRequest(TString path, std::shared_ptr<const TPrometheusMetrics> metrics)
            : Path_(std::move(path))
            , Metrics_(std::move(metrics))
        {
        }

        bool DoReply(const TReplyParams& params) override {
            TParsedHttpFull parsed(params.Input.FirstLine());
            if (TStringBuf(parsed.Method) != TStringBuf("GET") || TStringBuf(parsed.Path) != TStringBuf(Path_)) {
                THttpResponse resp(HTTP_NOT_FOUND);
                resp.SetContent(TString{"not found"});
                resp.OutTo(params.Output);
                return true;
            }
            const TString body = Metrics_->RenderText();
            THttpResponse resp(HTTP_OK);
            resp.SetContentType("text/plain; version=0.0.4");
            resp.SetContent(body);
            resp.OutTo(params.Output);
            return true;
        }

    private:
        TString Path_;
        std::shared_ptr<const TPrometheusMetrics> Metrics_;
    };

    TClientRequest* CreateClient() override {
        return new TRequest(Path_, Metrics_);
    }

private:
    TString Path_;
    std::shared_ptr<const TPrometheusMetrics> Metrics_;
};

struct TPrometheusHttpServer {
    TString Listen;
    TString Path;
    TPrometheusHttpCallback Callback;
    THttpServer Http;

    TPrometheusHttpServer(TString listen, TString path, std::shared_ptr<const TPrometheusMetrics> metrics)
        : Listen(std::move(listen))
        , Path(std::move(path))
        , Callback(Path, std::move(metrics))
        , Http(&Callback, MakeOptions(Listen))
    {
    }

    static THttpServerOptions MakeOptions(const TString& listen) {
        TString host;
        ui16 port = 0;
        ParseListenMetrics(listen, &host, &port);
        if (host == TStringBuf("0.0.0.0")) {
            host.clear();
        }
        THttpServerOptions opts;
        opts.AddBindAddress(host, port);
        opts.SetThreads(1);
        return opts;
    }

    void Start() {
        InitNetworkSubSystem();
        THttpServerOptions::TBindAddresses addrs;
        try {
            Http.Options().BindAddresses(addrs);
        } catch (const std::exception& ex) {
            ythrow yexception() << "metrics: invalid listen address \"" << Listen << "\": " << ex.what();
        }
        if (!Http.Start()) {
            ythrow yexception() << "metrics HTTP server failed to start (errno " << Http.GetErrorCode() << "): " << Http.GetError();
        }
    }

    void Stop() {
        Http.Stop();
        Http.Wait();
    }
};

static ui64 OwnedRowApproxBytes(const TOwnedLogRow& r) noexcept {
    return static_cast<ui64>(r.Service.size() + r.Cluster.size() + r.RecordId.size() + r.Message.size() + r.LabelsJson.size()
        + r.MetaJson.size() + 64);
}

static TOwnedLogRow MakeOwnedLogRow(const TResourceRowCtx& res, const LogRecord& lr) {
    TString recordId;
    THashMap<TString, TString> logLabels;
    THashMap<TString, TString> logMeta;
    SplitLogAttributes(lr.attributes(), &recordId, &logLabels, &logMeta);
    if (recordId.empty()) {
        recordId = CreateGuidAsString();
    }

    THashMap<TString, TString> labels = logLabels;
    MergeStringMap(res.ResourceLabels, &labels);

    TString tid = TraceHex(lr.trace_id());
    if (!tid.empty()) {
        labels[TString{"trace_id"}] = tid;
    }
    TString sid = TraceHex(lr.span_id());
    if (!sid.empty()) {
        labels[TString{"span_id"}] = sid;
    }

    THashMap<TString, TString> meta = logMeta;
    MergeStringMap(res.ResourceMeta, &meta);

    TOwnedLogRow row;
    row.Ts = TimestampFromNanos(lr.time_unix_nano() ? lr.time_unix_nano() : lr.observed_time_unix_nano());
    row.Service = res.Service;
    row.Cluster = res.Cluster;
    row.RecordId = std::move(recordId);
    row.Level = static_cast<i32>(lr.severity_number());
    row.Message = BodyToMessageUtf8(lr.body());
    row.LabelsJson = JsonStringifyMap(labels);
    row.MetaJson = JsonStringifyMap(meta);
    return row;
}

static void ArrowCheckStatus(const arrow::Status& st, TStringBuf ctx) {
    if (!st.ok()) {
        ythrow yexception() << ctx << ": " << st.ToString();
    }
}

static void YdbArrowSerializeBulkPayload(const std::shared_ptr<arrow::RecordBatch>& batch, TString* schemaWire, TString* dataWire) {
    {
        auto sch = arrow::ipc::SerializeSchema(*batch->schema());
        if (!sch.ok()) {
            ythrow yexception() << "SerializeSchema: " << sch.status().ToString();
        }
        const std::shared_ptr<arrow::Buffer>& buf = *sch;
        *schemaWire = TString(reinterpret_cast<const char*>(buf->data()), buf->size());
    }
    arrow::ipc::IpcWriteOptions opts = arrow::ipc::IpcWriteOptions::Defaults();
    opts.use_threads = false;
    arrow::ipc::IpcPayload payload;
    ArrowCheckStatus(arrow::ipc::GetRecordBatchPayload(*batch, opts, &payload), "GetRecordBatchPayload");
    int32_t metadata_length = 0;
    arrow::io::MockOutputStream mock;
    ArrowCheckStatus(arrow::ipc::WriteIpcPayload(payload, opts, &mock, &metadata_length), "WriteIpcPayload(mock)");
    auto outRes = arrow::io::BufferOutputStream::Create(mock.GetExtentBytesWritten());
    ArrowCheckStatus(outRes.status(), "BufferOutputStream::Create");
    std::shared_ptr<arrow::io::BufferOutputStream> outStream = outRes.ValueOrDie();
    ArrowCheckStatus(arrow::ipc::WriteIpcPayload(payload, opts, outStream.get(), &metadata_length), "WriteIpcPayload");
    auto done = outStream->Finish();
    ArrowCheckStatus(done.status(), "Finish");
    const std::shared_ptr<arrow::Buffer>& buf = done.ValueOrDie();
    *dataWire = TString(reinterpret_cast<const char*>(buf->data()), buf->size());
}

/// Arrow layout matches Go `processors/ydb-supplier/arrow_batch.go` (YDB column BulkUpsert wire format).
static void SerializeLogsBulkArrow(
    const TBuck& buck,
    const TVector<TOwnedLogRow>& rows,
    size_t begin,
    size_t end,
    TString* schemaWire,
    TString* dataWire)
{
    const size_t n = end - begin;
    if (n == 0) {
        ythrow yexception() << "SerializeLogsBulkArrow: empty range";
    }
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    const std::shared_ptr<arrow::DataType> tsType = arrow::timestamp(arrow::TimeUnit::MICRO);

    auto appendUtf8 = [](arrow::StringBuilder& b, const TString& s) {
        ArrowCheckStatus(b.Append(s.data(), static_cast<int32_t>(s.size())), "StringBuilder::Append");
    };

    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Array>> columns;

    switch (buck.Schema) {
        case ELogsPkSchema::PerProjectHeap: {
            schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field("timestamp", tsType, false),
                arrow::field("service", arrow::utf8(), false),
                arrow::field("cluster", arrow::utf8(), false),
                arrow::field("record_id", arrow::utf8(), false),
                arrow::field("level", arrow::int32(), true),
                arrow::field("message", arrow::utf8(), true),
                arrow::field("labels", arrow::utf8(), true),
                arrow::field("meta", arrow::utf8(), true),
            });
            arrow::TimestampBuilder tsB(tsType, pool);
            arrow::StringBuilder svcB(pool);
            arrow::StringBuilder clB(pool);
            arrow::StringBuilder idB(pool);
            arrow::Int32Builder lvlB(pool);
            arrow::StringBuilder msgB(pool);
            arrow::StringBuilder labB(pool);
            arrow::StringBuilder metaB(pool);
            ArrowCheckStatus(tsB.Reserve(static_cast<int64_t>(n)), "tsB.Reserve");
            ArrowCheckStatus(svcB.Reserve(static_cast<int64_t>(n)), "svcB.Reserve");
            ArrowCheckStatus(clB.Reserve(static_cast<int64_t>(n)), "clB.Reserve");
            ArrowCheckStatus(idB.Reserve(static_cast<int64_t>(n)), "idB.Reserve");
            ArrowCheckStatus(lvlB.Reserve(static_cast<int64_t>(n)), "lvlB.Reserve");
            ArrowCheckStatus(msgB.Reserve(static_cast<int64_t>(n)), "msgB.Reserve");
            ArrowCheckStatus(labB.Reserve(static_cast<int64_t>(n)), "labB.Reserve");
            ArrowCheckStatus(metaB.Reserve(static_cast<int64_t>(n)), "metaB.Reserve");
            for (size_t i = begin; i < end; ++i) {
                const TOwnedLogRow& row = rows[i];
                ArrowCheckStatus(tsB.Append(static_cast<int64_t>(row.Ts.MicroSeconds())), "tsB.Append");
                appendUtf8(svcB, row.Service);
                appendUtf8(clB, Utf8Safe(row.Cluster));
                appendUtf8(idB, row.RecordId);
                ArrowCheckStatus(lvlB.Append(row.Level), "lvlB.Append");
                appendUtf8(msgB, row.Message);
                appendUtf8(labB, row.LabelsJson);
                appendUtf8(metaB, row.MetaJson);
            }
            std::shared_ptr<arrow::Array> a0, a1, a2, a3, a4, a5, a6, a7;
            ArrowCheckStatus(tsB.Finish(&a0), "tsB.Finish");
            ArrowCheckStatus(svcB.Finish(&a1), "svcB.Finish");
            ArrowCheckStatus(clB.Finish(&a2), "clB.Finish");
            ArrowCheckStatus(idB.Finish(&a3), "idB.Finish");
            ArrowCheckStatus(lvlB.Finish(&a4), "lvlB.Finish");
            ArrowCheckStatus(msgB.Finish(&a5), "msgB.Finish");
            ArrowCheckStatus(labB.Finish(&a6), "labB.Finish");
            ArrowCheckStatus(metaB.Finish(&a7), "metaB.Finish");
            columns = {std::move(a0), std::move(a1), std::move(a2), std::move(a3), std::move(a4), std::move(a5), std::move(a6), std::move(a7)};
            break;
        }
        case ELogsPkSchema::Dedicated: {
            schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field("timestamp", tsType, false),
                arrow::field("record_id", arrow::utf8(), false),
                arrow::field("level", arrow::int32(), true),
                arrow::field("message", arrow::utf8(), true),
                arrow::field("labels", arrow::utf8(), true),
                arrow::field("meta", arrow::utf8(), true),
            });
            arrow::TimestampBuilder tsB(tsType, pool);
            arrow::StringBuilder idB(pool);
            arrow::Int32Builder lvlB(pool);
            arrow::StringBuilder msgB(pool);
            arrow::StringBuilder labB(pool);
            arrow::StringBuilder metaB(pool);
            ArrowCheckStatus(tsB.Reserve(static_cast<int64_t>(n)), "tsB.Reserve");
            ArrowCheckStatus(idB.Reserve(static_cast<int64_t>(n)), "idB.Reserve");
            ArrowCheckStatus(lvlB.Reserve(static_cast<int64_t>(n)), "lvlB.Reserve");
            ArrowCheckStatus(msgB.Reserve(static_cast<int64_t>(n)), "msgB.Reserve");
            ArrowCheckStatus(labB.Reserve(static_cast<int64_t>(n)), "labB.Reserve");
            ArrowCheckStatus(metaB.Reserve(static_cast<int64_t>(n)), "metaB.Reserve");
            for (size_t i = begin; i < end; ++i) {
                const TOwnedLogRow& row = rows[i];
                ArrowCheckStatus(tsB.Append(static_cast<int64_t>(row.Ts.MicroSeconds())), "tsB.Append");
                appendUtf8(idB, row.RecordId);
                ArrowCheckStatus(lvlB.Append(row.Level), "lvlB.Append");
                appendUtf8(msgB, row.Message);
                appendUtf8(labB, row.LabelsJson);
                appendUtf8(metaB, row.MetaJson);
            }
            std::shared_ptr<arrow::Array> a0, a1, a2, a3, a4, a5;
            ArrowCheckStatus(tsB.Finish(&a0), "tsB.Finish");
            ArrowCheckStatus(idB.Finish(&a1), "idB.Finish");
            ArrowCheckStatus(lvlB.Finish(&a2), "lvlB.Finish");
            ArrowCheckStatus(msgB.Finish(&a3), "msgB.Finish");
            ArrowCheckStatus(labB.Finish(&a4), "labB.Finish");
            ArrowCheckStatus(metaB.Finish(&a5), "metaB.Finish");
            columns = {std::move(a0), std::move(a1), std::move(a2), std::move(a3), std::move(a4), std::move(a5)};
            break;
        }
        default: {
            schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field("timestamp", tsType, false),
                arrow::field("cluster", arrow::utf8(), false),
                arrow::field("record_id", arrow::utf8(), false),
                arrow::field("level", arrow::int32(), true),
                arrow::field("message", arrow::utf8(), true),
                arrow::field("labels", arrow::utf8(), true),
                arrow::field("meta", arrow::utf8(), true),
            });
            arrow::TimestampBuilder tsB(tsType, pool);
            arrow::StringBuilder clB(pool);
            arrow::StringBuilder idB(pool);
            arrow::Int32Builder lvlB(pool);
            arrow::StringBuilder msgB(pool);
            arrow::StringBuilder labB(pool);
            arrow::StringBuilder metaB(pool);
            ArrowCheckStatus(tsB.Reserve(static_cast<int64_t>(n)), "tsB.Reserve");
            ArrowCheckStatus(clB.Reserve(static_cast<int64_t>(n)), "clB.Reserve");
            ArrowCheckStatus(idB.Reserve(static_cast<int64_t>(n)), "idB.Reserve");
            ArrowCheckStatus(lvlB.Reserve(static_cast<int64_t>(n)), "lvlB.Reserve");
            ArrowCheckStatus(msgB.Reserve(static_cast<int64_t>(n)), "msgB.Reserve");
            ArrowCheckStatus(labB.Reserve(static_cast<int64_t>(n)), "labB.Reserve");
            ArrowCheckStatus(metaB.Reserve(static_cast<int64_t>(n)), "metaB.Reserve");
            for (size_t i = begin; i < end; ++i) {
                const TOwnedLogRow& row = rows[i];
                ArrowCheckStatus(tsB.Append(static_cast<int64_t>(row.Ts.MicroSeconds())), "tsB.Append");
                appendUtf8(clB, Utf8Safe(row.Cluster));
                appendUtf8(idB, row.RecordId);
                ArrowCheckStatus(lvlB.Append(row.Level), "lvlB.Append");
                appendUtf8(msgB, row.Message);
                appendUtf8(labB, row.LabelsJson);
                appendUtf8(metaB, row.MetaJson);
            }
            std::shared_ptr<arrow::Array> a0, a1, a2, a3, a4, a5, a6;
            ArrowCheckStatus(tsB.Finish(&a0), "tsB.Finish");
            ArrowCheckStatus(clB.Finish(&a1), "clB.Finish");
            ArrowCheckStatus(idB.Finish(&a2), "idB.Finish");
            ArrowCheckStatus(lvlB.Finish(&a3), "lvlB.Finish");
            ArrowCheckStatus(msgB.Finish(&a4), "msgB.Finish");
            ArrowCheckStatus(labB.Finish(&a5), "labB.Finish");
            ArrowCheckStatus(metaB.Finish(&a6), "metaB.Finish");
            columns = {std::move(a0), std::move(a1), std::move(a2), std::move(a3), std::move(a4), std::move(a5), std::move(a6)};
            break;
        }
    }

    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(n), std::move(columns));
    if (!batch) {
        ythrow yexception() << "RecordBatch::Make returned null";
    }
    YdbArrowSerializeBulkPayload(batch, schemaWire, dataWire);
}

} // namespace

struct TShardBuffer {
    explicit TShardBuffer(TBuck buck)
        : Buck(std::move(buck))
    {
    }

    const TBuck Buck;
    std::mutex Mu;
    TVector<TOwnedLogRow> Rows;
    ui64 ApproxBytes = 0;
    /// When unsent rows first appeared after the bucket was empty (Go `pendingSince`).
    TInstant PendingSince{TInstant::Zero()};
    /// Last successful BulkUpsert to this shard bucket (Go `lastYdbFlushTime`; Zero = never).
    TInstant LastYdbFlushOk{TInstant::Zero()};
};

static ui64 SumOwnedRowsApproxBytes(const TVector<TOwnedLogRow>& rows) {
    ui64 sum = 0;
    for (const TOwnedLogRow& r : rows) {
        sum += OwnedRowApproxBytes(r);
    }
    return sum;
}

static bool NeedsYdbWaitFlush(
    size_t rowCount,
    TInstant pendingSince,
    TInstant lastFlushOk,
    ui64 intervalSec,
    TInstant now)
{
    if (intervalSec == 0 || rowCount == 0) {
        return false;
    }
    const TDuration interval = TDuration::Seconds(static_cast<ui32>(intervalSec));
    if (lastFlushOk != TInstant::Zero()) {
        return (now - lastFlushOk) >= interval;
    }
    return pendingSince != TInstant::Zero() && (now - pendingSince) >= interval;
}

struct TFlushWorkItem {
    TBuck Buck;
    TVector<TOwnedLogRow> Rows;
    TShardBuffer* BufForFlushOk = nullptr;
};

class TYdbFlushQueue {
public:
    explicit TYdbFlushQueue(size_t cap, TPrometheusMetrics* metrics)
        : Cap_(Max<size_t>(size_t(1), cap))
        , M_(metrics)
    {
        if (M_) {
            M_->SetYdbFlushQueueCapacity(Cap_);
        }
    }

    void PushBlocking(TFlushWorkItem item) {
        {
            std::unique_lock<std::mutex> lk(Mu_);
            CvPush_.wait(lk, [&] { return Stopped_ || Q_.size() < Cap_; });
            if (Stopped_) {
                return;
            }
            Q_.push_back(std::move(item));
        }
        PublishDepth();
        CvPop_.notify_one();
    }

    void Stop() {
        std::lock_guard<std::mutex> g(Mu_);
        Stopped_ = true;
        CvPush_.notify_all();
        CvPop_.notify_all();
        PublishDepth();
    }

    bool WaitPop(TFlushWorkItem* out) {
        {
            std::unique_lock<std::mutex> lk(Mu_);
            CvPop_.wait(lk, [&] { return Stopped_ || !Q_.empty(); });
            if (Q_.empty()) {
                return false;
            }
            *out = std::move(Q_.front());
            Q_.pop_front();
        }
        PublishDepth();
        return true;
    }

private:
    void PublishDepth() {
        if (!M_) {
            return;
        }
        size_t depth = 0;
        {
            std::lock_guard<std::mutex> g(Mu_);
            depth = Q_.size();
        }
        M_->SetYdbFlushQueueDepth(depth);
    }

    size_t Cap_;
    TPrometheusMetrics* M_ = nullptr;
    std::mutex Mu_;
    std::condition_variable CvPush_;
    std::condition_variable CvPop_;
    std::deque<TFlushWorkItem> Q_;
    bool Stopped_ = false;
};

class TYdbFlushWorkerBusyGuard {
public:
    explicit TYdbFlushWorkerBusyGuard(TPrometheusMetrics* m)
        : M_(m)
    {
        if (M_) {
            M_->IncYdbFlushWorkersBusy();
        }
    }

    ~TYdbFlushWorkerBusyGuard() {
        if (M_) {
            M_->DecYdbFlushWorkersBusy();
        }
    }

    TYdbFlushWorkerBusyGuard(const TYdbFlushWorkerBusyGuard&) = delete;
    TYdbFlushWorkerBusyGuard& operator=(const TYdbFlushWorkerBusyGuard&) = delete;

private:
    TPrometheusMetrics* M_ = nullptr;
};

struct TOtelLogsServer::TImpl {
    TServerConfig Cfg;
    NYdb::TDriver& Driver;
    /// Shared so in-flight `/metrics` `DoReply` keeps storage alive after `THttpServer::Stop()` returns.
    std::shared_ptr<TPrometheusMetrics> Metrics;
    std::shared_ptr<TYdbBulkWriteLimiter> BulkLimiter;
    std::unique_ptr<THealthCheckServer> HealthServer;
    std::unique_ptr<NYdb::NTable::TTableClient> TableClient;
    std::unique_ptr<TDdlEnsurer> Ddl;
    TIngestQueue Queue;
    TYdbFlushQueue FlushQueue;
    std::unique_ptr<TPrometheusHttpServer> MetricsServer;
    std::vector<std::thread> Workers;
    std::vector<std::thread> FlushWorkers;
    std::atomic<bool> StopPipeline{false};
    std::unique_ptr<grpc::CallbackGenericService> GrpcGenericService;
    std::unique_ptr<grpc::Server> GrpcServer;

    THashMap<TBuck, std::unique_ptr<TShardBuffer>, TBuckHash> ShardBuf_;
    std::shared_mutex ShardBufMapMu_;
    /// Buffers that may need time-based flush; sweeper walks this instead of the full map.
    THashSet<TShardBuffer*> PendingSweep_;
    std::mutex PendingSweepMu_;
    std::thread Sweeper_;
    std::atomic<bool> StopSweeper{false};
    TImpl(TServerConfig cfg, NYdb::TDriver& driver)
        : Cfg(std::move(cfg))
        , Driver(driver)
        , Metrics(std::make_shared<TPrometheusMetrics>())
        , BulkLimiter(std::make_shared<TYdbBulkWriteLimiter>(Cfg.YdbMaxConcurrentBulkUpserts, Metrics.get()))
        , Queue(Cfg.IngestQueueMax, Metrics.get())
        , FlushQueue(Cfg.FlushQueueMax, Metrics.get())
    {
        TableClient = std::make_unique<NYdb::NTable::TTableClient>(Driver);
        Ddl = std::make_unique<TDdlEnsurer>(Cfg);
    }

    static size_t ResolveYdbFlushWorkers(const TServerConfig& cfg) {
        if (cfg.YdbFlushWorkers > 0) {
            return cfg.YdbFlushWorkers;
        }
        return Max<size_t>(size_t(1), static_cast<size_t>(cfg.YdbMaxConcurrentBulkUpserts));
    }

    void ProcessExport(const ologs::ExportLogsServiceRequest& request);
    void IngestWireBatch(grpc::ByteBuffer* buf, ui64 protoBytes);
    void AppendOwnedShards(THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash>&& buckets);
    void RegisterPendingSweep(TShardBuffer* buf);
    void UnregisterPendingSweep(TShardBuffer* buf);
    void TryFlushShardBuffer(TShardBuffer* buf);
    void EnqueueFlushWork(TFlushWorkItem item);
    void BulkUpsertOwnedRows(const TBuck& buck, TVector<TOwnedLogRow>&& rows, TShardBuffer* bufForFlushOk = nullptr);
    void SweepShardBuffersIdle();
    void DrainAllShardBuffers();
    void SweeperLoop();
    void YdbFlushWorkerLoop();

    void WorkerLoop() {
        thread_local google::protobuf::Arena tlsParseArena;
        for (;;) {
            TIngestWirePayload payload;
            if (!Queue.WaitPop(&payload)) {
                if (StopPipeline.load(std::memory_order_acquire)) {
                    return;
                }
                continue;
            }
            const TIngestWorkerBusyGuard busy(Metrics.get());
            const ui64 protoBytes = static_cast<ui64>(payload.Buf.Length());
            if (Cfg.IngestWireToOwned) {
                IngestWireBatch(&payload.Buf, protoBytes);
                continue;
            }
            tlsParseArena.Reset();
            const ologs::ExportLogsServiceRequest* req = ParseExportWire(&payload.Buf, &tlsParseArena);
            if (!req) {
                continue;
            }
            if (!RunOptionalLogValidation(Cfg.ValidationEnabled, *req)) {
                continue;
            }
            if (!Cfg.ExportRoutableWirePrecheck && !ExportHasRoutableLogRows(Cfg, *req)) {
                continue;
            }
            const size_t logRows = CountExportLogRecords(*req);
            Metrics->AddPipelineIn(static_cast<ui64>(logRows), protoBytes);
            ProcessExport(*req);
        }
    }

    bool TryEnqueue(TIngestWirePayload payload) {
        return Queue.TryPush(std::move(payload));
    }

    void Run() {
        StopPipeline.store(false, std::memory_order_release);
        StopSweeper.store(false, std::memory_order_release);
        const size_t nWorkers = Max<size_t>(size_t(1), Cfg.IngestWorkers);
        const size_t nFlushWorkers = ResolveYdbFlushWorkers(Cfg);
        Metrics->SetIngestQueueCapacity(Cfg.IngestQueueMax);
        Metrics->SetIngestWorkersTotal(nWorkers);
        Metrics->SetYdbFlushWorkersTotal(nFlushWorkers);
        Workers.reserve(nWorkers);
        for (size_t i = 0; i < nWorkers; ++i) {
            Workers.emplace_back([this] { WorkerLoop(); });
        }
        FlushWorkers.reserve(nFlushWorkers);
        for (size_t i = 0; i < nFlushWorkers; ++i) {
            FlushWorkers.emplace_back([this] { YdbFlushWorkerLoop(); });
        }
        Sweeper_ = std::thread([this] { SweeperLoop(); });

        grpc::ServerBuilder builder;
        const TString listenAddr{TStringBuf(Cfg.ListenAddress.data(), Cfg.ListenAddress.size())};
        builder.AddListeningPort(listenAddr, grpc::InsecureServerCredentials());
        builder.SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, Cfg.GrpcMaxPollers);
        builder.RegisterCallbackGenericService(GrpcGenericService.get());
        GrpcServer = builder.BuildAndStart();
        if (!GrpcServer) {
            ythrow yexception() << "failed to start gRPC server on " << Cfg.ListenAddress;
        }
        if (!Cfg.HealthListen.empty()) {
            HealthServer = std::make_unique<THealthCheckServer>(
                TString{Cfg.HealthListen.data(), Cfg.HealthListen.size()},
                TString{Cfg.HealthPath.data(), Cfg.HealthPath.size()});
            HealthServer->Start();
        }
        if (!Cfg.MetricsListen.empty()) {
            MetricsServer = std::make_unique<TPrometheusHttpServer>(
                TString{Cfg.MetricsListen.data(), Cfg.MetricsListen.size()},
                TString{"/metrics"},
                Metrics);
            MetricsServer->Start();
        }
        std::cerr << "otel_logs_to_ydb listening grpc://" << Cfg.ListenAddress << " ydb=" << Cfg.YdbEndpoint
                  << " db=" << Cfg.YdbDatabase << " prefix=" << Cfg.TablesPrefix << " layout=" << Cfg.TableLayout
                  << " ingest_queue=" << Cfg.IngestQueueMax << " workers=" << nWorkers << " flush_queue="
                  << Cfg.FlushQueueMax << " ydb_flush_workers=" << nFlushWorkers << " ydb_max_concurrent_bulk="
                  << Cfg.YdbMaxConcurrentBulkUpserts << " export_routable_wire_precheck="
                  << (Cfg.ExportRoutableWirePrecheck ? "true" : "false") << " ingest_wire_to_owned="
                  << (Cfg.IngestWireToOwned ? "true" : "false") << " allowed_projects=";
        if (Cfg.AllowedProjects.empty()) {
            std::cerr << "all";
        } else {
            for (size_t j = 0; j < Cfg.AllowedProjects.size(); ++j) {
                if (j) {
                    std::cerr << ",";
                }
                std::cerr << Cfg.AllowedProjects[j];
            }
        }
        std::cerr << std::endl;
        if (HealthServer) {
            std::cerr << "health_check http GET http://" << Cfg.HealthListen << Cfg.HealthPath << std::endl;
        }
        if (MetricsServer) {
            std::cerr << "metrics http GET http://" << Cfg.MetricsListen << "/metrics" << std::endl;
        }
        GrpcServer->Wait();
    }

    void Stop() {
        if (GrpcServer) {
            GrpcServer->Shutdown();
        }
        StopPipeline.store(true, std::memory_order_release);
        StopSweeper.store(true, std::memory_order_release);
        Queue.Stop();
        for (std::thread& t : Workers) {
            if (t.joinable()) {
                t.join();
            }
        }
        Workers.clear();
        if (Sweeper_.joinable()) {
            Sweeper_.join();
        }
        DrainAllShardBuffers();
        FlushQueue.Stop();
        for (std::thread& t : FlushWorkers) {
            if (t.joinable()) {
                t.join();
            }
        }
        FlushWorkers.clear();
        if (MetricsServer) {
            MetricsServer->Stop();
            MetricsServer.reset();
        }
        HealthServer.reset();
    }
};

class TExportReactor final : public grpc::ServerGenericBidiReactor {
public:
    explicit TExportReactor(TOtelLogsServer::TImpl* impl)
        : Impl_(impl)
    {
        StartRead(&ReadBuf_);
    }

    void OnReadDone(bool ok) override {
        struct TInflightGuard {
            TPrometheusMetrics* M = nullptr;
            explicit TInflightGuard(TPrometheusMetrics* m)
                : M(m)
            {
                if (M) {
                    M->IncGrpcExportRpcInflight();
                }
            }
            ~TInflightGuard() {
                if (M) {
                    M->DecGrpcExportRpcInflight();
                }
            }
        };
        const TInflightGuard inflight(Impl_->Metrics.get());

        if (!ok) {
            Finish(grpc::Status(grpc::StatusCode::INTERNAL, "read failed"));
            return;
        }

        if (!ReadBuf_.Valid() || ReadBuf_.Length() == 0) {
            Finish(grpc::Status(grpc::StatusCode::INTERNAL, "empty request"));
            return;
        }

        const ui64 protoBytes = static_cast<ui64>(ReadBuf_.Length());
        Impl_->Metrics->AddGrpcExportRequestBytes(protoBytes);

        if (Impl_->Cfg.ExportRoutableWirePrecheck && !ExportWireHasRoutableLogRows(&ReadBuf_, Impl_->Cfg)) {
            Finish(grpc::Status::OK);
            return;
        }

        Impl_->Metrics->AddIngestOfferBytes(protoBytes);

        TIngestWirePayload payload;
        payload.Buf = std::move(ReadBuf_);
        if (!Impl_->TryEnqueue(std::move(payload))) {
            Impl_->Metrics->AddRefused(0, protoBytes);
            Impl_->Metrics->IncIngestRejectedQueueFull();
            Finish(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "ingest queue full"));
            return;
        }
        Impl_->Metrics->IncIngestAccepted();
        Finish(grpc::Status::OK);
    }

    void OnDone() override {
        delete this;
    }

private:
    TOtelLogsServer::TImpl* Impl_;
    grpc::ByteBuffer ReadBuf_;
};

class TUnknownGrpcMethodReactor final : public grpc::ServerGenericBidiReactor {
public:
    TUnknownGrpcMethodReactor() {
        Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "unknown method"));
    }

    void OnDone() override {
        delete this;
    }
};

class TLogsGrpcGenericService final : public grpc::CallbackGenericService {
public:
    TLogsGrpcGenericService(TOtelLogsServer::TImpl* impl, TServerConfig cfg)
        : Impl_(impl)
        , Cfg_(std::move(cfg))
    {
    }

    grpc::ServerGenericBidiReactor* CreateReactor(grpc::GenericCallbackServerContext* ctx) override {
        if (TStringBuf(ctx->method()) != kExportMethod) {
            return new TUnknownGrpcMethodReactor();
        }
        return new TExportReactor(Impl_);
    }

private:
    TOtelLogsServer::TImpl* Impl_;
    TServerConfig Cfg_;
};

void TOtelLogsServer::TImpl::IngestWireBatch(grpc::ByteBuffer* buf, ui64 protoBytes) {
    if (!Cfg.ExportRoutableWirePrecheck && !ExportWireHasRoutableLogRows(buf, Cfg)) {
        return;
    }
    THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash> buckets;
    TWireExportParseStats stats;
    if (!ProcessExportWire(buf, Cfg, &buckets, &stats)) {
        return;
    }
    if (stats.LogRows == 0) {
        return;
    }
    Metrics->AddPipelineIn(static_cast<ui64>(stats.LogRows), protoBytes);
    AppendOwnedShards(std::move(buckets));
}

void TOtelLogsServer::TImpl::ProcessExport(const ologs::ExportLogsServiceRequest& request) {
    const bool perProject = IsPerProjectLayout(Cfg);
    THashSet<TString> allowed;
    for (const std::string& p : Cfg.AllowedProjects) {
        allowed.insert(TString{p.data(), p.size()});
    }
    const bool hasFilter = !allowed.empty();

    THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash> buckets;

    for (const ResourceLogs& rl : request.resource_logs()) {
        const TProjectService ps = ExtractProjectService(rl.resource());
        if (hasFilter && !allowed.contains(ps.Project)) {
            continue;
        }
        const TResourceRowCtx resCtx = BuildResourceRowCtx(rl.resource(), ps);

        const TRoutedTable route = ResolveLogsTable(Cfg, ps.Project, ps.Service, resCtx.Cluster, perProject);
        if (route.Drop) {
            continue;
        }

        const int partCount = (route.PkSchema == ELogsPkSchema::Dedicated) ? Cfg.PartitionCountDedicated : Cfg.PartitionCountCommon;

        for (const ScopeLogs& sl : rl.scope_logs()) {
            for (const LogRecord& lr : sl.log_records()) {
                TOwnedLogRow row = MakeOwnedLogRow(resCtx, lr);
                TBuck bk;
                bk.Table = route.TablePath;
                bk.Schema = route.PkSchema;
                bk.Shard = 0;
                if (Cfg.BatchByShardHash && partCount > 0) {
                    bk.Shard = ShardIndexFromHash(HashOwnedLogRow(route.PkSchema, row), partCount);
                }
                buckets[bk].push_back(std::move(row));
            }
        }
    }

    AppendOwnedShards(std::move(buckets));
}

void TOtelLogsServer::TImpl::RegisterPendingSweep(TShardBuffer* buf) {
    std::lock_guard<std::mutex> g(PendingSweepMu_);
    PendingSweep_.insert(buf);
}

void TOtelLogsServer::TImpl::UnregisterPendingSweep(TShardBuffer* buf) {
    std::lock_guard<std::mutex> g(PendingSweepMu_);
    PendingSweep_.erase(buf);
}

void TOtelLogsServer::TImpl::AppendOwnedShards(THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash>&& buckets) {
    if (buckets.empty()) {
        return;
    }

    TVector<std::pair<TShardBuffer*, TVector<TOwnedLogRow>*>> work;
    work.reserve(buckets.size());
    {
        std::unique_lock<std::shared_mutex> lock(ShardBufMapMu_);
        for (auto& it : buckets) {
            if (it.second.empty()) {
                continue;
            }
            std::unique_ptr<TShardBuffer>& slot = ShardBuf_[it.first];
            if (!slot) {
                slot = std::make_unique<TShardBuffer>(it.first);
            }
            work.emplace_back(slot.get(), &it.second);
        }
    }

    for (auto& [buf, chunk] : work) {
        ui64 chunkBytes = 0;
        for (const TOwnedLogRow& r : *chunk) {
            chunkBytes += OwnedRowApproxBytes(r);
        }

        bool registerSweep = false;
        {
            std::lock_guard<std::mutex> g(buf->Mu);
            const bool wasEmpty = buf->Rows.empty();
            buf->ApproxBytes += chunkBytes;
            for (auto& r : *chunk) {
                buf->Rows.push_back(std::move(r));
            }
            if (wasEmpty && !buf->Rows.empty()) {
                buf->PendingSince = TInstant::Now();
                registerSweep = true;
            }
        }
        if (registerSweep) {
            RegisterPendingSweep(buf);
        }
        TryFlushShardBuffer(buf);
    }
}

void TOtelLogsServer::TImpl::TryFlushShardBuffer(TShardBuffer* buf) {
    constexpr size_t kHardMaxRows = 5'000'000;
    const TInstant now = TInstant::Now();
    const ui64 flushIntervalSec = Cfg.ShardBufferFlushIntervalSec;
    const size_t minFlushRecords = Cfg.ShardBufferMinFlushRecords > 0 ? static_cast<size_t>(Cfg.ShardBufferMinFlushRecords) : 0;
    const ui64 minFlushBytes = Cfg.ShardBufferMinFlushBytes > 0 ? static_cast<ui64>(Cfg.ShardBufferMinFlushBytes) : 0;

    for (;;) {
        TVector<TOwnedLogRow> out;
        TVector<TOwnedLogRow> restRows;
        bool recalcApprox = false;
        bool unregisterSweep = false;

        {
            std::lock_guard<std::mutex> g(buf->Mu);
            if (buf->Rows.empty()) {
                buf->PendingSince = TInstant::Zero();
                unregisterSweep = true;
                break;
            }

            const size_t rowCount = buf->Rows.size();
            const bool needSize = (minFlushRecords > 0 && rowCount >= minFlushRecords)
                || (minFlushBytes > 0 && buf->ApproxBytes >= minFlushBytes);
            const bool needTime = NeedsYdbWaitFlush(
                rowCount,
                buf->PendingSince,
                buf->LastYdbFlushOk,
                flushIntervalSec,
                now);
            const bool needHard = rowCount >= kHardMaxRows;
            if (!needSize && !needTime && !needHard) {
                break;
            }

            if (needHard && rowCount > kHardMaxRows) {
                out.reserve(kHardMaxRows);
                for (size_t i = 0; i < kHardMaxRows; ++i) {
                    out.push_back(std::move(buf->Rows[i]));
                }
                restRows.reserve(rowCount - kHardMaxRows);
                for (size_t i = kHardMaxRows; i < rowCount; ++i) {
                    restRows.push_back(std::move(buf->Rows[i]));
                }
                buf->Rows.clear();
                buf->ApproxBytes = 0;
                recalcApprox = true;
            } else {
                out = std::move(buf->Rows);
                buf->ApproxBytes = 0;
                buf->PendingSince = TInstant::Zero();
                unregisterSweep = true;
            }
        }

        if (unregisterSweep) {
            UnregisterPendingSweep(buf);
        }

        if (recalcApprox) {
            const ui64 restBytes = SumOwnedRowsApproxBytes(restRows);
            std::lock_guard<std::mutex> g(buf->Mu);
            buf->Rows = std::move(restRows);
            buf->ApproxBytes = restBytes;
        }

        if (out.empty()) {
            break;
        }

        TFlushWorkItem item;
        item.Buck = buf->Buck;
        item.Rows = std::move(out);
        item.BufForFlushOk = buf;
        EnqueueFlushWork(std::move(item));
    }
}

void TOtelLogsServer::TImpl::EnqueueFlushWork(TFlushWorkItem item) {
    FlushQueue.PushBlocking(std::move(item));
}

void TOtelLogsServer::TImpl::YdbFlushWorkerLoop() {
    for (;;) {
        TFlushWorkItem item;
        if (!FlushQueue.WaitPop(&item)) {
            return;
        }
        const TYdbFlushWorkerBusyGuard busy(Metrics.get());
        BulkUpsertOwnedRows(item.Buck, std::move(item.Rows), item.BufForFlushOk);
    }
}

void TOtelLogsServer::TImpl::BulkUpsertOwnedRows(const TBuck& buck, TVector<TOwnedLogRow>&& rows, TShardBuffer* bufForFlushOk) {
    if (rows.empty()) {
        return;
    }
    const TString tablePath = buck.Table;
    const std::string tableStd(tablePath.data(), tablePath.size());
    constexpr size_t kChunkRows = 100'000;
    const size_t total = rows.size();
    for (size_t off = 0; off < total;) {
        const size_t end = Min(total, off + kChunkRows);
        for (int attempt = 0; attempt <= Max(0, Cfg.MaxRetries); ++attempt) {
            TString schemaWire;
            TString dataWire;
            const TInstant tEncode0 = TInstant::Now();
            try {
                SerializeLogsBulkArrow(buck, rows, off, end, &schemaWire, &dataWire);
            } catch (const std::exception& ex) {
                std::cerr << "Arrow encode failed table=" << tablePath << " err=" << ex.what() << std::endl;
                Metrics->IncBulkFail();
                Metrics->IncLogsBatchesErrors();
                break;
            }
            const ui64 encodeMs = (TInstant::Now() - tEncode0).MilliSeconds();

            NYdb::NTable::TBulkUpsertSettings upsertSettings;
            upsertSettings.OperationTimeout(TDuration::Seconds(Cfg.YdbWriteTimeoutSec));

            const TInstant tRpc0 = TInstant::Now();
            const NYdb::NTable::TBulkUpsertResult res = [&] {
                TYdbBulkWriteLimiter::TSlot slot(*BulkLimiter);
                return TableClient
                    ->BulkUpsert(
                        tableStd,
                        NYdb::NTable::EDataFormat::ApacheArrow,
                        dataWire,
                        schemaWire,
                        upsertSettings)
                    .ExtractValueSync();
            }();
            const ui64 rpcMs = (TInstant::Now() - tRpc0).MilliSeconds();

            const ui64 nrows = static_cast<ui64>(end - off);

            if (res.IsSuccess()) {
                Metrics->IncBulkOk();
                Metrics->IncLogsBatchesStored();
                Metrics->AddYdbWritten(nrows, static_cast<ui64>(schemaWire.size() + dataWire.size()));
                Metrics->ObserveBulkArrowEncodeMs(encodeMs);
                Metrics->ObserveBulkYdbRpcMs(rpcMs);
                Metrics->ObserveBulkUpsertRows(nrows);
                if (bufForFlushOk) {
                    std::lock_guard<std::mutex> fg(bufForFlushOk->Mu);
                    bufForFlushOk->LastYdbFlushOk = TInstant::Now();
                }
                break;
            }
            const TString issues = res.GetIssues().ToOneLineString();
            if (Cfg.AutoCreateMissingTables && ShouldTryDdlAfterBulkError(issues)) {
                TString derr;
                if (Ddl->EnsureLogsTable(*TableClient, tablePath, buck.Schema, &derr)) {
                    Metrics->IncDdlRuns();
                    continue;
                }
                std::cerr << "Auto-DDL failed table=" << tablePath << " err=" << derr << std::endl;
            }
            if (attempt < Cfg.MaxRetries) {
                std::this_thread::sleep_for(std::chrono::milliseconds(Cfg.RetryBackoffMs));
            } else {
                Metrics->IncBulkFail();
                Metrics->IncLogsBatchesErrors();
                std::cerr << "BulkUpsert failed table=" << tablePath << " issues=" << issues << std::endl;
            }
        }
        off = end;
    }
}

void TOtelLogsServer::TImpl::SweepShardBuffersIdle() {
    TVector<TShardBuffer*> targets;
    {
        std::lock_guard<std::mutex> g(PendingSweepMu_);
        targets.reserve(PendingSweep_.size());
        for (TShardBuffer* buf : PendingSweep_) {
            targets.push_back(buf);
        }
    }
    for (TShardBuffer* buf : targets) {
        TryFlushShardBuffer(buf);
    }
}

void TOtelLogsServer::TImpl::DrainAllShardBuffers() {
    TVector<TShardBuffer*> buffers;
    {
        std::shared_lock<std::shared_mutex> lock(ShardBufMapMu_);
        buffers.reserve(ShardBuf_.size());
        for (const auto& kv : ShardBuf_) {
            if (kv.second) {
                buffers.push_back(kv.second.get());
            }
        }
    }
    for (TShardBuffer* buf : buffers) {
        for (;;) {
            TVector<TOwnedLogRow> out;
            bool unregisterSweep = false;
            {
                std::lock_guard<std::mutex> bg(buf->Mu);
                if (buf->Rows.empty()) {
                    unregisterSweep = true;
                    break;
                }
                out = std::move(buf->Rows);
                buf->ApproxBytes = 0;
                buf->PendingSince = TInstant::Zero();
                unregisterSweep = true;
            }
            if (unregisterSweep) {
                UnregisterPendingSweep(buf);
            }
            TFlushWorkItem item;
            item.Buck = buf->Buck;
            item.Rows = std::move(out);
            item.BufForFlushOk = buf;
            EnqueueFlushWork(std::move(item));
        }
    }
    {
        std::lock_guard<std::mutex> g(PendingSweepMu_);
        PendingSweep_.clear();
    }
}

static std::chrono::milliseconds StaleSweepPeriodMs(ui64 intervalSec) {
    if (intervalSec == 0) {
        return std::chrono::seconds(1);
    }
    ui64 ms = intervalSec * 100U;
    if (ms < 100) {
        ms = 100;
    }
    if (ms > 1000) {
        ms = 1000;
    }
    return std::chrono::milliseconds(ms);
}

void TOtelLogsServer::TImpl::SweeperLoop() {
    const auto period = StaleSweepPeriodMs(Cfg.ShardBufferFlushIntervalSec);
    while (!StopSweeper.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(period);
        SweepShardBuffersIdle();
    }
}

TOtelLogsServer::TOtelLogsServer(TServerConfig cfg)
    : Cfg_(std::move(cfg))
    , Driver_(
          [](const TServerConfig& c) {
              NYdb::TDriverConfig dcfg;
              dcfg.SetEndpoint(c.YdbEndpoint).SetDatabase(c.YdbDatabase);
              if (!c.YdbToken.empty()) {
                  dcfg.SetAuthToken(c.YdbToken);
              } else {
                  dcfg.SetCredentialsProviderFactory(NYdb::CreateInsecureCredentialsProviderFactory());
              }
              return NYdb::TDriver(dcfg);
          }(Cfg_))
    , Impl_(std::make_unique<TImpl>(Cfg_, Driver_))
{
    Impl_->GrpcGenericService = std::make_unique<TLogsGrpcGenericService>(Impl_.get(), Cfg_);
}

TOtelLogsServer::~TOtelLogsServer() {
    Stop();
}

void TOtelLogsServer::Run() {
    Impl_->Run();
}

void TOtelLogsServer::Stop() {
    Impl_->Stop();
}

} // namespace NColumnShard::NOtelLogsToYdb
