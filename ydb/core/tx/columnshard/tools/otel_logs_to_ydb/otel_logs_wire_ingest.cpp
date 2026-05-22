#include "otel_logs_wire_ingest.h"

#include "otel_logs_json.h"
#include "otel_logs_routing.h"
#include "otel_logs_shard_hash.h"

#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>
#include <opentelemetry/proto/common/v1/common.pb.h>
#include <opentelemetry/proto/logs/v1/logs.pb.h>
#include <opentelemetry/proto/resource/v1/resource.pb.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite.h>

#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/proto_buffer_reader.h>

#include <util/charset/utf8.h>
#include <util/charset/wide.h>
#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/hex.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

using WFL = google::protobuf::internal::WireFormatLite;
using CIS = google::protobuf::io::CodedInputStream;

namespace cplog = opentelemetry::proto::collector::logs::v1;
namespace logspb = opentelemetry::proto::logs::v1;
namespace respb = opentelemetry::proto::resource::v1;
namespace commonpb = opentelemetry::proto::common::v1;

constexpr TStringBuf AttrProject = "project";
constexpr TStringBuf AttrCluster = "cluster";
constexpr TStringBuf AttrService = "service";
constexpr TStringBuf AttrServiceName = "service.name";
constexpr TStringBuf AttrK8DeploymentName = "k8s.deployment.name";
constexpr TStringBuf AttrK8NamespaceName = "k8s.namespace.name";
constexpr TStringBuf AttrRecordId = "record_id";
constexpr TStringBuf AttrRequestId = "request_id";

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

bool IsPerProjectLayout(const TServerConfig& cfg) {
    return cfg.TableLayout == "per_project";
}

TString Utf8SafeString(TStringBuf raw) {
    if (IsUtf(raw)) {
        return TString{raw};
    }
    TUtf16String wide = UTF8ToWide<true>(raw.Data(), raw.Size());
    return WideToUTF8(TWtringBuf(wide));
}

TString Utf8Safe(TString raw) {
    return Utf8SafeString(raw);
}

bool WireReadUtf8String(CIS* input, TString* out) {
    if (!WFL::ReadString(input, out)) {
        return false;
    }
    *out = Utf8SafeString(*out);
    return true;
}

TInstant TimestampFromNanos(ui64 ns) {
    if (!ns) {
        return TInstant::Now();
    }
    return TInstant::MicroSeconds(ns / 1000);
}

TString TraceHex(TStringBuf id) {
    if (id.size() == 16 || id.size() == 8) {
        return HexEncode(id.data(), id.size());
    }
    return {};
}

struct TResourceWireCtx {
    TString Project;
    TString Service;
    TString Cluster;
    THashMap<TString, TString> ResourceLabels;
    THashMap<TString, TString> ResourceMeta;
};

void SplitResourceAttrs(THashMap<TString, TString> attrs, TResourceWireCtx* ctx) {
    ctx->ResourceLabels.clear();
    ctx->ResourceMeta.clear();
    for (auto& [key, val] : attrs) {
        if (val.empty()) {
            continue;
        }
        if (key == AttrProject) {
            ctx->Project = std::move(val);
            continue;
        }
        if (key == AttrCluster) {
            ctx->Cluster = std::move(val);
            continue;
        }
        if (ResourceLabelAttrKeys().contains(key)) {
            ctx->ResourceLabels[std::move(key)] = std::move(val);
        } else {
            ctx->ResourceMeta[std::move(key)] = std::move(val);
        }
    }
}

TString ExtractServiceFromAttrs(const THashMap<TString, TString>& labels) {
    const TVector<TStringBuf> serviceKeys = {
        AttrService,
        AttrServiceName,
        AttrK8DeploymentName,
        AttrK8NamespaceName,
    };
    for (TStringBuf key : serviceKeys) {
        if (auto it = labels.find(TString{key}); it != labels.end() && !it->second.empty()) {
            return it->second;
        }
    }
    return {};
}

bool WireReadBytes(CIS* input, TString* out) {
    return WFL::ReadString(input, out);
}

bool WireParseAnyValueString(CIS* input, ui32 len, TString* out) {
    const auto limit = input->PushLimit(static_cast<int>(len));
    bool got = false;
    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        switch (WFL::GetTagFieldNumber(tag)) {
            case commonpb::AnyValue::kStringValueFieldNumber:
                if (WFL::GetTagWireType(tag) != WFL::WIRETYPE_LENGTH_DELIMITED) {
                    input->PopLimit(limit);
                    return false;
                }
                if (!WireReadUtf8String(input, out)) {
                    input->PopLimit(limit);
                    return false;
                }
                got = true;
                break;
            case commonpb::AnyValue::kBoolValueFieldNumber: {
                ui64 v = 0;
                if (!input->ReadVarint64(&v)) {
                    input->PopLimit(limit);
                    return false;
                }
                *out = v ? TString{"true"} : TString{"false"};
                got = true;
                break;
            }
            case commonpb::AnyValue::kIntValueFieldNumber: {
                ui64 v = 0;
                if (!input->ReadVarint64(&v)) {
                    input->PopLimit(limit);
                    return false;
                }
                *out = ToString(static_cast<i64>(v));
                got = true;
                break;
            }
            case commonpb::AnyValue::kDoubleValueFieldNumber: {
                double v = 0.0;
                if (!WFL::ReadPrimitive<double, WFL::TYPE_DOUBLE>(input, &v)) {
                    input->PopLimit(limit);
                    return false;
                }
                *out = ToString(v);
                got = true;
                break;
            }
            case commonpb::AnyValue::kBytesValueFieldNumber:
                if (WFL::GetTagWireType(tag) != WFL::WIRETYPE_LENGTH_DELIMITED) {
                    input->PopLimit(limit);
                    return false;
                }
                if (!WireReadBytes(input, out)) {
                    input->PopLimit(limit);
                    return false;
                }
                got = true;
                break;
            default:
                if (!WFL::SkipField(input, tag)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
        }
    }
    input->PopLimit(limit);
    if (!got) {
        *out = TString{"<non_scalar>"};
    }
    return true;
}

void WireClassifyLogAttribute(
    TString key,
    TString val,
    TOwnedLogRow* row,
    THashMap<TString, TString>* logLabels,
    THashMap<TString, TString>* logMeta)
{
    if (val.empty()) {
        return;
    }
    if (key == AttrRecordId) {
        row->RecordId = std::move(val);
        return;
    }
    if (LogLabelAttrKeys().contains(key)) {
        (*logLabels)[std::move(key)] = std::move(val);
    } else {
        (*logMeta)[std::move(key)] = std::move(val);
    }
}

bool WireParseKeyValue(CIS* input, ui32 len, TString* key, TString* val) {
    const auto limit = input->PushLimit(static_cast<int>(len));
    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        switch (WFL::GetTagFieldNumber(tag)) {
            case commonpb::KeyValue::kKeyFieldNumber:
                if (!WireReadUtf8String(input, key)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            case commonpb::KeyValue::kValueFieldNumber: {
                ui32 vlen = 0;
                if (!input->ReadVarint32(&vlen)) {
                    input->PopLimit(limit);
                    return false;
                }
                if (!WireParseAnyValueString(input, vlen, val)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            }
            default:
                if (!WFL::SkipField(input, tag)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
        }
    }
    input->PopLimit(limit);
    return true;
}

bool WireReadFixed64(CIS* input, ui32 tag, ui64* out) {
    if (WFL::GetTagWireType(tag) == WFL::WIRETYPE_FIXED64) {
        return input->ReadLittleEndian64(out);
    }
    return input->ReadVarint64(out);
}

bool WireParseAttributes(
    CIS* input,
    ui32 len,
    int repeatedKvFieldNumber,
    THashMap<TString, TString>* out)
{
    const auto limit = input->PushLimit(static_cast<int>(len));
    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        if (static_cast<int>(WFL::GetTagFieldNumber(tag)) != repeatedKvFieldNumber) {
            if (!WFL::SkipField(input, tag)) {
                input->PopLimit(limit);
                return false;
            }
            continue;
        }
        ui32 kvLen = 0;
        if (!input->ReadVarint32(&kvLen)) {
            input->PopLimit(limit);
            return false;
        }
        TString key;
        TString val;
        if (!WireParseKeyValue(input, kvLen, &key, &val)) {
            input->PopLimit(limit);
            return false;
        }
        if (!key.empty() && !val.empty()) {
            (*out)[std::move(key)] = std::move(val);
        }
    }
    input->PopLimit(limit);
    return true;
}

bool WireParseLogRecord(
    CIS* input,
    ui32 len,
    const TResourceWireCtx& res,
    TOwnedLogRow* row,
    THashMap<TString, TString>* logLabels,
    THashMap<TString, TString>* logMeta)
{
    const auto limit = input->PushLimit(static_cast<int>(len));
    ui64 timeNs = 0;
    ui64 observedNs = 0;
    i32 severity = 0;
    TString body;
    TString traceRaw;
    TString spanRaw;

    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        switch (WFL::GetTagFieldNumber(tag)) {
            case logspb::LogRecord::kTimeUnixNanoFieldNumber:
                if (!WireReadFixed64(input, tag, &timeNs)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            case logspb::LogRecord::kObservedTimeUnixNanoFieldNumber:
                if (!WireReadFixed64(input, tag, &observedNs)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            case logspb::LogRecord::kSeverityNumberFieldNumber: {
                ui32 sev = 0;
                if (!input->ReadVarint32(&sev)) {
                    input->PopLimit(limit);
                    return false;
                }
                severity = static_cast<i32>(sev);
                break;
            }
            case logspb::LogRecord::kBodyFieldNumber: {
                ui32 blen = 0;
                if (!input->ReadVarint32(&blen)) {
                    input->PopLimit(limit);
                    return false;
                }
                if (!WireParseAnyValueString(input, blen, &body)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            }
            case logspb::LogRecord::kAttributesFieldNumber: {
                // repeated KeyValue: each wire tag is field 6 with one KeyValue message (tags 1/2 inside).
                ui32 kvLen = 0;
                if (!input->ReadVarint32(&kvLen)) {
                    input->PopLimit(limit);
                    return false;
                }
                TString key;
                TString val;
                if (!WireParseKeyValue(input, kvLen, &key, &val)) {
                    input->PopLimit(limit);
                    return false;
                }
                WireClassifyLogAttribute(std::move(key), std::move(val), row, logLabels, logMeta);
                break;
            }
            case logspb::LogRecord::kTraceIdFieldNumber:
                if (!WireReadBytes(input, &traceRaw)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            case logspb::LogRecord::kSpanIdFieldNumber:
                if (!WireReadBytes(input, &spanRaw)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            default:
                if (!WFL::SkipField(input, tag)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
        }
    }
    input->PopLimit(limit);

    if (row->RecordId.empty()) {
        row->RecordId = CreateGuidAsString();
    }

    row->Ts = TimestampFromNanos(timeNs ? timeNs : observedNs);
    row->Service = res.Service;
    row->Cluster = Utf8Safe(res.Cluster);
    row->Level = severity;
    row->Message = body.empty() ? TString{} : Utf8Safe(std::move(body));

    THashMap<TString, TString> labels = std::move(*logLabels);
    const TString tid = TraceHex(traceRaw);
    if (!tid.empty()) {
        labels[TString{"trace_id"}] = tid;
    }
    const TString sid = TraceHex(spanRaw);
    if (!sid.empty()) {
        labels[TString{"span_id"}] = sid;
    }
    MergeStringMap(res.ResourceLabels, &labels);

    THashMap<TString, TString> meta = std::move(*logMeta);
    MergeStringMap(res.ResourceMeta, &meta);

    row->LabelsJson = JsonStringifyMap(labels);
    row->MetaJson = JsonStringifyMap(meta);
    return true;
}

bool WireParseScopeLogs(
    CIS* input,
    ui32 len,
    const TResourceWireCtx& res,
    const TRoutedTable& route,
    const TServerConfig& cfg,
    THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash>* buckets,
    size_t* logRows)
{
    const auto limit = input->PushLimit(static_cast<int>(len));
    const int partCount = (route.PkSchema == ELogsPkSchema::Dedicated) ? cfg.PartitionCountDedicated : cfg.PartitionCountCommon;

    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        if (WFL::GetTagFieldNumber(tag) != logspb::ScopeLogs::kLogRecordsFieldNumber) {
            if (!WFL::SkipField(input, tag)) {
                input->PopLimit(limit);
                return false;
            }
            continue;
        }
        ui32 recLen = 0;
        if (!input->ReadVarint32(&recLen) || recLen == 0) {
            continue;
        }
        TOwnedLogRow row;
        THashMap<TString, TString> logLabels;
        THashMap<TString, TString> logMeta;
        if (!WireParseLogRecord(input, recLen, res, &row, &logLabels, &logMeta)) {
            input->PopLimit(limit);
            return false;
        }

        TBuck bk;
        bk.Table = route.TablePath;
        bk.Schema = route.PkSchema;
        bk.Shard = 0;
        if (cfg.BatchByShardHash && partCount > 0) {
            bk.Shard = ShardIndexFromHash(HashOwnedLogRow(route.PkSchema, row), partCount);
        }
        (*buckets)[bk].push_back(std::move(row));
        ++*logRows;
    }
    input->PopLimit(limit);
    return true;
}

void BuildResourceWireCtx(THashMap<TString, TString> resourceAttrs, TResourceWireCtx* res) {
    SplitResourceAttrs(std::move(resourceAttrs), res);
    TString service = ExtractServiceFromAttrs(res->ResourceLabels);
    if (service.empty()) {
        for (const auto& [k, v] : res->ResourceMeta) {
            if (k == AttrService || k == AttrServiceName) {
                service = v;
                break;
            }
        }
    }
    res->Service = service.empty() ? TString{"_unknown"} : Utf8Safe(std::move(service));
}

bool WireParseResourceLogsInner(
    CIS* sub,
    const TServerConfig& cfg,
    const THashSet<TString>& allowed,
    bool hasFilter,
    const TResourceWireCtx& res,
    THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash>* buckets,
    size_t* logRows)
{
    while (sub->BytesUntilLimit() > 0) {
        const ui32 tag = sub->ReadTag();
        if (tag == 0) {
            break;
        }
        if (WFL::GetTagFieldNumber(tag) != logspb::ResourceLogs::kScopeLogsFieldNumber) {
            if (!WFL::SkipField(sub, tag)) {
                return false;
            }
            continue;
        }
        ui32 slen = 0;
        if (!sub->ReadVarint32(&slen)) {
            return false;
        }
        if (hasFilter && !allowed.contains(res.Project)) {
            if (!sub->Skip(static_cast<int>(slen))) {
                return false;
            }
            continue;
        }
        const TRoutedTable route = ResolveLogsTable(
            cfg, res.Project, res.Service, res.Cluster, IsPerProjectLayout(cfg));
        if (route.Drop) {
            if (!sub->Skip(static_cast<int>(slen))) {
                return false;
            }
            continue;
        }
        if (!WireParseScopeLogs(sub, slen, res, route, cfg, buckets, logRows)) {
            return false;
        }
    }
    return true;
}

bool WireParseResourceLogs(
    CIS* input,
    ui32 len,
    const TServerConfig& cfg,
    const THashSet<TString>& allowed,
    bool hasFilter,
    THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash>* buckets,
    size_t* logRows)
{
    if (len == 0) {
        return true;
    }
    const auto limit = input->PushLimit(static_cast<int>(len));
    if (input->BytesUntilLimit() < static_cast<int>(len)) {
        input->PopLimit(limit);
        return false;
    }
    TVector<char> blob;
    blob.yresize(len);
    if (!input->ReadRaw(blob.data(), static_cast<int>(len))) {
        input->PopLimit(limit);
        return false;
    }
    input->PopLimit(limit);

    const ui8* blobData = reinterpret_cast<const ui8*>(blob.data());
    const int blobSize = static_cast<int>(blob.size());

    TResourceWireCtx res;
    bool haveRes = false;

    {
        CIS sub(blobData, blobSize);
        while (sub.BytesUntilLimit() > 0) {
            const ui32 tag = sub.ReadTag();
            if (tag == 0) {
                break;
            }
            if (WFL::GetTagFieldNumber(tag) != logspb::ResourceLogs::kResourceFieldNumber) {
                if (!WFL::SkipField(&sub, tag)) {
                    return false;
                }
                continue;
            }
            ui32 rlen = 0;
            if (!sub.ReadVarint32(&rlen)) {
                return false;
            }
            THashMap<TString, TString> resourceAttrs;
            if (!WireParseAttributes(&sub, rlen, respb::Resource::kAttributesFieldNumber, &resourceAttrs)) {
                return false;
            }
            BuildResourceWireCtx(std::move(resourceAttrs), &res);
            haveRes = true;
        }
    }

    if (!haveRes) {
        BuildResourceWireCtx({}, &res);
    }

    CIS sub(blobData, blobSize);
    return WireParseResourceLogsInner(&sub, cfg, allowed, hasFilter, res, buckets, logRows);
}

} // namespace

bool ProcessExportWire(
    grpc::ByteBuffer* buf,
    const TServerConfig& cfg,
    THashMap<TBuck, TVector<TOwnedLogRow>, TBuckHash>* buckets,
    TWireExportParseStats* stats)
{
    if (!buf || !buf->Valid() || !buckets) {
        return false;
    }
    buckets->clear();

    grpc::ProtoBufferReader reader(buf);
    if (!reader.status().ok()) {
        return false;
    }

    THashSet<TString> allowed;
    for (const std::string& p : cfg.AllowedProjects) {
        allowed.insert(TString{p.data(), p.size()});
    }
    const bool hasFilter = !allowed.empty();

    CIS coded(&reader);
    size_t logRows = 0;

    while (true) {
        const ui32 tag = coded.ReadTag();
        if (tag == 0) {
            break;
        }
        if (WFL::GetTagFieldNumber(tag) != cplog::ExportLogsServiceRequest::kResourceLogsFieldNumber) {
            if (!WFL::SkipField(&coded, tag)) {
                return false;
            }
            continue;
        }
        ui32 len = 0;
        if (!coded.ReadVarint32(&len)) {
            return false;
        }
        if (!WireParseResourceLogs(&coded, len, cfg, allowed, hasFilter, buckets, &logRows)) {
            return false;
        }
    }

    if (stats) {
        stats->LogRows = logRows;
    }
    return true;
}

} // namespace NColumnShard::NOtelLogsToYdb
