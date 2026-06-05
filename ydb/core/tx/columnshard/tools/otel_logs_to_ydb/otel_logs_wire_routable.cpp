#include "otel_logs_wire_routable.h"

#include "otel_logs_routing.h"
#include "otel_logs_service.h"

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
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

using WFL = google::protobuf::internal::WireFormatLite;
using CIS = google::protobuf::io::CodedInputStream;

namespace cplog = opentelemetry::proto::collector::logs::v1;
namespace logspb = opentelemetry::proto::logs::v1;
namespace respb = opentelemetry::proto::resource::v1;
namespace commonpb = opentelemetry::proto::common::v1;

constexpr int ExportResourceLogsTagFieldNumber = cplog::ExportLogsServiceRequest::kResourceLogsFieldNumber;
constexpr int ResourceLogsResourceTagFieldNumber = logspb::ResourceLogs::kResourceFieldNumber;
constexpr int ResourceLogsScopeLogsTagFieldNumber = logspb::ResourceLogs::kScopeLogsFieldNumber;
constexpr int ResourceAttributesTagFieldNumber = respb::Resource::kAttributesFieldNumber;
constexpr int ScopeLogsLogRecordsTagFieldNumber = logspb::ScopeLogs::kLogRecordsFieldNumber;
constexpr int KeyValueKeyTagFieldNumber = commonpb::KeyValue::kKeyFieldNumber;
constexpr int KeyValueValueTagFieldNumber = commonpb::KeyValue::kValueFieldNumber;
constexpr int AnyValueStringValueTagFieldNumber = commonpb::AnyValue::kStringValueFieldNumber;

// ReadTag() returns 0 at end of stream / on error — not a protobuf field number.

constexpr TStringBuf AttrProject = "project";
constexpr TStringBuf AttrCluster = "cluster";
constexpr TStringBuf AttrService = "service";
constexpr TStringBuf AttrServiceName = "service.name";
constexpr TStringBuf AttrK8DeploymentName = "k8s.deployment.name";
constexpr TStringBuf AttrK8NamespaceName = "k8s.namespace.name";

TString Utf8SafeString(TStringBuf raw) {
    if (IsUtf(raw)) {
        return TString{raw};
    }
    TUtf16String wide = UTF8ToWide<true>(raw.Data(), raw.Size());
    return WideToUTF8(TWtringBuf(wide));
}

bool WireReadUtf8String(CIS* input, TString* out) {
    if (!WFL::ReadString(input, out)) {
        return false;
    }
    *out = Utf8SafeString(*out);
    return true;
}

bool WireParseResourceAttributes(CIS* input, ui32 len, THashMap<TString, TString>* attrs) {
    const auto limit = input->PushLimit(static_cast<int>(len));
    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        if (WFL::GetTagFieldNumber(tag) != ResourceAttributesTagFieldNumber) {
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
        const auto kvLimit = input->PushLimit(static_cast<int>(kvLen));
        TString key;
        TString val;
        while (input->BytesUntilLimit() > 0) {
            const ui32 kvTag = input->ReadTag();
            if (kvTag == 0) {
                break;
            }
            switch (WFL::GetTagFieldNumber(kvTag)) {
                case KeyValueKeyTagFieldNumber:
                    if (!WireReadUtf8String(input, &key)) {
                        input->PopLimit(kvLimit);
                        input->PopLimit(limit);
                        return false;
                    }
                    break;
                case KeyValueValueTagFieldNumber: {
                    ui32 vlen = 0;
                    if (!input->ReadVarint32(&vlen)) {
                        input->PopLimit(kvLimit);
                        input->PopLimit(limit);
                        return false;
                    }
                    const auto vLimit = input->PushLimit(static_cast<int>(vlen));
                    while (input->BytesUntilLimit() > 0) {
                        const ui32 vTag = input->ReadTag();
                        if (vTag == 0) {
                            break;
                        }
                        if (WFL::GetTagFieldNumber(vTag) == AnyValueStringValueTagFieldNumber
                            && WFL::GetTagWireType(vTag) == WFL::WIRETYPE_LENGTH_DELIMITED)
                        {
                            if (!WireReadUtf8String(input, &val)) {
                                input->PopLimit(vLimit);
                                input->PopLimit(kvLimit);
                                input->PopLimit(limit);
                                return false;
                            }
                        } else if (!WFL::SkipField(input, vTag)) {
                            input->PopLimit(vLimit);
                            input->PopLimit(kvLimit);
                            input->PopLimit(limit);
                            return false;
                        }
                    }
                    input->PopLimit(vLimit);
                    break;
                }
                default:
                    if (!WFL::SkipField(input, kvTag)) {
                        input->PopLimit(kvLimit);
                        input->PopLimit(limit);
                        return false;
                    }
                    break;
            }
        }
        input->PopLimit(kvLimit);
        if (!key.empty() && !val.empty()) {
            (*attrs)[std::move(key)] = std::move(val);
        }
    }
    input->PopLimit(limit);
    return true;
}

bool WireScopeLogsHasLogRecords(CIS* input, ui32 len) {
    const auto limit = input->PushLimit(static_cast<int>(len));
    bool has = false;
    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        if (WFL::GetTagFieldNumber(tag) == ScopeLogsLogRecordsTagFieldNumber) {
            ui32 recLen = 0;
            if (!input->ReadVarint32(&recLen)) {
                input->PopLimit(limit);
                return false;
            }
            if (recLen > 0) {
                has = true;
            }
            if (!input->Skip(recLen)) {
                input->PopLimit(limit);
                return false;
            }
        } else if (!WFL::SkipField(input, tag)) {
            input->PopLimit(limit);
            return false;
        }
    }
    input->PopLimit(limit);
    return has;
}

TString ExtractServiceFromAttrs(const THashMap<TString, TString>& attrs) {
    const TVector<TStringBuf> serviceKeys = {
        AttrService,
        AttrServiceName,
        AttrK8DeploymentName,
        AttrK8NamespaceName,
    };
    for (TStringBuf key : serviceKeys) {
        if (auto it = attrs.find(TString{key}); it != attrs.end() && !it->second.empty()) {
            return it->second;
        }
    }
    return {};
}

bool ResourceLogsGroupRoutable(CIS* input, ui32 len, const TServerConfig& cfg) {
    const auto limit = input->PushLimit(static_cast<int>(len));
    THashMap<TString, TString> attrs;
    bool hasLogRecords = false;
    while (input->BytesUntilLimit() > 0) {
        const ui32 tag = input->ReadTag();
        if (tag == 0) {
            break;
        }
        switch (WFL::GetTagFieldNumber(tag)) {
            case ResourceLogsResourceTagFieldNumber: {
                ui32 rlen = 0;
                if (!input->ReadVarint32(&rlen)) {
                    input->PopLimit(limit);
                    return false;
                }
                if (!WireParseResourceAttributes(input, rlen, &attrs)) {
                    input->PopLimit(limit);
                    return false;
                }
                break;
            }
            case ResourceLogsScopeLogsTagFieldNumber: {
                ui32 slen = 0;
                if (!input->ReadVarint32(&slen)) {
                    input->PopLimit(limit);
                    return false;
                }
                if (WireScopeLogsHasLogRecords(input, slen)) {
                    hasLogRecords = true;
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

    if (!hasLogRecords) {
        return false;
    }

    TString project;
    if (auto it = attrs.find(TString{AttrProject}); it != attrs.end()) {
        project = it->second;
    }
    const TString service = ExtractServiceFromAttrs(attrs);
    TString cluster;
    if (auto it = attrs.find(TString{AttrCluster}); it != attrs.end()) {
        cluster = it->second;
    }

    if (!cfg.AllowedProjects.empty()) {
        bool ok = false;
        for (const std::string& p : cfg.AllowedProjects) {
            if (TStringBuf(p.data(), p.size()) == TStringBuf(project)) {
                ok = true;
                break;
            }
        }
        if (!ok) {
            return false;
        }
    }

    const bool perProject = (cfg.TableLayout == "per_project");
    const auto routes = ResolveLogsTables(cfg, project, service, cluster, perProject);
    for (const auto& r : routes) {
        if (!r.Drop) {
            return true;
        }
    }
    return false;
}

} // namespace

bool ExportWireHasRoutableLogRows(grpc::ByteBuffer* buf, const TServerConfig& cfg) {
    if (!buf || !buf->Valid()) {
        return false;
    }

    grpc::ProtoBufferReader reader(buf);
    if (!reader.status().ok()) {
        return true;
    }

    CIS coded(&reader);
    while (true) {
        const ui32 tag = coded.ReadTag();
        if (tag == 0) {
            break;
        }
        if (WFL::GetTagFieldNumber(tag) != ExportResourceLogsTagFieldNumber) {
            if (!WFL::SkipField(&coded, tag)) {
                return true;
            }
            continue;
        }
        ui32 len = 0;
        if (!coded.ReadVarint32(&len)) {
            return true;
        }
        if (ResourceLogsGroupRoutable(&coded, len, cfg)) {
            return true;
        }
    }
    return false;
}

} // namespace NColumnShard::NOtelLogsToYdb
