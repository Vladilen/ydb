#include "otel_logs_json.h"

#include <library/cpp/json/json_writer.h>

#include <util/stream/str.h>

namespace NColumnShard::NOtelLogsToYdb {

void MergeStringMap(const THashMap<TString, TString>& src, THashMap<TString, TString>* dst) {
    for (const auto& [k, v] : src) {
        if (v.empty() || dst->contains(k)) {
            continue;
        }
        (*dst)[k] = v;
    }
}

TString JsonStringifyMap(const THashMap<TString, TString>& m) {
    if (m.empty()) {
        return TString{"{}"};
    }
    TStringStream out;
    NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
    buf.BeginObject();
    for (const auto& [k, v] : m) {
        buf.WriteKey(k);
        buf.WriteString(v);
    }
    buf.EndObject();
    return std::move(out.Str());
}

} // namespace NColumnShard::NOtelLogsToYdb
