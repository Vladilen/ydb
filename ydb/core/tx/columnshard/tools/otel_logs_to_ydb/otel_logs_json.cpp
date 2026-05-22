#include "otel_logs_json.h"

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

namespace NColumnShard::NOtelLogsToYdb {

void MergeStringMap(const THashMap<TString, TString>& src, THashMap<TString, TString>* dst) {
    for (const auto& [k, v] : src) {
        if (!dst->contains(k)) {
            (*dst)[k] = v;
        }
    }
}

TString JsonStringifyMap(const THashMap<TString, TString>& m) {
    if (m.empty()) {
        return TString{"{}"};
    }
    NJson::TJsonValue jv;
    jv.SetType(NJson::JSON_MAP);
    for (const auto& [k, v] : m) {
        jv.InsertValue(k, v);
    }
    return NJson::WriteJson(&jv, /*formatOutput*/ false);
}

} // namespace NColumnShard::NOtelLogsToYdb
