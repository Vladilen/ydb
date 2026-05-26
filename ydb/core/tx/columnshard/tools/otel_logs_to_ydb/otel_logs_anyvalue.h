#pragma once

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/string.h>

namespace opentelemetry::proto::common::v1 {
class AnyValue;
class KeyValueList;
class ArrayValue;
} // namespace opentelemetry::proto::common::v1

namespace NColumnShard::NOtelLogsToYdb {

/// Go pdata `Value.AsString()`: scalars as text; map/slice as compact JSON; bytes as base64.
void JsonValueToOtelAsString(const NJson::TJsonValue& jv, TString* out);

void AnyValueToOtelAsString(const opentelemetry::proto::common::v1::AnyValue& v, TString* out);

/// `streamingJson`: nested map/array serialized with `NJsonWriter` (no `TJsonValue` tree).
void AnyValueToOtelAsString(
    const opentelemetry::proto::common::v1::AnyValue& v,
    TString* out,
    bool streamingJson);

void KeyValueListToJsonValue(const opentelemetry::proto::common::v1::KeyValueList& list, NJson::TJsonValue* jv);

void ArrayValueToJsonValue(const opentelemetry::proto::common::v1::ArrayValue& arr, NJson::TJsonValue* jv);

void AnyValueToJsonValue(const opentelemetry::proto::common::v1::AnyValue& v, NJson::TJsonValue* jv);

} // namespace NColumnShard::NOtelLogsToYdb
