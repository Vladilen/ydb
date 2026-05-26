#include "otel_logs_anyvalue.h"

#include <opentelemetry/proto/common/v1/common.pb.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/cast.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace commonpb = opentelemetry::proto::common::v1;

namespace {

void AnyValueToJsonValueImpl(const commonpb::AnyValue& v, NJson::TJsonValue* jv);

} // namespace

void KeyValueListToJsonValue(const commonpb::KeyValueList& list, NJson::TJsonValue* jv) {
    jv->SetType(NJson::JSON_MAP);
    for (const commonpb::KeyValue& kv : list.values()) {
        NJson::TJsonValue val;
        AnyValueToJsonValueImpl(kv.value(), &val);
        jv->InsertValue(TString{kv.key()}, std::move(val));
    }
}

void ArrayValueToJsonValue(const commonpb::ArrayValue& arr, NJson::TJsonValue* jv) {
    jv->SetType(NJson::JSON_ARRAY);
    for (const commonpb::AnyValue& el : arr.values()) {
        NJson::TJsonValue item;
        AnyValueToJsonValueImpl(el, &item);
        jv->AppendValue(std::move(item));
    }
}

void AnyValueToJsonValue(const commonpb::AnyValue& v, NJson::TJsonValue* jv) {
    AnyValueToJsonValueImpl(v, jv);
}

namespace {

void AnyValueToJsonValueImpl(const commonpb::AnyValue& v, NJson::TJsonValue* jv) {
    switch (v.value_case()) {
        case commonpb::AnyValue::kStringValue:
            *jv = TString{v.string_value()};
            break;
        case commonpb::AnyValue::kBoolValue:
            *jv = v.bool_value();
            break;
        case commonpb::AnyValue::kIntValue:
            *jv = v.int_value();
            break;
        case commonpb::AnyValue::kDoubleValue:
            *jv = v.double_value();
            break;
        case commonpb::AnyValue::kBytesValue:
            *jv = Base64Encode(TStringBuf{v.bytes_value()});
            break;
        case commonpb::AnyValue::kArrayValue:
            ArrayValueToJsonValue(v.array_value(), jv);
            break;
        case commonpb::AnyValue::kKvlistValue:
            KeyValueListToJsonValue(v.kvlist_value(), jv);
            break;
        default:
            *jv = NJson::TJsonValue{};
            break;
    }
}

} // namespace

void JsonValueToOtelAsString(const NJson::TJsonValue& jv, TString* out) {
    switch (jv.GetType()) {
        case NJson::JSON_NULL:
        case NJson::JSON_UNDEFINED:
            out->clear();
            break;
        case NJson::JSON_STRING:
            *out = jv.GetString();
            break;
        case NJson::JSON_BOOLEAN:
            *out = jv.GetBoolean() ? TString{"true"} : TString{"false"};
            break;
        case NJson::JSON_INTEGER:
            *out = ToString(jv.GetInteger());
            break;
        case NJson::JSON_UINTEGER:
            *out = ToString(jv.GetUInteger());
            break;
        case NJson::JSON_DOUBLE:
            *out = ToString(jv.GetDouble());
            break;
        case NJson::JSON_MAP:
        case NJson::JSON_ARRAY:
            *out = NJson::WriteJson(&jv, /*formatOutput*/ false);
            break;
        default:
            out->clear();
            break;
    }
}

namespace {

void AnyValueWriteToJsonWriter(const commonpb::AnyValue& v, NJsonWriter::TBuf* w) {
    switch (v.value_case()) {
        case commonpb::AnyValue::kStringValue:
            w->WriteString(TStringBuf{v.string_value()});
            break;
        case commonpb::AnyValue::kBoolValue:
            w->WriteBool(v.bool_value());
            break;
        case commonpb::AnyValue::kIntValue:
            w->WriteLongLong(v.int_value());
            break;
        case commonpb::AnyValue::kDoubleValue:
            w->WriteDouble(v.double_value());
            break;
        case commonpb::AnyValue::kBytesValue:
            w->WriteString(TStringBuf{Base64Encode(TStringBuf{v.bytes_value()})});
            break;
        case commonpb::AnyValue::kArrayValue: {
            auto list = w->BeginList();
            for (const commonpb::AnyValue& el : v.array_value().values()) {
                AnyValueWriteToJsonWriter(el, w);
            }
            w->EndList();
            Y_UNUSED(list);
            break;
        }
        case commonpb::AnyValue::kKvlistValue: {
            auto obj = w->BeginObject();
            for (const commonpb::KeyValue& kv : v.kvlist_value().values()) {
                obj.WriteKey(TStringBuf{kv.key()});
                AnyValueWriteToJsonWriter(kv.value(), w);
            }
            w->EndObject();
            Y_UNUSED(obj);
            break;
        }
        default:
            w->WriteNull();
            break;
    }
}

void AnyValueNestedToJsonString(const commonpb::AnyValue& v, TString* out) {
    TStringStream ss;
    NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss);
    AnyValueWriteToJsonWriter(v, &buf);
    *out = std::move(ss.Str());
}

} // namespace

void AnyValueToOtelAsString(const commonpb::AnyValue& v, TString* out) {
    AnyValueToOtelAsString(v, out, false);
}

void AnyValueToOtelAsString(const commonpb::AnyValue& v, TString* out, bool streamingJson) {
    if (!streamingJson) {
        NJson::TJsonValue jv;
        AnyValueToJsonValueImpl(v, &jv);
        JsonValueToOtelAsString(jv, out);
        return;
    }
    switch (v.value_case()) {
        case commonpb::AnyValue::kStringValue:
            *out = TString{v.string_value()};
            return;
        case commonpb::AnyValue::kBoolValue:
            *out = v.bool_value() ? TString{"true"} : TString{"false"};
            return;
        case commonpb::AnyValue::kIntValue:
            *out = ToString(v.int_value());
            return;
        case commonpb::AnyValue::kDoubleValue:
            *out = ToString(v.double_value());
            return;
        case commonpb::AnyValue::kBytesValue:
            *out = Base64Encode(TStringBuf{v.bytes_value()});
            return;
        case commonpb::AnyValue::kArrayValue:
        case commonpb::AnyValue::kKvlistValue:
            AnyValueNestedToJsonString(v, out);
            return;
        default:
            out->clear();
            return;
    }
}

} // namespace NColumnShard::NOtelLogsToYdb
