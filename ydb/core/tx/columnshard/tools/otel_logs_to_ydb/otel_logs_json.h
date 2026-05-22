#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
namespace NColumnShard::NOtelLogsToYdb {

/// Same as proto path: `THashMap` → `NJson::TJsonValue` map → `NJson::WriteJson`.
TString JsonStringifyMap(const THashMap<TString, TString>& m);

/// `dst` keeps existing keys; fills only keys missing in `dst` (proto labels/meta merge).
void MergeStringMap(const THashMap<TString, TString>& src, THashMap<TString, TString>* dst);

} // namespace NColumnShard::NOtelLogsToYdb
