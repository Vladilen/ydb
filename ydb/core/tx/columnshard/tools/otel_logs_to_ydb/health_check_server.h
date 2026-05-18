#pragma once

#include <util/generic/string.h>

#include <memory>

namespace NColumnShard::NOtelLogsToYdb {

/// Minimal HTTP server compatible with OpenTelemetry collector `health_check` extension defaults:
/// bind address (host:port, or ":port" for all interfaces), GET on `path` → 200 + `{}`.
class THealthCheckServer {
public:
    /// @param listen  "host:port" or ":port" (all interfaces). Empty → no HTTP server.
    THealthCheckServer(TString listen, TString path);
    ~THealthCheckServer();

    THealthCheckServer(const THealthCheckServer&) = delete;
    THealthCheckServer& operator=(const THealthCheckServer&) = delete;

    /// No-op if listen was empty. Throws if bind/listen fails.
    void Start();
    void Stop();

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NColumnShard::NOtelLogsToYdb
