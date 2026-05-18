#include "health_check_server.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>

#include <util/generic/yexception.h>
#include <util/network/init.h>
#include <util/string/cast.h>

namespace NColumnShard::NOtelLogsToYdb {

namespace {

void ParseListen(const TString& listen, TString* host, ui16* port) {
    TStringBuf buf(listen);
    TStringBuf h;
    TStringBuf p;
    if (buf.TryRSplit(':', h, p)) {
        // Empty host means "all interfaces" via TNetworkAddress(port) (AI_PASSIVE), same as THttpServerOptions.
        *host = h.empty() ? TString{} : TString{h};
        *port = FromString<ui16>(TString{p});
    } else if (!buf.empty()) {
        *host = TString{buf};
        *port = 13133;
    } else {
        ythrow yexception() << "health listen address is empty";
    }
}

class THealthCheckCallback final : public THttpServer::ICallBack {
public:
    explicit THealthCheckCallback(TString path)
        : Path_(std::move(path))
    {
    }

    class TRequest final : public TRequestReplier {
    public:
        explicit TRequest(TString path)
            : Path_(std::move(path))
        {
        }

        bool DoReply(const TReplyParams& params) override {
            TParsedHttpFull parsed(params.Input.FirstLine());
            if (TStringBuf(parsed.Method) != TStringBuf("GET") || parsed.Path != TStringBuf(Path_)) {
                THttpResponse resp(HTTP_NOT_FOUND);
                resp.SetContent(TString{"not found"});
                resp.OutTo(params.Output);
                return true;
            }
            THttpResponse resp(HTTP_OK);
            resp.SetContentType("application/json");
            resp.SetContent(TString{"{}"});
            resp.OutTo(params.Output);
            return true;
        }

    private:
        TString Path_;
    };

    TClientRequest* CreateClient() override {
        return new TRequest(Path_);
    }

private:
    TString Path_;
};

} // namespace

struct THealthCheckServer::TImpl {
    TString Listen;
    TString Path;
    THealthCheckCallback Callback;
    THttpServer Http;

    TImpl(TString listen, TString path)
        : Listen(std::move(listen))
        , Path(std::move(path))
        , Callback(Path)
        , Http(&Callback, MakeOptions(Listen))
    {
    }

    static THttpServerOptions MakeOptions(const TString& listen) {
        TString host;
        ui16 port = 0;
        ParseListen(listen, &host, &port);
        // Literal "0.0.0.0" is resolved via getaddrinfo with AI_ADDRCONFIG and can fail (EAI_ADDRFAMILY)
        // on some hosts; passive bind uses nullptr host — see library/cpp/http/server/options.cpp ToNetworkAddr.
        if (host == TStringBuf("0.0.0.0")) {
            host.clear();
        }
        THttpServerOptions opts;
        opts.AddBindAddress(host, port);
        opts.SetThreads(2);
        return opts;
    }

    void Start() {
        InitNetworkSubSystem();
        // THttpServer::TryToBindAddresses swallows exceptions from BindAddresses and then
        // reports errno, which is often unrelated (e.g. EINPROGRESS). Surface resolution/bind prep errors here.
        THttpServerOptions::TBindAddresses addrs;
        try {
            Http.Options().BindAddresses(addrs);
        } catch (const std::exception& ex) {
            ythrow yexception() << "health check: invalid listen address \"" << Listen << "\": " << ex.what();
        }
        if (!Http.Start()) {
            ythrow yexception() << "health check HTTP server failed to start (errno " << Http.GetErrorCode() << "): "
                                << Http.GetError();
        }
    }

    void Stop() {
        Http.Stop();
    }
};

THealthCheckServer::THealthCheckServer(TString listen, TString path) {
    if (!listen.empty()) {
        Impl_ = std::make_unique<TImpl>(std::move(listen), std::move(path));
    }
}

THealthCheckServer::~THealthCheckServer() {
    Stop();
}

void THealthCheckServer::Start() {
    if (Impl_) {
        Impl_->Start();
    }
}

void THealthCheckServer::Stop() {
    if (Impl_) {
        Impl_->Stop();
    }
}

} // namespace NColumnShard::NOtelLogsToYdb
