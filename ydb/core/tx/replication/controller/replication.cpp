#include "lag_provider.h"
#include "private_events.h"
#include "replication.h"
#include "secret_resolver.h"
#include "target_discoverer.h"
#include "target_table.h"
#include "target_transfer.h"
#include "tenant_resolver.h"
#include "util.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>

namespace NKikimr::NReplication::NController {

class TReplication::TImpl: public TLagProvider {
    friend class TReplication;

    struct TTarget: public TItemWithLag {
        THolder<ITarget> Ptr;

        explicit TTarget(ITarget* iface)
            : Ptr(iface)
        {
        }

        inline ITarget* operator->() const {
            Y_ABORT_UNLESS(Ptr);
            return Ptr.Get();
        }
    };

    void ResolveSecret(const TString& secretName, const TActorContext& ctx) {
        if (SecretResolver) {
            return;
        }

        SecretResolver = ctx.Register(CreateSecretResolver(ctx.SelfID, ReplicationId, PathId, secretName));
    }

    template <typename... Args>
    ITarget* CreateTarget(TReplication* self, ui64 id, ETargetKind kind, Args&&... args) const {
        switch (kind) {
        case ETargetKind::Table:
            return new TTargetTable(self, id, std::forward<Args>(args)...);
        case ETargetKind::IndexTable:
            return new TTargetIndexTable(self, id, std::forward<Args>(args)...);
        case ETargetKind::Transfer:
            return new TTargetTransfer(self, id, std::forward<Args>(args)...);
        }
    }

    void DiscoverTargets(const TActorContext& ctx) {
        if (TargetDiscoverer) {
            return;
        }

        switch (Config.GetTargetCase()) {
            case NKikimrReplication::TReplicationConfig::kEverything:
                return ErrorState("Not implemented");

            case NKikimrReplication::TReplicationConfig::kSpecific:
            case NKikimrReplication::TReplicationConfig::kTransferSpecific:
                TargetDiscoverer = ctx.Register(CreateTargetDiscoverer(ctx.SelfID, ReplicationId, YdbProxy, Config));
                break;

            default:
                return ErrorState(TStringBuilder() << "Unexpected targets: " << Config.GetTargetCase());
        }
    }

    void ProgressTargets(const TActorContext& ctx) {
        for (auto& [_, target] : Targets) {
            target->Progress(ctx);
        }
    }

public:
    template <typename T, typename D>
    explicit TImpl(ui64 id, const TPathId& pathId, T&& config, D&& database)
        : ReplicationId(id)
        , PathId(pathId)
        , Config(std::forward<T>(config))
        , Database(std::forward<D>(database))
    {
    }

    template <typename... Args>
    ui64 AddTarget(TReplication* self, ui64 id, ETargetKind kind, Args&&... args) {
        TargetTablePaths.clear();
        const auto res = Targets.emplace(id, CreateTarget(self, id, kind, std::forward<Args>(args)...));
        Y_VERIFY_S(res.second, "Duplicate target: " << id);
        TLagProvider::AddPendingLag(id);
        return id;
    }

    template <typename... Args>
    ui64 AddTarget(TReplication* self, ETargetKind kind, Args&&... args) {
        return AddTarget(self, NextTargetId++, kind, std::forward<Args>(args)...);
    }

    ITarget* FindTarget(ui64 id) {
        auto it = Targets.find(id);
        return it != Targets.end()
            ? it->second.Ptr.Get()
            : nullptr;
    }

    void RemoveTarget(ui64 id) {
        Targets.erase(id);
        TargetTablePaths.clear();
    }

    const TVector<TString>& GetTargetTablePaths() const {
        if (!TargetTablePaths) {
            TargetTablePaths.reserve(Targets.size());
            for (const auto& [_, target] : Targets) {
                switch (target->GetKind()) {
                case ETargetKind::Table:
                case ETargetKind::IndexTable:
                case ETargetKind::Transfer:
                    TargetTablePaths.push_back(target->GetDstPath());
                    break;
                }
            }
        }

        return TargetTablePaths;
    }

    void Progress(const TActorContext& ctx) {
        if (!YdbProxy && !(State == EState::Removing && !Targets)) {
            THolder<IActor> ydbProxy;
            const auto& params = Config.GetSrcConnectionParams();
            const auto& endpoint = params.GetEndpoint();
            const auto& database = params.GetDatabase();
            const bool ssl = params.GetEnableSsl();

            if (endpoint.empty()) {
                ydbProxy.Reset(CreateLocalYdbProxy(Database));
            } else {
                switch (params.GetCredentialsCase()) {
                case NKikimrReplication::TConnectionParams::kStaticCredentials:
                    if (!params.GetStaticCredentials().HasPassword()) {
                        return ResolveSecret(params.GetStaticCredentials().GetPasswordSecretName(), ctx);
                    }
                    ydbProxy.Reset(CreateYdbProxy(endpoint, database, ssl, params.GetStaticCredentials()));
                    break;
                case NKikimrReplication::TConnectionParams::kOAuthToken:
                    if (!params.GetOAuthToken().HasToken()) {
                        return ResolveSecret(params.GetOAuthToken().GetTokenSecretName(), ctx);
                    }
                    ydbProxy.Reset(CreateYdbProxy(endpoint, database, ssl, params.GetOAuthToken().GetToken()));
                    break;
                default:
                    ErrorState(TStringBuilder() << "Unexpected credentials: " << params.GetCredentialsCase());
                    break;
                }
            }

            if (ydbProxy) {
                YdbProxy = ctx.Register(ydbProxy.Release());
            }
        }

        if (!Tenant && !TenantResolver) {
            TenantResolver = ctx.Register(CreateTenantResolver(ctx.SelfID, ReplicationId, PathId));
        }

        switch (State) {
        case EState::Ready:
        case EState::Paused:
            if (!Targets) {
                return DiscoverTargets(ctx);
            } else {
                return ProgressTargets(ctx);
            }
        case EState::Removing:
            if (!Targets) {
                return (void)ctx.Send(ctx.SelfID, new TEvPrivate::TEvDropReplication(ReplicationId));
            } else {
                return ProgressTargets(ctx);
            }
        case EState::Done:
        case EState::Error:
            return;
        }
    }

    void Shutdown(const TActorContext& ctx) {
        for (auto& [_, target] : Targets) {
            target->Shutdown(ctx);
        }

        for (auto* x : TVector<TActorId*>{&SecretResolver, &TargetDiscoverer, &TenantResolver, &YdbProxy}) {
            if (auto actorId = std::exchange(*x, {})) {
                ctx.Send(actorId, new TEvents::TEvPoison());
            }
        }
    }

    void SetState(EState state, TString issue = {}) {
        State = state;
        Issue = TruncatedIssue(issue);
    }

    void SetConfig(NKikimrReplication::TReplicationConfig&& config) {
        Config = config;
    }

    void ErrorState(TString issue) {
        SetState(EState::Error, issue);
    }

    void UpdateLag(ui64 targetId, TDuration lag) {
        auto it = Targets.find(targetId);
        if (it == Targets.end()) {
            return;
        }

        TLagProvider::UpdateLag(it->second, targetId, lag);
    }

private:
    const ui64 ReplicationId;
    const TPathId PathId;
    TString Tenant;

    NKikimrReplication::TReplicationConfig Config;
    TString Database;
    EState State = EState::Ready;
    TString Issue;
    EState DesiredState = EState::Ready;
    ui64 NextTargetId = 1;
    THashMap<ui64, TTarget> Targets;
    THashSet<ui64> PendingAlterTargets;
    mutable TVector<TString> TargetTablePaths;
    TActorId SecretResolver;
    TActorId YdbProxy;
    TActorId TenantResolver;
    TActorId TargetDiscoverer;

}; // TImpl

TReplication::TReplication(ui64 id, const TPathId& pathId, const NKikimrReplication::TReplicationConfig& config, const TString& database)
    : Impl(std::make_shared<TImpl>(id, pathId, config, database))
{
}

TReplication::TReplication(ui64 id, const TPathId& pathId, NKikimrReplication::TReplicationConfig&& config, TString&& database)
    : Impl(std::make_shared<TImpl>(id, pathId, std::move(config), std::move(database)))
{
}

static auto ParseConfig(const TString& config) {
    NKikimrReplication::TReplicationConfig cfg;
    Y_ABORT_UNLESS(cfg.ParseFromString(config));
    return cfg;
}

TReplication::TReplication(ui64 id, const TPathId& pathId, const TString& config, const TString& database)
    : Impl(std::make_shared<TImpl>(id, pathId, ParseConfig(config), database))
{
}

ui64 TReplication::AddTarget(ETargetKind kind, const ITarget::IConfig::TPtr& config) {
    return Impl->AddTarget(this, kind, config);
}

TReplication::ITarget* TReplication::AddTarget(ui64 id, ETargetKind kind, const ITarget::IConfig::TPtr& config) {
    Impl->AddTarget(this, id, kind, config);
    return Impl->FindTarget(id);
}

const TReplication::ITarget* TReplication::FindTarget(ui64 id) const {
    return Impl->FindTarget(id);
}

TReplication::ITarget* TReplication::FindTarget(ui64 id) {
    return Impl->FindTarget(id);
}

void TReplication::RemoveTarget(ui64 id) {
    return Impl->RemoveTarget(id);
}

const TVector<TString>& TReplication::GetTargetTablePaths() const {
    return Impl->GetTargetTablePaths();
}

void TReplication::Progress(const TActorContext& ctx) {
    Impl->Progress(ctx);
}

void TReplication::Shutdown(const TActorContext& ctx) {
    Impl->Shutdown(ctx);
}

ui64 TReplication::GetId() const {
    return Impl->ReplicationId;
}

const TPathId& TReplication::GetPathId() const {
    return Impl->PathId;
}

const TActorId& TReplication::GetYdbProxy() const {
    return Impl->YdbProxy;
}

ui64 TReplication::GetSchemeShardId() const {
    return GetPathId().OwnerId;
}

void TReplication::SetConfig(NKikimrReplication::TReplicationConfig&& config) {
    Impl->SetConfig(std::move(config));
}

const NKikimrReplication::TReplicationConfig& TReplication::GetConfig() const {
    return Impl->Config;
}

const TString& TReplication::GetDatabase() const {
    return Impl->Database;
}

void TReplication::SetState(EState state, TString issue) {
    Impl->SetState(state, issue);
}

TReplication::EState TReplication::GetState() const {
    return Impl->State;
}

const TString& TReplication::GetIssue() const {
    return Impl->Issue;
}

TReplication::EState TReplication::GetDesiredState() const {
    return Impl->DesiredState;
}

void TReplication::SetDesiredState(EState state) {
    Impl->DesiredState = state;
}

void TReplication::SetNextTargetId(ui64 value) {
    Impl->NextTargetId = value;
}

ui64 TReplication::GetNextTargetId() const {
    return Impl->NextTargetId;
}

void TReplication::UpdateSecret(const TString& secretValue) {
    auto& params = *Impl->Config.MutableSrcConnectionParams();
    switch (params.GetCredentialsCase()) {
    case NKikimrReplication::TConnectionParams::kStaticCredentials:
        params.MutableStaticCredentials()->SetPassword(secretValue);
        break;
    case NKikimrReplication::TConnectionParams::kOAuthToken:
        params.MutableOAuthToken()->SetToken(secretValue);
        break;
    default:
        Y_ABORT("unreachable");
    }
}

void TReplication::SetTenant(const TString& value) {
    Impl->Tenant = value;
    Impl->TenantResolver = {};
}

const TString& TReplication::GetTenant() const {
    return Impl->Tenant;
}

void TReplication::SetDropOp(const TActorId& sender, const std::pair<ui64, ui32>& opId) {
    DropOp = {sender, opId};
}

const std::optional<TReplication::TDropOp>& TReplication::GetDropOp() const {
    return DropOp;
}

void TReplication::AddPendingAlterTarget(ui64 id) {
    Impl->PendingAlterTargets.insert(id);
}

void TReplication::RemovePendingAlterTarget(ui64 id) {
    Impl->PendingAlterTargets.erase(id);
}

bool TReplication::CheckAlterDone() const {
    return (Impl->State == EState::Ready || Impl->State == EState::Paused) && Impl->PendingAlterTargets.empty();
}

void TReplication::UpdateLag(ui64 targetId, TDuration lag) {
    Impl->UpdateLag(targetId, lag);
}

const TMaybe<TDuration> TReplication::GetLag() const {
    return Impl->GetLag();
}

}

Y_DECLARE_OUT_SPEC(, NKikimrReplication::TReplicationConfig::TargetCase, stream, value) {
    stream << static_cast<int>(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrReplication::TConnectionParams::CredentialsCase, stream, value) {
    stream << static_cast<int>(value);
}
