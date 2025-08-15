#include "overload_subscribers.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr::NColumnShard::NOverload {

namespace {

void SendViaSession(const TActorId& sessionId,
    const TActorId& target,
    const TActorId& src,
    IEventBase* event) {
    THolder<IEventHandle> ev = MakeHolder<IEventHandle>(target, src, event, 0, 0);
    if (sessionId) {
        ev->Rewrite(TEvInterconnect::EvForward, sessionId);
    }
    ev->is_overload = true;

    bool result = TActivationContext::Send(ev.Release());
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "!!!! SendViaSession")("sessionId", sessionId)("target", target)("src", src)("result", result);
}

} // namespace

void TOverloadSubscribers::AddPipeServer(const NActors::TActorId& serverId, const NActors::TActorId& interconnectSession) {
    auto res = PipeServers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(serverId),
        std::forward_as_tuple());
    AFL_VERIFY(res.second)("serverId", serverId);

    res.first->second.InterconnectSession = interconnectSession;

    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "ADD PIPE SERVER")("serverId", serverId)("interconnectSession", interconnectSession);
}

void TOverloadSubscribers::RemovePipeServer(const NActors::TActorId& serverId) {
    auto it = PipeServers.find(serverId);
    AFL_VERIFY(it != PipeServers.end())("serverId", serverId);

    DiscardOverloadSubscribers(it->second);

    PipeServers.erase(it);
}

void TOverloadSubscribers::DiscardOverloadSubscribers(TPipeServerInfo& pipeServer) {
    for (auto it = pipeServer.OverloadSubscribers.begin(); it != pipeServer.OverloadSubscribers.end(); ++it) {
        TOverloadSubscriber& entry = it->second;
        EnumerateRejectReasons(entry.Reasons, [&](ERejectReason reason) {
            OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
        });
    }
    pipeServer.OverloadSubscribers.clear();
    PipeServersWithOverloadSubscribers.Remove(&pipeServer);
}

bool TOverloadSubscribers::AddOverloadSubscriber(const TActorId& pipeServerId, const TActorId& actorId, const TActorContext& ctx, ui64 seqNo, ERejectReasons reasons) {
    auto it = PipeServers.find(pipeServerId);
    if (it == PipeServers.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "NO PIPE SERVER ID")("pipeServerId", pipeServerId);
        return false;
    }

    bool wasEmpty = it->second.OverloadSubscribers.empty();
    auto& entry = it->second.OverloadSubscribers[actorId];
    if (!entry.ctx) {
        entry.ctx = std::make_unique<TActorContext>(ctx);
    }
    if (entry.SeqNo <= seqNo) {
        entry.SeqNo = seqNo;
        // Increment counter for every new reason
        EnumerateRejectReasons(reasons - entry.Reasons, [&](ERejectReason reason) {
            OverloadSubscribersByReason[RejectReasonIndex(reason)]++;
        });
        entry.Reasons |= reasons;
    }

    if (wasEmpty) {
        PipeServersWithOverloadSubscribers.PushBack(&it->second);
    }

    return true;
}

bool TOverloadSubscribers::HasPipeServer(const TActorId& pipeServerId) {
    return PipeServers.contains(pipeServerId);
}

void TOverloadSubscribers::NotifyOverloadSubscribers(ERejectReason reason, const TActorId& sourceActorId, ui64 sourceTabletId) {
    if (OverloadSubscribersByReason[RejectReasonIndex(reason)] == 0) {
        // Avoid spending time when we know it is pointless
        return;
    }
    ERejectReasons reasons = MakeRejectReasons(reason);

    TPipeServersWithOverloadSubscribers left;
    while (!PipeServersWithOverloadSubscribers.Empty()) {
        TPipeServerInfo* pipeServer = PipeServersWithOverloadSubscribers.PopFront();
        for (auto it = pipeServer->OverloadSubscribers.begin(); it != pipeServer->OverloadSubscribers.end();) {
            auto current = it++;
            const TActorId& actorId = current->first;
            TOverloadSubscriber& entry = current->second;
            if ((entry.Reasons & reasons) != reasons) {
                // Reasons don't match
                continue;
            }
            entry.Reasons -= reasons;
            OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
            if (entry.Reasons == ERejectReasons::None) {
                SendViaSession(
                    pipeServer->InterconnectSession,
                    actorId,
                    sourceActorId,
                    new TEvColumnShard::TEvOverloadReady(sourceTabletId, entry.SeqNo));
                pipeServer->OverloadSubscribers.erase(current);
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "!!!! NOTIFY NO OVERLOAD")("actorId", actorId)("seqNo", entry.SeqNo)("pipeServer->InterconnectSession", pipeServer->InterconnectSession)("sourceActorId", sourceActorId);
            }
        }
        if (!pipeServer->OverloadSubscribers.empty()) {
            left.PushBack(pipeServer);
        }
    }
    PipeServersWithOverloadSubscribers.Append(left);
}

void TOverloadSubscribers::NotifyAllOverloadSubscribers(const TActorId& sourceActorId, ui64 sourceTabletId) {
    bool clearedSubscribers = false;
    while (!PipeServersWithOverloadSubscribers.Empty()) {
        TPipeServerInfo* pipeServer = PipeServersWithOverloadSubscribers.PopFront();
        for (auto it = pipeServer->OverloadSubscribers.begin(); it != pipeServer->OverloadSubscribers.end(); ++it) {
            const TActorId& actorId = it->first;
            TOverloadSubscriber& entry = it->second;
            SendViaSession(
                pipeServer->InterconnectSession,
                actorId,
                sourceActorId,
                new TEvColumnShard::TEvOverloadReady(sourceTabletId, entry.SeqNo));
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "!!!! NOTIFY NO OVERLOAD ALL");
        }
        pipeServer->OverloadSubscribers.clear();
        clearedSubscribers = true;
    }

    if (clearedSubscribers) {
        for (int i = 0; i < RejectReasonCount; ++i) {
            OverloadSubscribersByReason[i] = 0;
        }
    }
}

void TOverloadSubscribers::RemoveOverloadSubscriber(TSeqNo seqNo, const TActorId& recipient, const TActorId& sender) {
    if (auto* pipeServer = PipeServers.FindPtr(recipient)) {
        auto it = pipeServer->OverloadSubscribers.find(sender);
        if (it != pipeServer->OverloadSubscribers.end()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "DELETED SOME!!!111");
            if (it->second.SeqNo == seqNo) {
                EnumerateRejectReasons(it->second.Reasons, [&](ERejectReason reason) {
                    OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
                });
                pipeServer->OverloadSubscribers.erase(it);
                if (pipeServer->OverloadSubscribers.empty()) {
                    PipeServersWithOverloadSubscribers.Remove(pipeServer);
                }
            }
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "NO ONE SOME!!!111");
        }
    }
}

} // namespace NKikimr::NColumnShard::NOverload
