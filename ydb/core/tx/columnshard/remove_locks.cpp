#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

void TColumnShard::SubscribeLock(const ui64 lockId, const ui32 lockNodeId) {
    Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()),
        new NLongTxService::TEvLongTxService::TEvSubscribeLock(
            lockId,
            lockNodeId));
}

} // namespace NKikimr::NDataShard
