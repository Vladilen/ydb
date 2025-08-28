#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TInternalFilterConstructor::TInternalFilterConstructor(const TEvRequestFilter::TPtr& request, std::function<void()> completeCallback)
    : OriginalRequest(request)
    , ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard(GetStageFeatures()))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
    , CompleteCallback(std::move(completeCallback))
{
    AFL_VERIFY(!!OriginalRequest);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
