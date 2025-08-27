#include "interval_borders.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

std::vector<TIntervalBorders::TPortionsSlice> TIntervalBorders::FindForSource(
    const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
    const std::shared_ptr<TPortionInfo>& mainSource,
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions) {
    auto getPortionVerified = [&portions](const ui64 portionId) -> const std::shared_ptr<TPortionInfo>& {
        const auto* portion = portions.FindPtr(portionId);
        AFL_VERIFY(portion)("portion", portionId);
        return *portion;
    };

    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    for (const auto& [portionId, _] : dataByPortion) {
        const auto& portion = getPortionVerified(portionId);
        borders.emplace(
            portionId, NArrow::TFirstLastSpecialKeys(portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->IndexKeyStart().GetSchema()));
    }

    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(mainSource->IndexKeyStart(), mainSource->IndexKeyEnd(), mainSource->IndexKeyStart().GetSchema()));

    std::vector<TIntervalBorders::TPortionsSlice> slices;
    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        slices.emplace_back(TPortionsSlice(splitter.GetIntervalFinish(i)));
    }

    for (const auto& [id, data] : dataByPortion) {
        auto intervals = splitter.SplitPortion(data, id, data->GetRecordsCount());
        AFL_VERIFY(intervals.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            slices[i].AddRange(id, intervals[i]);
        }
    }

    return slices;
}

std::vector<TIntervalBorders::TPortionsSlice> TIntervalBordersCached::FindForSource(
    const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
    const std::shared_ptr<TPortionInfo>& mainSource,
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions) {
    auto getPortionVerified = [&portions](const ui64 portionId) -> const std::shared_ptr<TPortionInfo>& {
        const auto* portion = portions.FindPtr(portionId);
        AFL_VERIFY(portion)("portion", portionId);
        return *portion;
    };

    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    borders.reserve(dataByPortion.size());
    for (const auto& [portionId, _] : dataByPortion) {
        const auto& portion = getPortionVerified(portionId);
        borders.emplace(
            portionId, NArrow::TFirstLastSpecialKeys(portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->IndexKeyStart().GetSchema()));
    }

    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(mainSource->IndexKeyStart(), mainSource->IndexKeyEnd(), mainSource->IndexKeyStart().GetSchema()));

    auto& bordersOffsets = splitter.GetBorders();
    for (auto& border : bordersOffsets) {
        if (auto found = CachedBorders.find(border); found != CachedBorders.end()) {
            border.Offsets = found->Offsets;
        }
    }

    std::vector<TIntervalBorders::TPortionsSlice> slices;
    slices.reserve(splitter.NumIntervals());
    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        slices.emplace_back(TPortionsSlice(splitter.GetIntervalFinish(i))).Reserve(dataByPortion.size());
    }

    for (const auto& [id, data] : dataByPortion) {
        const auto& portion = getPortionVerified(id);
        auto intervals = splitter.SplitPortion(data, id, portion->GetRecordsCount());
        AFL_VERIFY(intervals.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            slices[i].AddRange(id, intervals[i]);
        }
    }

    CachedPortions.insert(mainSource->GetPortionId());
    for (auto& border : bordersOffsets) {
        CachedBorders.emplace(std::move(border));
    }

    // AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "FindForSource")("hits", splitter.hits)("misses", splitter.misses);

    return slices;
}

bool TIntervalBordersCached::IsPortionInCache(const TPortionId& portionId) {
    return CachedPortions.contains(portionId); // Todo: save it somehow
}

} // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering