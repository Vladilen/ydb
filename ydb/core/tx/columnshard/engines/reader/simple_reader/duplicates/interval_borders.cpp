#include "interval_borders.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

std::vector<TIntervalBorders::TPortionsSlice> TIntervalBorders::FindForSource(
    const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
    const std::shared_ptr<TPortionInfo>& mainSource,
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions) {
    // static std::atomic_int count = 0;

    // std::fstream fs;
    // fs.open("/tmp/test/" + std::to_string(count.fetch_add(1)), std::ios::out);
    // AFL_VERIFY(fs.is_open());
    // fs << "Context:" << std::endl;
    // fs << context->DebugString() << std::endl;

    auto getPortionVerified = [&portions](const ui64 portionId) -> const std::shared_ptr<TPortionInfo>& {
        const auto* portion = portions.FindPtr(portionId);
        AFL_VERIFY(portion)("portion", portionId);
        return *portion;
    };

    // fs << "dataByPortion:" << std::endl;
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    // borders.reserve(dataByPortion.size());
    for (const auto& [portionId, dt] : dataByPortion) {
        const auto& portion = getPortionVerified(portionId);
        // fs << "Start Id: " << portionId << std::endl;
        // fs << "Info: " << portion->DebugString(true) << std::endl;
        // fs << "Data: " << dt->DebugString(true) << std::endl;
        // fs << "End Id: " << portionId << std::endl;
        borders.emplace(
            portionId, NArrow::TFirstLastSpecialKeys(portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->IndexKeyStart().GetSchema()));
    }

    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(mainSource->IndexKeyStart(), mainSource->IndexKeyEnd(), mainSource->IndexKeyStart().GetSchema()));

    std::vector<TIntervalBorders::TPortionsSlice> slices;
    // slices.reserve(splitter.NumIntervals());
    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        slices.emplace_back(TPortionsSlice(splitter.GetIntervalFinish(i))); //.Reserve(dataByPortion.size());
    }

    for (const auto& [id, data] : dataByPortion) {
        auto intervals = splitter.SplitPortion(data); // Save  id ?
        AFL_VERIFY(intervals.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            slices[i].AddRange(id, intervals[i]);
        }
    }

    // fs.flush();
    // fs.close();

    return slices;
}

} // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering