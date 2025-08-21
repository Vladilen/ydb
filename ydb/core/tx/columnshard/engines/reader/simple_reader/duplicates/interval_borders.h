#pragma once

#include "common.h"
#include "splitter.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TIntervalBorders {
public:
    class TPortionsSlice {
    private:
        THashMap<ui64, TRowRange> RangeByPortion;
        TColumnDataSplitter::TBorder IntervalEnd;

    public:
        TPortionsSlice(const TColumnDataSplitter::TBorder& end)
            : IntervalEnd(end) {
        }

        void Reserve(size_t size) {
            RangeByPortion.reserve(size);
        }

        void AddRange(const ui64 portion, const TRowRange& range) {
            if (range.NumRows() == 0) {
                return;
            }
            AFL_VERIFY(RangeByPortion.emplace(portion, range).second);
        }

        const TRowRange* GetRangeOptional(const ui64 portion) const {
            return RangeByPortion.FindPtr(portion);
        }

        THashMap<ui64, TRowRange> GetRanges() const {
            return RangeByPortion;
        }

        const TColumnDataSplitter::TBorder& GetEnd() const {
            return IntervalEnd;
        }
    };

    std::vector<TPortionsSlice> FindForSource(
        const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
        const std::shared_ptr<TPortionInfo>& mainSource,
        const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions);
};

} // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering