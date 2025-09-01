#pragma once

#include "common.h"
#include "splitter.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TIntervalBorders {
public:
    using TPortionId = ui64;

    class TPortionsSlice {
    private:
        THashMap<TPortionId, TRowRange> RangeByPortion;
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

        const THashMap<ui64, TRowRange>& GetRanges() const {
            return RangeByPortion;
        }

        const TColumnDataSplitter::TBorder& GetEnd() const {
            return IntervalEnd;
        }
    };

    virtual std::vector<TPortionsSlice> FindForSource(
        const THashMap<TPortionId, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
        const std::shared_ptr<TPortionInfo>& mainSource,
        const THashMap<TPortionId, std::shared_ptr<TPortionInfo>>& portions);

    virtual ~TIntervalBorders() = default;
};

class TIntervalBordersCached: public TIntervalBorders {
public:
    std::vector<TPortionsSlice> FindForSource(
        const THashMap<TPortionId, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
        const std::shared_ptr<TPortionInfo>& mainSource,
        const THashMap<TPortionId, std::shared_ptr<TPortionInfo>>& portions) override;

    bool IsPortionInCache(const TPortionId& portionId);

private:
    using TBorder = TColumnDataSplitter::TBorder;

    std::set<TBorder> CachedBorders;
    THashSet<TPortionId> CachedPortions;
};

} // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering