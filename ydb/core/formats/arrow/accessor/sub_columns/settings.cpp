#include "settings.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TSettings::TColumnsDistributor::EColumnType TSettings::TColumnsDistributor::TakeAndDetect(const ui64 columnSize, const ui32 columnValuesCount) {
    if (!!PredSize) {
        AFL_VERIFY(columnSize <= *PredSize)("col", columnSize)("pred", PredSize);
    }
    PredSize = columnSize;
    if (Settings.GetColumnsLimit() <= SeparatedCount) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)
            ("event", "VLAD_other_1")
            ("Settings.GetColumnsLimit()", Settings.GetColumnsLimit())
            ("SeparatedCount", SeparatedCount)
            ;
        return EColumnType::Other;
    }
    AFL_VERIFY(SumSize >= CurrentColumnsSize)("sum", SumSize)("columns", CurrentColumnsSize);
    if (!SumSize || 1.0 * CurrentColumnsSize / SumSize < 1 - Settings.GetOthersAllowedFraction()) {
        CurrentColumnsSize += columnSize;
        ++SeparatedCount;
        return EColumnType::Separated;
    } else if (!RecordsCount || RecordsCount < Settings.GetSparsedDetectorKff() * columnValuesCount) {
        CurrentColumnsSize += columnSize;
        ++SeparatedCount;
        return EColumnType::Separated;
    }

    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)
            ("event", "VLAD_other_2")
            ("Settings.GetColumnsLimit()", Settings.GetColumnsLimit())
            ("SeparatedCount", SeparatedCount)
            ("sum", SumSize)
            ("CurrentColumnsSize", CurrentColumnsSize)
            ("RecordsCount", RecordsCount)
            ("columnValuesCount", columnValuesCount)
            ("Settings.GetSparsedDetectorKff()", Settings.GetSparsedDetectorKff())
            ("Settings.GetOthersAllowedFraction()", Settings.GetOthersAllowedFraction())
            ;

    return EColumnType::Other;
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
