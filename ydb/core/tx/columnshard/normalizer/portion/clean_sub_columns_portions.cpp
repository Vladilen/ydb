#include "clean_sub_columns_portions.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <util/string/vector.h>

namespace NKikimr::NOlap::NNormalizer::NCleanSubColumnsPortions {

class TCleanSubColumnsPortionsNormalizer::TNormalizerResult: public INormalizerChanges {
    std::vector<TPortionDataAccessor> SubColumnsPortions;

public:
    TNormalizerResult(std::vector<TPortionDataAccessor>&& portions)
        : SubColumnsPortions(std::move(portions)) {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& normController) const override {
        NOlap::TBlobManagerDb blobManagerDb(txc.DB);
        TDbWrapper db(txc.DB, nullptr);
        for (auto&& portionInfo : SubColumnsPortions) {
            auto copy = portionInfo.GetPortionInfo().MakeCopy();
            copy->SetRemoveSnapshot(TSnapshot(1, 1));
            db.WritePortion(portionInfo.GetBlobIds(), *copy);
        }
        if (SubColumnsPortions.size()) {
            NIceDb::TNiceDb db(txc.DB);
            normController.AddNormalizerEvent(db, "REMOVE_PORTIONS", DebugString());
        }
        return true;
    }

    void ApplyOnComplete(const TNormalizationController& /* normController */) const override {
    }

    ui64 GetSize() const override {
        return SubColumnsPortions.size();
    }

    TString DebugString() const override {
        TStringBuilder sb;
        ui64 recordsCount = 0;
        for (auto&& p : SubColumnsPortions) {
            recordsCount += p.GetPortionInfo().GetRecordsCount();
        }
        sb << "records_count=" << recordsCount;
        sb << ";sub_columns_portions_count=" << SubColumnsPortions.size();
        return sb;
    }
};

bool TCleanSubColumnsPortionsNormalizer::CheckPortion(const NColumnShard::TTablesManager& /*tablesManager*/, const TPortionDataAccessor& /*portionInfo*/) const {
    return false;
}

INormalizerTask::TPtr TCleanSubColumnsPortionsNormalizer::BuildTask(
    std::vector<TPortionDataAccessor>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const {
    std::vector<TPortionDataAccessor> subColumnsPortions;
    std::vector<long> deletedTimestamps;
    AFL_VERIFY(schemas);

    for (auto&& portion : portions) {
        if (portion.GetPortionInfo().HasRemoveSnapshot()) {
             continue;
        }
        auto lpk = portion.GetPortionInfo().GetMeta().IndexKeyEnd();
        bool added = false;
        if (auto timestampIdx = lpk.GetSchema()->GetFieldIndex("timestamp"); timestampIdx != -1) {
            if (auto val = lpk.GetScalar(timestampIdx); val && val->type) {
                switch (val->type->id()) {
                    case arrow::Type::TIMESTAMP: {
                        auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(val->type);
                        arrow::TimeUnit::type unit = timestamp_type->unit();
                        if (unit == arrow::TimeUnit::MICRO) {
                            auto v = std::dynamic_pointer_cast<arrow::TimestampScalar>(val);
                            if (v && v->value < 1761951600000000l) {
                                deletedTimestamps.push_back(v->value);
                                subColumnsPortions.push_back(std::move(portion));
                                added = true;
                                break;
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        }

        if (added) {
            continue;
        }

        auto it = schemas->find(portion.GetPortionInfo().GetPortionId());
        AFL_VERIFY(it != schemas->end());
        const auto& indexInfo = it->second->GetIndexInfo();
        for (auto&& j : portion.GetIndexesVerified()) {
            if (!indexInfo.GetIndexNameOptional(j.GetEntityId())) {
                subColumnsPortions.push_back(std::move(portion));
                added = true;
                break;
            }
        }
        if (added) {
            continue;
        }

        for (ui32 i = 0; i < it->second->GetColumnsCount(); ++i) {
            auto loader = indexInfo.GetColumnLoaderOptional(i);
            if (loader && loader->GetAccessorConstructor()->GetClassName() == "SUB_COLUMNS") {
                subColumnsPortions.push_back(std::move(portion));
                break;
            }
        }
    }
    auto taskResult = std::make_shared<TNormalizerResult>(std::move(subColumnsPortions));
    ACFL_WARN("normalizer", "TCleanSubColumnsPortionsNormalizer")("message", taskResult->DebugString());
    ACFL_WARN("normalizer", "TCleanSubColumnsPortionsNormalizer")("all portions", portions.size());
    ACFL_WARN("normalizer", "TCleanSubColumnsPortionsNormalizer")("deleted timestamps", JoinSeq(',', deletedTimestamps));
    return std::make_shared<TTrivialNormalizerTask>(taskResult);
}

TConclusion<bool> TCleanSubColumnsPortionsNormalizer::DoInitImpl(const TNormalizationController&, NTabletFlatExecutor::TTransactionContext&) {
    return true;
}

}   // namespace NKikimr::NOlap::NNormalizer::NCleanSubColumnsPortions
