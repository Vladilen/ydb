#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <reader/simple_reader/duplicates/manager.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <regex>

#include <fstream>

using namespace NKikimr;
using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NReader;
using namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering;

namespace {

std::shared_ptr<arrow::RecordBatch> ExtractBatch(std::shared_ptr<arrow::Table> table) {
    std::shared_ptr<arrow::RecordBatch> batch;

    arrow::TableBatchReader reader(*table);
    auto result = reader.Next();
    Y_ABORT_UNLESS(result.ok());
    batch = *result;
    result = reader.Next();
    Y_ABORT_UNLESS(result.ok() && !(*result));
    return batch;
}

static std::shared_ptr<arrow::Schema>
MakePKSchema() {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
        arrow::field("log_id", arrow::utf8(), false)};

    return std::make_shared<arrow::Schema>(std::move(fields));
}

static std::shared_ptr<arrow::Schema>
MakeFullSchema() {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("log_id", arrow::utf8(), false),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
        arrow::field("_yql_plan_step", arrow::uint64(), true),
        arrow::field("_yql_tx_id", arrow::uint64(), true),
        arrow::field("_yql_write_id", arrow::uint64(), true)};

    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::tuple<ui64, THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>, THashMap<ui64, std::shared_ptr<TPortionInfo>>>
readFile(const std::string& fileName) {
    THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> data;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> info;

    const std::regex infoRegex(R"(Info: \(portion_id:([0-9]*);path_id:([0-9]*);records_count:([0-9]*);schema_version:([0-9]*);level:([0-9]*);records_snapshot_min:\(plan_step=([0-9]*);tx_id=([0-9]*);\);records_snapshot_max:\(plan_step=([0-9]*);tx_id=([0-9]*);\);from:([0-9]*),([0-9a-z\-]*),;to:([0-9]*),([0-9a-z\-]*),;.*;column_size:([0-9]*);index_size:([0-9]*);)");

    std::ifstream file(fileName);
    UNIT_ASSERT(file.is_open());

    const auto pkSchema = MakePKSchema();
    const auto fullSchema = MakeFullSchema();

    ui64 source = 0;
    ui64 currentId = 0;
    std::string line;
    bool foundSource = false;
    bool foundStart = false;
    while (std::getline(file, line)) {
        if (!foundSource) {
            if (line.starts_with("Context:")) {
                continue;
            }
            auto sourceStart = line.find("Portion=");
            UNIT_ASSERT_UNEQUAL(std::string::npos, sourceStart);
            source = std::stoull(line.c_str() + sourceStart + 8);
            foundSource = true;
            continue;
        }

        if (!foundStart) {
            if (!line.starts_with("Start Id:")) {
                continue;
            }
            currentId = std::stoull(line.c_str() + 9);

            foundStart = true;
            continue;
        }

        if (line.starts_with("End Id:")) {
            foundStart = false;
            continue;
        }

        if (line.starts_with("Info:")) {
            std::smatch match;
            UNIT_ASSERT(std::regex_search(line, match, infoRegex));
            UNIT_ASSERT_EQUAL(16, match.size());

            NKikimrTxColumnShard::TIndexPortionMeta portionMeta;
            portionMeta.SetRecordsCount(std::stoull(match[3].str()));
            portionMeta.SetColumnBlobBytes(std::stoull(match[14].str()));
            portionMeta.SetColumnRawBytes(std::stoull(match[14].str()));
            portionMeta.MutableRecordSnapshotMin()->SetPlanStep(std::stoull(match[6].str()));
            portionMeta.MutableRecordSnapshotMin()->SetTxId(std::stoull(match[7].str()));
            portionMeta.MutableRecordSnapshotMax()->SetPlanStep(std::stoull(match[8].str()));
            portionMeta.MutableRecordSnapshotMax()->SetTxId(std::stoull(match[9].str()));

            arrow::TimestampBuilder timestamp_B(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
            UNIT_ASSERT(timestamp_B.Append(std::stoull(match[10].str())).ok());
            UNIT_ASSERT(timestamp_B.Append(std::stoull(match[12].str())).ok());

            arrow::StringBuilder logId_B;
            UNIT_ASSERT(logId_B.Append(match[11].str()).ok());
            UNIT_ASSERT(logId_B.Append(match[13].str()).ok());

            std::shared_ptr<arrow::TimestampArray> timestamp_A;
            std::shared_ptr<arrow::StringArray> logId_A;
            UNIT_ASSERT(timestamp_B.Finish(&timestamp_A).ok());
            UNIT_ASSERT(logId_B.Finish(&logId_A).ok());

            std::shared_ptr<arrow::Table> table = arrow::Table::Make(pkSchema, {timestamp_A, logId_A});
            auto batch = ExtractBatch(table);
            NKikimr::NArrow::TSimpleRow firstRow(batch, 0);
            NKikimr::NArrow::TSimpleRow lastRow(batch, 1);

            NKikimr::NArrow::TFirstLastSpecialKeys keys(firstRow, lastRow, pkSchema);

            portionMeta.SetPrimaryKeyBorders(keys.SerializeFullToString());

            TPortionMetaConstructor constr;

            UNIT_ASSERT(constr.LoadMetadata2(portionMeta, keys));
            auto b = constr.Build();
            std::shared_ptr<TPortionInfo> currentInfo = std::make_shared<TCompactedPortionInfo>(std::move(b));

            currentInfo->SetPathId(TInternalPathId::FromRawValue(std::stoull(match[2].str())));
            currentInfo->SetPortionId(std::stoull(match[1].str()));
            currentInfo->SetSchemaVersion(std::stoull(match[4].str()));

            info[currentId] = currentInfo;
            continue;
        }

        if (line.starts_with("Data:")) {
            NJson::TJsonValue jsonData = NJson::ReadJsonFastTree(line.c_str() + 6, true);

            arrow::StringBuilder logId_B;
            arrow::TimestampBuilder timestamp_B(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
            arrow::UInt64Builder _yql_plan_step_B;
            arrow::UInt64Builder _yql_tx_id_B;
            arrow::UInt64Builder _yql_write_id_B;

            auto logId_S = jsonData["data"][0]["internal"]["data"].GetString();
            auto timestamp_S = jsonData["data"][1]["internal"]["data"].GetString();
            auto _yql_plan_step_S = jsonData["data"][2]["internal"]["data"].GetString();
            auto _yql_tx_id_S = jsonData["data"][3]["internal"]["data"].GetString();
            auto _yql_write_id_S = jsonData["data"][4]["internal"]["data"].GetString();

            const std::regex logIdRegex(R"!("([0-9a-z\-]*)")!");
            auto begin = std::sregex_iterator{logId_S.begin(), logId_S.end(), logIdRegex};
            auto end = std::sregex_iterator();
            for (std::sregex_iterator i = begin; i != end; ++i) {
                UNIT_ASSERT(logId_B.Append((*i)[1].str()).ok());
            }

            const std::regex timestampRegex(R"(([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]).([0-9][0-9][0-9][0-9][0-9][0-9]))");
            begin = std::sregex_iterator{timestamp_S.begin(), timestamp_S.end(), timestampRegex};
            for (std::sregex_iterator i = begin; i != end; ++i) {
                std::istringstream ss((*i)[1].str());
                std::tm t{};
                ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S");

                std::time_t timestamp = mktime(&t);
                UNIT_ASSERT(timestamp > 0);
                auto ttt = timestamp * 1000000ull + std::stoull((*i)[2].str());

                UNIT_ASSERT(timestamp_B.Append(ttt).ok());
            }

            const std::regex numberOrNullRegex(R"((\d+|null))");
            begin = std::sregex_iterator{_yql_plan_step_S.begin(), _yql_plan_step_S.end(), numberOrNullRegex};
            for (std::sregex_iterator i = begin; i != end; ++i) {
                if ((*i)[1].str() == "null") {
                    UNIT_ASSERT(_yql_plan_step_B.AppendNull().ok());
                } else {
                    UNIT_ASSERT(_yql_plan_step_B.Append(std::stoull((*i)[1].str())).ok());
                }
            }

            begin = std::sregex_iterator{_yql_tx_id_S.begin(), _yql_tx_id_S.end(), numberOrNullRegex};
            for (std::sregex_iterator i = begin; i != end; ++i) {
                if ((*i)[1].str() == "null") {
                    UNIT_ASSERT(_yql_tx_id_B.AppendNull().ok());
                } else {
                    UNIT_ASSERT(_yql_tx_id_B.Append(std::stoull((*i)[1].str())).ok());
                }
            }

            begin = std::sregex_iterator{_yql_write_id_S.begin(), _yql_write_id_S.end(), numberOrNullRegex};
            for (std::sregex_iterator i = begin; i != end; ++i) {
                if ((*i)[1].str() == "null") {
                    UNIT_ASSERT(_yql_write_id_B.AppendNull().ok());
                } else {
                    UNIT_ASSERT(_yql_write_id_B.Append(std::stoull((*i)[1].str())).ok());
                }
            }

            std::shared_ptr<arrow::StringArray> logId_A;
            std::shared_ptr<arrow::TimestampArray> timestamp_A;
            std::shared_ptr<arrow::UInt64Array> _yql_plan_step_A;
            std::shared_ptr<arrow::UInt64Array> _yql_tx_id_A;
            std::shared_ptr<arrow::UInt64Array> _yql_write_id_A;

            UNIT_ASSERT(logId_B.Finish(&logId_A).ok());
            UNIT_ASSERT(timestamp_B.Finish(&timestamp_A).ok());
            UNIT_ASSERT(_yql_plan_step_B.Finish(&_yql_plan_step_A).ok());
            UNIT_ASSERT(_yql_tx_id_B.Finish(&_yql_tx_id_A).ok());
            UNIT_ASSERT(_yql_write_id_B.Finish(&_yql_write_id_A).ok());

            std::shared_ptr<arrow::Table> table = arrow::Table::Make(fullSchema, {logId_A, timestamp_A, _yql_plan_step_A, _yql_tx_id_A, _yql_write_id_A});

            data[currentId] = std::make_shared<NKikimr::NArrow::TGeneralContainer>(table);

            continue;
        }
    }
    (void)currentId;
    file.close();

    return {source, data, info};
}

} // namespace

Y_UNIT_TEST_SUITE(TestSimpleReader) {

Y_UNIT_TEST(FindIntervalBordersFromFiles) {
    {
        auto [source, data, info] = readFile("/home/vladilenmuz/test-duplication/1284");
        auto res = TDuplicateManager::FindIntervalBordersInPortions(data, info[source], info);
        UNIT_ASSERT_EQUAL(265, res.size());
    }
    {
        auto [source, data, info] = readFile("/home/vladilenmuz/test-duplication/1322");
        auto res = TDuplicateManager::FindIntervalBordersInPortions(data, info[source], info);
        UNIT_ASSERT_EQUAL(185, res.size());
    }
    {
        auto [source, data, info] = readFile("/home/vladilenmuz/test-duplication/1419");
        auto res = TDuplicateManager::FindIntervalBordersInPortions(data, info[source], info);
        UNIT_ASSERT_EQUAL(158, res.size());
    }
    {
        auto [source, data, info] = readFile("/home/vladilenmuz/test-duplication/3097");
        auto res = TDuplicateManager::FindIntervalBordersInPortions(data, info[source], info);
        UNIT_ASSERT_EQUAL(303, res.size());
    }
}

} // Y_UNIT_TEST_SUITE(TestSimpleReader)
