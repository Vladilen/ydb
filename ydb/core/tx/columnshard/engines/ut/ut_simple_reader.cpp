#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <reader/simple_reader/duplicates/interval_borders.h>
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

std::shared_ptr<arrow::Schema>
MakePKSchema() {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
        arrow::field("log_id", arrow::utf8(), false)};

    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::shared_ptr<arrow::Schema>
MakeFullSchema() {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("log_id", arrow::utf8(), false),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
        arrow::field("_yql_plan_step", arrow::uint64(), true),
        arrow::field("_yql_tx_id", arrow::uint64(), true),
        arrow::field("_yql_write_id", arrow::uint64(), true)};

    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::shared_ptr<arrow::Schema>
MakeFullSchemaNoWriteId() {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("log_id", arrow::utf8(), false),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
        arrow::field("_yql_plan_step", arrow::uint64(), true),
        arrow::field("_yql_tx_id", arrow::uint64(), true)};

    return std::make_shared<arrow::Schema>(std::move(fields));
}

using TPortionId = ui64;
using TPortionDataPtr = std::shared_ptr<NArrow::TGeneralContainer>;
using TPortionInfoPtr = std::shared_ptr<TPortionInfo>;
using TDataByPortionId = THashMap<TPortionId, TPortionDataPtr>;
using TInfoByPortionId = THashMap<TPortionId, TPortionInfoPtr>;
using TPortionsData = std::tuple<TDataByPortionId, TInfoByPortionId>;
using TPortionsDataWithSource = std::tuple<TPortionId, TDataByPortionId, TInfoByPortionId>;

struct TTestPortionInfo {
    ui64 PortionId = 0;
    ui64 PathId = 0;
    ui64 RecordsCount = 0;
    ui64 SchemaVersion = 0;
    ui64 Level = 0;
    ui64 RecordsSnapshotMinPlanStep = 0;
    ui64 RecordsSnapshotMinTxId = 0;
    ui64 RecordsSnapshotMaxPlanStep = 0;
    ui64 RecordsSnapshotMaxTxId = 0;
    ui64 FromTimestamp = 0;
    std::string FromLogId;
    ui64 ToTimestamp = 0;
    std::string ToLogId;
    ui64 ColumnSize = 0;
    ui64 IndexSize = 0;
};

TPortionInfoPtr CreatePortionFromTestPortionInfo(const TTestPortionInfo& info) {
    static const auto pkSchema = MakePKSchema();

    arrow::TimestampBuilder timestamp_B(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    UNIT_ASSERT(timestamp_B.Append(info.FromTimestamp).ok());
    UNIT_ASSERT(timestamp_B.Append(info.ToTimestamp).ok());

    arrow::StringBuilder logId_B;
    UNIT_ASSERT(logId_B.Append(info.FromLogId).ok());
    UNIT_ASSERT(logId_B.Append(info.ToLogId).ok());

    std::shared_ptr<arrow::TimestampArray> timestamp_A;
    std::shared_ptr<arrow::StringArray> logId_A;
    UNIT_ASSERT(timestamp_B.Finish(&timestamp_A).ok());
    UNIT_ASSERT(logId_B.Finish(&logId_A).ok());

    std::shared_ptr<arrow::Table> table = arrow::Table::Make(pkSchema, {timestamp_A, logId_A});
    auto batch = ExtractBatch(table);
    NKikimr::NArrow::TSimpleRow firstRow(batch, 0);
    NKikimr::NArrow::TSimpleRow lastRow(batch, 1);

    NKikimr::NArrow::TFirstLastSpecialKeys keys(firstRow, lastRow, pkSchema);

    NKikimrTxColumnShard::TIndexPortionMeta portionMeta;
    portionMeta.SetRecordsCount(info.RecordsCount);
    portionMeta.MutableRecordSnapshotMin()->SetPlanStep(info.RecordsSnapshotMinPlanStep);
    portionMeta.MutableRecordSnapshotMin()->SetTxId(info.RecordsSnapshotMinTxId);
    portionMeta.MutableRecordSnapshotMax()->SetPlanStep(info.RecordsSnapshotMaxPlanStep);
    portionMeta.MutableRecordSnapshotMax()->SetTxId(info.RecordsSnapshotMaxTxId);
    portionMeta.SetColumnBlobBytes(info.ColumnSize);
    portionMeta.SetColumnRawBytes(info.ColumnSize);
    portionMeta.SetCompactionLevel(info.Level);
    portionMeta.SetIndexRawBytes(info.IndexSize);
    portionMeta.SetIndexBlobBytes(info.IndexSize);
    portionMeta.SetPrimaryKeyBorders(keys.SerializeFullToString());

    TPortionMetaConstructor constr;
    UNIT_ASSERT(constr.LoadMetadata2(portionMeta, keys));

    NKikimrColumnShardDataSharingProto::TPortionInfo protoPortionInfo;
    protoPortionInfo.SetPathId(info.PathId);
    protoPortionInfo.SetPortionId(info.PortionId);
    protoPortionInfo.SetSchemaVersion(info.SchemaVersion);

    TPortionInfoPtr currentInfo = std::make_shared<TCompactedPortionInfo>(constr.Build());
    UNIT_ASSERT(currentInfo->DeserializeFromProto(protoPortionInfo).Ok());

    return currentInfo;
}

struct TTestDataBuilder {
    arrow::StringBuilder log_id;
    arrow::TimestampBuilder timestamp = arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::UInt64Builder _yql_plan_step;
    arrow::UInt64Builder _yql_tx_id;
    arrow::UInt64Builder _yql_write_id;
};

TPortionDataPtr CreateDataFromTestDataBuilder(TTestDataBuilder&& builder) {
    static const auto fullSchema = MakeFullSchema();
    static const auto fullSchemaNoWriteId = MakeFullSchemaNoWriteId();

    std::shared_ptr<arrow::StringArray> log_id_A;
    std::shared_ptr<arrow::TimestampArray> timestamp_A;
    std::shared_ptr<arrow::UInt64Array> _yql_plan_step_A;
    std::shared_ptr<arrow::UInt64Array> _yql_tx_id_A;
    std::shared_ptr<arrow::UInt64Array> _yql_write_id_A;

    UNIT_ASSERT(builder.log_id.Finish(&log_id_A).ok());
    UNIT_ASSERT(builder.timestamp.Finish(&timestamp_A).ok());
    UNIT_ASSERT(builder._yql_plan_step.Finish(&_yql_plan_step_A).ok());
    UNIT_ASSERT(builder._yql_tx_id.Finish(&_yql_tx_id_A).ok());
    if (builder._yql_write_id.length() > 0) {
        UNIT_ASSERT(builder._yql_write_id.Finish(&_yql_write_id_A).ok());
    }

    std::shared_ptr<arrow::Table> table;

    if (_yql_write_id_A) {
        table = arrow::Table::Make(fullSchema, {log_id_A, timestamp_A, _yql_plan_step_A, _yql_tx_id_A, _yql_write_id_A});
    } else {
        table = arrow::Table::Make(fullSchemaNoWriteId, {log_id_A, timestamp_A, _yql_plan_step_A, _yql_tx_id_A});
    }

    return std::make_shared<NKikimr::NArrow::TGeneralContainer>(table);
}

#if 0

TPortionInfoPtr ReadPortionInfo(const std::string_view line) {
    enum RegexMatchField {
        PortionId = 1,
        PathId,
        RecordsCount,
        SchemaVersion,
        Level,
        RecordsSnapshotMinPlanStep,
        RecordsSnapshotMinTxId,
        RecordsSnapshotMaxPlanStep,
        RecordsSnapshotMaxTxId,
        FromTimestamp,
        FromLogId,
        ToTimestamp,
        ToLogId,
        ColumnSize,
        IndexSize,
    };
    static const std::regex infoRegex(R"(\(portion_id:([0-9]*);path_id:([0-9]*);records_count:([0-9]*);schema_version:([0-9]*);level:([0-9]*);records_snapshot_min:\(plan_step=([0-9]*);tx_id=([0-9]*);\);records_snapshot_max:\(plan_step=([0-9]*);tx_id=([0-9]*);\);from:([0-9]*),([0-9a-z\-]*),;to:([0-9]*),([0-9a-z\-]*),;.*;column_size:([0-9]*);index_size:([0-9]*);)");

    std::smatch match;
    UNIT_ASSERT(std::regex_search(line.data(), match, infoRegex));
    UNIT_ASSERT_EQUAL(16, match.size());

    auto getUi64Field = [&match](RegexMatchField field) -> ui64 { return std::stoull(match[field].str()); };

    TTestPortionInfo testPortionInfo{
        .PortionId = getUi64Field(PortionId),
        .PathId = getUi64Field(PathId),
        .RecordsCount = getUi64Field(RecordsCount),
        .SchemaVersion = getUi64Field(SchemaVersion),
        .Level = getUi64Field(Level),
        .RecordsSnapshotMinPlanStep = getUi64Field(RecordsSnapshotMinPlanStep),
        .RecordsSnapshotMinTxId = getUi64Field(RecordsSnapshotMinTxId),
        .RecordsSnapshotMaxPlanStep = getUi64Field(RecordsSnapshotMaxPlanStep),
        .RecordsSnapshotMaxTxId = getUi64Field(RecordsSnapshotMaxTxId),
        .FromTimestamp = getUi64Field(FromTimestamp),
        .FromLogId = match[FromLogId].str(),
        .ToTimestamp = getUi64Field(ToTimestamp),
        .ToLogId = match[ToLogId].str(),
        .ColumnSize = getUi64Field(ColumnSize),
        .IndexSize = getUi64Field(IndexSize)};

    return CreatePortionFromTestPortionInfo(testPortionInfo);
}

TPortionDataPtr ReadData(const std::string_view line) {
    NJson::TJsonValue jsonData = NJson::ReadJsonFastTree(line, true);

    TTestDataBuilder builder;

    auto logId_S = jsonData["data"][0]["internal"]["data"].GetString();
    auto timestamp_S = jsonData["data"][1]["internal"]["data"].GetString();
    auto _yql_plan_step_S = jsonData["data"][2]["internal"]["data"].GetString();
    auto _yql_tx_id_S = jsonData["data"][3]["internal"]["data"].GetString();
    auto _yql_write_id_S = jsonData["data"][4]["internal"]["data"].GetString();

    const std::regex logIdRegex(R"!("([0-9a-z\-]*)")!");
    auto begin = std::sregex_iterator{logId_S.begin(), logId_S.end(), logIdRegex};
    auto end = std::sregex_iterator();
    for (std::sregex_iterator i = begin; i != end; ++i) {
        UNIT_ASSERT(builder.log_id.Append((*i)[1].str()).ok());
    }

    static const std::regex timestampRegex(R"(([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]).([0-9][0-9][0-9][0-9][0-9][0-9]))");
    begin = std::sregex_iterator{timestamp_S.begin(), timestamp_S.end(), timestampRegex};
    for (std::sregex_iterator i = begin; i != end; ++i) {
        std::istringstream ss((*i)[1].str());
        std::tm t{};
        ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S");

        std::time_t timestamp = mktime(&t);
        UNIT_ASSERT(timestamp > 0);
        auto timestampMicro = timestamp * 1000000ull + std::stoull((*i)[2].str());

        UNIT_ASSERT(builder.timestamp.Append(timestampMicro).ok());
    }

    static const std::regex numberOrNullRegex(R"((\d+|null))");
    begin = std::sregex_iterator{_yql_plan_step_S.begin(), _yql_plan_step_S.end(), numberOrNullRegex};
    for (std::sregex_iterator i = begin; i != end; ++i) {
        if ((*i)[1].str() == "null") {
            UNIT_ASSERT(builder._yql_plan_step.AppendNull().ok());
        } else {
            UNIT_ASSERT(builder._yql_plan_step.Append(std::stoull((*i)[1].str())).ok());
        }
    }

    begin = std::sregex_iterator{_yql_tx_id_S.begin(), _yql_tx_id_S.end(), numberOrNullRegex};
    for (std::sregex_iterator i = begin; i != end; ++i) {
        if ((*i)[1].str() == "null") {
            UNIT_ASSERT(builder._yql_tx_id.AppendNull().ok());
        } else {
            UNIT_ASSERT(builder._yql_tx_id.Append(std::stoull((*i)[1].str())).ok());
        }
    }

    begin = std::sregex_iterator{_yql_write_id_S.begin(), _yql_write_id_S.end(), numberOrNullRegex};
    for (std::sregex_iterator i = begin; i != end; ++i) {
        if ((*i)[1].str() == "null") {
            UNIT_ASSERT(builder._yql_write_id.AppendNull().ok());
        } else {
            UNIT_ASSERT(builder._yql_write_id.Append(std::stoull((*i)[1].str())).ok());
        }
    }

    return CreateDataFromTestDataBuilder(std::move(builder));
}

TPortionsDataWithSource ReadFile(const std::string& fileName) {
    TDataByPortionId dataByPortionId;
    TInfoByPortionId infoByPortionId;

    std::ifstream file(fileName);
    UNIT_ASSERT(file.is_open());

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
            infoByPortionId[currentId] = ReadPortionInfo(line.c_str() + 5);
            continue;
        }

        if (line.starts_with("Data:")) {
            dataByPortionId[currentId] = ReadData(line.c_str() + 5);
            continue;
        }
    }
    file.close();

    return {source, dataByPortionId, infoByPortionId};
}
#endif

// Generates portions with the following pattern
//   ---
//  ---
// ---
TPortionsData GeneratePortions(ui32 portionsCount, ui32 recordsInPortionCount, bool printOutput = false) {
    UNIT_ASSERT_GE(portionsCount, 2);
    UNIT_ASSERT_GE(recordsInPortionCount, portionsCount);
    std::vector<std::vector<ui32>> recordsInPortions(portionsCount);
    std::vector<std::string> output(portionsCount * ui32(printOutput));
    ui32 current = 0;

    auto addValue = [&output, &recordsInPortions, &current, &printOutput](size_t index) {
        auto val = current++;
        recordsInPortions[index].emplace_back(val);
        if (printOutput) {
            for (size_t i = 0; i < output.size(); ++i) {
                if (i != index) {
                    output[i] += "   ";
                } else {
                    output[i] += (val < 10 ? "0" : "") + std::to_string(val) + " ";
                }
            }
        }
    };

    addValue(0);
    for (ui32 i = 1; i < portionsCount; ++i) {
        addValue(i);
        for (int j = i - 1; j >= 0; --j) {
            addValue(j);
        }
    }

    for (ui32 i = 0; i < portionsCount; ++i) {
        for (ui32 j = 1; j < recordsInPortionCount - portionsCount + i + 1; ++j) {
            addValue(i);
        }
    }

    TDataByPortionId dataByPortionId;
    TInfoByPortionId infoByPortionId;

    for (ui32 i = 0; i < portionsCount; ++i) {
        infoByPortionId[i] = CreatePortionFromTestPortionInfo(TTestPortionInfo{
            .PortionId = i,
            .PathId = 2,
            .RecordsCount = recordsInPortionCount,
            .SchemaVersion = 2,
            .Level = 0,
            .RecordsSnapshotMinPlanStep = i,
            .RecordsSnapshotMinTxId = i,
            .RecordsSnapshotMaxPlanStep = i,
            .RecordsSnapshotMaxTxId = i,
            .FromTimestamp = recordsInPortions[i].front(),
            .FromLogId = "",
            .ToTimestamp = recordsInPortions[i].back(),
            .ToLogId = "",
            .ColumnSize = recordsInPortions[i].size(),
            .IndexSize = 0});

        TTestDataBuilder testDataBuilder;
        for (auto v : recordsInPortions[i]) {
            UNIT_ASSERT(testDataBuilder.log_id.Append("").ok());
            UNIT_ASSERT(testDataBuilder.timestamp.Append(v).ok());
            UNIT_ASSERT(testDataBuilder._yql_plan_step.Append(i).ok());
            UNIT_ASSERT(testDataBuilder._yql_tx_id.Append(i).ok());
        }
        dataByPortionId[i] = CreateDataFromTestDataBuilder(std::move(testDataBuilder));
    }

    if (printOutput) {
        ui32 portion = 0;
        std::string space;
        ui32 pc = (portionsCount - 1) / 10;
        while (pc % 10 > 0) {
            space += " ";
            pc /= 10;
        }
        Cerr << "Generated:" << Endl;
        for (auto& line : output) {
            Cerr << space << portion++ << ": " << line << Endl;
        }
    }

    return {dataByPortionId, infoByPortionId};
}

} // namespace

Y_UNIT_TEST_SUITE(TestSimpleReader) {

#if 0

Y_UNIT_TEST(FindIntervalBordersFromFiles) {
    TIntervalBorders borders;
    std::pair<std::string, size_t> files[] =
        {{"/home/vladilenmuz/test-duplication/1284", 265},
            {"/home/vladilenmuz/test-duplication/1322", 185},
            {"/home/vladilenmuz/test-duplication/1419", 158},
            {"/home/vladilenmuz/test-duplication/3097", 303}};

    for (const auto& [file, expectedBorderCount] : files) {
        auto [source, dataByPortionId, infoByPortionId] = ReadFile(file);
        auto res = borders.FindForSource(dataByPortionId, infoByPortionId[source], infoByPortionId);
        UNIT_ASSERT_EQUAL(expectedBorderCount, res.size());
    }
}

#endif

Y_UNIT_TEST(FindIntervalBordersGeneratedPerfTest) {
    const ui32 runningCount = 10;
    std::vector<std::vector<ui32>> tests;
    for (ui32 i = 2; i <= 200; ++i) {
        tests.push_back({i, 1000});
    }

    Cerr << "x,y,z" << Endl;
    for (auto& test : tests) {
        Cerr << test[0] << ",";
        ui64 dur = 0;
        for (ui32 i = 0; i < runningCount; ++i) {
            TIntervalBorders bordersOrig;
            auto [dataByPortionId, infoByPortionId] = GeneratePortions(test[0], test[1]);

            auto start = std::chrono::steady_clock::now();
            for (ui32 j = 0; j < test[0]; ++j) {
                auto res = bordersOrig.FindForSource(dataByPortionId, infoByPortionId[j], infoByPortionId);
            }
            auto end = std::chrono::steady_clock::now();

            // UNIT_ASSERT_EQUAL(test[0], res.size());
            dur += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        }
        Cerr << dur / runningCount << ",";

        dur = 0;
        for (ui32 i = 0; i < runningCount; ++i) {
            TIntervalBordersCached bordersCached;

            auto [dataByPortionId, infoByPortionId] = GeneratePortions(test[0], test[1]);

            auto start = std::chrono::steady_clock::now();
            for (ui32 j = 0; j < test[0]; ++j) {
                auto res = bordersCached.FindForSource(dataByPortionId, infoByPortionId[j], infoByPortionId);
            }
            auto end = std::chrono::steady_clock::now();

            // UNIT_ASSERT_EQUAL(test[0], res.size());
            dur += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        }
        Cerr << dur / runningCount << Endl;
    }
}

Y_UNIT_TEST(FindIntervalBordersOnePortion) {
    TTestPortionInfo testPortionInfo{
        .PortionId = 1,
        .PathId = 2,
        .RecordsCount = 1,
        .SchemaVersion = 2,
        .Level = 0,
        .RecordsSnapshotMinPlanStep = 3,
        .RecordsSnapshotMinTxId = 4,
        .RecordsSnapshotMaxPlanStep = 3,
        .RecordsSnapshotMaxTxId = 4,
        .FromTimestamp = 5,
        .FromLogId = "",
        .ToTimestamp = 5,
        .ToLogId = "",
        .ColumnSize = 1,
        .IndexSize = 0};
    auto info = CreatePortionFromTestPortionInfo(testPortionInfo);

    TTestDataBuilder testDataBuilder;
    UNIT_ASSERT(testDataBuilder.log_id.Append(testPortionInfo.FromLogId).ok());
    UNIT_ASSERT(testDataBuilder.timestamp.Append(testPortionInfo.FromTimestamp).ok());
    UNIT_ASSERT(testDataBuilder._yql_plan_step.Append(testPortionInfo.RecordsSnapshotMinPlanStep).ok());
    UNIT_ASSERT(testDataBuilder._yql_tx_id.Append(testPortionInfo.RecordsSnapshotMinTxId).ok());

    auto data = CreateDataFromTestDataBuilder(std::move(testDataBuilder));

    TDataByPortionId dataByPortionId;
    TInfoByPortionId infoByPortionId;
    dataByPortionId[testPortionInfo.PortionId] = data;
    infoByPortionId[testPortionInfo.PortionId] = info;

    TIntervalBorders borders;
    auto res = borders.FindForSource(dataByPortionId, info, infoByPortionId);

    UNIT_ASSERT_EQUAL(1, res.size());

    auto intervalEnd = res[0].GetEnd();
    UNIT_ASSERT(intervalEnd.GetIsLast());
    UNIT_ASSERT_EQUAL(0, intervalEnd.GetKey().GetPosition());

    auto ranges = res[0].GetRanges();
    UNIT_ASSERT_EQUAL(1, ranges.size());

    auto range = *ranges.begin();
    UNIT_ASSERT_EQUAL(testPortionInfo.PortionId, range.first);

    UNIT_ASSERT_EQUAL(0, range.second.GetBegin());
    UNIT_ASSERT_EQUAL(1, range.second.GetEnd());
}

Y_UNIT_TEST(FindIntervalBordersNonIntersectingPortions) {
    TTestPortionInfo testPortionInfo1{
        .PortionId = 1,
        .PathId = 2,
        .RecordsCount = 1,
        .SchemaVersion = 2,
        .Level = 0,
        .RecordsSnapshotMinPlanStep = 3,
        .RecordsSnapshotMinTxId = 4,
        .RecordsSnapshotMaxPlanStep = 3,
        .RecordsSnapshotMaxTxId = 4,
        .FromTimestamp = 5,
        .FromLogId = "",
        .ToTimestamp = 5,
        .ToLogId = "",
        .ColumnSize = 1,
        .IndexSize = 0};
    auto info1 = CreatePortionFromTestPortionInfo(testPortionInfo1);

    TTestPortionInfo testPortionInfo2{
        .PortionId = 2,
        .PathId = 2,
        .RecordsCount = 1,
        .SchemaVersion = 2,
        .Level = 0,
        .RecordsSnapshotMinPlanStep = 5,
        .RecordsSnapshotMinTxId = 6,
        .RecordsSnapshotMaxPlanStep = 5,
        .RecordsSnapshotMaxTxId = 6,
        .FromTimestamp = 7,
        .FromLogId = "",
        .ToTimestamp = 7,
        .ToLogId = "",
        .ColumnSize = 1,
        .IndexSize = 0};
    auto info2 = CreatePortionFromTestPortionInfo(testPortionInfo2);

    TTestDataBuilder testDataBuilder1;
    UNIT_ASSERT(testDataBuilder1.log_id.Append(testPortionInfo1.FromLogId).ok());
    UNIT_ASSERT(testDataBuilder1.timestamp.Append(testPortionInfo1.FromTimestamp).ok());
    UNIT_ASSERT(testDataBuilder1._yql_plan_step.Append(testPortionInfo1.RecordsSnapshotMinPlanStep).ok());
    UNIT_ASSERT(testDataBuilder1._yql_tx_id.Append(testPortionInfo1.RecordsSnapshotMinTxId).ok());

    auto data1 = CreateDataFromTestDataBuilder(std::move(testDataBuilder1));

    TTestDataBuilder testDataBuilder2;
    UNIT_ASSERT(testDataBuilder2.log_id.Append(testPortionInfo2.FromLogId).ok());
    UNIT_ASSERT(testDataBuilder2.timestamp.Append(testPortionInfo2.FromTimestamp).ok());
    UNIT_ASSERT(testDataBuilder2._yql_plan_step.Append(testPortionInfo2.RecordsSnapshotMinPlanStep).ok());
    UNIT_ASSERT(testDataBuilder2._yql_tx_id.Append(testPortionInfo2.RecordsSnapshotMinTxId).ok());

    auto data2 = CreateDataFromTestDataBuilder(std::move(testDataBuilder2));

    TDataByPortionId dataByPortionId;
    TInfoByPortionId infoByPortionId;
    dataByPortionId[testPortionInfo1.PortionId] = data1;
    infoByPortionId[testPortionInfo1.PortionId] = info1;
    dataByPortionId[testPortionInfo2.PortionId] = data2;
    infoByPortionId[testPortionInfo2.PortionId] = info2;

    TIntervalBorders borders;
    auto res = borders.FindForSource(dataByPortionId, info1, infoByPortionId);
    UNIT_ASSERT_EQUAL(2, res.size());
}

Y_UNIT_TEST(FindIntervalBordersSplitsPortionsCorrectly) {
    constexpr ui32 portionCount = 5;
    constexpr ui32 recordsCount = 5;
    TIntervalBorders bordersOrig;
    TIntervalBordersCached bordersCached;

    auto [dataByPortionId, infoByPortionId] = GeneratePortions(portionCount, recordsCount, true);

    for (ui32 i = 0; i < portionCount; ++i) {
        const auto resOrig = bordersOrig.FindForSource(dataByPortionId, infoByPortionId[i], infoByPortionId);
        const auto resCached = bordersCached.FindForSource(dataByPortionId, infoByPortionId[i], infoByPortionId);

        Cerr << "Slices (" << resOrig.size() << ") ORIG for source: " << i << Endl;
        for (auto& intervalOrig : resOrig) {
            Cerr << intervalOrig.GetEnd().DebugString() << "; size: " << intervalOrig.GetRanges().size() << Endl;
            for (auto& range : intervalOrig.GetRanges()) {
                Cerr << range.first << ": [";
                Cerr << dataByPortionId[range.first]->GetColumns()[1]->GetScalar(range.second.GetBegin())->CastTo(std::make_shared<arrow::UInt64Type>())->get()->ToString() << " ";
                Cerr << dataByPortionId[range.first]->GetColumns()[1]->GetScalar(range.second.GetEnd() - 1)->CastTo(std::make_shared<arrow::UInt64Type>())->get()->ToString() << "]; ";
            }
            Cerr << Endl;
        }

        Cerr << "Slices (" << resCached.size() << ") CACHED for source: " << i << Endl;

        for (auto& intervalCached : resCached) {
            Cerr << intervalCached.GetEnd().DebugString() << "; size: " << intervalCached.GetRanges().size() << Endl;
            for (auto& range : intervalCached.GetRanges()) {
                Cerr << range.first << ": [";
                Cerr << dataByPortionId[range.first]->GetColumns()[1]->GetScalar(range.second.GetBegin())->CastTo(std::make_shared<arrow::UInt64Type>())->get()->ToString() << " ";
                Cerr << dataByPortionId[range.first]->GetColumns()[1]->GetScalar(range.second.GetEnd() - 1)->CastTo(std::make_shared<arrow::UInt64Type>())->get()->ToString() << "]; ";
            }
            Cerr << Endl;
        }

        Cerr << " " << Endl;

        UNIT_ASSERT_EQUAL(portionCount, resOrig.size());
        UNIT_ASSERT_EQUAL(resOrig.size(), resCached.size());
        for (ui32 j = 0; j < resOrig.size(); ++j) {
            const auto& intervalOrig = resOrig[j];
            const auto& intervalCached = resCached[j];

            UNIT_ASSERT_EQUAL(intervalOrig.GetEnd(), intervalCached.GetEnd());
            UNIT_ASSERT_EQUAL(intervalOrig.GetRanges(), intervalCached.GetRanges());
        }
    }
}

} // Y_UNIT_TEST_SUITE(TestSimpleReader)
