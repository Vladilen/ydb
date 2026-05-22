# otel_logs_to_ydb — parity с Go `ydb-supplier` и отличия

Цель: приём **OTel logs по OTLP/gRPC** и запись в **YDB column tables** с тем же семантическим маппингом, что у observability collector `ydb_supplier`.

## Согласовано с Go

- Wire: **OTLP protobuf** `ExportLogsServiceRequest` (как приёмник коллектора).
- Поля строк таблицы: `timestamp`, `cluster`, `record_id`, `level`, `message`, `labels`/`meta` как JsonDocument; при `per_project` — колонки `service`, `cluster`.
- Фильтр `allowed_projects`.
- После **успешной постановки** батча во внутреннюю очередь — **OTLP `Export` возвращает OK**; ошибки **BulkUpsert** не пробрасываются в gRPC (лог + метрики + ретраи).
- **Shard hash**: XXH64 seed 0, порядок колонок PK как в Go `shardhash`; `ShardIndex` как `TConsistencySharding64::MakeSharding` (без special info).

## Намеренные отличия от Go

| Область | Go collector | otel_logs_to_ydb |
|--------|----------------|------------------|
| Переполнение внутренней очереди | часто **дроп** батча + WARN, RPC может быть OK | **`RESOURCE_EXHAUSTED`** клиенту |
| Метрики Prometheus | смешанные имена collector | префикс **`otel_logs_to_ydb_*`** |
| OTLP HTTP | есть | **не в v1** |
| `additional_dbs` | поддерживается | **не реализовано в v1** |
| `write_idx_table` | опционально | **не в v1** |

## Конфиг

- Основной: **YAML** с корнем `otel_logs_to_ydb:`; подстановка `${env:VAR}`.
- Параметры `batch_*` / `buffer_size` из collector **не используются** — только **`shard_buffer_*`**, **`supplier_pool_size`**, очередь ingest.

## NFR

- Короткий **gRPC `Export`**: постановка в очередь, тяжёлая работа во **воркерах**.
- Минимизация аллокаций: Arena на батч, переиспользование буферов shard, `reserve` для JSON где возможно.

## Модульность / ydbd

- Логика разнесена по `.cpp` подключаемым из `lib` (`otel_logs_to_ydb_lib`): codec/ingress/pipeline/routing/ddl/metrics — чтобы позже переиспользовать в `ydbd` без gRPC-сервера.

## Метрики Prometheus

- Полная матрица **Go `otelcol_*` ↔ C++ `otel_logs_to_ydb_*`**: [METRICS_PARITY.md](METRICS_PARITY.md) (в т.ч. **`log_bytes_ydb_written` = len(Arrow schema IPC)+len(data IPC)** за чанк).
- Метрики процесса / Go runtime (`otelcol_process_*`, `otelcol_runtime_*`) **не дублируются** в бинарнике: для CPU, RSS, GC, goroutines используйте **node_exporter** (или аналог на хосте) и внешние дашборды.
