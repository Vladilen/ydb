# Метрики: Go OpenTelemetry Collector vs C++ `otel_logs_to_ydb`

Источник списка Go: уникальные `# HELP` из дампа `otelcol_*` (51 имя). В C++ используется префикс **`otel_logs_to_ydb_*`** (см. [`PARITY.md`](PARITY.md)).

Колонка **Рек.** — рекомендация по умолчанию из плана (*Да* / *Нет* / *Опц*). Колонка **Решение** — оставьте пустым, если согласны с **Рек.**; иначе укажите `Да`/`Нет`/`Опц`.

---

## Сводка: перенесено, не перенесено, отличия

### Перенесено в C++ (сопоставимо с Go по смыслу и/или границам гистограмм)

| Go (фрагмент имени) | C++ |
|---------------------|-----|
| `…_log_rows_pipeline_in` | `otel_logs_to_ydb_log_rows_pipeline_in_total` |
| `…_log_bytes_pipeline_in` | `otel_logs_to_ydb_log_bytes_pipeline_in_total` |
| `…_log_rows_ydb_written` / `…_rows_stored` | `otel_logs_to_ydb_log_rows_ydb_written_total` |
| `…_log_bytes_ydb_written` | `otel_logs_to_ydb_log_bytes_ydb_written_total` |
| `…_logs_batches_stored` | `otel_logs_to_ydb_logs_batches_stored_total` и дублирующий по смыслу счётчик успешных RPC `…_bulk_upsert_ok_total` |
| `…_logs_batches_errors` | `otel_logs_to_ydb_logs_batches_errors_total` и `…_bulk_upsert_fail_total` |
| `…_logs_write_processing_time` | `otel_logs_to_ydb_bulk_ydb_rpc_duration_milliseconds` — только **слот + RPC** до ответа; **те же** пороги `le`, что в `processors/ydb-supplier/metrics.go`: `50…10000` **мс**. Отдельно: `otel_logs_to_ydb_bulk_arrow_encode_duration_milliseconds` — **только Arrow IPC encode** до отправки (в Go в эту метрику не входит) |
| `…_log_rows_per_batch` | `otel_logs_to_ydb_bulk_upsert_rows` — **те же** пороги `le`: `100…100000` |
| `otelcol_receiver_accepted_log_records` (строки) | по смыслу ближе всего `log_rows_pipeline_in_total` (см. отличия ниже) |
| `otelcol_receiver_refused_log_records` | `log_rows_refused_total` + `ingest_rejected_queue_full_total` |
| `otelcol_requests_refused_by_count_limiter` | `ingest_rejected_*` + refused rows/bytes |
| `otelcol_inflight_requests` | частично: `ingest_queue_depth` + `grpc_export_rpc_inflight` |
| `otelcol_grpc_logs_export_in_payload_uncompressed_bytes` (близко) | `grpc_export_request_bytes_total` (`ByteSizeLong` на запрос) |
| `otelcol_process_cpu_seconds` | `otel_logs_to_ydb_process_cpu_seconds` (`getrusage`, scrape) |
| `otelcol_process_memory_rss` | `otel_logs_to_ydb_process_memory_rss` (Linux `statm`, scrape) |

### В C++ нет (намеренно или «не нужно дублировать»)

| Go / группа | Почему нет в `otel_logs_to_ydb` |
|-------------|----------------------------------|
| `otelcol_process_uptime`, heap/runtime и прочие `otelcol_process_*` кроме cpu/rss; все `otelcol_runtime_*` | Uptime/heap — **node_exporter** / системные счётчики; Go runtime — неприменимо к C++ |
| `…_log_bytes_pipeline_in_bps`, `…_log_bytes_ydb_written_bps` | Мгновенные B/s; в Prometheus — **`rate()`** по `_total` |
| `otelcol_grpc_logs_export_in_payload_compressed_bytes`, `…_wire_bytes` | Нет отдельного учёта сжатия/framing у gRPC в C++ |
| `…_batches_per_request` | Гистограмма «батчей на Export» не экспортируется |
| `…_log_ydb_bulk_payload_bytes` | В Go — гистограмма размера Arrow IPC payload; в C++ запись **тот же Arrow wire** (`BulkUpsert` + `ApacheArrow`), но **отдельной метрики размера payload нет** (есть эвристика `log_bytes_ydb_written_total`) |
| `otelcol_processor_otlpwithyandexvalidation_*` | Отмена / время валидации в процессоре — **не переносились** |
| Метрики **idx** (`idx_write_processing_time`, `idx_rows_per_batch`, …) | В `otel_logs_to_ydb` только **логи** в таблицы логов, без отдельного idx-пайплайна как в Go |

### Чем отличается от Go (важно для алертов и дашбордов)

1. **Префикс и лейблы**: Go — `otelcol_*` + лейблы `service_instance_id`, `service_name`, …; C++ — **`otel_logs_to_ydb_*`** без набора лейблов OTel collector (один процесс на scrape).
2. **`log_rows_pipeline_in_total` vs `log_rows_ydb_written_total`**: pipeline_in считается **на границе enqueue** по полному `Export` (**до** фильтрации в воркере: `allowed_projects`, `route.Drop`, шардирование без записи). **Разрыв с ydb_written не равен «ошибке»**: часть строк отфильтрована, часть может ждать сброса **shard buffer**. Ошибки смотреть по `*_refused_*`, `*_fail_*`, `*_errors_*`.
3. **`log_bytes_ydb_written_total`**: не размер Arrow, а **оценка** (в коде — эвристика на запись), не тождественна Go `log_bytes_ydb_written`.
4. **Успешные RPC / батчи**: один логический сброс может дать **несколько** BulkUpsert (чанки, лимит строк на RPC); счётчики успеха отражают **факты RPC/чанков**, не «один Export = один upsert».
5. **Гистограммы времени записи**: **`bulk_ydb_rpc_duration_milliseconds`** по смыслу ближе всего к Go `logs_write_processing_time` (только ожидание слота + `BulkUpsert` до ответа). **`bulk_arrow_encode_duration_milliseconds`** — отдельно время сборки Arrow IPC до send; сумма двух гистограмм за один успешный чанк ≈ бывший объединённый замер.

---

## Уже есть в C++ (`otel_logs_to_ydb_*`)

| Метрика C++ | Смысл | Аналог Go (оценка) | Рек. | Решение |
|-------------|--------|-------------------|------|---------|
| `otel_logs_to_ydb_ingest_accepted_total` | Батчи OTLP поставлены во внутреннюю очередь | `otelcol_receiver_*` + pipeline | — | |
| `otel_logs_to_ydb_ingest_rejected_queue_full_total` | Отказ `RESOURCE_EXHAUSTED`, очередь полна | `otelcol_requests_refused_by_count_limiter` (близко по смыслу) | — | |
| `otel_logs_to_ydb_bulk_upsert_ok_total` | Успешный BulkUpsert (RPC) | `otelcol_processor_ydb_supplier_logs_batches_stored` | — | |
| `otel_logs_to_ydb_bulk_upsert_fail_total` | Ошибка BulkUpsert после ретраев | `otelcol_processor_ydb_supplier_logs_batches_errors` | — | |
| `otel_logs_to_ydb_ddl_runs_total` | Запуски auto-DDL ensure | — | — | |

## Добавлено в C++ (parity по плану «Да»)

| Метрика C++ | Смысл | Аналог Go | Рек. | Решение |
|-------------|--------|-----------|------|---------|
| `otel_logs_to_ydb_ingest_queue_depth` | Текущая глубина внутренней очереди (gauge) | `otelcol_inflight_requests` (частично) | Да | |
| `otel_logs_to_ydb_grpc_export_rpc_inflight` | Число одновременных gRPC `Export` до ответа (gauge) | `otelcol_inflight_requests` (частично) | Да | |
| `otel_logs_to_ydb_log_rows_pipeline_in_total` | Log records приняты в очередь | `otelcol_processor_ydb_supplier_log_rows_pipeline_in` | Да | |
| `otel_logs_to_ydb_log_bytes_pipeline_in_total` | Байты OTLP (`ByteSizeLong`) приняты в очередь | `otelcol_processor_ydb_supplier_log_bytes_pipeline_in` | Да | |
| `otel_logs_to_ydb_log_rows_refused_total` | Log records в отклонённом батче (очередь полна) | `otelcol_receiver_refused_log_records` | Да | |
| `otel_logs_to_ydb_log_bytes_refused_total` | Байты в отклонённом батче | — | Да | |
| `otel_logs_to_ydb_log_rows_ydb_written_total` | Строки успешно записанные BulkUpsert | `otelcol_processor_ydb_supplier_log_rows_ydb_written` / `rows_stored` | Да | |
| `otel_logs_to_ydb_log_bytes_ydb_written_total` | Оценка байт записанного payload (эвристика) | `otelcol_processor_ydb_supplier_log_bytes_ydb_written` | Да | |
| `otel_logs_to_ydb_logs_batches_stored_total` | Успешные батчи (чанки BulkUpsert) | `otelcol_processor_ydb_supplier_logs_batches_stored` | Да | |
| `otel_logs_to_ydb_logs_batches_errors_total` | Ошибки батчей после ретраев | `otelcol_processor_ydb_supplier_logs_batches_errors` | Да | |
| `otel_logs_to_ydb_grpc_export_request_bytes_total` | `ByteSizeLong` каждого входящего Export (опц. дубль к pipeline in) | `otelcol_grpc_logs_export_in_payload_uncompressed_bytes` (близко) | Опц | |
| `otel_logs_to_ydb_bulk_arrow_encode_duration_milliseconds` | Гистограмма: Arrow IPC encode для чанка до отправки (**ms**; `le` 50…10000) | — (в Go encode вне `logs_write_processing_time`) | Да | |
| `otel_logs_to_ydb_bulk_ydb_rpc_duration_milliseconds` | Гистограмма: слот `ydb_max_concurrent_bulk` + `BulkUpsert` до ответа (**ms**; `le` как у Go) | `otelcol_processor_ydb_supplier_logs_write_processing_time` | Да | |
| `otel_logs_to_ydb_bulk_upsert_rows` | Histogram числа строк на BulkUpsert (границы как у Go: 100…100000) | `otelcol_processor_ydb_supplier_log_rows_per_batch` | Да | |
| `otel_logs_to_ydb_process_cpu_seconds` | CPU user+system, сек (на scrape; `getrusage`) | `otelcol_process_cpu_seconds` | Да | |
| `otel_logs_to_ydb_process_memory_rss` | RSS в байтах (на scrape; Linux `statm`) | `otelcol_process_memory_rss` | Да | |

## gRPC OTLP logs Export (Go)

| Метрика Go | Смысл | В C++ | Рек. | Решение |
|------------|--------|-------|------|---------|
| `otelcol_grpc_logs_export_in_payload_compressed_bytes` | Сжатые байты тела gRPC | Частично: нет отдельного счётчика сжатия | Опц | |
| `otelcol_grpc_logs_export_in_payload_uncompressed_bytes` | Несжатые байты payload | `otel_logs_to_ydb_grpc_export_request_bytes_total` (`ByteSizeLong` на каждый Export) | Опц | |
| `otelcol_grpc_logs_export_in_payload_wire_bytes` | Сжатые + framing | Нет | Опц | |

## Pipeline / limiter (Go)

| Метрика Go | Смысл | В C++ | Рек. | Решение |
|------------|--------|-------|------|---------|
| `otelcol_inflight_requests` | Запросы в обработке | `ingest_queue_depth` + `grpc_export_rpc_inflight` | Да | |
| `otelcol_requests_refused_by_count_limiter` | Отказ по inflight | `ingest_rejected_queue_full` (батч), + refused rows/bytes | Да | |

## Receiver (Go)

| Метрика Go | Смысл | В C++ | Рек. | Решение |
|------------|--------|-------|------|---------|
| `otelcol_receiver_accepted_log_records` | Приняты в pipeline | `log_rows_pipeline_in_total` | Да | |
| `otelcol_receiver_refused_log_records` | Не приняты | `log_rows_refused_total` | Да | |

## Processor `otlpwithyandexvalidation` (Go)

| Метрика Go | Смысл | В C++ | Рек. | Решение |
|------------|--------|-------|------|---------|
| `otelcol_processor_otlpwithyandexvalidation_canceled_requests` | Отмена клиентом | Нет | Нет | |
| `otelcol_processor_otlpwithyandexvalidation_processing_time` | Время обработки, ms | Нет (histogram enqueue не экспортирован отдельно) | Опц | |

## Processor `ydb_supplier` — объёмы и строки (Go)

| Метрика Go | Смысл | В C++ | Рек. | Решение |
|------------|--------|-------|------|---------|
| `otelcol_processor_ydb_supplier_log_bytes_pipeline_in` | OTLP в pipeline | `log_bytes_pipeline_in_total` | Да | |
| `otelcol_processor_ydb_supplier_log_bytes_pipeline_in_bps` | B/s окно 1с | Нет (rate в Prometheus) | Опц | |
| `otelcol_processor_ydb_supplier_log_bytes_ydb_written` | Байты в YDB | `log_bytes_ydb_written_total` (оценка) | Да | |
| `otelcol_processor_ydb_supplier_log_bytes_ydb_written_bps` | B/s | Нет (rate) | Опц | |
| `otelcol_processor_ydb_supplier_log_rows_pipeline_in` | Строки в очередь | `log_rows_pipeline_in_total` | Да | |
| `otelcol_processor_ydb_supplier_log_rows_ydb_written` | Строки в YDB | `log_rows_ydb_written_total` | Да | |
| `otelcol_processor_ydb_supplier_batches_per_request` | Батчей на запрос | Нет | Опц | |
| `otelcol_processor_ydb_supplier_log_rows_per_batch` | Гистограмма строк/батч | `bulk_upsert_rows` | Да | |
| `otelcol_processor_ydb_supplier_log_ydb_bulk_payload_bytes` | Размер Arrow payload | Нет отдельной гистограммы; запись Arrow как в Go, см. эвристику `log_bytes_ydb_written_total` | Опц | |

## Processor `ydb_supplier` — успех / ошибки / время (Go)

| Метрика Go | Смысл | В C++ | Рек. | Решение |
|------------|--------|-------|------|---------|
| `otelcol_processor_ydb_supplier_logs_batches_stored` | Успешные батчи | `logs_batches_stored_total` | Да | |
| `otelcol_processor_ydb_supplier_logs_batches_errors` | Ошибки батчей | `logs_batches_errors_total` | Да | |
| `otelcol_processor_ydb_supplier_logs_write_processing_time` | Длительность только RPC BulkUpsert (в Go) | `bulk_ydb_rpc_duration_milliseconds` (`bulk_arrow_encode_duration_milliseconds` — отдельно) | Да | |
| `otelcol_processor_ydb_supplier_rows_stored` | Строк записано | `log_rows_ydb_written_total` | Да | |

## Process / Go runtime (`otelcol_process_*`, `otelcol_runtime_*`)

**Go runtime** (`otelcol_runtime_*`) в C++ **не дублируются** — снимайте с хоста (**node_exporter** и т.п.).

**CPU и RSS процесса** (аналог `otelcol_process_cpu_seconds`, `otelcol_process_memory_rss`): в C++ при каждом scrape `/metrics` выставляются **`otel_logs_to_ydb_process_cpu_seconds`** (counter, `getrusage` user+system) и **`otel_logs_to_ydb_process_memory_rss`** (gauge, Linux `statm`×page size; вне Linux RSS = 0). Имена с префиксом `otel_logs_to_ydb_`; для тех же имен, что у collector, используйте `metric_relabel_configs` в Prometheus.

| Группа | Примеры | В C++ | Рек. | Решение |
|--------|---------|-------|------|---------|
| Process | `otelcol_process_uptime`, `otelcol_process_cpu_seconds`, `otelcol_process_memory_rss`, `otelcol_process_runtime_*` | CPU/RSS: `otel_logs_to_ydb_process_*`; остальное — node_exporter | Частично | |
| Runtime | все `otelcol_runtime_process_*` | Нет | Нет | |
