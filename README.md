# Распределённый анализ вакансий DevOps в RustFS

Лабораторная работа для индивидуального варианта 11 «DevOps инженер».
Собственная MapReduce-реализация читает NDJSON из RustFS чанками через S3 Range
GET и считает частоты отдельных навыков, пар и троек. Независимый SQL в DuckDB
обращается к тому же S3-объекту через `httpfs`. Результаты сверяются полностью.

## Результат

- 6 215 вакансий варианта из 229 388 записей исходного набора;
- 1 541 уникальный навык, 25 462 пары и 195 366 троек;
- точное совпадение MapReduce и DuckDB для всех комбинаций;
- зарплатный анализ на 1 364 вакансиях с сопоставимыми RUR-значениями;
- сравнение Москвы, Санкт-Петербурга и Екатеринбурга;
- три запуска каждого метода с замером wall time и пика RSS;
- тест контролируемого отказа маппера и успешного retry;
- сравнение MapReduce до и после локального combiner.

## Структура

- `src/prepare_dataset.py` - фильтр варианта, нормализация и NDJSON;
- `src/s3_io.py` - создание бакета, загрузка и инвентаризация;
- `src/mapreduce_s3.py` - собственный MapReduce по S3 Range GET;
- `src/duckdb_counts_s3.py` - сопоставимый SQL-подсчёт в DuckDB;
- `src/duckdb_analysis.py` - зарплаты и регионы;
- `src/verify_results.py` - полное сравнение результатов;
- `src/profile_pipeline.py` - три серии измерений времени и памяти;
- `evidence/` - журналы запусков и скриншоты RustFS;
- `report/` - итоговые DOCX и PDF.

## Подготовка

Исходный файл `it_vacancies_2024.csv` объёмом 847 619 384 байта не включён в
репозиторий. Его SHA-256:
`303db922b541e4440906f5a7e8be32fb55e8808f950ffddb8c272f1548212a65`.
Файл нужно положить рядом с каталогом проекта либо скорректировать путь в
команде подготовки.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Значения `RUSTFS_ACCESS_KEY` и `RUSTFS_SECRET_KEY` задаются локально и не
фиксируются в Git. Для нового стенда можно использовать четыре каталога из
`docker-compose.yml`:

```powershell
docker compose --env-file .env up -d
$env:RUSTFS_ACCESS_KEY = "<access-key>"
$env:RUSTFS_SECRET_KEY = "<secret-key>"
$env:RUSTFS_ENDPOINT = "http://127.0.0.1:9000"
$env:RUSTFS_BUCKET = "hh-lake"
python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
```

## Воспроизведение

```powershell
python src/prepare_dataset.py ..\it_vacancies_2024.csv --out-dir data/prepared
python src/s3_io.py upload ..\it_vacancies_2024.csv raw/it_vacancies_2024.csv
python src/s3_io.py upload data/prepared/devops_vacancies.ndjson variants/devops/vacancies.ndjson
python src/mapreduce_s3.py --out-dir results/mapreduce --mode optimized --workers 4 --chunk-bytes 524288
python src/duckdb_counts_s3.py --out-dir results/duckdb_counts_only --threads 4
python src/duckdb_analysis.py --out-dir results/duckdb --threads 4
python src/duckdb_variant_check.py --out evidence/duckdb_variant_check.json
python src/verify_results.py results/mapreduce results/duckdb_counts_only --out evidence/verification.json
python src/profile_pipeline.py --attempts 3 --out evidence/profiling.json
```

Для теста retry используется тот же входной объект; ошибка внедряется только в
первую попытку второго чанка:

```powershell
python src/mapreduce_s3.py --out-dir results/failure-test --mode optimized --inject-failure
```

