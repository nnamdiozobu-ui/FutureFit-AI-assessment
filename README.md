# FutureFit AI career analytics take-home

This project turns the supplied nested professional profiles into a small DuckDB model for career analysis. It includes the ETL, data-quality checks, career metrics, tests, and an Airflow DAG.

## Project structure

```text
data/full-professionals-json.json        supplied source data
src/career_analytics/etl.py              transformation and loading
src/career_analytics/quality.py          data-quality rules
src/career_analytics/metrics.py          career and transition metrics
dags/career_analytics_dag.py             Airflow 2.10.1 DAG
docs/requirements_analysis.md            requirements analysis
tests/test_pipeline.py                   automated tests
output/                                  generated database and CSV files
```

The assignment refers to `professionals_nested.json`, but the supplied file is named `full-professionals-json.json`, so I used the supplied filename.

## Running the project

The project targets Python 3.11. I used `uv` to create the environment:

```bash
uv venv --python 3.11
source .venv/bin/activate
uv pip install -e '.[dev]'

python -m career_analytics.etl \
  --source data/full-professionals-json.json \
  --output output/career_analytics.duckdb \
  --as-of-date 2023-02-28 \
  --strict

pytest -q
```

The pandas and DuckDB versions are aligned with Airflow 2.10.1's official Python 3.11 constraints. In an Airflow environment, install Airflow with that constraints file before installing this project.

The source does not include an extract date. I used 2023-02-28, the latest completed-job end date, as the date of the sample snapshot. In production, I would use the actual extract timestamp instead. The `--strict` option stops the load if an error-level quality problem is found.

To process a later snapshot without reloading unchanged professionals:

```bash
python -m career_analytics.etl \
  --source data/full-professionals-json.json \
  --output output/career_analytics.duckdb \
  --mode incremental \
  --as-of-date 2023-02-28 \
  --strict
```

The run creates the DuckDB database and three review files in `output/`: `data_quality_issues.csv`, `career_metrics.csv`, and `industry_progression.csv`.

## Approach

I first separate each professional's jobs, skills, certifications, and education into tables. Jobs retain the professional ID so a person's career history can be rebuilt and ordered by date. For values without source IDs, such as company and role names, I create repeatable IDs from the normalized text.

The pipeline builds and checks the transformed tables before writing them to DuckDB. A full run replaces the current snapshot. An incremental run compares a hash of each professional's record and reloads only new or changed professionals. It also removes professionals that no longer appear, because the input file is treated as a complete snapshot.

After loading the facts and dimensions, the pipeline calculates job transitions, salary-band growth, tenure, and time to advancement.

## Data model

| Table | What one row represents |
| --- | --- |
| `dim_professional` | One professional |
| `dim_company`, `dim_industry`, `dim_role` | One company, industry, or role |
| `dim_skill`, `dim_certification`, `dim_institution` | One skill, certification, or institution |
| `fact_job_history` | One job held by one professional |
| `bridge_professional_skill` | One professional and skill combination |
| `fact_professional_certification` | One certification held by one professional |
| `fact_education` | One education record for one professional |
| `mart_job_transitions` | One move from a previous job to the next job |
| `mart_professional_career_metrics` | One career summary per professional |
| `mart_industry_progression` | One from-industry/to-industry grouping |

I use salary-band movement as the main advancement signal. I did not try to infer seniority from job titles because the data does not provide a standard role-level mapping.

## Data-quality checks

The pipeline checks for:

- duplicate professional, job, and child-record IDs;
- missing IDs and required job or profile fields;
- dates that cannot be parsed or occur in the wrong order;
- overlapping job periods;
- unreasonable professional or skill experience;
- salary bands outside the assumed 1-10 range;
- certification expiration before the earned date;
- inconsistent names attached to the same skill or certification ID;
- differences between source and transformed record counts.

An overlap is a warning rather than an automatic failure because a person may hold two roles at the same time. A job with no end date is treated as current as of the selected snapshot date.

## Results from the supplied data

The full run produced:

- 10 professionals
- 32 jobs
- 34 professional-skill records
- 19 certifications
- 16 education records
- 22 job transitions
- 0 quality errors and 0 warnings

All 22 observed transitions stay within the same industry, so this sample cannot tell us whether changing industries improves career outcomes.

The test suite contains four tests covering record reconciliation, bad-data detection, creation of the final model, and incremental loading. All four tests pass.

## Airflow and production notes

The DAG checks that the source exists, runs the validated incremental load, and reconciles the final counts. The load includes the quality gate and metric refresh. Failed tasks are retried with increasing delays, and only one run can write to the DuckDB file at a time. Tasks exchange paths and counts rather than entire datasets, which keeps orchestration light for the specified 1-vCPU/2-GB Airflow components. The source and DuckDB paths are configurable and must be on storage shared by the worker tasks. Airflow's 2-vCPU/4-GB database remains a metadata database rather than an analytics target.

I left a separate persistent staging layer and detailed run-audit tables out of this take-home to keep the implementation focused. In production, I would add them so failed batches can be quarantined and every load can be traced by source, time, status, and row counts.

I would also:

- use MongoDB `updated_at` values or change streams instead of hashing a complete file;
- keep raw source snapshots in object storage so runs can be replayed;
- load the final model into a separate analytical database rather than Airflow's metadata database;
- add monitoring and alerts for failures, freshness, row-count changes, and unusual null or duplicate rates.

The Part 2 response is in [`docs/requirements_analysis.md`](docs/requirements_analysis.md).
