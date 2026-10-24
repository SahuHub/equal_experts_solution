# Votes data pipeline — Equal Experts coding assignment

This submission contains two independently runnable tasks:

1. `ingest.py` incrementally upserts JSONL vote records into `blog_analysis.votes`.
2. `outliers.py` recreates `blog_analysis.outlier_weeks` in SQL and prints the returned rows.

The implementation uses Python, DuckDB and SQL only. It does not use pandas, Polars, PySpark, or a custom testing framework.

## Assumptions made because the PDF does not include the source JSONL or starter repository

- Each JSONL line is one vote record, and one row contributes one vote to its calendar week.
- A stable key is present as one of `vote_id`, `id`, `voteid`, `vote_id_key`, or `_id`.
- A timestamp/date field is present under a common name such as `voted_at`, `vote_timestamp`, `vote_date`, `timestamp`, or `created_at`. `outliers.py` also accepts a single DATE/TIMESTAMP-typed column if its name is not recognised.
- If a single file contains multiple versions of the same key, a recognised `updated_at`/`modified_at`-style field must order those versions uniquely. Exact duplicate lines are harmless. If conflicting versions cannot be ordered safely, ingestion fails rather than choosing an arbitrary winner.
- The weekly mean is calculated across weeks represented in the data. Weeks with no votes are not generated as zero-count weeks. A week is anomalous only when the absolute deviation is strictly greater than 20%; exactly 20% is not an anomaly.
- Column names/types are inferred from the JSONL. The target table keeps the source fields instead of inventing a narrower business schema. The input column set must remain stable between runs.

These are explicit assumptions rather than facts specified by the PDF. If the provided exercise repository or sample data defines a different key/timestamp column or expected output shape, align the aliases and tests to that contract before submission.

## Setup

```bash
poetry install
```

DuckDB is installed as a project dependency. Use Python 3.10 or newer.

## Run independently

Place the JSONL input at `votes.jsonl`, then run ingestion:

```bash
poetry run python ingest.py --input votes.jsonl --database votes.duckdb
```

Run outlier detection separately after the table has been populated:

```bash
poetry run python outliers.py --database votes.duckdb
```

The database and input paths can be changed with the command-line options. The default database is `votes.duckdb`. Ingestion does not create or refresh the outlier view; that is intentionally a separate task.

You can also use the installed entry points:

```bash
poetry run ingest-votes --input votes.jsonl --database votes.duckdb
poetry run outliers --database votes.duckdb
```

## Design choices

### Ingestion

- DuckDB reads JSONL directly using `read_json_auto`, avoiding a Python-side in-memory copy of the full file.
- The inferred source columns are retained in the target table. The identifier column is a primary key when the table is first created.
- Existing keys are upserted: all non-key columns are replaced by the incoming version. A rerun of the same file therefore does not increase row count.
- Exact duplicates in a batch are removed. Conflicting in-file versions are resolved by the newest unique update timestamp; ambiguous conflicts fail loudly.
- The write is wrapped in a transaction so a failed write does not leave a partially applied batch.

### Outlier view

- `DATE_TRUNC('week', ...)` groups timestamps into Monday-based calendar weeks.
- `COUNT(*)` gives weekly vote totals when each row represents a vote.
- The average is calculated from the observed weekly totals, then an absolute relative deviation is tested against `0.20`.
- The view returns `week_start`, `vote_count`, `average_weekly_votes`, and `deviation_pct` (positive means above average; negative means below average).

## Tests

The tests use a fresh temporary DuckDB file and temporary JSONL files per test; they do not share a database or depend on execution order.

```bash
poetry run pytest
```

The test suite covers repeatable ingestion, update-in-place behavior, duplicate versions in one batch, ambiguous duplicate handling, strict 20% threshold behavior, and repeatable view creation.

The PDF refers to `poetry run exercise check-ingestion` and `poetry run exercise check-outliers`. Those appear to be commands supplied by the original exercise harness. The PDF alone did not include that harness, its fixture data, or the repository structure, so those external checks cannot be verified from this standalone package. Run them in the provided starter repository after integrating the solution.

## AI tool usage disclosure

OpenAI ChatGPT was used to help draft the initial implementation, tests, and documentation. The candidate should review every line, execute the tests in the target environment, and be prepared to explain or modify the solution during the follow-up interview.
