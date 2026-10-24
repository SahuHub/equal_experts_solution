"""Recreate and print the weekly vote outlier view using SQL only."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

DEFAULT_DATABASE = "votes.duckdb"
SCHEMA_NAME = "blog_analysis"
TABLE_FQN = f"{SCHEMA_NAME}.votes"
VIEW_FQN = f"{SCHEMA_NAME}.outlier_weeks"

_TIME_CANDIDATES = (
    "voted_at",
    "vote_timestamp",
    "voted_on",
    "vote_date",
    "timestamp",
    "created_at",
    "created_on",
    "created",
    "event_time",
    "event_timestamp",
    "time_created",
    "date",
    "time",
    "dt",
    "ts",
)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _columns(connection: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    rows = connection.execute(
        """SELECT column_name, data_type
           FROM information_schema.columns
           WHERE table_schema = ? AND table_name = ?
           ORDER BY ordinal_position""",
        [SCHEMA_NAME, "votes"],
    ).fetchall()
    return [(str(name), str(data_type)) for name, data_type in rows]


def _find_time_column(columns: list[tuple[str, str]]) -> str:
    by_name = {name.lower().replace("-", "_"): name for name, _ in columns}
    for candidate in _TIME_CANDIDATES:
        if candidate in by_name:
            return by_name[candidate]

    typed_candidates = [
        name
        for name, data_type in columns
        if "TIMESTAMP" in data_type.upper() or data_type.upper() == "DATE"
    ]
    if len(typed_candidates) == 1:
        return typed_candidates[0]
    raise ValueError(
        "Could not identify the vote timestamp/date column. Rename it to a recognised "
        "name (for example voted_at, vote_timestamp, vote_date, or created_at), or "
        "ensure there is exactly one DATE/TIMESTAMP column."
    )


def create_outlier_view(connection: duckdb.DuckDBPyConnection) -> None:
    """Create the view of observed weeks >20% away from the mean weekly count.

    Each row of blog_analysis.votes represents one vote. Weeks are Monday-based
    calendar weeks; the mean uses weeks present in the data (not missing weeks).
    The strict `>` implements “more than 20%”, so exactly 20% is not an outlier.
    """
    columns = _columns(connection)
    if not columns:
        raise RuntimeError(
            "Table blog_analysis.votes does not exist or has no columns. Run ingestion first."
        )
    timestamp_column = quote_identifier(_find_time_column(columns))

    connection.execute(
        f"""CREATE OR REPLACE VIEW {VIEW_FQN} AS
        WITH weekly_counts AS (
            SELECT
                CAST(DATE_TRUNC('week', CAST({timestamp_column} AS TIMESTAMP)) AS DATE) AS week_start,
                COUNT(*)::BIGINT AS vote_count
            FROM {TABLE_FQN}
            WHERE {timestamp_column} IS NOT NULL
            GROUP BY 1
        ), weekly_mean AS (
            SELECT AVG(vote_count) AS average_weekly_votes
            FROM weekly_counts
        )
        SELECT
            w.week_start,
            w.vote_count,
            m.average_weekly_votes,
            100.0 * (w.vote_count - m.average_weekly_votes)
                / NULLIF(m.average_weekly_votes, 0) AS deviation_pct
        FROM weekly_counts AS w
        CROSS JOIN weekly_mean AS m
        WHERE ABS(w.vote_count - m.average_weekly_votes)
              > m.average_weekly_votes * 0.20"""
    )


def run_outliers(database_path: str | Path = DEFAULT_DATABASE) -> list[tuple[object, ...]]:
    connection = duckdb.connect(str(database_path))
    try:
        create_outlier_view(connection)
        return connection.execute(f"SELECT * FROM {VIEW_FQN} ORDER BY week_start").fetchall()
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recreate the weekly vote outlier view.")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help=f"DuckDB file (default: {DEFAULT_DATABASE})")
    args = parser.parse_args(argv)
    for row in run_outliers(args.database):
        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
