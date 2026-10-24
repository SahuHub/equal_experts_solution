"""Incrementally upsert JSONL vote records into DuckDB.

The input schema is inferred from JSONL. A stable identifier column is required;
common names are recognised so the code can work with a few sensible source
schemas without discarding source columns.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import duckdb

DEFAULT_DATABASE = "votes.duckdb"
DEFAULT_INPUT = "votes.jsonl"
SCHEMA_NAME = "blog_analysis"
TABLE_NAME = "votes"
TABLE_FQN = f"{SCHEMA_NAME}.{TABLE_NAME}"

_ID_CANDIDATES = ("vote_id", "id", "voteid", "vote_id_key", "_id")
_UPDATED_CANDIDATES = (
    "updated_at",
    "updated_on",
    "updated",
    "modified_at",
    "modified_on",
    "last_updated",
)


def quote_identifier(identifier: str) -> str:
    """Quote a DuckDB identifier safely, including names containing spaces."""
    return '"' + identifier.replace('"', '""') + '"'


def _column_definitions(
    connection: duckdb.DuckDBPyConnection, table: str, *, qualified: bool = False
) -> list[tuple[str, str]]:
    if qualified:
        relation = f"{quote_identifier(SCHEMA_NAME)}.{quote_identifier(TABLE_NAME)}"
    else:
        relation = quote_identifier(table)
    rows = connection.execute(f"DESCRIBE {relation}").fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _find_column(columns: list[tuple[str, str]], candidates: tuple[str, ...]) -> str | None:
    by_normalized_name = {name.lower().replace("-", "_"): name for name, _ in columns}
    for candidate in candidates:
        if candidate in by_normalized_name:
            return by_normalized_name[candidate]
    return None


def _table_exists(connection: duckdb.DuckDBPyConnection) -> bool:
    return bool(
        connection.execute(
            """SELECT 1
               FROM information_schema.tables
               WHERE table_schema = ? AND table_name = ?""",
            [SCHEMA_NAME, TABLE_NAME],
        ).fetchone()
    )


def _create_target_table(
    connection: duckdb.DuckDBPyConnection,
    columns: list[tuple[str, str]],
    key_column: str,
) -> None:
    definitions = [f"{quote_identifier(name)} {data_type}" for name, data_type in columns]
    definitions.append(f"PRIMARY KEY ({quote_identifier(key_column)})")
    connection.execute(f"CREATE TABLE {TABLE_FQN} ({', '.join(definitions)})")


def _deduplicate_batch(
    connection: duckdb.DuckDBPyConnection,
    key_column: str,
    updated_column: str | None,
) -> None:
    """Remove exact duplicates and resolve in-file versions deterministically.

    If the same key has different payloads in one file, a non-null, unique
    updated timestamp is required for each version. Without that ordering
    signal, choosing a winner would silently invent business semantics.
    """
    connection.execute("CREATE TEMP TABLE _incoming_distinct AS SELECT DISTINCT * FROM _incoming")
    key = quote_identifier(key_column)

    null_keys = connection.execute(
        f"SELECT COUNT(*) FROM _incoming_distinct WHERE {key} IS NULL"
    ).fetchone()[0]
    if null_keys:
        raise ValueError(f"Input contains {null_keys} row(s) with a null key in {key_column!r}.")

    duplicate_group_count = connection.execute(
        f"""SELECT COUNT(*) FROM (
                SELECT {key}
                FROM _incoming_distinct
                GROUP BY {key}
                HAVING COUNT(*) > 1
            )"""
    ).fetchone()[0]

    if duplicate_group_count and not updated_column:
        raise ValueError(
            f"Input contains conflicting records for the same {key_column!r}, "
            "but no updated-at column was found to determine which version wins."
        )

    if duplicate_group_count and updated_column:
        updated = quote_identifier(updated_column)
        ambiguous = connection.execute(
            f"""SELECT COUNT(*) FROM (
                    SELECT {key}
                    FROM _incoming_distinct
                    GROUP BY {key}
                    HAVING COUNT(*) > 1
                       AND (COUNT({updated}) <> COUNT(*) OR COUNT(DISTINCT {updated}) <> COUNT(*))
                )"""
        ).fetchone()[0]
        if ambiguous:
            raise ValueError(
                f"Conflicting duplicate {key_column!r} values have missing or tied "
                f"{updated_column!r} values; refusing to choose an arbitrary record."
            )
        connection.execute(
            f"""CREATE TEMP TABLE _incoming_clean AS
                SELECT * FROM _incoming_distinct
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY {key} ORDER BY {updated} DESC
                ) = 1"""
        )
    else:
        connection.execute("CREATE TEMP TABLE _incoming_clean AS SELECT * FROM _incoming_distinct")


def ingest_votes(input_path: str | Path, database_path: str | Path = DEFAULT_DATABASE) -> int:
    """Load JSONL records and upsert them by their stable vote identifier.

    Returns the number of unique records in the input batch. The function is
    safe to rerun: an existing identifier is updated rather than duplicated.
    """
    source = Path(input_path)
    if not source.is_file():
        raise FileNotFoundError(f"JSONL input file does not exist: {source}")
    if source.stat().st_size == 0:
        raise ValueError(f"JSONL input file is empty: {source}")

    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
        connection.execute(
            """CREATE TEMP TABLE _incoming AS
               SELECT * FROM read_json_auto(?, format = 'newline_delimited')""",
            [str(source.resolve())],
        )

        incoming_columns = _column_definitions(connection, "_incoming")
        if not incoming_columns:
            raise ValueError("The JSONL input does not contain any columns.")

        key_column = _find_column(incoming_columns, _ID_CANDIDATES)
        if key_column is None:
            raise ValueError(
                "No stable identifier column found. Expected one of: "
                + ", ".join(_ID_CANDIDATES)
            )
        updated_column = _find_column(incoming_columns, _UPDATED_CANDIDATES)
        _deduplicate_batch(connection, key_column, updated_column)
        input_count = int(connection.execute("SELECT COUNT(*) FROM _incoming_clean").fetchone()[0])

        connection.execute("BEGIN TRANSACTION")
        try:
            if not _table_exists(connection):
                _create_target_table(connection, incoming_columns, key_column)
                connection.execute(
                    f"INSERT INTO {TABLE_FQN} BY NAME SELECT * FROM _incoming_clean"
                )
            else:
                target_columns = [
                    name for name, _ in _column_definitions(connection, TABLE_FQN, qualified=True)
                ]
                incoming_names = [name for name, _ in incoming_columns]
                if {name.lower() for name in target_columns} != {
                    name.lower() for name in incoming_names
                }:
                    raise ValueError(
                        "The incoming JSONL schema differs from blog_analysis.votes. "
                        f"Target columns: {target_columns}; input columns: {incoming_names}."
                    )
                target_key = _find_column([(name, "") for name in target_columns], _ID_CANDIDATES)
                if target_key is None or target_key.lower() != key_column.lower():
                    raise ValueError("The target table's identifier column differs from the input.")

                update_columns = [name for name in target_columns if name.lower() != key_column.lower()]
                if update_columns:
                    assignments = ", ".join(
                        f"{quote_identifier(name)} = excluded.{quote_identifier(name)}"
                        for name in update_columns
                    )
                    conflict_action = f"DO UPDATE SET {assignments}"
                else:
                    conflict_action = "DO NOTHING"
                connection.execute(
                    f"""INSERT INTO {TABLE_FQN} BY NAME
                        SELECT * FROM _incoming_clean
                        ON CONFLICT ({quote_identifier(target_key)}) {conflict_action}"""
                )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

        return input_count
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Incrementally upsert vote records into DuckDB.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help=f"JSONL input path (default: {DEFAULT_INPUT})")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help=f"DuckDB file (default: {DEFAULT_DATABASE})")
    args = parser.parse_args(argv)
    ingest_votes(args.input, args.database)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
