import json

import duckdb
import pytest

from votes_pipeline.ingestion import ingest_votes


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def read_votes(database):
    connection = duckdb.connect(str(database), read_only=True)
    try:
        return connection.execute(
            "SELECT vote_id, blog_id, voted_at, vote_value, updated_at "
            "FROM blog_analysis.votes ORDER BY vote_id"
        ).fetchall()
    finally:
        connection.close()


def test_ingestion_inserts_and_reruns_without_duplicates(tmp_path):
    database = tmp_path / "votes.duckdb"
    source = tmp_path / "votes.jsonl"
    rows = [
        {"vote_id": 1, "blog_id": 10, "voted_at": "2025-01-06T10:00:00", "vote_value": 1, "updated_at": "2025-01-06T10:00:00"},
        {"vote_id": 2, "blog_id": 11, "voted_at": "2025-01-07T10:00:00", "vote_value": 1, "updated_at": "2025-01-07T10:00:00"},
    ]
    write_jsonl(source, rows)

    assert ingest_votes(source, database) == 2
    assert ingest_votes(source, database) == 2
    assert len(read_votes(database)) == 2


def test_existing_vote_is_updated_not_duplicated(tmp_path):
    database = tmp_path / "votes.duckdb"
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    base = {"vote_id": 7, "blog_id": 3, "voted_at": "2025-01-06T10:00:00", "vote_value": 1, "updated_at": "2025-01-06T10:00:00"}
    revised = {**base, "vote_value": -1, "updated_at": "2025-01-08T10:00:00"}
    write_jsonl(first, [base])
    write_jsonl(second, [revised])

    ingest_votes(first, database)
    ingest_votes(second, database)

    actual = read_votes(database)
    assert len(actual) == 1
    assert actual[0][0] == 7
    assert actual[0][3] == -1


def test_conflicting_versions_in_one_batch_choose_latest_update(tmp_path):
    database = tmp_path / "votes.duckdb"
    source = tmp_path / "votes.jsonl"
    older = {"vote_id": 4, "blog_id": 8, "voted_at": "2025-01-06T10:00:00", "vote_value": 1, "updated_at": "2025-01-06T10:00:00"}
    newer = {**older, "vote_value": -1, "updated_at": "2025-01-07T10:00:00"}
    write_jsonl(source, [older, newer])

    ingest_votes(source, database)
    actual = read_votes(database)
    assert len(actual) == 1
    assert actual[0][3] == -1


def test_conflicting_duplicate_without_version_column_fails_clearly(tmp_path):
    database = tmp_path / "votes.duckdb"
    source = tmp_path / "votes.jsonl"
    write_jsonl(source, [{"id": 1, "value": "old"}, {"id": 1, "value": "new"}])

    with pytest.raises(ValueError, match="no updated-at column"):
        ingest_votes(source, database)
