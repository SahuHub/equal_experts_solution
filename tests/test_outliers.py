import json
from datetime import date

import duckdb

from votes_pipeline.ingestion import ingest_votes
from votes_pipeline.outliers import create_outlier_view, run_outliers


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_only_week_more_than_twenty_percent_from_mean_is_returned(tmp_path):
    database = tmp_path / "votes.duckdb"
    source = tmp_path / "votes.jsonl"
    rows = []
    vote_id = 1
    # Four observed weeks have 10, 10, 10 and 20 votes. The mean is 12.5.
    # Each 10-vote week is exactly 20% below the mean and must not qualify;
    # the 20-vote week is 60% above it and must qualify.
    for week_start, count in [
        ("2025-01-06", 10),
        ("2025-01-13", 10),
        ("2025-01-20", 10),
        ("2025-01-27", 20),
    ]:
        for day_vote in range(count):
            rows.append({
                "vote_id": vote_id,
                "blog_id": 100 + day_vote,
                "voted_at": f"{week_start}T12:00:00",
                "vote_value": 1,
                "updated_at": f"{week_start}T12:00:00",
            })
            vote_id += 1
    write_jsonl(source, rows)
    ingest_votes(source, database)

    result = run_outliers(database)

    assert len(result) == 1
    assert result[0][0] == date(2025, 1, 27)
    assert result[0][1] == 20
    assert float(result[0][2]) == 12.5
    assert round(float(result[0][3]), 1) == 60.0


def test_create_view_is_repeatable_and_view_is_persistent(tmp_path):
    database = tmp_path / "votes.duckdb"
    source = tmp_path / "votes.jsonl"
    write_jsonl(source, [
        {"vote_id": 1, "blog_id": 2, "voted_at": "2025-02-03T00:00:00", "vote_value": 1, "updated_at": "2025-02-03T00:00:00"},
        {"vote_id": 2, "blog_id": 2, "voted_at": "2025-02-03T00:00:00", "vote_value": 1, "updated_at": "2025-02-03T00:00:00"},
    ])
    ingest_votes(source, database)

    connection = duckdb.connect(str(database))
    try:
        create_outlier_view(connection)
        create_outlier_view(connection)
        view_exists = connection.execute(
            "SELECT 1 FROM information_schema.views "
            "WHERE table_schema = 'blog_analysis' AND table_name = 'outlier_weeks'"
        ).fetchone()
        assert view_exists is not None
    finally:
        connection.close()
