"""Convenience script entry point for the ingestion task."""
from votes_pipeline.ingestion import main

if __name__ == "__main__":
    raise SystemExit(main())
