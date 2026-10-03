from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import duckdb

from career_analytics.etl import flatten_professionals, load_source, run_pipeline
from career_analytics.quality import validate_frames


PROJECT_ROOT = Path(__file__).parents[1]
SOURCE_PATH = PROJECT_ROOT / "data" / "full-professionals-json.json"
AS_OF_DATE = date(2023, 2, 28)


def test_flatten_reconciles_all_nested_rows() -> None:
    professionals = load_source(SOURCE_PATH)
    frames, counts, state = flatten_professionals(professionals, AS_OF_DATE)

    assert counts == {
        "professionals": 10,
        "jobs": 32,
        "skills": 34,
        "certifications": 19,
        "education": 16,
    }
    assert len(frames["dim_professional"]) == counts["professionals"]
    assert len(frames["fact_job_history"]) == counts["jobs"]
    assert len(frames["bridge_professional_skill"]) == counts["skills"]
    assert len(frames["fact_professional_certification"]) == counts["certifications"]
    assert len(frames["fact_education"]) == counts["education"]
    assert len(state) == counts["professionals"]


def test_quality_rules_find_requested_problem_types() -> None:
    professionals = [
        {
            "professional_id": "P_BAD",
            "years_experience": 3,
            "current_industry": "Technology",
            "current_role": "Engineer",
            "education_level": "Bachelors",
            "jobs": [
                {
                    "job_id": "J_DUP",
                    "company": "A",
                    "industry": "Technology",
                    "role": "Analyst",
                    "start_date": "2022-10-01",
                    "end_date": "2022-01-01",
                    "salary_band": 2,
                },
                {
                    "job_id": "J_DUP",
                    "company": None,
                    "industry": "Technology",
                    "role": "Engineer",
                    "start_date": "2022-06-01",
                    "end_date": None,
                    "salary_band": 99,
                },
            ],
            "skills": [
                {
                    "skill_id": "S1",
                    "skill_name": "Python",
                    "proficiency_level": "Advanced",
                    "years_experience": 8,
                }
            ],
            "certifications": [
                {
                    "certification_id": "C1",
                    "certification_name": "Example",
                    "issuing_organization": "Issuer",
                    "date_earned": "2024-01-01",
                    "expiration_date": "2023-01-01",
                }
            ],
            "education": [],
        }
    ]
    frames, counts, _ = flatten_professionals(professionals, AS_OF_DATE)
    issues = validate_frames(frames, counts, AS_OF_DATE)
    checks = set(issues["check_name"])

    assert "duplicate_id" in checks
    assert "job_end_before_start" in checks
    assert "overlapping_job_periods" in checks
    assert "missing_critical_field" in checks
    assert "skill_experience_exceeds_career" in checks
    assert "salary_band_outlier" in checks
    assert "certification_expiration_before_earned" in checks


def test_pipeline_builds_expected_tables_and_metrics(tmp_path: Path) -> None:
    database = tmp_path / "career_analytics.duckdb"
    summary = run_pipeline(SOURCE_PATH, database, mode="full", as_of_date=AS_OF_DATE)

    assert summary["status"] == "success"
    assert summary["source_counts"]["professionals"] == 10
    with duckdb.connect(str(database), read_only=True) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert {
            "dim_professional",
            "dim_company",
            "dim_role",
            "fact_job_history",
            "bridge_professional_skill",
            "mart_job_transitions",
            "mart_professional_career_metrics",
            "mart_industry_progression",
            "data_quality_issues",
        } <= table_names
        assert connection.execute("SELECT COUNT(*) FROM dim_professional").fetchone()[0] == 10
        assert connection.execute("SELECT COUNT(*) FROM fact_job_history").fetchone()[0] == 32
        assert (
            connection.execute("SELECT COUNT(*) FROM mart_professional_career_metrics").fetchone()[0]
            == 10
        )


def test_incremental_load_is_idempotent_and_updates_one_professional(tmp_path: Path) -> None:
    source_copy = tmp_path / "professionals.json"
    payload = json.loads(SOURCE_PATH.read_text(encoding="utf-8"))
    source_copy.write_text(json.dumps(payload), encoding="utf-8")
    database = tmp_path / "career_analytics.duckdb"

    run_pipeline(source_copy, database, mode="full", as_of_date=AS_OF_DATE)
    unchanged = run_pipeline(source_copy, database, mode="incremental", as_of_date=AS_OF_DATE)
    assert unchanged["changed_professionals"] == 0

    changed_payload = copy.deepcopy(payload)
    changed_payload["professionals"][0]["jobs"][-1]["salary_band"] = 6
    source_copy.write_text(json.dumps(changed_payload), encoding="utf-8")
    changed = run_pipeline(source_copy, database, mode="incremental", as_of_date=AS_OF_DATE)

    assert changed["changed_professionals"] == 1
    with duckdb.connect(str(database), read_only=True) as connection:
        assert (
            connection.execute(
                "SELECT salary_band FROM fact_job_history WHERE professional_id='P001' "
                "ORDER BY start_date DESC LIMIT 1"
            ).fetchone()[0]
            == 6
        )
        assert connection.execute("SELECT COUNT(*) FROM fact_job_history").fetchone()[0] == 32
