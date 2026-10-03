from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from .metrics import refresh_derived_metrics
from .quality import error_messages, validate_frames


DIMENSION_TABLES = [
    "dim_company",
    "dim_industry",
    "dim_role",
    "dim_skill",
    "dim_certification",
    "dim_institution",
]
SUBJECT_TABLES = [
    "dim_professional",
    "fact_job_history",
    "bridge_professional_skill",
    "fact_professional_certification",
    "fact_education",
]
def _stable_id(prefix: str, value: object) -> str | None:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    normalized = " ".join(str(value).strip().lower().split())
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _record_hash(record: dict[str, Any]) -> str:
    payload = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_date(value: object) -> pd.Timestamp:
    if value is None or str(value).strip() == "":
        return pd.NaT
    return pd.to_datetime(value, errors="coerce")


def _unique_frame(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        return frame
    return frame.drop_duplicates().reset_index(drop=True)


def load_source(source_path: Path) -> list[dict[str, Any]]:
    with source_path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    if not isinstance(payload, dict) or not isinstance(payload.get("professionals"), list):
        raise ValueError("Source JSON must contain a top-level 'professionals' array.")
    return payload["professionals"]


def flatten_professionals(
    professionals: list[dict[str, Any]], as_of_date: date
) -> tuple[dict[str, pd.DataFrame], dict[str, int], pd.DataFrame]:
    professional_rows: list[dict[str, Any]] = []
    company_rows: list[dict[str, Any]] = []
    industry_rows: list[dict[str, Any]] = []
    role_rows: list[dict[str, Any]] = []
    skill_rows: list[dict[str, Any]] = []
    certification_rows: list[dict[str, Any]] = []
    institution_rows: list[dict[str, Any]] = []
    job_rows: list[dict[str, Any]] = []
    professional_skill_rows: list[dict[str, Any]] = []
    professional_certification_rows: list[dict[str, Any]] = []
    education_rows: list[dict[str, Any]] = []
    state_rows: list[dict[str, Any]] = []

    expected_counts = {
        "professionals": len(professionals),
        "jobs": 0,
        "skills": 0,
        "certifications": 0,
        "education": 0,
    }

    for professional in professionals:
        professional_id = professional.get("professional_id")
        current_industry = professional.get("current_industry")
        current_role = professional.get("current_role")
        professional_rows.append(
            {
                "professional_id": professional_id,
                "years_experience": professional.get("years_experience"),
                "current_industry_id": _stable_id("ind", current_industry),
                "current_role_id": _stable_id("role", current_role),
                "education_level": professional.get("education_level"),
            }
        )
        industry_rows.append(
            {"industry_id": _stable_id("ind", current_industry), "industry_name": current_industry}
        )
        role_rows.append({"role_id": _stable_id("role", current_role), "role_name": current_role})
        state_rows.append(
            {
                "professional_id": professional_id,
                "record_hash": _record_hash(professional),
            }
        )

        jobs = professional.get("jobs") or []
        expected_counts["jobs"] += len(jobs)
        for job in jobs:
            company = job.get("company")
            industry = job.get("industry")
            role = job.get("role")
            start = _parse_date(job.get("start_date"))
            end = _parse_date(job.get("end_date"))
            effective_end = pd.Timestamp(as_of_date) if pd.isna(end) else end
            tenure_months = None
            if not pd.isna(start) and effective_end >= start:
                tenure_months = round((effective_end - start).days / 30.4375, 1)
            company_id = _stable_id("company", company)
            industry_id = _stable_id("ind", industry)
            role_id = _stable_id("role", role)
            company_rows.append({"company_id": company_id, "company_name": company})
            industry_rows.append({"industry_id": industry_id, "industry_name": industry})
            role_rows.append({"role_id": role_id, "role_name": role})
            job_rows.append(
                {
                    "professional_id": professional_id,
                    "job_id": job.get("job_id"),
                    "company_id": company_id,
                    "industry_id": industry_id,
                    "role_id": role_id,
                    "start_date": start,
                    "end_date": end,
                    "source_start_date": job.get("start_date"),
                    "source_end_date": job.get("end_date"),
                    "salary_band": job.get("salary_band"),
                    "tenure_months": tenure_months,
                    "is_current": pd.isna(end),
                }
            )

        skills = professional.get("skills") or []
        expected_counts["skills"] += len(skills)
        for skill in skills:
            skill_rows.append(
                {
                    "skill_id": skill.get("skill_id"),
                    "skill_name": skill.get("skill_name"),
                }
            )
            professional_skill_rows.append(
                {
                    "professional_id": professional_id,
                    "skill_id": skill.get("skill_id"),
                    "proficiency_level": skill.get("proficiency_level"),
                    "years_experience": skill.get("years_experience"),
                }
            )

        certifications = professional.get("certifications") or []
        expected_counts["certifications"] += len(certifications)
        for certification in certifications:
            certification_rows.append(
                {
                    "certification_id": certification.get("certification_id"),
                    "certification_name": certification.get("certification_name"),
                    "issuing_organization": certification.get("issuing_organization"),
                }
            )
            professional_certification_rows.append(
                {
                    "professional_id": professional_id,
                    "certification_id": certification.get("certification_id"),
                    "date_earned": _parse_date(certification.get("date_earned")),
                    "expiration_date": _parse_date(certification.get("expiration_date")),
                    "source_date_earned": certification.get("date_earned"),
                    "source_expiration_date": certification.get("expiration_date"),
                }
            )

        education = professional.get("education") or []
        expected_counts["education"] += len(education)
        for item in education:
            institution = item.get("institution")
            institution_id = _stable_id("institution", institution)
            institution_rows.append(
                {"institution_id": institution_id, "institution_name": institution}
            )
            education_rows.append(
                {
                    "professional_id": professional_id,
                    "education_id": item.get("education_id"),
                    "degree": item.get("degree"),
                    "institution_id": institution_id,
                    "field_of_study": item.get("field_of_study"),
                    "graduation_date": _parse_date(item.get("graduation_date")),
                    "source_graduation_date": item.get("graduation_date"),
                }
            )

    frames = {
        "dim_professional": pd.DataFrame(
            professional_rows,
            columns=[
                "professional_id",
                "years_experience",
                "current_industry_id",
                "current_role_id",
                "education_level",
            ],
        ),
        "dim_company": _unique_frame(company_rows, ["company_id", "company_name"]),
        "dim_industry": _unique_frame(industry_rows, ["industry_id", "industry_name"]),
        "dim_role": _unique_frame(role_rows, ["role_id", "role_name"]),
        "dim_skill": _unique_frame(skill_rows, ["skill_id", "skill_name"]),
        "dim_certification": _unique_frame(
            certification_rows,
            ["certification_id", "certification_name", "issuing_organization"],
        ),
        "dim_institution": _unique_frame(
            institution_rows, ["institution_id", "institution_name"]
        ),
        "fact_job_history": pd.DataFrame(
            job_rows,
            columns=[
                "professional_id",
                "job_id",
                "company_id",
                "industry_id",
                "role_id",
                "start_date",
                "end_date",
                "source_start_date",
                "source_end_date",
                "salary_band",
                "tenure_months",
                "is_current",
            ],
        ),
        "bridge_professional_skill": pd.DataFrame(
            professional_skill_rows,
            columns=["professional_id", "skill_id", "proficiency_level", "years_experience"],
        ),
        "fact_professional_certification": pd.DataFrame(
            professional_certification_rows,
            columns=[
                "professional_id",
                "certification_id",
                "date_earned",
                "expiration_date",
                "source_date_earned",
                "source_expiration_date",
            ],
        ),
        "fact_education": pd.DataFrame(
            education_rows,
            columns=[
                "professional_id",
                "education_id",
                "degree",
                "institution_id",
                "field_of_study",
                "graduation_date",
                "source_graduation_date",
            ],
        ),
    }
    state = pd.DataFrame(state_rows, columns=["professional_id", "record_hash"])
    return frames, expected_counts, state


def _raise_for_quality_errors(issues: pd.DataFrame) -> None:
    errors = issues[issues["severity"] == "error"] if not issues.empty else issues
    if not errors.empty:
        details = "\n".join(error_messages(errors))
        raise ValueError(f"Data quality gate failed with {len(errors)} error(s):\n{details}")


def _replace_table(
    connection: duckdb.DuckDBPyConnection, table_name: str, frame: pd.DataFrame
) -> None:
    connection.register("_incoming", frame)
    connection.execute(f"DELETE FROM {table_name}")
    connection.execute(f"INSERT INTO {table_name} SELECT * FROM _incoming")
    connection.unregister("_incoming")


def write_model(
    frames: dict[str, pd.DataFrame],
    issues: pd.DataFrame,
    source_state: pd.DataFrame,
    target_path: Path,
    mode: str,
) -> int:
    if mode not in {"full", "incremental"}:
        raise ValueError("mode must be 'full' or 'incremental'")
    target_path.parent.mkdir(parents=True, exist_ok=True)

    incoming_tables = {
        **frames,
        "data_quality_issues": issues,
        "source_record_state": source_state,
    }
    with duckdb.connect(str(target_path)) as connection:
        if mode == "full":
            for table_name, frame in incoming_tables.items():
                connection.register("_incoming", frame)
                connection.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _incoming"
                )
                connection.unregister("_incoming")
            return len(source_state)

        for table_name, frame in incoming_tables.items():
            connection.register("_incoming", frame)
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS "
                "SELECT * FROM _incoming WHERE 1 = 0"
            )
            connection.unregister("_incoming")

        existing_state = dict(
            connection.execute("SELECT professional_id, record_hash FROM source_record_state").fetchall()
        )
        incoming_state = dict(
            source_state[["professional_id", "record_hash"]].itertuples(index=False, name=None)
        )
        changed_ids = {
            professional_id
            for professional_id, record_hash in incoming_state.items()
            if existing_state.get(professional_id) != record_hash
        } | (set(existing_state) - set(incoming_state))

        connection.execute("BEGIN TRANSACTION")
        try:
            for table_name in DIMENSION_TABLES:
                _replace_table(connection, table_name, frames[table_name])

            changed = pd.DataFrame(
                {"professional_id": pd.Series(sorted(changed_ids), dtype="string")}
            )
            connection.register("_changed", changed)
            for table_name in SUBJECT_TABLES:
                connection.execute(
                    f"DELETE FROM {table_name} WHERE professional_id IN "
                    "(SELECT professional_id FROM _changed)"
                )
                connection.register("_incoming", frames[table_name])
                connection.execute(
                    f"INSERT INTO {table_name} "
                    "SELECT i.* FROM _incoming i JOIN _changed USING (professional_id)"
                )
                connection.unregister("_incoming")
            connection.unregister("_changed")

            _replace_table(connection, "source_record_state", source_state)
            _replace_table(connection, "data_quality_issues", issues)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    return len(changed_ids)


def export_review_outputs(target_path: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(target_path), read_only=True) as connection:
        connection.execute("SELECT * FROM data_quality_issues ORDER BY severity, check_name").fetchdf().to_csv(
            output_dir / "data_quality_issues.csv", index=False
        )
        connection.execute(
            "SELECT * FROM mart_professional_career_metrics ORDER BY professional_id"
        ).fetchdf().to_csv(output_dir / "career_metrics.csv", index=False)
        connection.execute(
            "SELECT * FROM mart_industry_progression ORDER BY transition_count DESC"
        ).fetchdf().to_csv(output_dir / "industry_progression.csv", index=False)


def run_pipeline(
    source_path: Path,
    target_path: Path,
    mode: str = "full",
    as_of_date: date | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    as_of_date = as_of_date or date.today()
    professionals = load_source(source_path)
    frames, expected_counts, source_state = flatten_professionals(professionals, as_of_date)
    issues = validate_frames(frames, expected_counts, as_of_date)
    if strict:
        _raise_for_quality_errors(issues)
    changed_count = write_model(frames, issues, source_state, target_path, mode)
    refresh_derived_metrics(target_path, as_of_date)
    export_review_outputs(target_path, target_path.parent)

    summary = {
        "status": "success",
        "mode": mode,
        "as_of_date": as_of_date.isoformat(),
        "database": str(target_path.resolve()),
        "changed_professionals": changed_count,
        "source_counts": expected_counts,
        "quality_errors": int((issues["severity"] == "error").sum()) if not issues.empty else 0,
        "quality_warnings": int((issues["severity"] == "warning").sum())
        if not issues.empty
        else 0,
    }
    summary_path = target_path.parent / "run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Nested professionals JSON")
    parser.add_argument("--output", type=Path, required=True, help="Target DuckDB database")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=date.today())
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail before publication when error-severity quality issues are present",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    summary = run_pipeline(
        source_path=args.source,
        target_path=args.output,
        mode=args.mode,
        as_of_date=args.as_of_date,
        strict=args.strict,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
