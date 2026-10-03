from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd


ISSUE_COLUMNS = [
    "severity",
    "check_name",
    "entity_type",
    "entity_id",
    "field_name",
    "message",
]


@dataclass(frozen=True)
class QualityIssue:
    severity: str
    check_name: str
    entity_type: str
    entity_id: str
    field_name: str
    message: str


def _blank(value: object) -> bool:
    return value is None or pd.isna(value) or str(value).strip() == ""


def _add(
    issues: list[QualityIssue],
    severity: str,
    check_name: str,
    entity_type: str,
    entity_id: object,
    field_name: str,
    message: str,
) -> None:
    issues.append(
        QualityIssue(
            severity=severity,
            check_name=check_name,
            entity_type=entity_type,
            entity_id="<missing>" if _blank(entity_id) else str(entity_id),
            field_name=field_name,
            message=message,
        )
    )


def _duplicate_checks(frames: dict[str, pd.DataFrame], issues: list[QualityIssue]) -> None:
    keys = {
        "dim_professional": ["professional_id"],
        "fact_job_history": ["job_id"],
        "bridge_professional_skill": ["professional_id", "skill_id"],
        "fact_professional_certification": ["professional_id", "certification_id"],
        "fact_education": ["professional_id", "education_id"],
    }
    for table, columns in keys.items():
        frame = frames[table]
        duplicates = frame[frame.duplicated(columns, keep=False)]
        for _, row in duplicates.iterrows():
            entity_id = "/".join(str(row.get(column, "")) for column in columns)
            _add(
                issues,
                "error",
                "duplicate_id",
                table,
                entity_id,
                ",".join(columns),
                f"Duplicate business key in {table}.",
            )

    for dimension, id_column, name_column in (
        ("dim_skill", "skill_id", "skill_name"),
        ("dim_certification", "certification_id", "certification_name"),
    ):
        frame = frames[dimension]
        if frame.empty:
            continue
        conflicting = frame.groupby(id_column, dropna=False)[name_column].nunique(dropna=False)
        for entity_id in conflicting[conflicting > 1].index:
            _add(
                issues,
                "error",
                "conflicting_id_mapping",
                dimension,
                entity_id,
                name_column,
                f"One {id_column} maps to multiple {name_column} values.",
            )


def _required_field_checks(frames: dict[str, pd.DataFrame], issues: list[QualityIssue]) -> None:
    required = {
        "dim_professional": [
            "professional_id",
            "years_experience",
            "current_industry_id",
            "current_role_id",
        ],
        "fact_job_history": [
            "professional_id",
            "job_id",
            "company_id",
            "industry_id",
            "role_id",
            "start_date",
            "salary_band",
        ],
        "bridge_professional_skill": ["professional_id", "skill_id", "proficiency_level"],
        "fact_professional_certification": [
            "professional_id",
            "certification_id",
            "date_earned",
        ],
        "fact_education": ["professional_id", "education_id", "degree", "institution_id"],
    }
    id_columns = {
        "dim_professional": "professional_id",
        "fact_job_history": "job_id",
        "bridge_professional_skill": "skill_id",
        "fact_professional_certification": "certification_id",
        "fact_education": "education_id",
    }
    for table, columns in required.items():
        frame = frames[table]
        for _, row in frame.iterrows():
            for column in columns:
                if _blank(row.get(column)):
                    _add(
                        issues,
                        "error",
                        "missing_critical_field",
                        table,
                        row.get(id_columns[table]),
                        column,
                        f"{column} is required.",
                    )


def _date_checks(
    frames: dict[str, pd.DataFrame], issues: list[QualityIssue], as_of_date: date
) -> None:
    jobs = frames["fact_job_history"]
    for _, row in jobs.iterrows():
        start = row["start_date"]
        end = row["end_date"]
        if not _blank(row["source_start_date"]) and pd.isna(start):
            _add(
                issues,
                "error",
                "invalid_job_start_date",
                "job",
                row["job_id"],
                "start_date",
                f"Could not parse start date {row['source_start_date']!r}.",
            )
            continue
        if not _blank(row["source_end_date"]) and pd.isna(end):
            _add(
                issues,
                "error",
                "invalid_job_end_date",
                "job",
                row["job_id"],
                "end_date",
                f"Could not parse end date {row['source_end_date']!r}.",
            )
        if pd.isna(start):
            continue
        if start.date() > as_of_date:
            _add(
                issues,
                "error",
                "future_job_start",
                "job",
                row["job_id"],
                "start_date",
                f"Job starts after the as-of date {as_of_date.isoformat()}.",
            )
        if not pd.isna(end) and end < start:
            _add(
                issues,
                "error",
                "job_end_before_start",
                "job",
                row["job_id"],
                "end_date",
                "Job end date is before its start date.",
            )
    for professional_id, group in jobs.dropna(subset=["start_date"]).groupby("professional_id"):
        ordered = group.sort_values(["start_date", "job_id"])
        previous_end = None
        previous_job_id = None
        for _, row in ordered.iterrows():
            effective_end = row["end_date"]
            if pd.isna(effective_end):
                effective_end = pd.Timestamp(as_of_date)
            if previous_end is not None and row["start_date"] <= previous_end:
                _add(
                    issues,
                    "warning",
                    "overlapping_job_periods",
                    "professional",
                    professional_id,
                    "jobs",
                    f"Jobs {previous_job_id} and {row['job_id']} overlap; concurrent roles may be valid.",
                )
            if previous_end is None or effective_end > previous_end:
                previous_end = effective_end
                previous_job_id = row["job_id"]

    certifications = frames["fact_professional_certification"]
    for _, row in certifications.iterrows():
        earned = row["date_earned"]
        expiration = row["expiration_date"]
        if not _blank(row["source_date_earned"]) and pd.isna(earned):
            _add(
                issues,
                "error",
                "invalid_certification_earned_date",
                "certification",
                row["certification_id"],
                "date_earned",
                f"Could not parse earned date {row['source_date_earned']!r}.",
            )
            continue
        if not _blank(row["source_expiration_date"]) and pd.isna(expiration):
            _add(
                issues,
                "error",
                "invalid_certification_expiration_date",
                "certification",
                row["certification_id"],
                "expiration_date",
                f"Could not parse expiration date {row['source_expiration_date']!r}.",
            )
        if pd.isna(earned):
            continue
        if not pd.isna(expiration) and expiration < earned:
            _add(
                issues,
                "error",
                "certification_expiration_before_earned",
                "certification",
                row["certification_id"],
                "expiration_date",
                "Certification expiration date is before the earned date.",
            )

    education = frames["fact_education"]
    for _, row in education.iterrows():
        if not _blank(row["source_graduation_date"]) and pd.isna(row["graduation_date"]):
            _add(
                issues,
                "error",
                "invalid_graduation_date",
                "education",
                row["education_id"],
                "graduation_date",
                f"Could not parse graduation date {row['source_graduation_date']!r}.",
            )


def _experience_checks(frames: dict[str, pd.DataFrame], issues: list[QualityIssue]) -> None:
    professionals = frames["dim_professional"]
    experience_by_professional = professionals.set_index("professional_id")[
        "years_experience"
    ].to_dict()

    for _, row in professionals.iterrows():
        years = row["years_experience"]
        if pd.isna(years):
            continue
        if years < 0 or years > 60:
            _add(
                issues,
                "error",
                "professional_experience_outlier",
                "professional",
                row["professional_id"],
                "years_experience",
                f"Reported experience of {years} years is outside the plausible 0-60 range.",
            )

    skills = frames["bridge_professional_skill"]
    for _, row in skills.iterrows():
        skill_years = row["years_experience"]
        professional_years = experience_by_professional.get(row["professional_id"])
        if pd.isna(skill_years):
            continue
        if skill_years < 0 or skill_years > 60:
            _add(
                issues,
                "error",
                "skill_experience_outlier",
                "professional_skill",
                f"{row['professional_id']}/{row['skill_id']}",
                "years_experience",
                f"Skill experience of {skill_years} years is outside the plausible 0-60 range.",
            )
        elif professional_years is not None and skill_years > professional_years + 1:
            _add(
                issues,
                "warning",
                "skill_experience_exceeds_career",
                "professional_skill",
                f"{row['professional_id']}/{row['skill_id']}",
                "years_experience",
                f"Skill experience ({skill_years}) exceeds reported career experience ({professional_years}).",
            )

    jobs = frames["fact_job_history"]
    for _, row in jobs.iterrows():
        salary_band = row["salary_band"]
        if pd.isna(salary_band) or salary_band < 1 or salary_band > 10:
            _add(
                issues,
                "error",
                "salary_band_outlier",
                "job",
                row["job_id"],
                "salary_band",
                "Salary band must be an integer from 1 through 10.",
            )

def _reconciliation_checks(
    frames: dict[str, pd.DataFrame],
    expected_counts: dict[str, int],
    issues: list[QualityIssue],
) -> None:
    mappings = {
        "professionals": "dim_professional",
        "jobs": "fact_job_history",
        "skills": "bridge_professional_skill",
        "certifications": "fact_professional_certification",
        "education": "fact_education",
    }
    for source_name, table_name in mappings.items():
        expected = expected_counts[source_name]
        actual = len(frames[table_name])
        if expected != actual:
            _add(
                issues,
                "error",
                "source_to_target_reconciliation",
                table_name,
                table_name,
                "row_count",
                f"Source has {expected} {source_name} rows but {table_name} has {actual}.",
            )


def validate_frames(
    frames: dict[str, pd.DataFrame],
    expected_counts: dict[str, int],
    as_of_date: date,
) -> pd.DataFrame:
    issues: list[QualityIssue] = []
    _duplicate_checks(frames, issues)
    _required_field_checks(frames, issues)
    _date_checks(frames, issues, as_of_date)
    _experience_checks(frames, issues)
    _reconciliation_checks(frames, expected_counts, issues)
    return pd.DataFrame((issue.__dict__ for issue in issues), columns=ISSUE_COLUMNS)


def error_messages(issues: pd.DataFrame) -> Iterable[str]:
    if issues.empty:
        return []
    errors = issues[issues["severity"] == "error"]
    return (
        f"{row.check_name} [{row.entity_type}:{row.entity_id}] {row.message}"
        for row in errors.itertuples()
    )
