from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb


def refresh_derived_metrics(target_path: Path, as_of_date: date) -> None:
    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_job_transitions AS
            WITH ordered AS (
                SELECT
                    professional_id,
                    job_id,
                    industry_id,
                    start_date,
                    salary_band,
                    ROW_NUMBER() OVER (
                        PARTITION BY professional_id ORDER BY start_date, job_id
                    ) AS job_sequence,
                    LAG(job_id) OVER (
                        PARTITION BY professional_id ORDER BY start_date, job_id
                    ) AS from_job_id,
                    LAG(industry_id) OVER (
                        PARTITION BY professional_id ORDER BY start_date, job_id
                    ) AS from_industry_id,
                    LAG(start_date) OVER (
                        PARTITION BY professional_id ORDER BY start_date, job_id
                    ) AS prior_start_date,
                    LAG(salary_band) OVER (
                        PARTITION BY professional_id ORDER BY start_date, job_id
                    ) AS from_salary_band
                FROM fact_job_history
                WHERE start_date IS NOT NULL
            )
            SELECT
                professional_id,
                job_sequence - 1 AS transition_number,
                from_job_id,
                job_id AS to_job_id,
                from_industry_id,
                industry_id AS to_industry_id,
                CAST(start_date AS DATE) AS transition_date,
                DATE_DIFF('month', prior_start_date, start_date) AS months_since_prior_job_start,
                from_salary_band,
                salary_band AS to_salary_band,
                salary_band - from_salary_band AS salary_band_change,
                salary_band > from_salary_band AS is_salary_band_increase,
                industry_id <> from_industry_id AS is_industry_change
            FROM ordered
            WHERE job_sequence > 1
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_professional_career_metrics AS
            WITH job_summary AS (
                SELECT
                    professional_id,
                    COUNT(*) AS job_count,
                    MIN(CAST(start_date AS DATE)) AS career_start_date,
                    MAX(COALESCE(CAST(end_date AS DATE), CAST(? AS DATE)))
                        AS observed_through_date,
                    ROUND(
                        DATE_DIFF(
                            'day',
                            MIN(CAST(start_date AS DATE)),
                            MAX(COALESCE(CAST(end_date AS DATE), CAST(? AS DATE)))
                        ) / 365.25,
                        2
                    ) AS career_span_years,
                    ROUND(AVG(tenure_months), 1) AS average_job_tenure_months,
                    ARG_MIN(salary_band, start_date) AS starting_salary_band,
                    ARG_MAX(salary_band, start_date) AS latest_salary_band
                FROM fact_job_history
                GROUP BY professional_id
            ),
            transition_summary AS (
                SELECT
                    professional_id,
                    COUNT(*) AS transition_count,
                    SUM(is_salary_band_increase::INTEGER) AS salary_band_increase_count,
                    SUM(is_industry_change::INTEGER) AS industry_change_count,
                    ROUND(
                        AVG(months_since_prior_job_start) FILTER (WHERE is_salary_band_increase), 1
                    ) AS average_months_between_salary_band_increases,
                    MIN(transition_date) FILTER (WHERE is_salary_band_increase)
                        AS first_salary_band_increase_date
                FROM mart_job_transitions
                GROUP BY professional_id
            )
            SELECT
                p.professional_id,
                p.years_experience AS reported_years_experience,
                j.job_count,
                j.career_start_date,
                j.observed_through_date,
                j.career_span_years,
                j.average_job_tenure_months,
                j.starting_salary_band,
                j.latest_salary_band,
                j.latest_salary_band - j.starting_salary_band AS salary_band_growth,
                COALESCE(t.transition_count, 0) AS transition_count,
                COALESCE(t.salary_band_increase_count, 0) AS salary_band_increase_count,
                COALESCE(t.industry_change_count, 0) AS industry_change_count,
                t.average_months_between_salary_band_increases,
                CASE
                    WHEN t.first_salary_band_increase_date IS NULL THEN NULL
                    ELSE DATE_DIFF('month', j.career_start_date, t.first_salary_band_increase_date)
                END AS months_to_first_salary_band_increase
            FROM dim_professional p
            LEFT JOIN job_summary j USING (professional_id)
            LEFT JOIN transition_summary t USING (professional_id)
            """,
            [as_of_date, as_of_date],
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_industry_progression AS
            SELECT
                from_industry.industry_name AS from_industry,
                to_industry.industry_name AS to_industry,
                COUNT(*) AS transition_count,
                ROUND(AVG(t.salary_band_change), 2) AS average_salary_band_change,
                ROUND(AVG(t.is_salary_band_increase::INTEGER), 3) AS upward_transition_rate,
                ROUND(AVG(t.months_since_prior_job_start), 1) AS average_months_between_job_starts
            FROM mart_job_transitions t
            JOIN dim_industry from_industry
                ON t.from_industry_id = from_industry.industry_id
            JOIN dim_industry to_industry
                ON t.to_industry_id = to_industry.industry_id
            GROUP BY 1, 2
            """
        )
