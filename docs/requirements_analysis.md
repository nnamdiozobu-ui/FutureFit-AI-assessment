# Requirements analysis

## Questions for Customer Support

1. When you say career growth, which outcome matters most: higher pay, a more senior role, staying in a target industry, or finding the next job faster?
2. Who will use this analysis, and what comparisons would be most useful? For example, should we compare people in the same industry, role, experience range, or location?
3. How should we treat career breaks, overlapping jobs, contract work, and lateral moves when measuring progression?

## Metrics I would start with

### Salary-band growth

I would compare each person's latest salary band with their starting band and show the change alongside the number of career years observed. Salary band is not the same as actual compensation, but it is the only consistent advancement measure in this dataset.

### Time to advancement

I would measure the number of months from the first observed job to the first salary-band increase. I would also calculate the average time between later increases. This could help Customer Support identify people whose progression is slower or faster than a comparable group.

### Outcome of industry changes

For moves between industries, I would calculate how often the move led to a higher salary band and the average size of the change. The transition count must be shown with the result so small groups are not presented as reliable trends.

The supplied sample has no cross-industry job transitions, so it cannot answer this question yet. I included the calculation because it would be useful once production data is available.

## How the model supports the analysis

The job-history table stores one row per professional and job, including the company, industry, role, dates, and salary band. Sorting those jobs by start date lets me compare each job with the one before it.

From that history, the pipeline creates a job-transition table and a career summary for each professional. Those outputs contain salary-band changes, tenure, company and industry changes, and time to advancement. Separate skill, education, and certification tables make it possible to compare those characteristics with career outcomes without repeating the job records.

I did not assign seniority levels based on job titles. Titles are free text, so that would require an agreed role taxonomy from the business.

## Additional data I would request

- Applications, interviews, offers, accepted jobs, and recommendation activity so we can measure whether FutureFit's guidance led to an outcome.
- Location, currency, employment type, standardized occupation, and salary ranges so comparisons are fair across different markets.
- Source timestamps, company identifiers, role levels, and career-break or concurrent-job indicators to improve incremental loading and reduce ambiguity.

## Assumptions

- A higher salary band represents career advancement, and bands are comparable across companies and industries.
- Jobs are ordered by start date.
- Overlapping jobs are flagged for review but are not automatically considered incorrect because someone may legitimately hold two jobs at once.
- A missing job end date means the job is current as of the selected snapshot date.
- Because the file has no extract timestamp, I used 2023-02-28, the latest completed-job end date, for the sample run.
- The supplied file is named `full-professionals-json.json`, although the assignment refers to `professionals_nested.json`.
- Ten synthetic professionals are enough to demonstrate the pipeline but not enough to support real career recommendations.
