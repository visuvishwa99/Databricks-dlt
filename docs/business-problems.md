# Simulated business scenarios

These cases use synthetic data and hypothetical incidents. They illustrate design decisions in this portfolio project; they are **not accounts of work performed for Lexia Learning or a customer**. The [evidence record](evidence.md) distinguishes tests and workspace runs from unperformed drills.

## School-local completion reporting

**Scenario.** A completed activity at 9:30 PM in Los Angeles is dated the next day in UTC. Grouping by the UTC date shifts it into the wrong school day. A fixed UTC offset also fails when daylight saving time changes.

**Implemented design.** Events retain UTC timestamps. The school dimension supplies an IANA time zone, and `gold_daily_completions` converts the timestamp before deriving `school_day`. A local Spark test exercises a daylight saving boundary. The real SIS Federation contract and historical school time-zone behavior still require acceptance testing.

**Interview explanation.** "I modeled reporting dates in each school's time zone while retaining UTC for storage and ordering. The synthetic test crosses a DST boundary; a production rollout would also validate the actual school dimension and late corrections."

## Atomic teacher-facing progress

**Scenario.** Direct Spark partition writes to SQL Server can leave a serving table partially updated if a task fails. Retrying after a successful commit can write again; a delayed older run can replace newer progress.

**Implemented design.** Spark stages a complete gold snapshot under a run ID and attempt ID. A SQL Server stored procedure validates counts and keys, takes a transaction-owned application lock, rejects stale snapshots and unapproved deletes, and updates serving and audit data in one transaction. A separate task compares every serving field with the audited staging attempt. CI exercises these database behaviors against a disposable SQL Server 2022 instance. The end-to-end publish from a real Databricks workspace to an external SQL Server has not yet been verified.

**Interview explanation.** "The SQL transaction is the publication boundary. Staging can be retried; the audited publish is idempotent per run, and serving remains unchanged when validation fails. The SQL behavior is integration-tested, while network and workload acceptance are still release gates."

## Drills to run before claiming an incident outcome

| Hypothetical condition | Existing control | Evidence still needed |
| --- | --- | --- |
| Partial exporter run | Manifest and loaded file/row reconciliation blocks publication | Workspace drill with part files arriving late |
| Offline, out-of-order revisions | AUTO CDC orders by source update time; conflict gate rejects ambiguous revisions | Workspace correction and same-time conflict runs |
| Malformed event line | Explicit corrupt-record capture and quarantine | Demo update with malformed synthetic input |
| Back-to-school volume spike | Serverless triggered pipeline and bounded file admission | Representative source/cardinality benchmarks and cost |
| School transfer | Latest event school in progress; per-event school in daily counts | Real roster/history semantics and transfer drill |
| Missing school time zone | Gold expectation fails the update | Federation failure drill and alert delivery |
| SQL Server outage | Complete snapshot on next successful run | End-to-end timeout and recovery run |
| Exporter stops | Manifest freshness gate when the job runs | Independent no-run/no-data alert and delivery test |

Do not present hypothetical dates, counts, customer impact, or remediation as observed production history.
