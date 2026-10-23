# Databricks DLT student-events pipeline

A production-oriented portfolio project: immutable JSON exports to Auto Loader, quality controls and AUTO CDC, school-local gold analytics, and an idempotent SQL Server publication protocol. Deployed synthetic data demonstrates the Databricks path; CI exercises real SQL Server transaction behavior. Production acceptance still requires the actual source, networking, identities, load and recovery tests.

This is a personal project, not work delivered for Lexia Learning or an employer.

## Start here

| Goal | Read / run |
| --- | --- |
| Understand the system and tradeoffs | [Architecture and ADRs](docs/architecture.md) |
| Learn source and consumer semantics | [Data contract](docs/data-contract.md) |
| Deploy into another workspace | [Deployment and CI/CD](docs/deployment.md) |
| Operate, tune, alert and recover | [Operations runbook](docs/operations.md) |
| Prepare infrastructure and access | [Security and infrastructure](docs/security-and-infrastructure.md) |
| Prepare for interviews | [Interview guide: walkthrough, questions, drills and rubric](docs/interview-guide.md) |
| See what was actually verified | [Evidence and release gates](docs/evidence.md) |

## Implemented controls

- Explicit Extended JSON schema, tolerant timestamp parsing, quarantine reason codes and fail/drop/warn expectations.
- Source completeness reconciliation, empty heartbeat handling, freshness and quarantine publication gates.
- SCD1 current documents, source revision conflict detection and timezone-aware business aggregates.
- SQL staging isolated by run/attempt, expected-count checks, duplicate retry handling, transaction lock, stale-snapshot fencing, audit-based replay, guarded deletion and full-field verification.
- Host-independent Databricks bundle with demo/dev/prod targets, production runtime principal, bounded retries/timeouts, failure notification configuration and paused schedules.
- GitHub Actions with pinned tools/actions, Spark tests, SQL Server failure tests, OIDC deployment and environment preflight.
- Synthetic export generator, reusable workspace smoke check, monitoring queries, recovery/rollback procedures and interview exercises.

## Run tests

Use Python 3.11 and Java 17 in a clean virtual environment:

```shell
python -m pip install -r requirements-dev.txt
ruff check .
python -m pytest tests --ignore=tests/integration -q
```

SQL integration tests run in GitHub Actions against an ephemeral SQL Server 2022 instance. The test database is created and removed per test; never point that harness at a production database. See [the workflow](.github/workflows/ci.yml).

## Run the demo

```shell
databricks bundle validate -t demo -p LEXIA --strict
databricks bundle plan -t demo -p LEXIA
databricks bundle deploy -t demo -p LEXIA
databricks bundle run -t demo -p LEXIA student_events_refresh
```

The supplied workspace already contains the synthetic sample. A new deployment must upload the part file before its manifest; commands are in the deployment guide. The demo deliberately omits external SQL Server/Federation and the fixed sample's freshness gate.

- [Live pipeline](https://dbc-7f4188e4-3fd1.cloud.databricks.com/pipelines/070eddb6-9b00-4850-9bea-2a1e83f06755?o=7474652460637329)
- [Successful hardened demo run](https://dbc-7f4188e4-3fd1.cloud.databricks.com/jobs/250557649698066/runs/82045733873178?o=7474652460637329)
- [GitHub Actions](https://github.com/visuvishwa99/Databricks-dlt/actions)

## Verify data and generate exercises

```shell
python scripts/smoke_test.py --warehouse-id YOUR_WAREHOUSE --catalog workspace --schema default --profile LEXIA --expected-current-rows 2
python scripts/generate_export.py --run-id exercise-001 --rows 10000 --rows-per-file 1000
```

The smoke query uses SQL warehouse compute. Generated data belongs in an isolated exercise prefix/schema. Never overwrite an existing export folder. Use a fixed event time and increasing revision for correction/replay experiments.

## Production boundaries

The full dev/prod workflow needs an S3 external location, source contract, federated school table, dedicated JDBC compute, secret scope, SQL migration, service principals and GitHub environment settings. Automatic dev deployment is disabled until the repository variable ENABLE_AUTO_DEV is true; manual deployment still validates all settings. Prod must use main and should have a protected environment. A deployment leaves schedules paused.

The v1 source contract does not support physical source deletes or partial update images. Gold can be partially refreshed when a pipeline fails; direct BI consumers needing atomic publication require a separate approved snapshot layer. File counts do not replace source reconciliation or checksums. SLOs, peak capacity, regulatory controls and disaster recovery are release gates, not claims established by the small demo.

For interviews, start with the guide, run the drills, and explain the verified results and limits in your own words.
