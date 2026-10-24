# Verification evidence and production release gates

Evidence updated: October 23, 2026 America/Phoenix (October 24 UTC). Results apply to the synthetic demo and disposable CI database, not a production customer workload.

The Unreleased code changes passed the hosted check and SQL integration jobs at commit d13e02c. Malformed-record quarantine was exercised in the demo workspace. The SQL Server cleanup and unchanged-row behavior were exercised only in the disposable CI database; the maintenance job remains paused in full environments.

## Verified

| Area | Evidence |
| --- | --- |
| Databricks access | Workspace 7474652460637329, authenticated LEXIA CLI profile |
| Bundle update | Demo plan: 0 add, 1 change, 0 delete; existing job/volume preserved |
| Hardened pipeline execution | [Job run 82045733873178](https://dbc-7f4188e4-3fd1.cloud.databricks.com/jobs/250557649698066/runs/82045733873178?o=7474652460637329), TERMINATED SUCCESS |
| Earlier baseline execution | Run 167012210378212, two bronze/current silver/daily gold rows, zero quarantine, one OK manifest |
| Workspace data checks | [Machine-readable smoke evidence](demo-smoke-evidence.json), produced by scripts/smoke_test.py |
| Hosted Spark and SQL tests | [Successful CI run 39910065820](https://github.com/visuvishwa99/Databricks-dlt/actions/runs/39910065820): 22 application/contract/generator tests and 8 SQL transaction tests at commit 88281bb |
| Local Spark | 20 tests passed with local Spark 4.0.1; CI uses pinned Spark 3.5.9 on Python 3.11/Java 17 |
| SQL replay and failure behavior | Real SQL Server 2022 service: same-run replay, duplicate writes, partial/empty/conflicting stage, stale snapshot, bounded deletion, competing publishers |
| Current PR CI | [Run 40048902266](https://github.com/visuvishwa99/Databricks-dlt/actions/runs/40048902266), commit d13e02c: check and SQL integration jobs both passed |
| Updated demo execution | [Job run 702650131595364](https://dbc-7f4188e4-3fd1.cloud.databricks.com/jobs/250557649698066/runs/702650131595364?o=7474652460637329), TERMINATED SUCCESS after parser update |
| Malformed-input drill | [Job run 829586113643927](https://dbc-7f4188e4-3fd1.cloud.databricks.com/jobs/250557649698066/runs/829586113643927?o=7474652460637329), TERMINATED SUCCESS; [machine-readable result](malformed-drill-evidence.json): four bronze, two current silver, two gold, two quarantined, zero incomplete exports/conflicts. The reason query distinguished `valid_change_type` from `no_corrupt_record`. |

The first hosted lint run failed because the local and hosted Ruff rules differed. Explicit repository rules, pinned tools and import/date fixes resolved it; the subsequent CI run passed. The initial baseline Databricks run failed on module imports; explicit bundle.sourcePath loading fixed the deployed execution context. These are actual debugging examples, not production incidents.

## Required before real production traffic

| Gate | Status | Acceptance evidence required |
| --- | --- | --- |
| Source export completeness and revision semantics | Pending source owner | Profile real documents, source-to-file counts, immutable folder protocol, authoritative update timestamps |
| AWS identity and external location | Pending platform | Read canary as runtime principal, KMS/file-event permissions, denied out-of-scope access |
| Federation and school history semantics | Pending source/platform | Real dimension contract, network/source-load test, missing/conflicting school behavior |
| Production JDBC route and identity | Pending database/platform | TLS-validated connection from dedicated Databricks runtime, least privilege, secret rotation |
| Full end-to-end dev run | Pending external prerequisites | Fresh S3 export through SQL serving; full-field reconciliation and run audit |
| Independent alerts and notification delivery | Pending operations | Failed-run notification, missing-job/no-data page, holiday/business-hour handling |
| GitHub environment trust and production protection | Pending account/repository configuration | Dev/Prod OIDC policies, approvals, main protection, runtime permissions, configured variables |
| Peak throughput/cost | Pending benchmark | Representative volume, skew, backlog drain, p95 latency, DBUs/SQL resource measurements |
| Recovery and rollback | Pending drill | Restore/replay/cutover test with measured RTO/RPO and retained source/checkpoint data |
| Student-data governance and delete lifecycle | Pending organization design | Mask/row-access tests, retention/access review and all-copy deletion procedure |
| Atomic BI publication if required | Pending consumer requirement | Versioned approved gold snapshot/view and failed-update visibility test |

Do not mark a gate complete based only on code review or an unrelated demo. Add the execution ID, date, principal/environment and observed result when it has been exercised. Keep the interview narrative aligned with this record.
