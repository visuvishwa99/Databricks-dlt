# Operations, recovery, performance and cost

## Proposed service objectives

These are planning targets, not measured SLAs. Confirm school-hour coverage and export cadence with owners.

| Signal | Target / action | Evidence |
| --- | --- | --- |
| Source-to-serving latency | p95 <= 30 minutes during agreed business hours | Producer finished_at through SQL audit applied_at; log source/export delay separately |
| Complete exports | 100% of published runs reconcile exactly | ops_export_reconciliation |
| Quarantine ratio | <= 1% over the configured freshness window; otherwise stop serving | Health job counts raw and rejected rows by run |
| Source freshness | At least one OK manifest in 60 minutes, including empty heartbeat | Health task; independent monitor required if scheduled job stops |
| Serving correctness | Zero full-row mismatches | SQL snapshot reconciliation task |
| Recovery | Proposed RTO 2 hours; RPO bounded by durable source export retention | Rehearsed recovery drill, not a platform guarantee |

Current alerts: failed full jobs notify the configured alert email. The schedule starts paused. A failed health task retries twice at one-minute intervals, then prevents write-back. Timeout/retry budgets are in databricks.yml. No-data monitoring must run outside the pipeline too: if the job never starts, its own health task cannot alert. Configure a warehouse alert or external scheduler from sql/monitoring.sql and page the actual on-call route before release. School hours, weekends, holidays and start-of-day freshness need explicit calendar behavior; the default cron is only an example.

## Incident runbook

| Symptom | Diagnose | Recovery / release check |
| --- | --- | --- |
| Missing manifest/files | Compare run folder and manifest with bronze source_file; inspect producer completion | Wait for producer or correct delivery; rerun update; require OK before publication |
| Duplicate manifest | Producer reused folder/run ID or replayed marker | Pause publisher; repair under a reviewed data correction plan; never delete checkpoints blindly |
| Malformed event JSON | Inspect `_corrupt_record` in restricted bronze/quarantine and the source file | Fix producer output, track quarantine threshold, then replay through a new immutable export if needed; malformed manifests still fail ingestion |
| Unknown version/rescued data | Compare actual fields/types with contract and quarantine reasons | Update schema/tests via PR, rehearse migration, replay affected input; do not accept rescued data automatically |
| Conflicting revision | Same _id/update timestamp has different payloads | Producer supplies authoritative revision; historical conflict may require reviewed correction of clean history or a new target, since append-only history retains it |
| Missing school/time zone | Validate federation availability and school metadata | Restore dimension; fix contradictory entries; rerun; measure source load |
| SQL connection fails | Verify private routes, firewall, TLS certificate, secret ACL, driver, dedicated compute | Retry after connectivity repair; never disable certificate validation |
| Partial JDBC stage | Compare audit absence and attempt row counts | Retry job; new attempt isolates leftovers; original serving data remains intact |
| Timeout after SQL commit | Look up run_id in audit | Replay same run ID: procedure is a no-op; reconcile retained published attempt |
| Stale snapshot rejected | Compare snapshot_at and latest audit | Do not force older data over new; verify latest published run; retry intentionally with current source |
| Deletion threshold fails | Investigate missing data, business corrections, or source outage | Review expected removals; explicitly opt in only after agreement; empty snapshot still blocked |
| Serving drift | Compare all fields to recorded attempt; inspect direct SQL writers | Restrict external writes; repair from approved snapshot with a new run ID and audit |

Preserve job/run/update IDs, commit SHA, affected run IDs, count evidence and UTC timestamps in the incident report. Avoid student identifiers or JDBC secrets in alerts. Record detection, customer impact, containment, root cause, recovery proof and follow-up owner. A useful interview story should describe an actual drill as a drill, not as a fictional employer incident.

## Replay and rollback

Code rollback: redeploy a reviewed previous Git commit to the same bundle target; preserve workspace root and deployment identity. Stop the schedule and wait for active runs first. Check bundle plan for deletion/replacement. Code rollback does not restore Delta or SQL data and cannot reverse incompatible schema changes.

Data recovery: retain immutable S3 exports and platform-managed checkpoints. Replay corrected business records with later source updated_at into a new run folder. Never use full refresh as the default repair; bronze explicitly blocks it. For incompatible migrations or corrupt historical state, build a new isolated catalog/schema/target from retained input, validate counts/business totals, then switch consumers under an approved cutover. Keep old state through the rollback window.

SQL recovery: restore from database backup to a separate recovery instance, verify the latest committed audit and staging snapshot, then execute the database team's restoration/cutover procedure. Do not run ad hoc DELETE or restore commands on a live serving database. The runtime login has no direct serving write privilege beyond the procedure. The separate paused cleanup job uses maintenance credentials to remove only unaudited attempts older than seven days; its procedure preserves every audited attempt and takes the publish lock. Activate it only after testing credentials, retention, and alert delivery. Audited staging retention needs a separately reviewed archive policy.

An operator can acknowledge an alert for paging purposes, but an acknowledgement does not change `ops_export_reconciliation`, CDC conflict expectations, or the publication health gate. Resolve the source inconsistency and rerun before publishing. Any exceptional exclusion requires a reviewed, time-limited data contract and independent verification of affected consumers.

## Performance and capacity experiment

Use scripts/generate_export.py in a separate load-test prefix/catalog. Benchmark 10k, 100k, then 1m synthetic events, multiple files, repeat exports and out-of-order updates. Set `--student-cardinality` explicitly (the default is 10k) so a 1m-row test does not accidentally represent only 10k serving keys. Record input bytes, distinct documents, distinct students, file count, update duration, backlog, shuffle/skew, max task time, DBUs, SQL write duration, SQL log growth and reconciliation scan time. Require a stable 30-minute p95 target at expected peak plus agreed headroom before setting a production SLO.

Auto Loader has a default 1,000-file microbatch admission limit. Tune file size and admission limits from measured backlog and processing time; unlimited tiny files can increase discovery/metadata costs. The conflict and export reconciliation views aggregate history and may perform expensive refreshes; inspect the actual refresh plan. Do not promise incremental materialization for every query. Long retention can require a separately maintained control table and producer revision key.

Daily grouping can skew on a large school/product. Inspect Spark UI stages and shuffle metrics before salting or repartitioning. Federation adds source latency and availability dependence; for frequent/large dimensions, ingest a validated dimension snapshot into Delta to decouple production reads. JDBC defaults to four output partitions (allowed range 1-8) to protect SQL Server; compare throughput to server CPU/log/lock pressure before raising it. Full snapshot publication is O(total students) per run; move to versioned change batches with a source/target reconciliation strategy if this exceeds the serving window.

Cost model: daily pipeline cost = sum(DBU usage x contracted rate) + storage/requests + SQL compute + network. Report cost per million source rows and per successful refresh, including retries. Query system.billing.usage for actual usage; no cost figure is asserted here. Tag jobs by project/environment, use triggered execution, pause unused schedules, set budgets in the cloud/platform account, and review retention before lowering it. Deleting checkpoints or raw data to reduce costs can invalidate recovery guarantees.

## Release gates

Run source contract profiling, full S3/Federation/JDBC test, credential rotation, notification delivery, no-run detection, deletion rejection, timeout-after-commit, peak load, recovery drill and BI authorization tests. Capture actual dates/owners/results in evidence.md. Unit/CI/demo success alone does not establish production readiness.
