# Interview preparation through this project

## How to describe the work honestly

This is a personal portfolio project implemented and demonstrated in a Databricks workspace. It is not a system delivered for Lexia Learning or another employer. State which paths were actually exercised: synthetic data through the deployed pipeline, local/CI Spark tests and isolated SQL Server transaction tests. Real S3 identity, Federation, production JDBC networking, peak load, DR and governance still require environment acceptance evidence. No preparation project can guarantee an interview result; being able to explain and modify it is the goal.

## Your 60-second explanation

“I built a Databricks pipeline for student activity exports. The source produces immutable JSON files and a final manifest. Auto Loader retains raw events and export metadata, silver validates Extended JSON and uses AUTO CDC to maintain current document state, and gold calculates school-local daily completions and student progress. I reconcile manifests against received files and rows before publishing. The SQL Server publisher uses isolated staging attempts, a locked transaction and a run audit so retries cannot partially replace serving data. I packaged the project in an environment-configurable Databricks bundle, added GitHub CI with Spark and SQL failure tests, and demonstrated the synthetic pipeline in a real workspace.”

Then name the actual evidence in evidence.md and explicitly identify the external integrations you have not validated. Do not add invented row volumes, latency savings, cost savings or production incidents.

## Your five-minute walkthrough

1. Business requirement and grain: what a completed document means, who reads each output, current state versus event history.
2. Producer boundary: incremental exports, source timestamps, immutable keys and manifest-last. Explain what is outside your control.
3. Bronze/silver/gold: distinguish physical file delivery, accepted occurrences, current documents and business aggregates.
4. Correctness: out-of-order updates, same-timestamp conflicts, quarantine, completeness and source freshness.
5. SQL boundary: show the crash timeline and why staging append + audit is safer than overwrite + blind merge.
6. Operations and rollout: bundle variables, service principals, GitHub environments, paused schedules, alerts, rollback and release gates.
7. Evidence and next steps: show the successful job and CI tests; acknowledge measured versus proposed targets.

## Questions you must answer without reading

| Question | Strong answer and follow-up |
| --- | --- |
| Why Databricks rather than plain Spark? | Managed pipeline dependency planning, expectation metrics, managed streaming state and UC integration reduce orchestration code. Trade off platform coupling and compute cost against running equivalent Spark jobs yourself. |
| Why triggered rather than continuous? | Upstream delivery is periodic files; continuous compute cannot remove export latency. Revisit both producer and transport for sub-minute freshness. |
| Is this a source CDC feed? | It is exported full document images with source update timestamps, not a DocumentDB change log. Physical deletes and intermediate updates may be absent. |
| What is exactly once here? | Distinguish checkpointed file ingestion, per-document SCD1 state, JDBC staging retries and atomic SQL publication. There is no transaction spanning both storage systems. |
| What if the same document appears in two files? | Bronze preserves both physical occurrences. Silver AUTO CDC converges on _id and sequence. Manifests count physical rows, so deduplicated silver counts must not be compared directly with manifest record_count. |
| Why SCD1 rather than SCD2? | Current progress is the serving requirement. SCD2 would preserve source revisions and enable historical/as-of analyses, with more storage and consumer logic. Bronze is replay history but not a convenient SCD2 dimension. |
| What if an older update arrives later? | Source updated_at is the first sequence field; later delivery cannot replace a higher source revision. Delivery metadata is only a tie-breaker. |
| What if source timestamps tie with different data? | The producer violated its revision contract. Conflict detection fails the update. A content hash can detect disagreement but cannot establish causal order. Ask for a monotonic source revision. |
| What if updated_at is absent? | V1 falls back to attempt time for immutable records. It is insufficient for mutable sources; require authoritative revision data before onboarding them. |
| Why not dropDuplicates(_id)? | It can keep the wrong revision and ignores correction order. Stateful streaming dedup also introduces retention/watermark decisions. |
| Where is the watermark? | There is no event-time windowed dedup aggregation here. Do not invent a watermark for AUTO CDC. Understand each operator's managed state and source history retention separately. |
| What happens to malformed input? | Syntactically broken JSON fails file ingestion. Parsed but invalid fields are quarantined; missing identity/unknown schema version fails the clean flow. Explain why contract errors can stop publication. |
| Can rescued data enter gold? | No: _rescued_data must be null for clean rows. Unknown fields trigger review rather than silent schema acceptance. |
| Why require a manifest? | It provides an expected completeness boundary. It cannot prove the exporter read every source record or that equal row counts contain the correct contents. |
| Are marker-last exports atomically ingested? | No. Discovery can span multiple microbatches. Health blocks SQL publication until all observed runs reconcile. |
| What is an empty export? | A heartbeat manifest with zero rows/files and no parts. An empty part file cannot be counted from row metadata, so this contract forbids it. |
| Why retain quarantine if you already drop invalid rows? | Dropping protects consumers; quarantine preserves evidence and supports repair. Include reason codes and protect its sensitive payloads. |
| How are local dates correct around DST? | UTC event instants are converted with each school's IANA timezone before to_date. Show the New York boundary test. Fixed UTC offsets are insufficient. |
| What is the gold completion grain? | Count completed current documents, grouped by product/school/grade/local date/change type. It is not necessarily unique completed units; confirm source semantics. |
| Can dimensions restate history? | Yes. A current school dimension can change past name/timezone attribution. Event-effective dimension history is required for strict historical reporting. |
| Why use Federation for schools? | Convenient current reference lookup without a separate ingest job. Source availability/load and network latency are tradeoffs; materialize a validated Delta snapshot if necessary. |
| What if pipeline update fails after bronze commits? | Tables do not share one all-or-nothing transaction. The downstream job gate blocks SQL, but direct BI reads require a separate publication layer for strict consistent visibility. |
| What if JDBC fails after writing half the rows? | No publish procedure runs. The attempt remains orphaned; serving stays unchanged. Retry uses a new attempt and verifies expected rows before commit. |
| What if an executor writes the same rows twice? | SQL takes DISTINCT within that attempt, rejects conflicting business keys, and checks the deduplicated count against the captured snapshot. |
| What if the driver loses connection after commit? | The run audit commits with serving data. Replaying the same run ID is a no-op, followed by reconciliation of the original recorded attempt. |
| How do you stop an old job overwriting new serving data? | Job concurrency plus a database application lock and snapshot-time fence. A source monotonic snapshot revision would avoid clock assumptions. |
| Why not MERGE directly from Spark? | Spark partitions commit independently. A staged, validated SQL transaction creates a database publication boundary and audit; the chosen procedure uses UPDATE/INSERT/DELETE under one lock. |
| Why compare all fields after publishing? | Equal counts or completion totals can hide changed school IDs, latest units, timestamps or duplicate/missing keys. Compare multisets in both directions against the exact published snapshot. |
| Why are deletes off by default? | Missing rows can indicate incomplete input. Explicit opt-in, a fraction bound and an empty-snapshot guard prevent accidental mass removal. Source physical deletions remain unsupported by v1. |
| How do you recover from incompatible schema changes? | Use a versioned source/target, replay retained raw input, validate business totals and cut over consumers. Redeploying old code alone does not restore data/checkpoints. |
| How does one repo deploy to different workspaces? | Host and dependency variables come from separate GitHub environments; the same bundle target structure is reused. Catalogs, prefixes, SQL databases and identities must be isolated too. |
| Why service principals and OIDC? | Automated deployment receives short-lived identity tokens scoped to repo/environment. Separate deploy/run identities and least privilege reduce reliance on personal credentials. |
| How would you handle ten times more traffic? | Measure file count/size, backlog, shuffle skew, conflict-history scans, Federation and SQL log pressure. Full snapshot write-back may become the bottleneck before ingestion. |
| What would you alert on? | Failures, missing scheduled runs, no recent OK heartbeat, completeness mismatch, excessive quarantine and serving drift. The job cannot detect its own failure to start. |
| What is your SLA? | Proposed objectives are documented, not measured production commitments. Explain the needed load tests, latency instrumentation and error budget before agreeing one. |
| How would privacy deletion work? | V1 is incomplete for that requirement. Design coordinated deletion/tombstone handling across raw/history/quarantine/gold/SQL/backups/caches under approved policy; do not claim compliance from masking alone. |

## Failure drills you can demonstrate

Use only isolated test data and targets. Never inject a conflicting revision into the shared demo unless you also have an approved historical-state repair plan: the conflict check retains accepted history.

| Drill | Action | Expected evidence |
| --- | --- | --- |
| Partial export | Publish marker with record_count larger than received rows | MISSING_RECORDS; publisher blocked |
| Empty heartbeat | Generate --rows 0 | Reconciliation OK with no source rows |
| Invalid student | Generate --quarantine-every 2 | Reason codes, fewer clean rows; full job blocked above threshold |
| Repeated export | Same fixed --event-time/revision and IDs in a new run folder | Bronze increases, current silver cardinality stays stable |
| Late correction | Increase --revision in a new folder; then send older revision | Current state retains highest source update time |
| Same-time conflict | Alter business field without changing updated_at in isolated target | Conflict gate fails; do not silently choose a winner |
| Partial SQL stage | Run test_bad_staging_rolls_back | No serving/audit change |
| Lost commit response | Run test_commit_and_replay_are_idempotent | One audit row, one logical publication |
| Unexpected removal | Run test_delete_requires_opt_in_and_threshold | Failure until explicit policy permits bounded removal |
| Rollback | Compare a prior commit's bundle plan in an isolated target | Explain code rollback separately from data restoration |

## Seven-day practice plan

| Day | Work | Pass criterion |
| --- | --- | --- |
| 1 | Draw the architecture from memory; read data-contract.md | State every grain/key and distinguish export rows from current documents |
| 2 | Trace one valid, one invalid and one late event through code | Explain every table affected and its expected row count |
| 3 | Run Spark tests; add a new edge case yourself | Predict the result before running it and explain null behavior |
| 4 | Read SQL procedure and integration tests; draw crash points | Explain before-stage, partial-stage, before-commit and after-commit recovery |
| 5 | Explain bundle targets, OIDC, runtime identity and promotion | Show how to change workspaces without editing business logic |
| 6 | Conduct a failure drill and write a short incident report | Evidence includes run IDs, root cause, recovery and prevention |
| 7 | Record a 45-minute mock interview and review gaps | Answer tradeoffs and limits without relying on memorized product names |

## Mock interview and scoring

Spend 5 minutes on requirements and volumes; 10 on architecture/keys; 10 on correctness/failure timelines; 10 on operations/security/cost; 10 on code and evidence. Score each area 0-4: 0 cannot explain, 1 repeats terminology, 2 explains normal behavior, 3 handles failures with code evidence, 4 compares alternatives and states limits. Aim for at least 3 in every area; a high total does not compensate for misunderstanding retries or data grain.

Coding exercise: add a test for missing school metadata and explain whether the pipeline should stop or quarantine. SQL exercise: show why equal row counts do not establish equality. Design exercise: change the requirement to 30-second freshness plus source deletes and identify which assumptions, contracts, operators, tests and cost controls must change. Capacity exercise: estimate cost and recovery time using measured input size and throughput, clearly labeling assumptions.

## Evidence to show during an interview

Open README.md, the live pipeline graph, one successful job run, the GitHub test run, a quarantine test, and the SQL procedure with its replay test. Use the actual numbers in evidence.md. Explain the import-path defect found during the initial deployment as a real debugging example: local imports passed, deployed pipeline execution used a different path, explicit bundle.sourcePath fixed it, and the workspace run verified the fix. Present later failure scenarios as tested drills, not invented incidents.
