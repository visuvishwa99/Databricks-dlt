# Producer and consumer contracts

## Producer v1

Use newline-delimited JSON, UTF-8, one complete Extended JSON object per line. The sample directory is the executable fixture. The bucket/prefix is environment-specific.

```text
student_events/run_date=YYYY-MM-DD/run_id=<unique-id>/
  part-00000.json
  part-00001.json
  manifest.json   # publish last after every part is durable
```

run_id is globally unique within a source prefix, not a retry counter. Never overwrite a completed folder, mutate a previously observed key, or publish a second manifest. Transport retries reuse the same immutable objects. Correcting business data uses a new export folder and a strictly later source updated_at. Replaying unchanged documents must retain source timestamps.

| Field | Rule |
| --- | --- |
| _id.$oid | Nonempty globally unique Mongo document identity |
| schema_version | Integer 1; incompatible change requires coordinated migration |
| product_name | Nonempty product identity |
| student_id | Nonempty synthetic/pseudonymous student identity |
| school_id, grade_level | Required; school must resolve to one consistent dimension row |
| attempt_timestamp.$date | ISO timestamp with explicit UTC/Z or offset, parseable, no more than five minutes ahead |
| updated_at.$date | Source revision time >= attempt time and <= now + five minutes; absent falls back to attempt time for immutable v1 documents only |
| is_completed | Boolean; semantics must be confirmed by product owner |
| change_type | ActivityStarted, ActivityIn_Progress, ActivityCompleted, LevelCompleted, UnitCompleted, StrandCompleted |
| unit_id, level_id, strand_id, activity_id, session_id | Nullable source attributes |
| unexpected fields/types | Rescue and quarantine; not silently accepted |
| malformed event JSON | Preserve the raw line in `_corrupt_record` and quarantine; a malformed manifest fails ingestion |

One (_id, updated_at) must identify one business payload. Timestamp precision must distinguish all meaningful updates. A source monotonic revision is required before adopting a producer that can emit multiple changes at the same timestamp. The current implementation detects conflicts rather than selecting an arbitrary winner. Partial update documents are unsupported: each exported record must be a complete current document image.

Manifest fields: run_id, schema_version=1, started_at, finished_at, file_count, record_count, max_event_ts. Counts refer to physical input rows, not accepted or deduplicated rows. Duplicate documents still count as received rows. A true no-data heartbeat uses file_count=0 and record_count=0 and no part files. Empty part files are unsupported because row-level metadata cannot count a file with no rows. Source timestamps must be UTC, started_at <= finished_at, and marker run_id must match its folder. max_event_ts is descriptive today; cross-checking it and file hashes is an additional producer verification gate, not implemented evidence.

Manifest-last is not atomic visibility: Auto Loader may discover files before the marker or discover only part of a run in one update. Health publication blocks until reconciliation is OK. Count equality detects many delivery errors, but cannot prove byte identity or correct source content. For stronger assurance, add per-file checksums and source snapshot reconciliation to the producer protocol.

## Schema evolution

Additive unknown fields are rescued and quarantined until reviewed. Update explicit schema, transformations, tests and consumer contracts together. Incompatible changes use a new versioned ingestion path/target and a dual-run comparison. A normal bundle rollback does not reverse table data or checkpoint schema changes. Do not mutate an existing CDC key/sequence shape without a migration plan and separate target.

## Consumers

SQL serving key is (product_name, student_id). Field types are declared in sql/sql_server_setup.sql. String widths must be confirmed against real source profiling; oversized data fails publication instead of truncating silently. UTC timestamps are stored as datetime2(6). An empty snapshot fails closed. Removals require an explicit reviewed run with --allow-deletes and a bounded --max-delete-fraction; the bundle does not enable deletion by default.

BI daily counts use school-local dates with IANA time zones. They describe the current corrected view, not an immutable historical ledger. Strict last-successful-update visibility is a separate publishing requirement; direct gold reads can see partial pipeline progress. SQL serving offers its own transaction boundary.

## Ownership before go-live

Assign a named owner for the exporter, source timestamps, schema notices, school dimension, source-to-file counts, deletion requests, pipeline on-call and SQL serving. Agree export cadence, outage behavior, source retention, replay window and schema-change lead time. Unknown owner/contact fields are release blockers, not implied production agreements.
