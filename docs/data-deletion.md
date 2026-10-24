# Student deletion design and release gate

The current v1 producer contract has no source tombstone or authoritative deletion sequence. No automated student erasure command is provided, because deleting a row from a current table would allow a retained export or replay to recreate it. A privacy deletion request requires an owner-approved plan for all copies and retention obligations.

## Required design before implementation

1. Resolve the request to the stable student identity across products, schools, source documents and SQL serving keys. Record who approved the scope without putting student identifiers in CI logs or public documentation.
2. Stop new source exports for that identity or publish an authoritative deletion/tombstone revision. Add a replay suppression mechanism that remains effective when old immutable files are reprocessed.
3. Inventory S3 source exports, bronze, clean/quarantine silver, CDC state, gold, SQL staging, serving, audit, checkpoints, backups, derived reports and downstream copies. Define retention or deletion treatment for each with its owner.
4. Implement a versioned producer and CDC contract for tombstones. Test deletion after normal ingestion, delayed files, backfill, full rebuild and restore. Do not infer source deletion from absence in an incremental export.
5. Verify absence or approved restricted retention in each system, and keep an access-controlled deletion audit. Reconcile SQL serving through the publication protocol; do not directly edit it with the runtime identity.

The schema, producer behavior, backup controls and legal retention decision are external prerequisites. Synthetic one-student tests can validate code after that contract exists; they cannot establish an end-to-end deletion lifecycle on their own.
