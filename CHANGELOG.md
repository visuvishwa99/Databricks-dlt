# Changelog

## v0.1.0 — 2026-10-23

Synthetic portfolio release; not a production certification.

- Migrate all pipeline definitions to `pyspark.pipelines` and verify the workspace refresh.
- Review historical quarantine by source-bound content fingerprint; retain fail-closed completeness/conflict gates.
- Retire SQL Server from active CI and deployment dependencies.
- Add isolated 100k/1M input benchmarks, governance template, local dashboard and cloud heartbeat monitor.

- Add PostgreSQL transactional publication, reconciliation, bounded staging retention, restricted roles, secret provisioning and outbound local end-to-end runner.
- Verify Databricks-to-local-PostgreSQL demo and add PostgreSQL integration CI.
- Document PowerShell/Git Bash secret setup and current deployment boundaries.

- Compare SQL serving text by bytes and length so case-only changes, trailing spaces, and NULL transitions publish correctly on case-insensitive databases.
- Preserve malformed event JSON in bronze and route it to quarantine with an explicit reason.
- Avoid SQL Server writes for unchanged serving rows and add a bounded cleanup procedure for old, unaudited staging attempts.
- Add a separate paused maintenance job and clarify release evidence, simulated scenarios, and deletion boundaries.
- Run CI checks for documentation and repository policy changes.

The Free Edition demo uses the verified outbound local PostgreSQL runner. Direct cloud JDBC, production governance, delivery-confirmed alerts, real source/Federation and production capacity remain separate acceptance gates.
