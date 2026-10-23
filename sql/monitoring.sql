-- Replace catalog/schema once for the target environment. These are warehouse queries.
-- Alert 1: any incomplete export older than grace, including files with no manifest.
SELECT run_id, status, loaded_records, record_count, loaded_files, file_count
FROM workspace.default.ops_export_reconciliation
WHERE status <> 'OK'
  AND coalesce(finished_at, first_exported_at) < current_timestamp() - INTERVAL 30 MINUTES;

-- Alert 2: no successful heartbeat. Configure business hours externally.
SELECT CASE WHEN max(finished_at) IS NULL
    OR max(finished_at) < current_timestamp() - INTERVAL 60 MINUTES
    THEN 1 ELSE 0 END AS stale_export
FROM workspace.default.ops_export_reconciliation WHERE status = 'OK';

-- Triage quality rules without putting student IDs in alerts.
SELECT reason, count(*) rejected_rows
FROM workspace.default.silver_quarantine_events
LATERAL VIEW explode(quality_errors) errors AS reason
WHERE quarantined_at >= current_timestamp() - INTERVAL 1 DAY
GROUP BY reason;

-- Expected result: zero. The pipeline itself fails if this view would contain conflicts.
SELECT count(*) AS conflicting_revisions FROM workspace.default.ops_cdc_conflicts;

-- Cost attribution: adapt resource tags/SKUs and date range to your billing configuration.
SELECT usage_date, sku_name, usage_unit, sum(usage_quantity) quantity
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
  AND (custom_tags['project'] = 'lexia_streaming'
       OR usage_metadata.dlt_pipeline_id = '070eddb6-9b00-4850-9bea-2a1e83f06755')
GROUP BY usage_date, sku_name, usage_unit;
