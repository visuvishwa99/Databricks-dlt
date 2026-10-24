# Deployment and CI/CD

## Repository layout

The bundle lives at repository root. Run every command here, not from the original second-brain folder. Source files, tests, SQL migrations and docs move together in Git. .databricks state/cache, generated fixtures and local dependencies are ignored. Keep bundle name and workspace root stable to update an existing deployment.

## Local checks

Use Python 3.11, Java 17, and the versions in requirements-dev.txt. A clean virtual environment avoids mixing system Spark and PySpark.

```powershell
python -m venv .venv
.venv/Scripts/Activate.ps1
python -m pip install -r requirements-dev.txt
ruff check .
python -m pytest tests --ignore=tests/integration -q
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile DEV
databricks bundle validate -t demo -p DEV --strict
databricks bundle plan -t demo -p DEV
databricks bundle deploy -t demo -p DEV
```

On Unix activate with source .venv/bin/activate. Unset a system SPARK_HOME if it points to a different Spark installation. SQL integration tests run automatically in GitHub Actions against a disposable SQL Server 2022 service, using a separate temporary test database for each test. The visible CI database password is only for that ephemeral service; it is not an application credential. CI also runs on documentation-only pull requests so required checks can complete.

## Existing demo

Workspace: 7474652460637329, host dbc-7f4188e4-3fd1.cloud.databricks.com, local profile LEXIA. The bundle uses workspace.default and a managed volume. For a new workspace without the workspace catalog, adjust the demo catalog, schema and volume path together. The full targets use environment variables instead.

```powershell
$dest = 'dbfs:/Volumes/workspace/default/lexia_sample_exports/student_events/run_date=2026-10-22/run_id=sample-001'
databricks fs mkdir $dest -p LEXIA
databricks fs cp sample_exports/run_date=2026-10-22/run_id=sample-001/part-00000.json "$dest/part-00000.json" -p LEXIA
databricks fs cp sample_exports/run_date=2026-10-22/run_id=sample-001/manifest.json "$dest/manifest.json" -p LEXIA
databricks bundle run -t demo -p LEXIA student_events_refresh
```

Upload only into a new run folder; do not overwrite the existing sample. The existing demo already contains it, plus a synthetic malformed-input drill that leaves two rows in quarantine. Pass `--max-quarantine-rows 2` to the smoke script for this workspace; a new workspace with only the original sample can use the default of zero. The fixed sample date makes it unsuitable for the full freshness gate; generate a fresh export for full integration tests.

## GitHub environments and multiple workspaces

Create Dev and Prod GitHub environments. The same Git commit deploys to different hosts and isolated data namespaces. The workflow uses GitHub OIDC, not a personal access token. Provision dependencies in security-and-infrastructure.md first.

| Kind | Name | Per-environment value |
| --- | --- | --- |
| Variable | DATABRICKS_HOST | HTTPS workspace origin, no browser path |
| Secret | DATABRICKS_CLIENT_ID | Deployment service principal application UUID |
| Variable | CATALOG, SCHEMA | Existing destination namespace |
| Variable | EXPORT_ROOT | Readable environment S3 prefix |
| Variable | SCHOOL_TABLE | Full federated table name |
| Variable | WRITEBACK_CLUSTER_ID | Dedicated cluster with JDBC/network access |
| Variable | ALERT_EMAIL | Real operations recipient; verify delivery before release |
| Variable | RUN_AS_SERVICE_PRINCIPAL | Production runtime application UUID; required for prod |
| Variable | SECRET_SCOPE | Optional, defaults to sis; contains jdbc_url, user, password |
| Variable | MAINTENANCE_SECRET_SCOPE | Optional, defaults to sis_maintenance; separate SQL identity with EXECUTE on the cleanup procedure |
| Variable | STAGING_RETENTION_DAYS | Optional, defaults to 7; must be between 7 and 365 |
| Variable | USE_MANAGED_FILE_EVENTS | Optional true/false, defaults true |
| Repository variable | ENABLE_AUTO_DEV | Set true only after Dev is fully configured; enables deployment on main pushes |

Configure the deployment principal's federation policy in the Databricks account. Issuer is https://token.actions.githubusercontent.com. Use the actual Databricks account ID as audience (it is not the workspace ID or metastore ID). Subjects are exactly repo:visuvishwa99/Databricks-dlt:environment:Dev and repo:visuvishwa99/Databricks-dlt:environment:Prod. Assign the principals to the appropriate workspaces and grant runtime/deployment permissions separately. Follow the [official federation setup](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github).

CI runs lint, Spark tests and SQL transaction tests on relevant pushes and pull requests. Deployment requires both jobs to pass. Manual workflow_dispatch chooses dev or prod; production must use main. Automatic dev is opt-in with ENABLE_AUTO_DEV=true. Branch/environment protection must require the check and sql-integration checks and restrict who can modify workflows. GitHub Actions are pinned by commit SHA and Python test tools by version; review dependency upgrades deliberately. The SQL Server container tracks 2022-latest to receive updates and must be revalidated when its image changes.

The deploy job checks environment settings and selected live resources, validates/plans/deploys the bundle, and records links in the run summary. run_job defaults false; when enabled it runs the full integration job after deployment. A separate daily staging-cleanup job uses the maintenance secret scope and has its own pause variable. It stays paused until SQL grants, retention, and alert delivery are verified. Its stored procedure only removes unaudited attempts older than the configured 7-to-365-day window. Schedules remain paused until an explicit operations decision. Deploying infrastructure successfully is not the same as validating data execution.

## Promotion and rollback

Promote an immutable reviewed commit after development data and failure drills pass. Prod approval is a GitHub environment gate. Review the deployment plan and verify run_as, host, catalog, schema, source prefix and SQL database. A different workspace does not automatically imply a different metastore or SQL database. Do not use a developer's personal OAuth login for unattended production.

For rollback, checkout the previously accepted commit, set the same environment settings, review bundle plan, and deploy to the same target/root. Keep the schedule paused during incompatible migrations. Restore data separately according to operations.md; a bundle rollback only restores code/resource configuration. Do not destroy/recreate a bundle as a rollback strategy. Adding a staging environment requires a new bundle target, GitHub environment and federation subject, plus separate data dependencies.
