# Security and infrastructure boundary

## Provisioning responsibility

| Resource | Owner / required setup | Why outside bundle |
| --- | --- | --- |
| DocumentDB export | Source team, immutable files and manifests, source reconciliation | Existing upstream system |
| S3 bucket and prefix | Cloud team, block public access, encryption, lifecycle and object ownership | AWS account infrastructure |
| IAM role and UC storage credential | Cloud/platform, tightly scoped S3/KMS permissions and approved UC trust | Cross-account trust must use real account/metastore external ID |
| UC external location | Platform, credential linked to environment prefix; file events if enabled | Needs environment storage identity |
| Destination catalog/schema | Platform, separate dev/prod namespaces and ownership | Data boundary persists beyond application deployment |
| Federation connection/catalog | Platform/database owners, SQL read-only school access and source network route | Source credentials and load ownership |
| Dedicated JDBC cluster | Platform, DBR 15.4+ with Python 3.11+, Microsoft JDBC driver, private route to SQL Server | Runtime/network policy varies per workspace |
| SQL Server schema/procedure | Database migration identity runs sql/sql_server_setup.sql | Runtime identity must not own DDL |
| OIDC service principals | Account admin, workspace assignment and repository/environment federation subjects | Account identity/trust configuration |
| Budget, audit, backup, external alerts | Operations | Required even if pipeline scheduling fails |

The demo uses a managed volume and synthetic school row. It proves pipeline execution, not AWS external-location IAM, real federation, network connectivity, or production JDBC execution. The SQL integration tests use an isolated SQL Server service in GitHub Actions and do not prove those networks either.

## Least privilege

Separate deployment, runtime, analytics and migration/cleanup identities. The deployer manages bundle resources and may use the selected runtime service principal under Databricks authorization. The runtime can read its export prefix and federated school relation; create/modify pipeline-owned tables in its schema; read the configured secret scope; use its dedicated compute; and publish through the SQL procedure. Analytics should receive only approved serving views and USE CATALOG/USE SCHEMA/SELECT as needed, not bronze or quarantine. Quarantine contains rejected student payloads and needs the same restricted access as raw data.

The SQL runtime login needs INSERT and SELECT on stg.student_progress_v2, SELECT on dbo.student_progress and audit, and EXECUTE on dbo.usp_publish_student_progress_v2. Use ownership chaining with objects controlled by the migration owner. Do not grant runtime direct UPDATE, DELETE, ALTER, CONTROL or database owner. A separate cleanup identity handles staging retention; a separate DBA identity handles backups. The procedure's transaction lock coordinates approved publishers, so allowing unrelated direct writers would break its assumptions.

## Secrets and transport

Use GitHub OIDC, with the subject scoped to this repository and exact GitHub environment; do not trust arbitrary branches/repositories. Protect Prod with reviewers and main-only deployment. A client ID is an identifier, while access tokens and passwords are secrets. No token belongs in source code or a bundle YAML file. Store jdbc_url/user/password in an environment-specific Databricks secret scope. The publisher requires encrypt=true;trustServerCertificate=false in the SQL JDBC URL. Validate certificate host names, DNS, private routing, firewall rules, secret ACLs and rotation using the actual runtime principal.

S3 should use identity-based temporary credentials through UC, encryption configured by the platform, and appropriate KMS permissions. Verify file-event permissions when enabled; use directory listing for an intentional fallback. The pipeline does not delete source objects. Keep checkpoints out of lifecycle deletion paths. Review log access and avoid logging data frames, student IDs, connection strings or secret values.

## Student data and BI

Use synthetic data for the demo and interviews. Real student identifiers need organization-approved classification, retention, access review and deletion workflows. This project does not assert regulatory compliance. Apply approved masking and school-level row access on serving views, test both permitted and denied users, and audit exports. SQL Server serving and backups need the same controls. Current source v1 has no delete events, so production privacy deletion requires a separately designed workflow covering raw objects, bronze, silver, quarantine, gold, SQL staging/serving, backups and BI caches. Do not imply that deleting only gold removes all copies.

## Deployment authorization checks

scripts/validate_environment.py validates required settings, production branch and runtime identity, destination schema, source table and dedicated cluster mode. It checks the deployer's visibility; it cannot prove every permission of a separate run_as identity. Before enabling production, run an end-to-end canary as that runtime identity, rotate credentials and verify the canary again. Confirm catalog workspace bindings when multiple workspaces share a metastore. Avoid sharing dev and prod source prefixes or SQL serving databases.
