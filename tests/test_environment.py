import pytest
from common import table_name, validate_run_id
from validate_environment import validate


def settings():
    return {
        "DATABRICKS_HOST": "https://example.cloud.databricks.com",
        "DATABRICKS_CLIENT_ID": "test-principal",
        "BUNDLE_VAR_catalog": "edtech_dev", "BUNDLE_VAR_schema": "lexia",
        "BUNDLE_VAR_export_root": "s3://test/exports/",
        "BUNDLE_VAR_school_table": "sis.dbo.schools",
        "BUNDLE_VAR_writeback_cluster_id": "test-cluster",
        "BUNDLE_VAR_alert_email": "ops@example.com",
    }


def test_missing_configuration_blocks_deployment():
    with pytest.raises(ValueError, match="Missing"):
        validate({}, "dev")


def test_dev_configuration():
    validate(settings(), "dev")


def test_production_requires_runtime_identity_and_main():
    env = settings()
    with pytest.raises(ValueError, match="run_as"):
        validate(env, "prod")
    env["BUNDLE_VAR_run_as_service_principal"] = "runtime-principal"
    env["GITHUB_REF"] = "refs/heads/feature"
    with pytest.raises(ValueError, match="main"):
        validate(env, "prod")


@pytest.mark.parametrize("value", ["x'; DROP TABLE t;--", "", "a.b"])
def test_sql_identifiers_are_validated(value):
    with pytest.raises(ValueError):
        table_name(value, "schema", "table")
    with pytest.raises(ValueError):
        validate_run_id(value)
