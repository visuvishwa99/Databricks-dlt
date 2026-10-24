-- TEMPLATE: replace tokens with validated environment identifiers/principals.
-- Apply only to an isolated, eligible workspace after testing both allowed and denied users.
-- Do not apply these policies to the Free Edition demo without checking feature eligibility.
-- The pipeline/publisher principal must belong to __PUBLISHER_GROUP__.
CREATE SCHEMA IF NOT EXISTS __CATALOG__.__GOVERNANCE_SCHEMA__;
CREATE TABLE IF NOT EXISTS __CATALOG__.__GOVERNANCE_SCHEMA__.school_entitlements (
  principal STRING NOT NULL,
  school_id STRING NOT NULL
) USING DELTA;
-- Only the governance owner may write entitlements; no analyst write grant.

CREATE OR REPLACE FUNCTION __CATALOG__.__GOVERNANCE_SCHEMA__.student_mask(value STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('__PUBLISHER_GROUP__') THEN value ELSE '[REDACTED]' END;

CREATE OR REPLACE FUNCTION __CATALOG__.__GOVERNANCE_SCHEMA__.school_access(value STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('__PUBLISHER_GROUP__') OR EXISTS (
  SELECT 1 FROM __CATALOG__.__GOVERNANCE_SCHEMA__.school_entitlements e
  WHERE e.principal = current_user() AND e.school_id = value
);

GRANT USE CATALOG ON CATALOG __CATALOG__ TO `__ANALYST_GROUP__`;
GRANT USE SCHEMA ON SCHEMA __CATALOG__.__DATA_SCHEMA__ TO `__ANALYST_GROUP__`;
GRANT USE SCHEMA ON SCHEMA __CATALOG__.__GOVERNANCE_SCHEMA__ TO `__ANALYST_GROUP__`;
GRANT EXECUTE ON FUNCTION __CATALOG__.__GOVERNANCE_SCHEMA__.student_mask TO `__ANALYST_GROUP__`;
GRANT EXECUTE ON FUNCTION __CATALOG__.__GOVERNANCE_SCHEMA__.school_access TO `__ANALYST_GROUP__`;
ALTER TABLE __CATALOG__.__DATA_SCHEMA__.gold_student_progress
  ALTER COLUMN student_id SET MASK __CATALOG__.__GOVERNANCE_SCHEMA__.student_mask;
ALTER TABLE __CATALOG__.__DATA_SCHEMA__.gold_student_progress
  SET ROW FILTER __CATALOG__.__GOVERNANCE_SCHEMA__.school_access ON (school_id);
ALTER TABLE __CATALOG__.__DATA_SCHEMA__.gold_daily_completions
  SET ROW FILTER __CATALOG__.__GOVERNANCE_SCHEMA__.school_access ON (school_id);
GRANT SELECT ON TABLE __CATALOG__.__DATA_SCHEMA__.gold_student_progress TO `__ANALYST_GROUP__`;
GRANT SELECT ON TABLE __CATALOG__.__DATA_SCHEMA__.gold_daily_completions TO `__ANALYST_GROUP__`;
-- No bronze, quarantine, or entitlement table access is granted to analysts.
-- Before deployment: test entitled analyst, unentitled analyst, publisher, and refreshed pipeline.
