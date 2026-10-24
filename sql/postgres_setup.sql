-- Run as migration owner in a dedicated database. All timestamps are UTC.
BEGIN;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS serving;
REVOKE ALL ON SCHEMA stg, serving FROM PUBLIC;
CREATE TABLE IF NOT EXISTS stg.student_progress (
    run_id text COLLATE "C" NOT NULL,
    attempt_id text COLLATE "C" NOT NULL,
    product_name text COLLATE "C" NOT NULL,
    student_id text COLLATE "C" NOT NULL,
    school_id text COLLATE "C",
    latest_unit_id text COLLATE "C",
    latest_level_id text COLLATE "C",
    last_completed_at timestamp,
    total_completions bigint NOT NULL CHECK (total_completions >= 0),
    staged_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS progress_attempt ON stg.student_progress(run_id, attempt_id);
CREATE INDEX IF NOT EXISTS progress_age ON stg.student_progress(staged_at);
CREATE TABLE IF NOT EXISTS serving.student_progress (
    product_name text COLLATE "C" NOT NULL,
    student_id text COLLATE "C" NOT NULL,
    school_id text COLLATE "C",
    latest_unit_id text COLLATE "C",
    latest_level_id text COLLATE "C",
    last_completed_at timestamp,
    total_completions bigint NOT NULL CHECK (total_completions >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (product_name, student_id)
);
CREATE TABLE IF NOT EXISTS serving.publish_audit (
    run_id text COLLATE "C" PRIMARY KEY,
    attempt_id text COLLATE "C" NOT NULL,
    staged_rows bigint NOT NULL,
    deleted_rows bigint NOT NULL,
    snapshot_at timestamptz NOT NULL UNIQUE,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    reconciled_at timestamptz
);

CREATE OR REPLACE FUNCTION serving.publish_progress(
    p_run text, p_attempt text, p_expected bigint, p_snapshot timestamptz,
    p_deletes boolean DEFAULT false, p_fraction double precision DEFAULT 0.05
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp SET lock_timeout = '30s'
AS $$
DECLARE n_delete bigint; n_target bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(7474652460637329);
    IF EXISTS (SELECT 1 FROM serving.publish_audit WHERE run_id=p_run) THEN RETURN; END IF;
    IF p_run IS NULL OR p_attempt IS NULL OR p_run !~ '^[A-Za-z0-9_-]{1,100}$'
        OR p_attempt !~ '^[A-Za-z0-9_-]{1,100}$' THEN RAISE EXCEPTION 'Invalid run/attempt ID'; END IF;
    IF p_snapshot IS NULL OR NOT isfinite(p_snapshot)
        OR p_snapshot > clock_timestamp() + interval '5 minutes' THEN
        RAISE EXCEPTION 'Invalid snapshot time'; END IF;
    IF EXISTS (SELECT 1 FROM serving.publish_audit WHERE snapshot_at >= p_snapshot) THEN
        RAISE EXCEPTION 'A newer snapshot was already published'; END IF;
    IF p_expected IS NULL OR p_expected <= 0 THEN RAISE EXCEPTION 'Empty snapshot blocked'; END IF;
    IF p_fraction IS NULL OR NOT (p_fraction BETWEEN 0 AND 1) THEN
        RAISE EXCEPTION 'Invalid deletion threshold'; END IF;
    -- Materialize once: later JDBC retries cannot change this transaction's candidate.
    -- Never reuse a caller-created temp table inside a SECURITY DEFINER function.
    CREATE TEMP TABLE progress_candidate ON COMMIT DROP AS
      SELECT DISTINCT product_name, student_id, school_id, latest_unit_id,
          latest_level_id, last_completed_at, total_completions
      FROM stg.student_progress WHERE run_id=p_run AND attempt_id=p_attempt;
    IF (SELECT count(*) FROM pg_temp.progress_candidate) <> p_expected THEN
        RAISE EXCEPTION 'Staged row count mismatch'; END IF;
    IF EXISTS (SELECT 1 FROM pg_temp.progress_candidate GROUP BY product_name, student_id HAVING count(*)>1)
        THEN RAISE EXCEPTION 'Conflicting staging keys'; END IF;
    SELECT count(*) INTO n_target FROM serving.student_progress;
    SELECT count(*) INTO n_delete FROM serving.student_progress t
      WHERE NOT EXISTS (SELECT 1 FROM pg_temp.progress_candidate s
        WHERE (s.product_name,s.student_id)=(t.product_name,t.student_id));
    IF n_delete>0 AND p_deletes IS DISTINCT FROM true THEN RAISE EXCEPTION 'Deletion disabled'; END IF;
    IF n_delete>n_target*p_fraction THEN RAISE EXCEPTION 'Deletion exceeds threshold'; END IF;
    INSERT INTO serving.student_progress AS t
      (product_name, student_id, school_id, latest_unit_id, latest_level_id, last_completed_at, total_completions)
      SELECT * FROM pg_temp.progress_candidate
      ON CONFLICT (product_name,student_id) DO UPDATE SET
        school_id=excluded.school_id, latest_unit_id=excluded.latest_unit_id,
        latest_level_id=excluded.latest_level_id, last_completed_at=excluded.last_completed_at,
        total_completions=excluded.total_completions, updated_at=clock_timestamp()
      WHERE (t.school_id,t.latest_unit_id,t.latest_level_id,t.last_completed_at,t.total_completions)
        IS DISTINCT FROM
        (excluded.school_id,excluded.latest_unit_id,excluded.latest_level_id,
         excluded.last_completed_at,excluded.total_completions);
    DELETE FROM serving.student_progress t WHERE NOT EXISTS
      (SELECT 1 FROM pg_temp.progress_candidate s WHERE (s.product_name,s.student_id)=(t.product_name,t.student_id));
    INSERT INTO serving.publish_audit(run_id,attempt_id,staged_rows,deleted_rows,snapshot_at)
      VALUES(p_run,p_attempt,p_expected,n_delete,p_snapshot);
END $$;

CREATE OR REPLACE FUNCTION serving.reconcile_progress(p_run text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp SET lock_timeout = '30s'
AS $$
DECLARE a serving.publish_audit;
BEGIN
    PERFORM pg_advisory_xact_lock(7474652460637329);
    SELECT * INTO a FROM serving.publish_audit ORDER BY snapshot_at DESC LIMIT 1;
    IF a.run_id IS NULL OR a.run_id IS DISTINCT FROM p_run THEN
      RAISE EXCEPTION 'Requested run is not the latest snapshot'; END IF;
    CREATE TEMP TABLE progress_verify ON COMMIT DROP AS
      SELECT DISTINCT product_name,student_id,school_id,latest_unit_id,latest_level_id,last_completed_at,total_completions
      FROM stg.student_progress WHERE run_id=a.run_id AND attempt_id=a.attempt_id;
    IF (SELECT count(*) FROM pg_temp.progress_verify) <> a.staged_rows THEN
      RAISE EXCEPTION 'Published staging snapshot missing or changed'; END IF;
    IF EXISTS (
      (SELECT * FROM pg_temp.progress_verify EXCEPT
       SELECT product_name,student_id,school_id,latest_unit_id,latest_level_id,last_completed_at,total_completions
       FROM serving.student_progress)
      UNION ALL
      (SELECT product_name,student_id,school_id,latest_unit_id,latest_level_id,last_completed_at,total_completions
       FROM serving.student_progress EXCEPT SELECT * FROM pg_temp.progress_verify)
    ) THEN RAISE EXCEPTION 'Serving data differs from published snapshot'; END IF;
    UPDATE serving.publish_audit SET reconciled_at=clock_timestamp() WHERE run_id=p_run;
    RETURN a.staged_rows;
END $$;

CREATE OR REPLACE FUNCTION serving.cleanup_staging(p_days integer DEFAULT 7) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp SET lock_timeout = '30s'
AS $$
DECLARE deleted bigint; latest text;
BEGIN
    IF p_days IS NULL OR p_days NOT BETWEEN 7 AND 365 THEN RAISE EXCEPTION 'Invalid retention'; END IF;
    PERFORM pg_advisory_xact_lock(7474652460637329);
    SELECT run_id INTO latest FROM serving.publish_audit ORDER BY snapshot_at DESC LIMIT 1;
    DELETE FROM stg.student_progress s WHERE staged_at < clock_timestamp()-make_interval(days=>p_days)
      AND (
        NOT EXISTS (SELECT 1 FROM serving.publish_audit a WHERE (a.run_id,a.attempt_id)=(s.run_id,s.attempt_id))
        OR EXISTS (SELECT 1 FROM serving.publish_audit a WHERE (a.run_id,a.attempt_id)=(s.run_id,s.attempt_id)
          AND a.reconciled_at IS NOT NULL AND a.run_id IS DISTINCT FROM latest)
      );
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA serving FROM PUBLIC;
COMMIT;
