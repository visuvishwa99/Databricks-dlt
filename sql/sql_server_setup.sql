-- SQL Server 2017+. Apply with sqlcmd/SSMS as migration owner, never the runtime login.
-- V2 tables coexist with the earlier demo staging/audit objects. Existing target is preserved.
IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');
GO
IF OBJECT_ID('stg.student_progress_v2', 'U') IS NULL
CREATE TABLE stg.student_progress_v2 (
    run_id nvarchar(100) NOT NULL,
    attempt_id nvarchar(100) NOT NULL,
    product_name nvarchar(100) NOT NULL,
    student_id nvarchar(100) NOT NULL,
    school_id nvarchar(100) NULL,
    latest_unit_id nvarchar(100) NULL,
    latest_level_id nvarchar(100) NULL,
    last_completed_at datetime2(6) NULL,
    total_completions bigint NOT NULL CHECK (total_completions >= 0),
    staged_at datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('stg.student_progress_v2') AND name='IX_progress_attempt')
CREATE INDEX IX_progress_attempt ON stg.student_progress_v2(run_id, attempt_id);
GO
IF OBJECT_ID('dbo.student_progress', 'U') IS NULL
CREATE TABLE dbo.student_progress (
    product_name nvarchar(100) NOT NULL,
    student_id nvarchar(100) NOT NULL,
    school_id nvarchar(100) NULL,
    latest_unit_id nvarchar(100) NULL,
    latest_level_id nvarchar(100) NULL,
    last_completed_at datetime2(6) NULL,
    total_completions bigint NOT NULL,
    updated_at datetime2(6) NOT NULL,
    CONSTRAINT PK_student_progress PRIMARY KEY (product_name, student_id)
);
GO
IF OBJECT_ID('dbo.student_progress_write_audit_v2', 'U') IS NULL
CREATE TABLE dbo.student_progress_write_audit_v2 (
    run_id nvarchar(100) NOT NULL PRIMARY KEY,
    attempt_id nvarchar(100) NOT NULL,
    staged_rows bigint NOT NULL,
    deleted_rows bigint NOT NULL,
    snapshot_at datetime2(6) NOT NULL,
    applied_at datetime2(6) NOT NULL
);
GO
CREATE OR ALTER PROCEDURE dbo.usp_publish_student_progress_v2
    @run_id nvarchar(100),
    @attempt_id nvarchar(100),
    @expected_rows bigint,
    @snapshot_at datetime2(6),
    @allow_deletes bit = 0,
    @max_delete_fraction float = 0.05
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        DECLARE @lock_result int;
        EXEC @lock_result = sys.sp_getapplock
            @Resource='lexia_student_progress_publish', @LockMode='Exclusive',
            @LockOwner='Transaction', @LockTimeout=30000;
        IF @lock_result < 0 THROW 51000, 'Publish lock unavailable', 1;

        -- Covers client timeout after a committed transaction.
        IF EXISTS (SELECT 1 FROM dbo.student_progress_write_audit_v2 WHERE run_id=@run_id)
        BEGIN
            COMMIT TRANSACTION;
            RETURN;
        END;
        IF @snapshot_at IS NULL OR @snapshot_at > DATEADD(minute, 5, SYSUTCDATETIME())
            THROW 51001, 'Invalid snapshot time', 1;
        IF EXISTS (SELECT 1 FROM dbo.student_progress_write_audit_v2 WHERE snapshot_at >= @snapshot_at)
            THROW 51002, 'A newer snapshot was already published', 1;
        IF @expected_rows IS NULL OR @expected_rows <= 0
            THROW 51003, 'Empty snapshot publication is blocked', 1;
        IF @max_delete_fraction IS NULL OR @max_delete_fraction < 0 OR @max_delete_fraction > 1
            THROW 51004, 'Invalid deletion threshold', 1;

        -- JDBC executor retries may duplicate identical rows. Conflicting values must fail.
        SELECT DISTINCT product_name, student_id, school_id, latest_unit_id,
            latest_level_id, last_completed_at, total_completions
        INTO #candidate
        FROM stg.student_progress_v2 WHERE run_id=@run_id AND attempt_id=@attempt_id;
        IF (SELECT COUNT_BIG(*) FROM #candidate) <> @expected_rows
            THROW 51005, 'Staged row count differs from captured snapshot', 1;
        IF EXISTS (SELECT 1 FROM #candidate GROUP BY product_name, student_id HAVING COUNT(*) > 1)
            THROW 51006, 'Conflicting staging keys', 1;

        DECLARE @delete_rows bigint = (
            SELECT COUNT_BIG(*) FROM dbo.student_progress t
            WHERE NOT EXISTS (SELECT 1 FROM #candidate s
                WHERE s.product_name=t.product_name AND s.student_id=t.student_id)
        );
        DECLARE @target_rows bigint = (SELECT COUNT_BIG(*) FROM dbo.student_progress);
        IF @delete_rows > 0 AND (@allow_deletes = 0 OR @allow_deletes IS NULL)
            THROW 51007, 'Snapshot would remove serving rows; deletion is disabled', 1;
        IF @delete_rows > @target_rows * @max_delete_fraction
            THROW 51008, 'Deletion exceeds configured threshold', 1;

        UPDATE t SET school_id=s.school_id, latest_unit_id=s.latest_unit_id,
            latest_level_id=s.latest_level_id, last_completed_at=s.last_completed_at,
            total_completions=s.total_completions, updated_at=SYSUTCDATETIME()
        FROM dbo.student_progress t JOIN #candidate s
            ON s.product_name=t.product_name AND s.student_id=t.student_id;
        INSERT dbo.student_progress (
            product_name, student_id, school_id, latest_unit_id,
            latest_level_id, last_completed_at, total_completions, updated_at
        )
        SELECT s.product_name,s.student_id,s.school_id,s.latest_unit_id,
            s.latest_level_id,s.last_completed_at,s.total_completions,SYSUTCDATETIME()
        FROM #candidate s WHERE NOT EXISTS (
            SELECT 1 FROM dbo.student_progress t
            WHERE t.product_name=s.product_name AND t.student_id=s.student_id
        );
        DELETE t FROM dbo.student_progress t
        WHERE NOT EXISTS (SELECT 1 FROM #candidate s
            WHERE s.product_name=t.product_name AND s.student_id=t.student_id);
        INSERT dbo.student_progress_write_audit_v2
            (run_id,attempt_id,staged_rows,deleted_rows,snapshot_at,applied_at)
        VALUES (@run_id,@attempt_id,@expected_rows,@delete_rows,@snapshot_at,SYSUTCDATETIME());
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
-- Runtime role: INSERT/SELECT on staging, SELECT on serving/audit, EXECUTE on procedure.
-- No direct UPDATE/DELETE/ALTER on serving. Use a separate migration and cleanup identity.
-- Keep successful staging attempts until downstream reconciliation and retention gates pass.
