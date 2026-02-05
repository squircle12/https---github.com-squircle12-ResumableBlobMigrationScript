-- =============================================================================
-- Blob Delta Jobs: Delta Engine and Operator Wrapper
-- -----------------------------------------------------------------------------
-- Purpose
--   Implement the master delta engine procedure (usp_BlobDelta_Run) and a thin
--   operator-friendly wrapper (usp_BlobDelta_RunOperator) that:
--     - Computes per-table time windows from high-watermarks and safety buffers.
--     - Runs the Roots, MissingParents, and Children steps via script templates.
--     - Maintains per-table high-watermarks and run history.
--     - Supports simple modes for operators / SQL Agent jobs.
--
--   This script assumes:
--     - BlobDeltaJobs schema has been created (03_BlobDeltaJobs_Schema.sql).
--     - Config and script templates have been seeded (04_BlobDeltaJobs_Seed_Config.sql).
--
-- Notes
--   The engine is intentionally conservative and logs progress per batch to
--   BlobDeltaRunStep. It runs tables sequentially per invocation to simplify
--   locking and observability.
-- =============================================================================

USE BlobDeltaJobs;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- -----------------------------------------------------------------------------
-- Helper: Resolve table/config values for a given TableName into local vars
-- -----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE dbo.usp_BlobDelta_ResolveTableConfig
    @TableName                 sysname,
    @SourceTableFull           nvarchar(776) OUTPUT,
    @TargetTableFull           nvarchar(776) OUTPUT,
    @MetadataTableFull         nvarchar(776) OUTPUT,
    @MetadataIdColumn          sysname       OUTPUT,
    @MetadataModifiedOnColumn  sysname       OUTPUT,
    @SafetyBufferMinutes       int           OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        @SourceTableFull = c.SourceDatabase + N'.' + c.SourceSchema + N'.' + c.SourceTable,
        @TargetTableFull = c.TargetDatabase + N'.' + c.TargetSchema + N'.' + c.TargetTable,
        @MetadataTableFull = c.MetadataDatabase + N'.' + c.MetadataSchema + N'.' + c.MetadataTable,
        @MetadataIdColumn = c.MetadataIdColumn,
        @MetadataModifiedOnColumn = c.MetadataModifiedOnCol,
        @SafetyBufferMinutes = c.SafetyBufferMinutes
    FROM dbo.BlobDeltaTableConfig c
    WHERE c.TableName = @TableName
      AND c.IsActive = 1;

    IF @SourceTableFull IS NULL
    BEGIN
        RAISERROR(N'TableName ''%s'' not found or inactive in BlobDeltaTableConfig.', 16, 1, @TableName);
    END
END;
GO

-- -----------------------------------------------------------------------------
-- Core engine: run delta for one or more tables in a single RunId
-- -----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE dbo.usp_BlobDelta_Run
    @RunId        uniqueidentifier = NULL OUTPUT,
    @RunType      nvarchar(20)     = N'Delta',  -- 'Full' or 'Delta'
    @TableName    sysname          = NULL,      -- NULL = all active tables
    @BatchSize    int              = 500,
    @MaxDOP       tinyint          = 2,
    @Reset        bit              = 0,
    @BusinessUnitId uniqueidentifier = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now datetime2(7) = SYSDATETIME();
    DECLARE @RequestedBy sysname = SUSER_SNAME();

    IF @RunId IS NULL
    BEGIN
        SET @RunId = NEWID();
        INSERT INTO dbo.BlobDeltaRun (RunId, RunType, RequestedBy, RunStartedAt, Status)
        VALUES (@RunId, @RunType, @RequestedBy, @Now, N'InProgress');
    END

    -- Determine tables to process for this run.
    IF OBJECT_ID('tempdb..#TablesToProcess') IS NOT NULL
        DROP TABLE #TablesToProcess;

    SELECT c.TableName
    INTO #TablesToProcess
    FROM dbo.BlobDeltaTableConfig c
    WHERE c.IsActive = 1
      AND (@TableName IS NULL OR c.TableName = @TableName);

    DECLARE @T_TableName sysname;

    DECLARE table_cursor CURSOR FAST_FORWARD FOR
        SELECT TableName FROM #TablesToProcess ORDER BY TableName;

    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @T_TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE
            @SourceTableFull          nvarchar(776),
            @TargetTableFull          nvarchar(776),
            @MetadataTableFull        nvarchar(776),
            @MetadataIdColumn         sysname,
            @MetadataModifiedOnColumn sysname,
            @SafetyBufferMinutes      int,
            @LastHighWater            datetime2(7),
            @WindowStart              datetime2(7),
            @WindowEnd                datetime2(7),
            @RunStartForTable         datetime2(7),
            @ExcludedStreamId         uniqueidentifier,
            @CurrentStep              tinyint,
            @BatchNumber              int,
            @Rows                     int,
            @TotalRows                int,
            @BatchStartedAt           datetime2(7),
            @BatchCompletedAt         datetime2(7),
            @Sql                      nvarchar(max);

        SET @RunStartForTable = SYSDATETIME();
        SET @ExcludedStreamId = '7F8D53EC-B98C-F011-B86B-005056A2DD37'; -- global exclusion, as in V2.

        BEGIN TRY
            -- Resolve config for this table.
            EXEC dbo.usp_BlobDelta_ResolveTableConfig
                @TableName                 = @T_TableName,
                @SourceTableFull           = @SourceTableFull OUTPUT,
                @TargetTableFull           = @TargetTableFull OUTPUT,
                @MetadataTableFull         = @MetadataTableFull OUTPUT,
                @MetadataIdColumn          = @MetadataIdColumn OUTPUT,
                @MetadataModifiedOnColumn  = @MetadataModifiedOnColumn OUTPUT,
                @SafetyBufferMinutes       = @SafetyBufferMinutes OUTPUT;

            IF @SafetyBufferMinutes IS NULL SET @SafetyBufferMinutes = 240;

            -- Load high-watermark and compute window.
            SELECT @LastHighWater = h.LastHighWaterModifiedOn
            FROM dbo.BlobDeltaHighWatermark h
            WHERE h.TableName = @T_TableName;

            SET @WindowEnd = DATEADD(MINUTE, -@SafetyBufferMinutes, @RunStartForTable);
            SET @WindowStart =
                CASE
                    WHEN @LastHighWater IS NULL
                        THEN DATEADD(DAY, -365, @WindowEnd) -- conservative baseline; can be adjusted.
                    ELSE DATEADD(MINUTE, -@SafetyBufferMinutes, @LastHighWater)
                END;

            -- Optional reset support for this run/table.
            IF @Reset = 1
            BEGIN
                DELETE FROM dbo.BlobDeltaMissingParentsQueue
                WHERE RunId = @RunId AND TableName = @T_TableName;

                DELETE FROM dbo.BlobDeltaRunStep
                WHERE RunId = @RunId AND TableName = @T_TableName;
            END

            -- Acquire simple lease for this table.
            UPDATE h
            SET IsRunning = 1,
                RunLeaseExpiresAt = DATEADD(MINUTE, 60, @RunStartForTable),
                LastRunId = @RunId
            FROM dbo.BlobDeltaHighWatermark h
            WHERE h.TableName = @T_TableName;

            -- -----------------------------------------------------------------
            -- Step 1: Roots
            -- -----------------------------------------------------------------
            IF NOT EXISTS (
                SELECT 1
                FROM dbo.BlobDeltaRunStep
                WHERE RunId = @RunId
                  AND TableName = @T_TableName
                  AND StepNumber = 1
                  AND Status = N'Completed'
            )
            BEGIN
                SET @CurrentStep = 1;
                SET @BatchNumber = 0;
                SET @TotalRows   = 0;

                WHILE 1 = 1
                BEGIN
                    SET @BatchNumber = @BatchNumber + 1;
                    SET @BatchStartedAt = SYSDATETIME();

                    SELECT @Sql = ScriptBody
                    FROM dbo.BlobDeltaStepScript
                    WHERE StepNumber = 1 AND ScriptKind = N'Roots';

                    -- Replace placeholders with table/column names and MaxDOP.
                    SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                        N'[SourceTableFull]',          @SourceTableFull),
                        N'[TargetTableFull]',          @TargetTableFull),
                        N'[MetadataTableFull]',        @MetadataTableFull),
                        N'[MetadataIdColumn]',         @MetadataIdColumn),
                        N'[MetadataModifiedOnColumn]', @MetadataModifiedOnColumn);

                    SET @Sql = REPLACE(@Sql, N'[MaxDOP]', CAST(@MaxDOP AS nvarchar(10)));

                    EXEC sp_executesql @Sql,
                        N'@BatchSize int,
                          @ExcludedStreamId uniqueidentifier,
                          @WindowStart datetime2(7),
                          @WindowEnd datetime2(7),
                          @BusinessUnitId uniqueidentifier',
                        @BatchSize      = @BatchSize,
                        @ExcludedStreamId = @ExcludedStreamId,
                        @WindowStart    = @WindowStart,
                        @WindowEnd      = @WindowEnd,
                        @BusinessUnitId = @BusinessUnitId;

                    SET @Rows = @@ROWCOUNT;
                    SET @TotalRows = @TotalRows + @Rows;
                    SET @BatchCompletedAt = SYSDATETIME();

                    INSERT INTO dbo.BlobDeltaRunStep
                        (RunId, TableName, StepNumber, BatchNumber,
                         RowsProcessed, TotalRowsProcessed,
                         WindowStart, WindowEnd,
                         BatchStartedAt, BatchCompletedAt,
                         Status, ErrorMessage)
                    VALUES
                        (@RunId, @T_TableName, 1, @BatchNumber,
                         @Rows, @TotalRows,
                         @WindowStart, @WindowEnd,
                         @BatchStartedAt, @BatchCompletedAt,
                         N'InProgress', NULL);

                    IF @Rows = 0 BREAK;
                END;

                INSERT INTO dbo.BlobDeltaRunStep
                    (RunId, TableName, StepNumber, BatchNumber,
                     RowsProcessed, TotalRowsProcessed,
                     WindowStart, WindowEnd,
                     BatchStartedAt, BatchCompletedAt,
                     Status, ErrorMessage)
                VALUES
                    (@RunId, @T_TableName, 1, 0,
                     0, @TotalRows,
                     @WindowStart, @WindowEnd,
                     SYSDATETIME(), SYSDATETIME(),
                     N'Completed', NULL);
            END

            -- -----------------------------------------------------------------
            -- Step 2: Missing parents (queue + batch inserts)
            -- -----------------------------------------------------------------
            IF NOT EXISTS (
                SELECT 1
                FROM dbo.BlobDeltaRunStep
                WHERE RunId = @RunId
                  AND TableName = @T_TableName
                  AND StepNumber = 2
                  AND Status = N'Completed'
            )
            BEGIN
                SET @CurrentStep = 2;
                SET @BatchNumber = 0;
                SET @TotalRows   = 0;

                -- Populate queue for this run/table if empty.
                IF NOT EXISTS (
                    SELECT 1
                    FROM dbo.BlobDeltaMissingParentsQueue
                    WHERE RunId = @RunId AND TableName = @T_TableName
                )
                BEGIN
                    SELECT @Sql = ScriptBody
                    FROM dbo.BlobDeltaQueuePopulationScript
                    WHERE TableName = @T_TableName;

                    SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                        N'[SourceTableFull]',          @SourceTableFull),
                        N'[TargetTableFull]',          @TargetTableFull),
                        N'[MetadataTableFull]',        @MetadataTableFull),
                        N'[MetadataIdColumn]',         @MetadataIdColumn),
                        N'[MetadataModifiedOnColumn]', @MetadataModifiedOnColumn);

                    EXEC sp_executesql @Sql,
                        N'@RunId uniqueidentifier,
                          @TableName sysname,
                          @ExcludedStreamId uniqueidentifier,
                          @WindowStart datetime2(7),
                          @WindowEnd datetime2(7),
                          @BusinessUnitId uniqueidentifier',
                        @RunId          = @RunId,
                        @TableName      = @T_TableName,
                        @ExcludedStreamId = @ExcludedStreamId,
                        @WindowStart    = @WindowStart,
                        @WindowEnd      = @WindowEnd,
                        @BusinessUnitId = @BusinessUnitId;
                END

                WHILE 1 = 1
                BEGIN
                    IF OBJECT_ID('tempdb..#Batch') IS NOT NULL
                        DROP TABLE #Batch;

                    CREATE TABLE #Batch (stream_id uniqueidentifier PRIMARY KEY);

                    INSERT INTO #Batch (stream_id)
                    SELECT TOP (@BatchSize) Q.stream_id
                    FROM dbo.BlobDeltaMissingParentsQueue Q WITH (READPAST)
                    WHERE Q.RunId = @RunId
                      AND Q.TableName = @T_TableName
                      AND Q.Processed = 0;

                    IF @@ROWCOUNT = 0
                    BEGIN
                        DROP TABLE #Batch;
                        BREAK;
                    END

                    SET @BatchNumber = @BatchNumber + 1;
                    SET @BatchStartedAt = SYSDATETIME();

                    SELECT @Sql = ScriptBody
                    FROM dbo.BlobDeltaStepScript
                    WHERE StepNumber = 2 AND ScriptKind = N'MissingParentsBatch';

                    SET @Sql = REPLACE(REPLACE(@Sql,
                        N'[SourceTableFull]', @SourceTableFull),
                        N'[TargetTableFull]', @TargetTableFull);

                    SET @Sql = REPLACE(@Sql, N'[MaxDOP]', CAST(@MaxDOP AS nvarchar(10)));

                    EXEC sp_executesql @Sql,
                        N'@BatchSize int',
                        @BatchSize = @BatchSize;

                    SET @Rows = @@ROWCOUNT;
                    SET @TotalRows = @TotalRows + @Rows;
                    SET @BatchCompletedAt = SYSDATETIME();

                    UPDATE Q
                    SET Processed = 1
                    FROM dbo.BlobDeltaMissingParentsQueue Q
                    WHERE Q.RunId = @RunId
                      AND Q.TableName = @T_TableName
                      AND Q.stream_id IN (SELECT stream_id FROM #Batch);

                    INSERT INTO dbo.BlobDeltaRunStep
                        (RunId, TableName, StepNumber, BatchNumber,
                         RowsProcessed, TotalRowsProcessed,
                         WindowStart, WindowEnd,
                         BatchStartedAt, BatchCompletedAt,
                         Status, ErrorMessage)
                    VALUES
                        (@RunId, @T_TableName, 2, @BatchNumber,
                         @Rows, @TotalRows,
                         @WindowStart, @WindowEnd,
                         @BatchStartedAt, @BatchCompletedAt,
                         N'InProgress', NULL);

                    DROP TABLE #Batch;
                END;

                INSERT INTO dbo.BlobDeltaRunStep
                    (RunId, TableName, StepNumber, BatchNumber,
                     RowsProcessed, TotalRowsProcessed,
                     WindowStart, WindowEnd,
                     BatchStartedAt, BatchCompletedAt,
                     Status, ErrorMessage)
                VALUES
                    (@RunId, @T_TableName, 2, 0,
                     0, @TotalRows,
                     @WindowStart, @WindowEnd,
                     SYSDATETIME(), SYSDATETIME(),
                     N'Completed', NULL);
            END

            -- -----------------------------------------------------------------
            -- Step 3: Children
            -- -----------------------------------------------------------------
            IF NOT EXISTS (
                SELECT 1
                FROM dbo.BlobDeltaRunStep
                WHERE RunId = @RunId
                  AND TableName = @T_TableName
                  AND StepNumber = 3
                  AND Status = N'Completed'
            )
            BEGIN
                SET @CurrentStep = 3;
                SET @BatchNumber = 0;
                SET @TotalRows   = 0;

                WHILE 1 = 1
                BEGIN
                    SET @BatchNumber = @BatchNumber + 1;
                    SET @BatchStartedAt = SYSDATETIME();

                    SELECT @Sql = ScriptBody
                    FROM dbo.BlobDeltaStepScript
                    WHERE StepNumber = 3 AND ScriptKind = N'Children';

                    SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                        N'[SourceTableFull]',          @SourceTableFull),
                        N'[TargetTableFull]',          @TargetTableFull),
                        N'[MetadataTableFull]',        @MetadataTableFull),
                        N'[MetadataIdColumn]',         @MetadataIdColumn),
                        N'[MetadataModifiedOnColumn]', @MetadataModifiedOnColumn);

                    -- Step 3 uses MAXDOP 1 in the template; no [MaxDOP] placeholder.

                    EXEC sp_executesql @Sql,
                        N'@BatchSize int,
                          @ExcludedStreamId uniqueidentifier,
                          @WindowStart datetime2(7),
                          @WindowEnd datetime2(7),
                          @BusinessUnitId uniqueidentifier',
                        @BatchSize      = @BatchSize,
                        @ExcludedStreamId = @ExcludedStreamId,
                        @WindowStart    = @WindowStart,
                        @WindowEnd      = @WindowEnd,
                        @BusinessUnitId = @BusinessUnitId;

                    SET @Rows = @@ROWCOUNT;
                    SET @TotalRows = @TotalRows + @Rows;
                    SET @BatchCompletedAt = SYSDATETIME();

                    INSERT INTO dbo.BlobDeltaRunStep
                        (RunId, TableName, StepNumber, BatchNumber,
                         RowsProcessed, TotalRowsProcessed,
                         WindowStart, WindowEnd,
                         BatchStartedAt, BatchCompletedAt,
                         Status, ErrorMessage)
                    VALUES
                        (@RunId, @T_TableName, 3, @BatchNumber,
                         @Rows, @TotalRows,
                         @WindowStart, @WindowEnd,
                         @BatchStartedAt, @BatchCompletedAt,
                         N'InProgress', NULL);

                    IF @Rows = 0 BREAK;
                END;

                INSERT INTO dbo.BlobDeltaRunStep
                    (RunId, TableName, StepNumber, BatchNumber,
                     RowsProcessed, TotalRowsProcessed,
                     WindowStart, WindowEnd,
                     BatchStartedAt, BatchCompletedAt,
                     Status, ErrorMessage)
                VALUES
                    (@RunId, @T_TableName, 3, 0,
                     0, @TotalRows,
                     @WindowStart, @WindowEnd,
                     SYSDATETIME(), SYSDATETIME(),
                     N'Completed', NULL);
            END

            -- -----------------------------------------------------------------
            -- Success for this table: advance high-watermark and clear lease.
            -- -----------------------------------------------------------------
            UPDATE dbo.BlobDeltaHighWatermark
            SET LastHighWaterModifiedOn = @WindowEnd,
                LastRunId                = @RunId,
                LastRunCompletedAt       = SYSDATETIME(),
                IsInitialFullLoadDone    = 1,
                IsRunning                = 0,
                RunLeaseExpiresAt        = NULL
            WHERE TableName = @T_TableName;
        END TRY
        BEGIN CATCH
            DECLARE @ErrMsg nvarchar(max) = ERROR_MESSAGE();

            INSERT INTO dbo.BlobDeltaRunStep
                (RunId, TableName, StepNumber, BatchNumber,
                 RowsProcessed, TotalRowsProcessed,
                 WindowStart, WindowEnd,
                 BatchStartedAt, BatchCompletedAt,
                 Status, ErrorMessage)
            VALUES
                (@RunId, @T_TableName,
                 ISNULL(@CurrentStep, 0), 999999,
                 0, ISNULL(@TotalRows, 0),
                 @WindowStart, @WindowEnd,
                 ISNULL(@BatchStartedAt, SYSDATETIME()),
                 SYSDATETIME(),
                 N'Failed', @ErrMsg);

            UPDATE dbo.BlobDeltaHighWatermark
            SET IsRunning         = 0,
                RunLeaseExpiresAt = NULL
            WHERE TableName = @T_TableName;

            UPDATE dbo.BlobDeltaRun
            SET Status       = N'Failed',
                ErrorMessage = COALESCE(ErrorMessage + N'; ', N'') + @ErrMsg,
                RunCompletedAt = SYSDATETIME()
            WHERE RunId = @RunId;

            CLOSE table_cursor;
            DEALLOCATE table_cursor;

            THROW;
        END CATCH;

        FETCH NEXT FROM table_cursor INTO @T_TableName;
    END

    CLOSE table_cursor;
    DEALLOCATE table_cursor;

    IF NOT EXISTS (SELECT 1 FROM dbo.BlobDeltaRun WHERE RunId = @RunId AND Status = N'Failed')
    BEGIN
        UPDATE dbo.BlobDeltaRun
        SET Status = N'Succeeded',
            RunCompletedAt = SYSDATETIME()
        WHERE RunId = @RunId;
    END
END;
GO

-- -----------------------------------------------------------------------------
-- Operator wrapper: simple modes for scheduled / ad-hoc runs
-- -----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE dbo.usp_BlobDelta_RunOperator
    @Mode          nvarchar(20) = N'AllTables',   -- 'AllTables','SingleTable'
    @TableName     sysname      = NULL,          -- used when @Mode = 'SingleTable'
    @BatchSize     int          = 500,
    @MaxDOP        tinyint      = 2,
    @BusinessUnitId uniqueidentifier = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunId uniqueidentifier;

    IF @Mode = N'AllTables'
    BEGIN
        EXEC dbo.usp_BlobDelta_Run
            @RunId        = @RunId OUTPUT,
            @RunType      = N'Delta',
            @TableName    = NULL,
            @BatchSize    = @BatchSize,
            @MaxDOP       = @MaxDOP,
            @Reset        = 0,
            @BusinessUnitId = @BusinessUnitId;
    END
    ELSE IF @Mode = N'SingleTable'
    BEGIN
        IF @TableName IS NULL
        BEGIN
            RAISERROR(N'TableName must be provided when Mode = ''SingleTable''.', 16, 1);
            RETURN;
        END

        EXEC dbo.usp_BlobDelta_Run
            @RunId        = @RunId OUTPUT,
            @RunType      = N'Delta',
            @TableName    = @TableName,
            @BatchSize    = @BatchSize,
            @MaxDOP       = @MaxDOP,
            @Reset        = 0,
            @BusinessUnitId = @BusinessUnitId;
    END
    ELSE
    BEGIN
        RAISERROR(N'Unsupported Mode. Use ''AllTables'' or ''SingleTable''.', 16, 1);
        RETURN;
    END

    SELECT @RunId AS RunId;
END;
GO

PRINT N'BlobDeltaJobs engine and operator wrapper created/updated successfully.';
GO

