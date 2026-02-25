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
    @RunType      nvarchar(20)     = N'Delta',  -- 'Full','Delta','DryRun'
    @TableName    sysname          = NULL,      -- NULL = all active tables (or filtered by @TargetDatabase)
    @BatchSize    int              = 500,
    @MaxDOP       tinyint          = 2,
    @Reset        bit              = 0,
    @DryRun       bit              = 0,         -- If 1, print dynamic SQL instead of executing it
    @BusinessUnitId uniqueidentifier = NULL,
    @TargetDatabase sysname        = NULL      -- Optional: filter to tables where TargetDatabase matches
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now datetime2(7) = SYSDATETIME();
    DECLARE @RequestedBy sysname = SUSER_SNAME();

    IF @DryRun = 1 AND @RunType <> N'DryRun'
    BEGIN
        SET @RunType = N'DryRun';
    END

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
      AND (@TableName IS NULL OR c.TableName = @TableName)
      AND (@TargetDatabase IS NULL OR c.TargetDatabase = @TargetDatabase);

    -- Diagnostic: report how many tables will be processed (helps when no rows are created)
    DECLARE @TablesToProcessCount int = (SELECT COUNT(*) FROM #TablesToProcess);
    IF @TablesToProcessCount = 0
    BEGIN
        PRINT N'No tables to process. Check that BlobDeltaTableConfig has a row where:';
        PRINT N'  - TableName = @TableName (e.g. YnysMon_LA_FileTable.dbo.ReferralAttachment)';
        PRINT N'  - TargetDatabase = @TargetDatabase when provided (e.g. YnysMon_LA_FileTable)';
        PRINT N'  - IsActive = 1. Run 04_BlobDeltaJobs_Seed_Config.sql for new databases with the correct @FileTableDatabase.';
        -- Still complete the run (update BlobDeltaRun status) but do nothing else
    END
    ELSE
    BEGIN
        PRINT N'Processing ' + CAST(@TablesToProcessCount AS nvarchar(10)) + N' table(s):';
        DECLARE @TList sysname;
        DECLARE list_cursor CURSOR FAST_FORWARD FOR SELECT TableName FROM #TablesToProcess ORDER BY TableName;
        OPEN list_cursor;
        FETCH NEXT FROM list_cursor INTO @TList;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            PRINT N'  - ' + @TList;
            FETCH NEXT FROM list_cursor INTO @TList;
        END
        CLOSE list_cursor;
        DEALLOCATE list_cursor;
    END

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
            @BusinessUnitTableFull    nvarchar(776),
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

            -- Validate that we got the required table names.
            IF @TargetTableFull IS NULL
            BEGIN
                RAISERROR(N'Failed to resolve TargetTableFull for table ''%s''.', 16, 1, @T_TableName);
            END

            -- Derive the Business Unit lookup table (e.g. <TargetDB>.<Schema>.LA_BU) from the target table.
            DECLARE @ParsedTargetDatabase sysname = PARSENAME(@TargetTableFull, 3);
            DECLARE @ParsedTargetSchema sysname = PARSENAME(@TargetTableFull, 2);
            
            IF @ParsedTargetDatabase IS NULL OR @ParsedTargetSchema IS NULL
            BEGIN
                RAISERROR(N'Failed to parse TargetTableFull ''%s'' for table ''%s''. Expected format: Database.Schema.Table', 16, 1, @TargetTableFull, @T_TableName);
            END
            
            SET @BusinessUnitTableFull = @ParsedTargetDatabase + N'.' + @ParsedTargetSchema + N'.LA_BU';

            -- Validate that the LA_BU table exists (helps catch configuration issues early)
            DECLARE @ButTableExists bit = 0;
            DECLARE @CheckButSql nvarchar(max) = N'
            IF EXISTS (
                SELECT 1 
                FROM ' + QUOTENAME(@ParsedTargetDatabase) + N'.sys.tables t
                INNER JOIN ' + QUOTENAME(@ParsedTargetDatabase) + N'.sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = N''LA_BU'' AND s.name = ' + QUOTENAME(@ParsedTargetSchema, '''') + N'
            )
                SELECT @Exists = 1
            ELSE
                SELECT @Exists = 0;';
            
            DECLARE @ButExistsParam nvarchar(max) = N'@Exists bit OUTPUT';
            EXEC sp_executesql @CheckButSql, @ButExistsParam, @Exists = @ButTableExists OUTPUT;
            
            IF @ButTableExists = 0
            BEGIN
                DECLARE @ButErrorMsg nvarchar(max) = N'LA_BU table not found at ' + @BusinessUnitTableFull + 
                    N' for table ''%s''. Please ensure the LA_BU table exists in the target database. ' +
                    N'TargetDatabase: ' + @ParsedTargetDatabase + N', TargetSchema: ' + @ParsedTargetSchema;
                RAISERROR(@ButErrorMsg, 16, 1, @T_TableName);
            END
            ELSE
            BEGIN
                -- Check if businessunit column exists
                DECLARE @ButColumnExists bit = 0;
                DECLARE @CheckButColumnSql nvarchar(max) = N'
                IF EXISTS (
                    SELECT 1 
                    FROM ' + QUOTENAME(@ParsedTargetDatabase) + N'.sys.columns c
                    INNER JOIN ' + QUOTENAME(@ParsedTargetDatabase) + N'.sys.tables t ON c.object_id = t.object_id
                    INNER JOIN ' + QUOTENAME(@ParsedTargetDatabase) + N'.sys.schemas s ON t.schema_id = s.schema_id
                    WHERE t.name = N''LA_BU'' AND s.name = ' + QUOTENAME(@ParsedTargetSchema, '''') + 
                    N' AND c.name = N''businessunit''
                )
                    SELECT @Exists = 1
                ELSE
                    SELECT @Exists = 0;';
                
                EXEC sp_executesql @CheckButColumnSql, @ButExistsParam, @Exists = @ButColumnExists OUTPUT;
                
                IF @ButColumnExists = 0
                BEGIN
                    DECLARE @ButColumnErrorMsg nvarchar(max) = N'Column ''businessunit'' not found in table ' + @BusinessUnitTableFull + 
                        N' for table ''%s''. Please verify the LA_BU table structure matches the expected schema.';
                    RAISERROR(@ButColumnErrorMsg, 16, 1, @T_TableName);
                END
            END

            IF @SafetyBufferMinutes IS NULL SET @SafetyBufferMinutes = 240;

            -- Load high-watermark and compute window.
            SELECT @LastHighWater = h.LastHighWaterModifiedOn
            FROM dbo.BlobDeltaHighWatermark h
            WHERE h.TableName = @T_TableName;

            SET @WindowEnd = DATEADD(MINUTE, -@SafetyBufferMinutes, @RunStartForTable);
            -- For RunType 'Full', always use a full window (ignore stored high-watermark) so initial/full loads work.
            IF @RunType = N'Full'
            BEGIN
                SET @WindowStart = DATEADD(YEAR, -25, @WindowEnd);
                PRINT N'Table ' + @T_TableName + N': RunType=Full, window last 25 years; WindowStart=' + CONVERT(nvarchar(30), @WindowStart, 126) + N', WindowEnd=' + CONVERT(nvarchar(30), @WindowEnd, 126);
            END
            ELSE
            BEGIN
                SET @WindowStart =
                    CASE
                        WHEN @LastHighWater IS NULL
                            THEN DATEADD(DAY, -365, @WindowEnd) -- conservative baseline; can be adjusted.
                        ELSE DATEADD(MINUTE, -@SafetyBufferMinutes, @LastHighWater)
                    END;
                PRINT N'Table ' + @T_TableName + N': WindowStart=' + CONVERT(nvarchar(30), @WindowStart, 126) + N', WindowEnd=' + CONVERT(nvarchar(30), @WindowEnd, 126) + N', LastHighWater=' + ISNULL(CONVERT(nvarchar(30), @LastHighWater, 126), N'NULL');
            END

            -- Optional reset support for this run/table.
            IF @Reset = 1 AND @DryRun = 0
            BEGIN
                DELETE FROM dbo.BlobDeltaMissingParentsQueue
                WHERE RunId = @RunId AND TableName = @T_TableName;

                DELETE FROM dbo.BlobDeltaRunStep
                WHERE RunId = @RunId AND TableName = @T_TableName;
            END

            -- Acquire simple lease for this table (skip in dry-run mode).
            IF @DryRun = 0
            BEGIN
                UPDATE h
                SET IsRunning = 1,
                    RunLeaseExpiresAt = DATEADD(MINUTE, 60, @RunStartForTable),
                    LastRunId = @RunId
                FROM dbo.BlobDeltaHighWatermark h
                WHERE h.TableName = @T_TableName;
            END

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

                    IF @Sql IS NULL
                    BEGIN
                        RAISERROR(N'Script template not found for StepNumber=1, ScriptKind=''Roots''.', 16, 1);
                    END

                    -- Validate required variables are set before replacement.
                    IF @BusinessUnitTableFull IS NULL
                    BEGIN
                        RAISERROR(N'BusinessUnitTableFull is NULL for table ''%s''. TargetTableFull: ''%s''', 16, 1, @T_TableName, @TargetTableFull);
                    END

                    -- Replace placeholders with table/column names, BU table, and MaxDOP.
                    SET @Sql = REPLACE(@Sql, N'[SourceTableFull]',          ISNULL(@SourceTableFull, N''));
                    SET @Sql = REPLACE(@Sql, N'[TargetTableFull]',          ISNULL(@TargetTableFull, N''));
                    SET @Sql = REPLACE(@Sql, N'[MetadataTableFull]',        ISNULL(@MetadataTableFull, N''));
                    SET @Sql = REPLACE(@Sql, N'[MetadataIdColumn]',         ISNULL(@MetadataIdColumn, N''));
                    SET @Sql = REPLACE(@Sql, N'[MetadataModifiedOnColumn]', ISNULL(@MetadataModifiedOnColumn, N''));
                    SET @Sql = REPLACE(@Sql, N'[BusinessUnitTableFull]',    @BusinessUnitTableFull);
                    SET @Sql = REPLACE(@Sql, N'[MaxDOP]',                   CAST(@MaxDOP AS nvarchar(10)));

                    IF @DryRun = 1
                    BEGIN
                        PRINT N'==== DRY RUN (Step 1 - Roots) for table ' + @T_TableName + N', batch ' + CAST(@BatchNumber AS nvarchar(10)) + N' ====';
                        PRINT @Sql;
                        PRINT N'-- Parameters:'
                            + N' @BatchSize=' + CAST(@BatchSize AS nvarchar(20))
                            + N', @ExcludedStreamId=' + CAST(@ExcludedStreamId AS nvarchar(50))
                            + N', @WindowStart=' + CAST(@WindowStart AS nvarchar(50))
                            + N', @WindowEnd=' + CAST(@WindowEnd AS nvarchar(50))
                            + N', @BusinessUnitId=' + ISNULL(CAST(@BusinessUnitId AS nvarchar(50)), N'NULL');
                        -- In dry run we only print the generated SQL once for this step.
                        BREAK;
                    END

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

                    IF @Rows = 0
                    BEGIN
                        IF @BatchNumber = 1
                            PRINT N'Step 1 (Roots) for ' + @T_TableName + N': 0 rows in first batch. Check source/metadata data in window and LA_BU join.';
                        BREAK;
                    END
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
                    -- Validate BusinessUnitTableFull is set before queue population
                    IF @BusinessUnitTableFull IS NULL
                    BEGIN
                        RAISERROR(N'BusinessUnitTableFull is NULL for table ''%s'' in Step 2. TargetTableFull: ''%s''', 16, 1, @T_TableName, @TargetTableFull);
                    END
                    
                    SELECT @Sql = ScriptBody
                    FROM dbo.BlobDeltaQueuePopulationScript
                    WHERE TableName = @T_TableName;
                    
                    IF @Sql IS NULL
                    BEGIN
                        RAISERROR(N'Queue population script not found for table ''%s''.', 16, 1, @T_TableName);
                    END

                    -- Replace placeholders, including BusinessUnitTableFull so we don't leave a literal
                    -- [BusinessUnitTableFull] token in the dynamic SQL.
                    SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                        N'[SourceTableFull]',          ISNULL(@SourceTableFull, N'')),
                        N'[TargetTableFull]',          ISNULL(@TargetTableFull, N'')),
                        N'[MetadataTableFull]',        ISNULL(@MetadataTableFull, N'')),
                        N'[MetadataIdColumn]',         ISNULL(@MetadataIdColumn, N'')),
                        N'[MetadataModifiedOnColumn]', ISNULL(@MetadataModifiedOnColumn, N'')),
                        N'[BusinessUnitTableFull]',    ISNULL(@BusinessUnitTableFull, N''));

                    IF @DryRun = 1
                    BEGIN
                        PRINT N'==== DRY RUN (Step 2 - Queue population) for table ' + @T_TableName + N' ====';
                        PRINT @Sql;
                        PRINT N'-- Parameters:'
                            + N' @RunId=' + CAST(@RunId AS nvarchar(50))
                            + N', @TableName=' + @T_TableName
                            + N', @ExcludedStreamId=' + CAST(@ExcludedStreamId AS nvarchar(50))
                            + N', @WindowStart=' + CAST(@WindowStart AS nvarchar(50))
                            + N', @WindowEnd=' + CAST(@WindowEnd AS nvarchar(50))
                            + N', @BusinessUnitId=' + ISNULL(CAST(@BusinessUnitId AS nvarchar(50)), N'NULL');
                    END
                    ELSE
                    BEGIN
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

                    SET @Sql = REPLACE(@Sql, N'[SourceTableFull]', @SourceTableFull);
                    SET @Sql = REPLACE(@Sql, N'[TargetTableFull]', @TargetTableFull);
                    SET @Sql = REPLACE(@Sql, N'[MaxDOP]',          CAST(@MaxDOP AS nvarchar(10)));

                    IF @DryRun = 1
                    BEGIN
                        PRINT N'==== DRY RUN (Step 2 - MissingParentsBatch) for table ' + @T_TableName + N', batch ' + CAST(@BatchNumber AS nvarchar(10)) + N' ====';
                        PRINT @Sql;
                        PRINT N'-- Parameters: @BatchSize=' + CAST(@BatchSize AS nvarchar(20));
                        -- In dry run we only print the generated SQL once for this step.
                        BREAK;
                    END

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

                    IF @Sql IS NULL
                    BEGIN
                        RAISERROR(N'Script template not found for StepNumber=3, ScriptKind=''Children''.', 16, 1);
                    END

                    -- Validate required variables are set before replacement.
                    IF @BusinessUnitTableFull IS NULL
                    BEGIN
                        RAISERROR(N'BusinessUnitTableFull is NULL for table ''%s''. TargetTableFull: ''%s''', 16, 1, @T_TableName, @TargetTableFull);
                    END

                    SET @Sql = REPLACE(@Sql, N'[SourceTableFull]',          ISNULL(@SourceTableFull, N''));
                    SET @Sql = REPLACE(@Sql, N'[TargetTableFull]',          ISNULL(@TargetTableFull, N''));
                    SET @Sql = REPLACE(@Sql, N'[MetadataTableFull]',        ISNULL(@MetadataTableFull, N''));
                    SET @Sql = REPLACE(@Sql, N'[MetadataIdColumn]',         ISNULL(@MetadataIdColumn, N''));
                    SET @Sql = REPLACE(@Sql, N'[MetadataModifiedOnColumn]', ISNULL(@MetadataModifiedOnColumn, N''));
                    SET @Sql = REPLACE(@Sql, N'[BusinessUnitTableFull]',    @BusinessUnitTableFull);

                    -- Step 3 uses MAXDOP 1 in the template; no [MaxDOP] placeholder.

                    IF @DryRun = 1
                    BEGIN
                        PRINT N'==== DRY RUN (Step 3 - Children) for table ' + @T_TableName + N', batch ' + CAST(@BatchNumber AS nvarchar(10)) + N' ====';
                        PRINT @Sql;
                        PRINT N'-- Parameters:'
                            + N' @BatchSize=' + CAST(@BatchSize AS nvarchar(20))
                            + N', @ExcludedStreamId=' + CAST(@ExcludedStreamId AS nvarchar(50))
                            + N', @WindowStart=' + CAST(@WindowStart AS nvarchar(50))
                            + N', @WindowEnd=' + CAST(@WindowEnd AS nvarchar(50))
                            + N', @BusinessUnitId=' + ISNULL(CAST(@BusinessUnitId AS nvarchar(50)), N'NULL');
                        -- In dry run we only print the generated SQL once for this step.
                        BREAK;
                    END

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
            -- In dry-run mode, we do not persist high-watermarks or lease changes.
            -- -----------------------------------------------------------------
            IF @DryRun = 0
            BEGIN
                UPDATE dbo.BlobDeltaHighWatermark
                SET LastHighWaterModifiedOn = @WindowEnd,
                    LastRunId                = @RunId,
                    LastRunCompletedAt       = SYSDATETIME(),
                    IsInitialFullLoadDone    = 1,
                    IsRunning                = 0,
                    RunLeaseExpiresAt        = NULL
                WHERE TableName = @T_TableName;
            END
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

            IF @DryRun = 0
            BEGIN
                UPDATE dbo.BlobDeltaHighWatermark
                SET IsRunning         = 0,
                    RunLeaseExpiresAt = NULL
                WHERE TableName = @T_TableName;
            END

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
    @DryRun        bit          = 0,             -- If 1, print dynamic SQL instead of executing it
    @BusinessUnitId uniqueidentifier = NULL,
    @TargetDatabase sysname     = NULL           -- Optional: filter to tables where TargetDatabase matches (works with 'AllTables' mode)
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
            @DryRun       = @DryRun,
            @BusinessUnitId = @BusinessUnitId,
            @TargetDatabase = @TargetDatabase;
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
            @DryRun       = @DryRun,
            @BusinessUnitId = @BusinessUnitId,
            @TargetDatabase = @TargetDatabase;
    END
    ELSE
    BEGIN
        RAISERROR(N'Unsupported Mode. Use ''AllTables'' or ''SingleTable''.', 16, 1);
        RETURN;
    END

    SELECT @RunId AS RunId;
END;
GO

-- -----------------------------------------------------------------------------
-- Remove database: Delete all configuration and related data for a database
-- -----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE dbo.usp_BlobDelta_RemoveDatabase
    @DatabaseName sysname,  -- Database name to remove (matches SourceDatabase, TargetDatabase, or MetadataDatabase)
    @DryRun       bit = 0   -- If 1, only report what would be deleted without actually deleting
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DeletedCounts TABLE
    (
        TableName          sysname,
        RowsDeleted        int,
        Description        nvarchar(256)
    );

    DECLARE @TableNamesToDelete TABLE (TableName sysname PRIMARY KEY);
    DECLARE @RowsAffected int;
    DECLARE @TotalRowsDeleted int = 0;

    -- Find all TableName entries where the database appears in SourceDatabase, TargetDatabase, or MetadataDatabase
    INSERT INTO @TableNamesToDelete (TableName)
    SELECT DISTINCT c.TableName
    FROM dbo.BlobDeltaTableConfig c
    WHERE c.SourceDatabase = @DatabaseName
       OR c.TargetDatabase = @DatabaseName
       OR c.MetadataDatabase = @DatabaseName;

    IF NOT EXISTS (SELECT 1 FROM @TableNamesToDelete)
    BEGIN
        PRINT N'No configuration found for database ' + QUOTENAME(@DatabaseName) + N'. Nothing to remove.';
        RETURN;
    END

    IF @DryRun = 1
    BEGIN
        DECLARE @RunStepCount int;
        DECLARE @QueueCount int;
        DECLARE @DeletionLogCount int;
        DECLARE @HighWatermarkCount int;
        DECLARE @QueueScriptCount int;
        DECLARE @TableConfigCount int;

        PRINT N'DRY RUN MODE: Would delete configuration for database ' + QUOTENAME(@DatabaseName) + N':';
        PRINT N'';
        
        SELECT 
            c.TableName,
            c.SourceDatabase,
            c.TargetDatabase,
            c.MetadataDatabase,
            c.IsActive
        FROM dbo.BlobDeltaTableConfig c
        INNER JOIN @TableNamesToDelete t ON c.TableName = t.TableName
        ORDER BY c.TableName;

        SELECT 
            COUNT(*) AS TablesAffected,
            SUM(CASE WHEN c.SourceDatabase = @DatabaseName THEN 1 ELSE 0 END) AS AsSourceDatabase,
            SUM(CASE WHEN c.TargetDatabase = @DatabaseName THEN 1 ELSE 0 END) AS AsTargetDatabase,
            SUM(CASE WHEN c.MetadataDatabase = @DatabaseName THEN 1 ELSE 0 END) AS AsMetadataDatabase
        FROM dbo.BlobDeltaTableConfig c
        INNER JOIN @TableNamesToDelete t ON c.TableName = t.TableName;

        -- Calculate counts into variables
        SELECT @RunStepCount = COUNT(*)
        FROM dbo.BlobDeltaRunStep s
        INNER JOIN @TableNamesToDelete t ON s.TableName = t.TableName;

        SELECT @QueueCount = COUNT(*)
        FROM dbo.BlobDeltaMissingParentsQueue q
        INNER JOIN @TableNamesToDelete t ON q.TableName = t.TableName;

        SELECT @DeletionLogCount = COUNT(*)
        FROM dbo.BlobDeltaDeletionLog d
        INNER JOIN @TableNamesToDelete t ON d.TableName = t.TableName;

        SELECT @HighWatermarkCount = COUNT(*)
        FROM dbo.BlobDeltaHighWatermark h
        INNER JOIN @TableNamesToDelete t ON h.TableName = t.TableName;

        SELECT @QueueScriptCount = COUNT(*)
        FROM dbo.BlobDeltaQueuePopulationScript s
        INNER JOIN @TableNamesToDelete t ON s.TableName = t.TableName;

        SELECT @TableConfigCount = COUNT(*)
        FROM @TableNamesToDelete;

        PRINT N'';
        PRINT N'Related data that would be deleted:';
        PRINT N'  - BlobDeltaRunStep rows: ' + CAST(@RunStepCount AS nvarchar(20));
        PRINT N'  - BlobDeltaMissingParentsQueue rows: ' + CAST(@QueueCount AS nvarchar(20));
        PRINT N'  - BlobDeltaDeletionLog rows: ' + CAST(@DeletionLogCount AS nvarchar(20));
        PRINT N'  - BlobDeltaHighWatermark rows: ' + CAST(@HighWatermarkCount AS nvarchar(20));
        PRINT N'  - BlobDeltaQueuePopulationScript rows: ' + CAST(@QueueScriptCount AS nvarchar(20));
        PRINT N'  - BlobDeltaTableConfig rows: ' + CAST(@TableConfigCount AS nvarchar(20));
        RETURN;
    END

    BEGIN TRANSACTION;

    BEGIN TRY
        -- 1. Delete from BlobDeltaRunStep (references RunId, but has TableName column)
        DELETE s
        FROM dbo.BlobDeltaRunStep s
        INNER JOIN @TableNamesToDelete t ON s.TableName = t.TableName;
        SET @RowsAffected = @@ROWCOUNT;
        INSERT INTO @DeletedCounts VALUES (N'BlobDeltaRunStep', @RowsAffected, N'Run step records');
        SET @TotalRowsDeleted = @TotalRowsDeleted + @RowsAffected;

        -- 2. Delete from BlobDeltaMissingParentsQueue
        DELETE q
        FROM dbo.BlobDeltaMissingParentsQueue q
        INNER JOIN @TableNamesToDelete t ON q.TableName = t.TableName;
        SET @RowsAffected = @@ROWCOUNT;
        INSERT INTO @DeletedCounts VALUES (N'BlobDeltaMissingParentsQueue', @RowsAffected, N'Missing parents queue entries');
        SET @TotalRowsDeleted = @TotalRowsDeleted + @RowsAffected;

        -- 3. Delete from BlobDeltaDeletionLog
        DELETE d
        FROM dbo.BlobDeltaDeletionLog d
        INNER JOIN @TableNamesToDelete t ON d.TableName = t.TableName;
        SET @RowsAffected = @@ROWCOUNT;
        INSERT INTO @DeletedCounts VALUES (N'BlobDeltaDeletionLog', @RowsAffected, N'Deletion log entries');
        SET @TotalRowsDeleted = @TotalRowsDeleted + @RowsAffected;

        -- 4. Delete from BlobDeltaQueuePopulationScript (TableName is PK)
        DELETE s
        FROM dbo.BlobDeltaQueuePopulationScript s
        INNER JOIN @TableNamesToDelete t ON s.TableName = t.TableName;
        SET @RowsAffected = @@ROWCOUNT;
        INSERT INTO @DeletedCounts VALUES (N'BlobDeltaQueuePopulationScript', @RowsAffected, N'Queue population scripts');
        SET @TotalRowsDeleted = @TotalRowsDeleted + @RowsAffected;

        -- 5. Delete from BlobDeltaHighWatermark (TableName is PK, references BlobDeltaTableConfig)
        DELETE h
        FROM dbo.BlobDeltaHighWatermark h
        INNER JOIN @TableNamesToDelete t ON h.TableName = t.TableName;
        SET @RowsAffected = @@ROWCOUNT;
        INSERT INTO @DeletedCounts VALUES (N'BlobDeltaHighWatermark', @RowsAffected, N'High watermark records');
        SET @TotalRowsDeleted = @TotalRowsDeleted + @RowsAffected;

        -- 6. Delete from BlobDeltaTableConfig (TableName is PK) - last, as other tables reference it
        DELETE c
        FROM dbo.BlobDeltaTableConfig c
        INNER JOIN @TableNamesToDelete t ON c.TableName = t.TableName;
        SET @RowsAffected = @@ROWCOUNT;
        INSERT INTO @DeletedCounts VALUES (N'BlobDeltaTableConfig', @RowsAffected, N'Table configuration records');
        SET @TotalRowsDeleted = @TotalRowsDeleted + @RowsAffected;

        COMMIT TRANSACTION;

        PRINT N'Successfully removed database ' + QUOTENAME(@DatabaseName) + N' from BlobDeltaJobs configuration.';
        PRINT N'Total rows deleted: ' + CAST(@TotalRowsDeleted AS nvarchar(20));
        PRINT N'';
        PRINT N'Breakdown by table:';
        
        SELECT 
            TableName AS [Table],
            RowsDeleted AS [Rows Deleted],
            Description
        FROM @DeletedCounts
        WHERE RowsDeleted > 0
        ORDER BY TableName;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        DECLARE @ErrMsg nvarchar(max) = ERROR_MESSAGE();
        DECLARE @ErrSeverity int = ERROR_SEVERITY();
        DECLARE @ErrState int = ERROR_STATE();

        PRINT N'Error removing database ' + QUOTENAME(@DatabaseName) + N': ' + @ErrMsg;
        THROW;
    END CATCH
END;
GO

PRINT N'BlobDeltaJobs engine and operator wrapper created/updated successfully.';
GO

