-- =============================================================================
-- Blob Migration V2: Master procedure (script-driven, resumable)
-- Runs migration for a single @TableName using scripts from BlobMigrationStepScript
-- and BlobMigrationQueuePopulationScript; config from BlobMigrationTableConfig.
--
-- Placeholders in scripts: [SourceTableFull], [TargetTableFull], [MetadataTableFull],
-- [MetadataIdColumn], [MaxDOP] (Step 1 only). Replaced at runtime from config + params.
--
-- Example:
--   New run:   DECLARE @R UNIQUEIDENTIFIER; EXEC dbo.usp_BlobMigration_Run_V2 @BatchSize=500, @MaxDOP=2, @RunId=@R OUTPUT; SELECT @R AS RunId;
--   Resume:    EXEC dbo.usp_BlobMigration_Run_V2 @RunId='<run-id>';
--   Other table: EXEC dbo.usp_BlobMigration_Run_V2 @TableName=N'Gwent_LA_FileTable.dbo.ClientAttachment', @RunId=@R OUTPUT;
-- =============================================================================

USE Gwent_LA_FileTable;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_BlobMigration_Run_V2]
    @BatchSize INT              = 500,
    @MaxDOP    TINYINT          = 2,
    @Reset     BIT              = 0,
    @RunId     UNIQUEIDENTIFIER = NULL OUTPUT,
    @TableName NVARCHAR(259)    = N'Gwent_LA_FileTable.dbo.ReferralAttachment'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStartedAt       DATETIME2(7);
    DECLARE @CurrentStep        TINYINT = 0;
    DECLARE @BatchNumber        INT = 0;
    DECLARE @RowsInserted       INT;
    DECLARE @TotalRowsInserted  INT = 0;
    DECLARE @BatchStartedAt     DATETIME2(7);
    DECLARE @BatchCompletedAt   DATETIME2(7);
    DECLARE @Sql                NVARCHAR(MAX);
    DECLARE @ExcludedStreamId   UNIQUEIDENTIFIER = '7F8D53EC-B98C-F011-B86B-005056A2DD37';

    -- Config (loaded once for this @TableName)
    DECLARE @SourceTableFull    NVARCHAR(389);
    DECLARE @TargetTableFull    NVARCHAR(389);
    DECLARE @MetadataTableFull NVARCHAR(389);
    DECLARE @MetadataIdColumn   NVARCHAR(128);

    BEGIN TRY
        -- ---------------------------------------------------------------------
        -- Validate @TableName: config must exist and be active
        -- ---------------------------------------------------------------------
        SELECT @SourceTableFull    = c.SourceDatabase    + N'.' + c.SourceSchema    + N'.' + c.SourceTable,
               @TargetTableFull    = c.TargetDatabase    + N'.' + c.TargetSchema    + N'.' + c.TargetTable,
               @MetadataTableFull  = c.MetadataDatabase + N'.' + c.MetadataSchema   + N'.' + c.MetadataTable,
               @MetadataIdColumn   = c.MetadataIdColumn
        FROM dbo.BlobMigrationTableConfig c
        WHERE c.TableName = @TableName AND c.IsActive = 1;

        IF @SourceTableFull IS NULL
        BEGIN
            RAISERROR(N'TableName ''%s'' not found in BlobMigrationTableConfig or IsActive = 0.', 16, 1, @TableName);
            RETURN;
        END

        -- ---------------------------------------------------------------------
        -- Resolve run: new vs resume
        -- ---------------------------------------------------------------------
        IF @RunId IS NULL
        BEGIN
            SET @RunId         = NEWID();
            SET @RunStartedAt  = SYSDATETIME();
        END
        ELSE
        BEGIN
            SELECT TOP (1) @RunStartedAt = RunStartedAt
            FROM dbo.BlobMigrationProgress
            WHERE RunId = @RunId AND TableName = @TableName;
            IF @RunStartedAt IS NULL
                SET @RunStartedAt = SYSDATETIME();
        END

        -- ---------------------------------------------------------------------
        -- Optional reset: clear progress and queue for this run + table
        -- ---------------------------------------------------------------------
        IF @Reset = 1
        BEGIN
            DELETE FROM dbo.BlobMigration_MissingParentsQueue WHERE RunId = @RunId AND TableName = @TableName;
            DELETE FROM dbo.BlobMigrationProgress            WHERE RunId = @RunId AND TableName = @TableName;
        END

        -- ---------------------------------------------------------------------
        -- Step 1: Roots (script from BlobMigrationStepScript; [MaxDOP] replaced)
        -- ---------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigrationProgress
                       WHERE RunId = @RunId AND TableName = @TableName AND Step = 1 AND Status = 'Completed')
        BEGIN
            SET @CurrentStep = 1;
            SET @BatchNumber = COALESCE((
                SELECT MAX(BatchNumber)
                FROM dbo.BlobMigrationProgress
                WHERE RunId = @RunId AND TableName = @TableName AND Step = 1 AND Status <> 'Completed'
            ), 0);
            SET @TotalRowsInserted = COALESCE((
                SELECT MAX(TotalRowsInserted)
                FROM dbo.BlobMigrationProgress
                WHERE RunId = @RunId AND TableName = @TableName AND Step = 1 AND Status <> 'Completed'
            ), 0);

            WHILE 1 = 1
            BEGIN
                SET @BatchNumber     = @BatchNumber + 1;
                SET @BatchStartedAt  = SYSDATETIME();

                SELECT @Sql = ScriptBody
                FROM dbo.BlobMigrationStepScript
                WHERE StepNumber = 1 AND ScriptKind = N'BatchInsert';

                SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                    N'[SourceTableFull]',    @SourceTableFull),
                    N'[TargetTableFull]',   @TargetTableFull),
                    N'[MetadataTableFull]', @MetadataTableFull),
                    N'[MetadataIdColumn]',  @MetadataIdColumn),
                    N'[MaxDOP]',            CAST(@MaxDOP AS NVARCHAR(10)));

                EXEC sp_executesql @Sql,
                    N'@BatchSize INT, @ExcludedStreamId UNIQUEIDENTIFIER',
                    @BatchSize, @ExcludedStreamId;

                SET @RowsInserted       = @@ROWCOUNT;
                SET @TotalRowsInserted  = @TotalRowsInserted + @RowsInserted;
                SET @BatchCompletedAt   = SYSDATETIME();

                INSERT INTO dbo.BlobMigrationProgress
                    (RunId, TableName, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                     BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
                VALUES
                    (@RunId, @TableName, @RunStartedAt, 1, @BatchNumber, @RowsInserted, @TotalRowsInserted,
                     @BatchStartedAt, @BatchCompletedAt, 'InProgress', NULL);

                IF @RowsInserted = 0
                    BREAK;
            END

            INSERT INTO dbo.BlobMigrationProgress
                (RunId, TableName, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                 BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
            VALUES
                (@RunId, @TableName, @RunStartedAt, 1, 0, 0, @TotalRowsInserted,
                 SYSDATETIME(), SYSDATETIME(), 'Completed', NULL);
        END

        -- ---------------------------------------------------------------------
        -- Step 2: Missing parents (queue from BlobMigrationQueuePopulationScript, then batch from StepScript)
        -- ---------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigrationProgress
                       WHERE RunId = @RunId AND TableName = @TableName AND Step = 2 AND Status = 'Completed')
        BEGIN
            SET @CurrentStep = 2;
            SET @BatchNumber = COALESCE((
                SELECT MAX(BatchNumber)
                FROM dbo.BlobMigrationProgress
                WHERE RunId = @RunId AND TableName = @TableName AND Step = 2 AND Status <> 'Completed'
            ), 0);
            SET @TotalRowsInserted = COALESCE((
                SELECT MAX(TotalRowsInserted)
                FROM dbo.BlobMigrationProgress
                WHERE RunId = @RunId AND TableName = @TableName AND Step = 2 AND Status <> 'Completed'
            ), 0);

            -- Populate queue if empty (script from BlobMigrationQueuePopulationScript)
            IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigration_MissingParentsQueue
                           WHERE RunId = @RunId AND TableName = @TableName)
            BEGIN
                SELECT @Sql = ScriptBody
                FROM dbo.BlobMigrationQueuePopulationScript
                WHERE TableName = @TableName;

                SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                    N'[SourceTableFull]',    @SourceTableFull),
                    N'[TargetTableFull]',   @TargetTableFull),
                    N'[MetadataTableFull]', @MetadataTableFull),
                    N'[MetadataIdColumn]',  @MetadataIdColumn),
                    N'[MaxDOP]',            CAST(@MaxDOP AS NVARCHAR(10)));

                EXEC sp_executesql @Sql,
                    N'@RunId UNIQUEIDENTIFIER, @TableName NVARCHAR(259), @ExcludedStreamId UNIQUEIDENTIFIER',
                    @RunId, @TableName, @ExcludedStreamId;
            END

            WHILE 1 = 1
            BEGIN
                IF OBJECT_ID('tempdb..#Batch') IS NOT NULL
                    DROP TABLE #Batch;
                CREATE TABLE #Batch (stream_id UNIQUEIDENTIFIER PRIMARY KEY);

                INSERT INTO #Batch (stream_id)
                SELECT TOP (@BatchSize) stream_id
                FROM dbo.BlobMigration_MissingParentsQueue
                WHERE RunId = @RunId AND TableName = @TableName AND Processed = 0;

                IF @@ROWCOUNT = 0
                BEGIN
                    DROP TABLE #Batch;
                    BREAK;
                END

                SET @BatchNumber    = @BatchNumber + 1;
                SET @BatchStartedAt = SYSDATETIME();

                SELECT @Sql = ScriptBody
                FROM dbo.BlobMigrationStepScript
                WHERE StepNumber = 2 AND ScriptKind = N'BatchInsert';

                SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                    N'[SourceTableFull]',    @SourceTableFull),
                    N'[TargetTableFull]',   @TargetTableFull),
                    N'[MetadataTableFull]', @MetadataTableFull),
                    N'[MetadataIdColumn]',  @MetadataIdColumn),
                    N'[MaxDOP]',            CAST(@MaxDOP AS NVARCHAR(10)));

                EXEC sp_executesql @Sql;

                SET @RowsInserted       = @@ROWCOUNT;
                SET @TotalRowsInserted  = @TotalRowsInserted + @RowsInserted;
                SET @BatchCompletedAt   = SYSDATETIME();

                UPDATE dbo.BlobMigration_MissingParentsQueue
                SET Processed = 1
                WHERE RunId = @RunId AND TableName = @TableName AND stream_id IN (SELECT stream_id FROM #Batch);

                INSERT INTO dbo.BlobMigrationProgress
                    (RunId, TableName, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                     BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
                VALUES
                    (@RunId, @TableName, @RunStartedAt, 2, @BatchNumber, @RowsInserted, @TotalRowsInserted,
                     @BatchStartedAt, @BatchCompletedAt, 'InProgress', NULL);

                DROP TABLE #Batch;
            END

            INSERT INTO dbo.BlobMigrationProgress
                (RunId, TableName, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                 BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
            VALUES
                (@RunId, @TableName, @RunStartedAt, 2, 0, 0, @TotalRowsInserted,
                 SYSDATETIME(), SYSDATETIME(), 'Completed', NULL);
        END

        -- ---------------------------------------------------------------------
        -- Step 3: Children (script from BlobMigrationStepScript; MAXDOP 1 in template)
        -- ---------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigrationProgress
                       WHERE RunId = @RunId AND TableName = @TableName AND Step = 3 AND Status = 'Completed')
        BEGIN
            SET @CurrentStep = 3;
            SET @BatchNumber = COALESCE((
                SELECT MAX(BatchNumber)
                FROM dbo.BlobMigrationProgress
                WHERE RunId = @RunId AND TableName = @TableName AND Step = 3 AND Status <> 'Completed'
            ), 0);
            SET @TotalRowsInserted = COALESCE((
                SELECT MAX(TotalRowsInserted)
                FROM dbo.BlobMigrationProgress
                WHERE RunId = @RunId AND TableName = @TableName AND Step = 3 AND Status <> 'Completed'
            ), 0);

            WHILE 1 = 1
            BEGIN
                SET @BatchNumber     = @BatchNumber + 1;
                SET @BatchStartedAt  = SYSDATETIME();

                SELECT @Sql = ScriptBody
                FROM dbo.BlobMigrationStepScript
                WHERE StepNumber = 3 AND ScriptKind = N'BatchInsert';

                SET @Sql = REPLACE(REPLACE(REPLACE(REPLACE(@Sql,
                    N'[SourceTableFull]',    @SourceTableFull),
                    N'[TargetTableFull]',   @TargetTableFull),
                    N'[MetadataTableFull]', @MetadataTableFull),
                    N'[MetadataIdColumn]',  @MetadataIdColumn);

                EXEC sp_executesql @Sql,
                    N'@BatchSize INT, @ExcludedStreamId UNIQUEIDENTIFIER',
                    @BatchSize, @ExcludedStreamId;

                SET @RowsInserted       = @@ROWCOUNT;
                SET @TotalRowsInserted  = @TotalRowsInserted + @RowsInserted;
                SET @BatchCompletedAt   = SYSDATETIME();

                INSERT INTO dbo.BlobMigrationProgress
                    (RunId, TableName, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                     BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
                VALUES
                    (@RunId, @TableName, @RunStartedAt, 3, @BatchNumber, @RowsInserted, @TotalRowsInserted,
                     @BatchStartedAt, @BatchCompletedAt, 'InProgress', NULL);

                IF @RowsInserted = 0
                    BREAK;
            END

            INSERT INTO dbo.BlobMigrationProgress
                (RunId, TableName, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                 BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
            VALUES
                (@RunId, @TableName, @RunStartedAt, 3, 0, 0, @TotalRowsInserted,
                 SYSDATETIME(), SYSDATETIME(), 'Completed', NULL);
        END

    END TRY
    BEGIN CATCH
        SET @BatchCompletedAt = SYSDATETIME();
        INSERT INTO dbo.BlobMigrationProgress
            (RunId, TableName, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
             BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
        VALUES
            (@RunId, @TableName, COALESCE(@RunStartedAt, SYSDATETIME()), COALESCE(@CurrentStep, 0),
             999999, 0, COALESCE(@TotalRowsInserted, 0),
             COALESCE(@BatchStartedAt, SYSDATETIME()), @BatchCompletedAt,
             'Failed', ERROR_MESSAGE());
        IF OBJECT_ID('tempdb..#Batch') IS NOT NULL
            DROP TABLE #Batch;
        THROW;
    END CATCH
END;
GO
