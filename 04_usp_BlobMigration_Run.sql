-- =============================================================================
-- Master stored procedure: batched, resumable blob migration
-- Run from SSMS (one-off). Use @RunId to resume after interruption.
-- =============================================================================

USE Gwent_LA_FileTable;
GO

CREATE OR ALTER PROCEDURE dbo.usp_BlobMigration_Run
    @BatchSize INT              = 500,
    @MaxDOP    TINYINT          = 2,
    @Reset     BIT              = 0,
    @RunId     UNIQUEIDENTIFIER = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStartedAt      DATETIME2(7);
    DECLARE @CurrentStep       TINYINT = 0;
    DECLARE @BatchNumber       INT = 0;
    DECLARE @RowsInserted      INT;
    DECLARE @TotalRowsInserted INT = 0;
    DECLARE @BatchStartedAt    DATETIME2(7);
    DECLARE @BatchCompletedAt  DATETIME2(7);
    DECLARE @Sql               NVARCHAR(MAX);
    DECLARE @ExcludedStreamId  UNIQUEIDENTIFIER = '7F8D53EC-B98C-F011-B86B-005056A2DD37';

    BEGIN TRY
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
            WHERE RunId = @RunId;
            IF @RunStartedAt IS NULL
                SET @RunStartedAt = SYSDATETIME();
        END

        -- ---------------------------------------------------------------------
        -- Optional reset: clear progress and queue for this run
        -- ---------------------------------------------------------------------
        IF @Reset = 1
        BEGIN
            DELETE FROM dbo.BlobMigration_MissingParentsQueue WHERE RunId = @RunId;
            DELETE FROM dbo.BlobMigrationProgress            WHERE RunId = @RunId;
        END

        -- ---------------------------------------------------------------------
        -- Step 1: Roots (parent_path_locator IS NULL)
        -- ---------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigrationProgress
                       WHERE RunId = @RunId AND Step = 1 AND Status = 'Completed')
        BEGIN
            SET @CurrentStep       = 1;
            SET @BatchNumber       = 0;
            SET @TotalRowsInserted = 0;

            WHILE 1 = 1
            BEGIN
                SET @BatchNumber     = @BatchNumber + 1;
                SET @BatchStartedAt  = SYSDATETIME();

                SET @Sql = N'
INSERT TOP (@BatchSize) INTO Gwent_LA_FileTable.dbo.ReferralAttachment
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT WITH (NOLOCK)
INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK)
    ON RAM.cw_referralattachmentId = RAFT.stream_id
INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
LEFT JOIN Gwent_LA_FileTable.dbo.ReferralAttachment LRA ON LRA.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NULL
  AND LRA.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
ORDER BY RAFT.stream_id
OPTION (MAXDOP ' + CAST(@MaxDOP AS NVARCHAR(10)) + N');
';

                EXEC sp_executesql @Sql,
                    N'@BatchSize INT, @ExcludedStreamId UNIQUEIDENTIFIER',
                    @BatchSize, @ExcludedStreamId;

                SET @RowsInserted    = @@ROWCOUNT;
                SET @TotalRowsInserted = @TotalRowsInserted + @RowsInserted;
                SET @BatchCompletedAt = SYSDATETIME();

                INSERT INTO dbo.BlobMigrationProgress
                    (RunId, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                     BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
                VALUES
                    (@RunId, @RunStartedAt, 1, @BatchNumber, @RowsInserted, @TotalRowsInserted,
                     @BatchStartedAt, @BatchCompletedAt, 'InProgress', NULL);

                IF @RowsInserted = 0
                    BREAK;
            END

            INSERT INTO dbo.BlobMigrationProgress
                (RunId, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                 BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
            VALUES
                (@RunId, @RunStartedAt, 1, 0, 0, @TotalRowsInserted,
                 SYSDATETIME(), SYSDATETIME(), 'Completed', NULL);
        END

        -- ---------------------------------------------------------------------
        -- Step 2: Missing parents (queue + batch, always MAXDOP 1)
        -- ---------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigrationProgress
                       WHERE RunId = @RunId AND Step = 2 AND Status = 'Completed')
        BEGIN
            SET @CurrentStep       = 2;
            SET @BatchNumber       = 0;
            SET @TotalRowsInserted = 0;

            -- Populate queue if empty (first time or after reset)
            IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigration_MissingParentsQueue
                           WHERE RunId = @RunId)
            BEGIN
                INSERT INTO dbo.BlobMigration_MissingParentsQueue (RunId, stream_id, Processed, CreatedAt)
                SELECT @RunId, Par.stream_id, 0, SYSDATETIME()
                FROM (
                    SELECT DISTINCT Par.stream_id
                    FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT WITH (NOLOCK)
                    INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK)
                        ON RAM.cw_referralattachmentId = RAFT.stream_id
                    INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
                    INNER JOIN AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment Par
                        ON Par.path_locator = RAFT.parent_path_locator
                    WHERE RAFT.parent_path_locator IS NOT NULL
                      AND RAFT.stream_id <> @ExcludedStreamId
                ) Par
                WHERE Par.stream_id <> @ExcludedStreamId
                  AND NOT EXISTS (
                      SELECT 1 FROM Gwent_LA_FileTable.dbo.ReferralAttachment T
                      WHERE T.stream_id = Par.stream_id
                  );
            END

            WHILE 1 = 1
            BEGIN
                IF OBJECT_ID('tempdb..#Batch') IS NOT NULL
                    DROP TABLE #Batch;
                CREATE TABLE #Batch (stream_id UNIQUEIDENTIFIER PRIMARY KEY);

                INSERT INTO #Batch (stream_id)
                SELECT TOP (@BatchSize) stream_id
                FROM dbo.BlobMigration_MissingParentsQueue
                WHERE RunId = @RunId AND Processed = 0;

                IF @@ROWCOUNT = 0
                BEGIN
                    DROP TABLE #Batch;
                    BREAK;
                END

                SET @BatchNumber    = @BatchNumber + 1;
                SET @BatchStartedAt = SYSDATETIME();

                INSERT INTO Gwent_LA_FileTable.dbo.ReferralAttachment
                    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
                     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
                SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
                       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
                       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
                       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
                FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT WITH (NOLOCK)
                INNER JOIN #Batch B ON B.stream_id = RAFT.stream_id
                OPTION (MAXDOP 1);

                SET @RowsInserted     = @@ROWCOUNT;
                SET @TotalRowsInserted = @TotalRowsInserted + @RowsInserted;
                SET @BatchCompletedAt = SYSDATETIME();

                UPDATE dbo.BlobMigration_MissingParentsQueue
                SET Processed = 1
                WHERE RunId = @RunId AND stream_id IN (SELECT stream_id FROM #Batch);

                INSERT INTO dbo.BlobMigrationProgress
                    (RunId, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                     BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
                VALUES
                    (@RunId, @RunStartedAt, 2, @BatchNumber, @RowsInserted, @TotalRowsInserted,
                     @BatchStartedAt, @BatchCompletedAt, 'InProgress', NULL);

                DROP TABLE #Batch;
            END

            INSERT INTO dbo.BlobMigrationProgress
                (RunId, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                 BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
            VALUES
                (@RunId, @RunStartedAt, 2, 0, 0, @TotalRowsInserted,
                 SYSDATETIME(), SYSDATETIME(), 'Completed', NULL);
        END

        -- ---------------------------------------------------------------------
        -- Step 3: Children (parent_path_locator IS NOT NULL)
        -- ---------------------------------------------------------------------
        IF NOT EXISTS (SELECT 1 FROM dbo.BlobMigrationProgress
                       WHERE RunId = @RunId AND Step = 3 AND Status = 'Completed')
        BEGIN
            SET @CurrentStep       = 3;
            SET @BatchNumber       = 0;
            SET @TotalRowsInserted = 0;

            WHILE 1 = 1
            BEGIN
                SET @BatchNumber     = @BatchNumber + 1;
                SET @BatchStartedAt  = SYSDATETIME();

                SET @Sql = N'
INSERT TOP (@BatchSize) INTO Gwent_LA_FileTable.dbo.ReferralAttachment
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT WITH (NOLOCK)
INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK)
    ON RAM.cw_referralattachmentId = RAFT.stream_id
INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
LEFT JOIN Gwent_LA_FileTable.dbo.ReferralAttachment LRA ON LRA.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NOT NULL
  AND LRA.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
ORDER BY RAFT.stream_id
OPTION (MAXDOP ' + CAST(@MaxDOP AS NVARCHAR(10)) + N');
';

                EXEC sp_executesql @Sql,
                    N'@BatchSize INT, @ExcludedStreamId UNIQUEIDENTIFIER',
                    @BatchSize, @ExcludedStreamId;

                SET @RowsInserted     = @@ROWCOUNT;
                SET @TotalRowsInserted = @TotalRowsInserted + @RowsInserted;
                SET @BatchCompletedAt = SYSDATETIME();

                INSERT INTO dbo.BlobMigrationProgress
                    (RunId, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                     BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
                VALUES
                    (@RunId, @RunStartedAt, 3, @BatchNumber, @RowsInserted, @TotalRowsInserted,
                     @BatchStartedAt, @BatchCompletedAt, 'InProgress', NULL);

                IF @RowsInserted = 0
                    BREAK;
            END

            INSERT INTO dbo.BlobMigrationProgress
                (RunId, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
                 BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
            VALUES
                (@RunId, @RunStartedAt, 3, 0, 0, @TotalRowsInserted,
                 SYSDATETIME(), SYSDATETIME(), 'Completed', NULL);
        END

    END TRY
    BEGIN CATCH
        SET @BatchCompletedAt = SYSDATETIME();
        INSERT INTO dbo.BlobMigrationProgress
            (RunId, RunStartedAt, Step, BatchNumber, RowsInserted, TotalRowsInserted,
             BatchStartedAt, BatchCompletedAt, Status, ErrorMessage)
        VALUES
            (@RunId, COALESCE(@RunStartedAt, SYSDATETIME()), COALESCE(@CurrentStep, 0),
             999999, 0, COALESCE(@TotalRowsInserted, 0),
             COALESCE(@BatchStartedAt, SYSDATETIME()), @BatchCompletedAt,
             'Failed', ERROR_MESSAGE());
        IF OBJECT_ID('tempdb..#Batch') IS NOT NULL
            DROP TABLE #Batch;
        THROW;
    END CATCH
END;
GO

-- Example usage:
-- New run:     DECLARE @R UNIQUEIDENTIFIER; EXEC dbo.usp_BlobMigration_Run @BatchSize=500, @MaxDOP=2, @RunId=@R OUTPUT; SELECT @R AS RunId;
-- Resume:      EXEC dbo.usp_BlobMigration_Run @RunId='<run-id>';
-- Reset+run:   EXEC dbo.usp_BlobMigration_Run @RunId='<run-id>', @Reset=1;
