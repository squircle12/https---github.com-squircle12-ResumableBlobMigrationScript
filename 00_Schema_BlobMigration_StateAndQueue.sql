-- =============================================================================
-- Blob migration: state and queue tables (Gwent_LA_FileTable)
-- Idempotent: creates tables/indexes only if they do not exist.
-- =============================================================================

USE Gwent_LA_FileTable;
GO

-- -----------------------------------------------------------------------------
-- 1. Progress table: one row per batch
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dbo.BlobMigrationProgress', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BlobMigrationProgress
    (
        RunId              UNIQUEIDENTIFIER NOT NULL,
        RunStartedAt       DATETIME2(7)     NOT NULL,
        Step               TINYINT          NOT NULL,
        BatchNumber        INT              NOT NULL,
        RowsInserted       INT              NOT NULL,
        TotalRowsInserted  INT              NOT NULL,
        BatchStartedAt     DATETIME2(7)     NOT NULL,
        BatchCompletedAt   DATETIME2(7)     NOT NULL,
        Status             VARCHAR(20)      NOT NULL,
        ErrorMessage       NVARCHAR(MAX)    NULL,
        CONSTRAINT PK_BlobMigrationProgress PRIMARY KEY (RunId, Step, BatchNumber)
    );

    CREATE NONCLUSTERED INDEX IX_BlobMigrationProgress_RunId_Step
        ON dbo.BlobMigrationProgress (RunId, Step);

    PRINT 'Created dbo.BlobMigrationProgress.';
END
ELSE
    PRINT 'dbo.BlobMigrationProgress already exists; skipping.';
GO

-- -----------------------------------------------------------------------------
-- 2. Step 2 queue: missing parents to process
-- -----------------------------------------------------------------------------
IF OBJECT_ID('dbo.BlobMigration_MissingParentsQueue', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.BlobMigration_MissingParentsQueue
    (
        RunId      UNIQUEIDENTIFIER NOT NULL,
        stream_id  UNIQUEIDENTIFIER NOT NULL,
        Processed  BIT              NOT NULL DEFAULT 0,
        CreatedAt  DATETIME2(7)     NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT PK_BlobMigration_MissingParentsQueue PRIMARY KEY (RunId, stream_id)
    );

    CREATE NONCLUSTERED INDEX IX_BlobMigration_MissingParentsQueue_RunId_Processed
        ON dbo.BlobMigration_MissingParentsQueue (RunId, Processed)
        INCLUDE (stream_id);

    PRINT 'Created dbo.BlobMigration_MissingParentsQueue.';
END
ELSE
    PRINT 'dbo.BlobMigration_MissingParentsQueue already exists; skipping.';
GO
