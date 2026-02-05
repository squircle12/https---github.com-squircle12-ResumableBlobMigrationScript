-- =============================================================================
-- Blob Delta Jobs: Schema and Core Tables
-- -----------------------------------------------------------------------------
-- Purpose
--   New job-run database to support recurring blob delta loads, built as a
--   clean V3-style project that uses the V2 migration ideas (scripts in tables,
--   stages/steps, queues) but separates orchestration from the FileTable DB.
--
--   This script creates:
--     - BlobDeltaJobs database
--     - Core config / high-watermark / run / queue / script / deletion tables
--
--   The actual delta engine procedure(s) and script seed data will live in
--   separate scripts, e.g.:
--     - 04_BlobDeltaJobs_Seed_Config.sql
--     - 05_BlobDeltaJobs_Engine.sql
--
-- Usage
--   Run once on the SQL Server instance that hosts the FileTable and metadata
--   databases. The job DB is intended to live on the same instance so three-
--   part names can be used to reach source/target/metadata tables.
-- =============================================================================

IF DB_ID(N'BlobDeltaJobs') IS NULL
BEGIN
    PRINT N'Creating BlobDeltaJobs database...';
    CREATE DATABASE BlobDeltaJobs;
END;
GO

USE BlobDeltaJobs;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- -----------------------------------------------------------------------------
-- 1. Per-table configuration (delta-aware, BU-aware)
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.BlobDeltaTableConfig', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaTableConfig...';
    CREATE TABLE dbo.BlobDeltaTableConfig
    (
        TableName              sysname        NOT NULL PRIMARY KEY,
            -- Logical key, typically the 3-part name of the target FileTable
            -- (e.g. Gwent_LA_FileTable.dbo.ReferralAttachment).

        SourceDatabase         sysname        NOT NULL,
        SourceSchema           sysname        NOT NULL,
        SourceTable            sysname        NOT NULL,

        TargetDatabase         sysname        NOT NULL,
        TargetSchema           sysname        NOT NULL,
        TargetTable            sysname        NOT NULL,

        MetadataDatabase       sysname        NOT NULL,
        MetadataSchema         sysname        NOT NULL,
        MetadataTable          sysname        NOT NULL,
        MetadataIdColumn       sysname        NOT NULL,
            -- Column in metadata table that links to stream_id in the blob table.

        MetadataModifiedOnCol  sysname        NOT NULL,
            -- Name of the [ModifiedOn] (or equivalent) column in the metadata
            -- table, used for delta-window filtering.

        SafetyBufferMinutes    int            NOT NULL
            CONSTRAINT DF_BlobDeltaTableConfig_SafetyBufferMinutes DEFAULT (240),
            -- Safety buffer applied around the high-watermark to avoid missing
            -- late/overlapping updates. 240 = 4 hours by default.

        IncludeUpdatesInDelta  bit            NOT NULL
            CONSTRAINT DF_BlobDeltaTableConfig_IncUpd DEFAULT (1),

        IncludeDeletesInDelta  bit            NOT NULL
            CONSTRAINT DF_BlobDeltaTableConfig_IncDel DEFAULT (0),

        IsActive               bit            NOT NULL
            CONSTRAINT DF_BlobDeltaTableConfig_IsActive DEFAULT (1),

        CreatedAt              datetime2(7)   NOT NULL
            CONSTRAINT DF_BlobDeltaTableConfig_CreatedAt DEFAULT (SYSDATETIME()),
        UpdatedAt              datetime2(7)   NULL
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 2. Per-table high-watermark and run lease
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.BlobDeltaHighWatermark', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaHighWatermark...';
    CREATE TABLE dbo.BlobDeltaHighWatermark
    (
        TableName                sysname       NOT NULL PRIMARY KEY
            REFERENCES dbo.BlobDeltaTableConfig (TableName),

        LastHighWaterModifiedOn  datetime2(7)  NULL,
            -- Last metadata.ModifiedOn value that has been fully captured in
            -- all successful delta runs for this table.

        LastRunId                uniqueidentifier NULL,
        LastRunCompletedAt       datetime2(7)  NULL,

        IsInitialFullLoadDone    bit           NOT NULL
            CONSTRAINT DF_BlobDeltaHWM_Initial DEFAULT (0),

        IsRunning                bit           NOT NULL
            CONSTRAINT DF_BlobDeltaHWM_IsRunning DEFAULT (0),
            -- Logical lock indicator to prevent overlapping runs for the same
            -- table. The engine should also use RunLeaseExpiresAt as a timeout.

        RunLeaseExpiresAt        datetime2(7)  NULL
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 3. Run header and per-step progress
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.BlobDeltaRun', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaRun...';
    CREATE TABLE dbo.BlobDeltaRun
    (
        RunId              uniqueidentifier NOT NULL PRIMARY KEY,

        RunType            nvarchar(20)     NOT NULL,
            -- e.g. 'Full' or 'Delta'.

        RequestedBy        sysname          NULL,

        RunStartedAt       datetime2(7)     NOT NULL,
        RunCompletedAt     datetime2(7)     NULL,

        Status             nvarchar(20)     NOT NULL,
            -- e.g. 'InProgress','Succeeded','Failed'.

        ErrorMessage       nvarchar(max)    NULL
    );
END;
GO

IF OBJECT_ID(N'dbo.BlobDeltaRunStep', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaRunStep...';
    CREATE TABLE dbo.BlobDeltaRunStep
    (
        RunId              uniqueidentifier NOT NULL
            REFERENCES dbo.BlobDeltaRun (RunId),

        TableName          sysname          NOT NULL,

        StepNumber         tinyint          NOT NULL,
            -- 1 = Roots, 2 = MissingParents, 3 = Children (for initial design).

        BatchNumber        int              NOT NULL,
            -- 0 = synthetic summary row for the step; >0 = per-batch entry.

        RowsProcessed      int              NOT NULL,
        TotalRowsProcessed int              NOT NULL,

        WindowStart        datetime2(7)     NULL,
        WindowEnd          datetime2(7)     NULL,

        BatchStartedAt     datetime2(7)     NOT NULL,
        BatchCompletedAt   datetime2(7)     NOT NULL,

        Status             nvarchar(20)     NOT NULL,
            -- e.g. 'InProgress','Completed','Failed'.

        ErrorMessage       nvarchar(max)    NULL,

        CONSTRAINT PK_BlobDeltaRunStep
            PRIMARY KEY (RunId, TableName, StepNumber, BatchNumber)
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 4. Missing-parents queue for delta runs
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.BlobDeltaMissingParentsQueue', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaMissingParentsQueue...';
    CREATE TABLE dbo.BlobDeltaMissingParentsQueue
    (
        RunId        uniqueidentifier NOT NULL,
        TableName    sysname          NOT NULL,
        stream_id    uniqueidentifier NOT NULL,

        BusinessUnit uniqueidentifier NULL,
            -- Optional BU tag if needed for debugging/troubleshooting.

        Processed    bit              NOT NULL
            CONSTRAINT DF_BlobDeltaMPQ_Processed DEFAULT (0),

        CreatedAt    datetime2(7)     NOT NULL
            CONSTRAINT DF_BlobDeltaMPQ_CreatedAt DEFAULT (SYSDATETIME()),

        CONSTRAINT PK_BlobDeltaMissingParentsQueue
            PRIMARY KEY (RunId, TableName, stream_id)
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 5. Step script templates and queue population scripts
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.BlobDeltaStepScript', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaStepScript...';
    CREATE TABLE dbo.BlobDeltaStepScript
    (
        StepNumber          tinyint        NOT NULL,
        ScriptKind          nvarchar(50)   NOT NULL,
            -- e.g. 'Roots','MissingParentsBatch','Children'.

        StageName           nvarchar(50)   NOT NULL,
            -- Documentation-only label, e.g. 'Roots','MissingParents','Children'.

        ScriptBody          nvarchar(max)  NOT NULL,
            -- Template body with placeholders for tables/columns, plus
            -- runtime parameters such as @BatchSize, @WindowStart, @WindowEnd.

        UseParameterizedMaxDOP bit         NOT NULL,

        Description         nvarchar(256)  NULL,

        CONSTRAINT PK_BlobDeltaStepScript
            PRIMARY KEY (StepNumber, ScriptKind)
    );
END;
GO

IF OBJECT_ID(N'dbo.BlobDeltaQueuePopulationScript', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaQueuePopulationScript...';
    CREATE TABLE dbo.BlobDeltaQueuePopulationScript
    (
        TableName  sysname        NOT NULL PRIMARY KEY,
            -- One row per logical table; script body populated by seed script.

        ScriptBody nvarchar(max)  NOT NULL
    );
END;
GO

-- -----------------------------------------------------------------------------
-- 6. Optional deletion log (for rare deletions)
-- -----------------------------------------------------------------------------

IF OBJECT_ID(N'dbo.BlobDeltaDeletionLog', N'U') IS NULL
BEGIN
    PRINT N'Creating dbo.BlobDeltaDeletionLog...';
    CREATE TABLE dbo.BlobDeltaDeletionLog
    (
        TableName   sysname          NOT NULL,
        BlobId      uniqueidentifier NOT NULL,
            -- Typically the stream_id of the deleted blob.

        DeletedOn   datetime2(7)     NOT NULL,

        Source      nvarchar(128)    NULL,
        Reason      nvarchar(256)    NULL,

        CreatedAt   datetime2(7)     NOT NULL
            CONSTRAINT DF_BlobDeltaDelLog_CreatedAt DEFAULT (SYSDATETIME()),

        CONSTRAINT PK_BlobDeltaDeletionLog
            PRIMARY KEY (TableName, BlobId, DeletedOn)
    );
END;
GO

PRINT N'BlobDeltaJobs core schema created/verified successfully.';
GO

