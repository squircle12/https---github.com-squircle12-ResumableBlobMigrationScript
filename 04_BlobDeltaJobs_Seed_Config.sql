-- =============================================================================
-- Blob Delta Jobs: Seed Table Config and Script Templates
-- -----------------------------------------------------------------------------
-- Purpose
--   Seed initial configuration and script templates for the BlobDeltaJobs
--   project, using the existing ReferralAttachment / ClientAttachment pattern
--   as a baseline and extending it for delta-window and BU-aware behaviour.
--
--   This script is designed to be idempotent via MERGE statements.
--
-- Prerequisite
--   03_BlobDeltaJobs_Schema.sql has been run and BlobDeltaJobs DB exists.
-- =============================================================================

USE BlobDeltaJobs;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- -----------------------------------------------------------------------------
-- Configuration defaults (adjust and rerun to seed for other databases)
-- -----------------------------------------------------------------------------
DECLARE @FileTableDatabase sysname = N'Gwent_LA_FileTable';   -- Target FileTable DB (e.g. per BU/tenant)
DECLARE @FileTableSchema   sysname = N'dbo';

DECLARE @ReferralTableName sysname = N'ReferralAttachment';
DECLARE @ClientTableName   sysname = N'ClientAttachment';

DECLARE @SourceDatabase    sysname = N'AdvancedRBSBlob_WCCIS';
DECLARE @SourceSchema      sysname = N'dbo';

DECLARE @MetadataDatabase           sysname = N'AdvancedRBS_MetaData';
DECLARE @MetadataSchema             sysname = N'dbo';
DECLARE @ReferralMetadataTable      sysname = N'cw_referralattachmentBase';
DECLARE @ReferralMetadataIdColumn   sysname = N'cw_referralattachmentId';
DECLARE @ClientMetadataTable        sysname = N'cw_clientattachmentBase';
DECLARE @ClientMetadataIdColumn     sysname = N'cw_clientattachmentId';
DECLARE @MetadataModifiedOnColumn   sysname = N'ModifiedOn';

-- -----------------------------------------------------------------------------
-- 1. Seed BlobDeltaTableConfig for known tables
--    (ReferralAttachment and ClientAttachment initial examples)
-- -----------------------------------------------------------------------------

MERGE dbo.BlobDeltaTableConfig AS t
USING (
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ReferralTableName AS TableName,
        @SourceDatabase          AS SourceDatabase,  @SourceSchema   AS SourceSchema,  @ReferralTableName  AS SourceTable,
        @FileTableDatabase       AS TargetDatabase,  @FileTableSchema AS TargetSchema, @ReferralTableName  AS TargetTable,
        @MetadataDatabase        AS MetadataDatabase, @MetadataSchema AS MetadataSchema, @ReferralMetadataTable AS MetadataTable,
        @ReferralMetadataIdColumn AS MetadataIdColumn,
        @MetadataModifiedOnColumn AS MetadataModifiedOnCol,
        CAST(240 AS INT)          AS SafetyBufferMinutes,
        CAST(1 AS BIT)            AS IncludeUpdatesInDelta,
        CAST(0 AS BIT)            AS IncludeDeletesInDelta,
        CAST(1 AS BIT)            AS IsActive
    UNION ALL
    SELECT
        @FileTableDatabase + N'.' + @FileTableSchema + N'.' + @ClientTableName,
        @SourceDatabase,  @SourceSchema,  @ClientTableName,
        @FileTableDatabase, @FileTableSchema, @ClientTableName,
        @MetadataDatabase, @MetadataSchema, @ClientMetadataTable,
        @ClientMetadataIdColumn,
        @MetadataModifiedOnColumn,
        CAST(240 AS INT),
        CAST(1 AS BIT),
        CAST(0 AS BIT),
        CAST(1 AS BIT)
) AS s
ON t.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        TableName,
        SourceDatabase, SourceSchema, SourceTable,
        TargetDatabase, TargetSchema, TargetTable,
        MetadataDatabase, MetadataSchema, MetadataTable, MetadataIdColumn,
        MetadataModifiedOnCol,
        SafetyBufferMinutes,
        IncludeUpdatesInDelta,
        IncludeDeletesInDelta,
        IsActive
    )
    VALUES (
        s.TableName,
        s.SourceDatabase, s.SourceSchema, s.SourceTable,
        s.TargetDatabase, s.TargetSchema, s.TargetTable,
        s.MetadataDatabase, s.MetadataSchema, s.MetadataTable, s.MetadataIdColumn,
        s.MetadataModifiedOnCol,
        s.SafetyBufferMinutes,
        s.IncludeUpdatesInDelta,
        s.IncludeDeletesInDelta,
        s.IsActive
    )
WHEN MATCHED THEN
    UPDATE SET
        SourceDatabase        = s.SourceDatabase,
        SourceSchema          = s.SourceSchema,
        SourceTable           = s.SourceTable,
        TargetDatabase        = s.TargetDatabase,
        TargetSchema          = s.TargetSchema,
        TargetTable           = s.TargetTable,
        MetadataDatabase      = s.MetadataDatabase,
        MetadataSchema        = s.MetadataSchema,
        MetadataTable         = s.MetadataTable,
        MetadataIdColumn      = s.MetadataIdColumn,
        MetadataModifiedOnCol = s.MetadataModifiedOnCol,
        SafetyBufferMinutes   = s.SafetyBufferMinutes,
        IncludeUpdatesInDelta = s.IncludeUpdatesInDelta,
        IncludeDeletesInDelta = s.IncludeDeletesInDelta,
        IsActive              = s.IsActive,
        UpdatedAt             = SYSDATETIME();
GO

-- Ensure matching high-watermark rows exist
MERGE dbo.BlobDeltaHighWatermark AS h
USING (
    SELECT TableName FROM dbo.BlobDeltaTableConfig
) AS s
ON h.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TableName, LastHighWaterModifiedOn, LastRunId, LastRunCompletedAt,
            IsInitialFullLoadDone, IsRunning, RunLeaseExpiresAt)
    VALUES (s.TableName, NULL, NULL, NULL, 0, 0, NULL);
GO

-- -----------------------------------------------------------------------------
-- 2. Step script templates (Roots, MissingParents batch, Children)
-- -----------------------------------------------------------------------------

-- Step 1: Roots (parent_path_locator IS NULL), delta-windowed and BU-aware.
MERGE dbo.BlobDeltaStepScript AS t
USING (
    SELECT
        CAST(1 AS tinyint)     AS StepNumber,
        N'Roots'               AS ScriptKind,
        N'Roots'               AS StageName,
        CAST(1 AS bit)         AS UseParameterizedMaxDOP,
        N'Roots: insert top batch from source where parent_path_locator IS NULL, delta-windowed by metadata ModifiedOn.' AS Description
) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (
        s.StepNumber,
        s.ScriptKind,
        s.StageName,
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        s.UseParameterizedMaxDOP,
        s.Description
    )
WHEN MATCHED THEN
    UPDATE SET
        StageName           = s.StageName,
        ScriptBody          =
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        UseParameterizedMaxDOP = s.UseParameterizedMaxDOP,
        Description            = s.Description;
GO

-- Step 2: Missing parents batch insert (from #Batch).
MERGE dbo.BlobDeltaStepScript AS t
USING (
    SELECT
        CAST(2 AS tinyint)     AS StepNumber,
        N'MissingParentsBatch' AS ScriptKind,
        N'MissingParents'      AS StageName,
        CAST(1 AS bit)         AS UseParameterizedMaxDOP,
        N'Step 2: insert parents from queue (#Batch) into target.' AS Description
) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (
        s.StepNumber,
        s.ScriptKind,
        s.StageName,
N'
INSERT INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN #Batch B
    ON B.stream_id = RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        s.UseParameterizedMaxDOP,
        s.Description
    )
WHEN MATCHED THEN
    UPDATE SET
        StageName           = s.StageName,
        ScriptBody          =
N'
INSERT INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN #Batch B
    ON B.stream_id = RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
',
        UseParameterizedMaxDOP = s.UseParameterizedMaxDOP,
        Description            = s.Description;
GO

-- Step 3: Children (parent_path_locator IS NOT NULL), delta-windowed and BU-aware.
MERGE dbo.BlobDeltaStepScript AS t
USING (
    SELECT
        CAST(3 AS tinyint)     AS StepNumber,
        N'Children'            AS ScriptKind,
        N'Children'            AS StageName,
        CAST(0 AS bit)         AS UseParameterizedMaxDOP,
        N'Children: insert top batch where parent_path_locator IS NOT NULL, delta-windowed by metadata ModifiedOn.' AS Description
) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (
        s.StepNumber,
        s.ScriptKind,
        s.StageName,
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NOT NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP 1);
',
        s.UseParameterizedMaxDOP,
        s.Description
    )
WHEN MATCHED THEN
    UPDATE SET
        StageName           = s.StageName,
        ScriptBody          =
N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],
     [is_archive],[is_system],[is_temporary])
SELECT
    RAFT.[stream_id],
    RAFT.[file_stream],
    RAFT.[name],
    RAFT.[path_locator],
    RAFT.[creation_time],
    RAFT.[last_write_time],
    RAFT.[last_access_time],
    RAFT.[is_directory],
    RAFT.[is_offline],
    RAFT.[is_hidden],
    RAFT.[is_readonly],
    RAFT.[is_archive],
    RAFT.[is_system],
    RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
    ON BU.businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] TGT WITH (NOLOCK)
    ON TGT.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NOT NULL
  AND TGT.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
  AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
  AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
  AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
ORDER BY RAFT.stream_id
OPTION (MAXDOP 1);
',
        UseParameterizedMaxDOP = s.UseParameterizedMaxDOP,
        Description            = s.Description;
GO

-- -----------------------------------------------------------------------------
-- 3. Queue population script (per-table, shared template for now)
-- -----------------------------------------------------------------------------

DECLARE @QueueScript nvarchar(max) = N'
INSERT INTO dbo.BlobDeltaMissingParentsQueue (RunId, TableName, stream_id, BusinessUnit, Processed, CreatedAt)
SELECT
    @RunId,
    @TableName,
    Par.stream_id,
    Par.businessunit,
    0,
    SYSDATETIME()
FROM (
    SELECT DISTINCT Par.stream_id, BU.businessunit
    FROM [SourceTableFull] RAFT WITH (NOLOCK)
    INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
        ON RAM.[MetadataIdColumn] = RAFT.stream_id
    INNER JOIN [BusinessUnitTableFull] BU WITH (NOLOCK)
        ON BU.businessunit = RAM.OwningBusinessUnit
    INNER JOIN [SourceTableFull] Par WITH (NOLOCK)
        ON Par.path_locator = RAFT.parent_path_locator
    WHERE RAFT.parent_path_locator IS NOT NULL
      AND RAFT.stream_id <> @ExcludedStreamId
      AND RAM.[MetadataModifiedOnColumn] >  @WindowStart
      AND RAM.[MetadataModifiedOnColumn] <= @WindowEnd
      AND (CASE WHEN @BusinessUnitId IS NULL THEN 1 WHEN BU.businessunit = @BusinessUnitId THEN 1 ELSE 0 END = 1)
) Par
WHERE Par.stream_id <> @ExcludedStreamId
  AND NOT EXISTS (
      SELECT 1
      FROM [TargetTableFull] T WITH (NOLOCK)
      WHERE T.stream_id = Par.stream_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM dbo.BlobDeltaMissingParentsQueue Q WITH (NOLOCK)
      WHERE Q.RunId = @RunId
        AND Q.TableName = @TableName
        AND Q.stream_id = Par.stream_id
  );
';

MERGE dbo.BlobDeltaQueuePopulationScript AS t
USING (
    SELECT TableName
    FROM dbo.BlobDeltaTableConfig
    WHERE SourceTable IN (N'ReferralAttachment', N'ClientAttachment')
) AS s
ON t.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TableName, ScriptBody)
    VALUES (s.TableName, @QueueScript)
WHEN MATCHED THEN
    UPDATE SET ScriptBody = @QueueScript;
GO

PRINT N'BlobDeltaJobs config and script templates seeded/updated successfully.';
GO

