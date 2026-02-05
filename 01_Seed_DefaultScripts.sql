-- =============================================================================
-- Blob Migration V2: Seed default table config and step script templates
-- Run after 00_Schema_V2.sql.
-- Idempotent: MERGE so re-running does not duplicate; update scripts if needed.
-- =============================================================================

USE Gwent_LA_FileTable;
GO

-- -----------------------------------------------------------------------------
-- 1. Table config: ReferralAttachment and ClientAttachment
-- TableName = logical key for progress/queue (typically target table 3-part name).
-- ExcludedStreamId NULL = proc uses global default.
-- -----------------------------------------------------------------------------
MERGE dbo.BlobMigrationTableConfig AS t
USING (
    SELECT N'Gwent_LA_FileTable.dbo.ReferralAttachment' AS TableName,
           N'AdvancedRBSBlob_WCCIS' AS SourceDatabase, N'dbo' AS SourceSchema, N'ReferralAttachment' AS SourceTable,
           N'Gwent_LA_FileTable' AS TargetDatabase, N'dbo' AS TargetSchema, N'ReferralAttachment' AS TargetTable,
           N'AdvancedRBS_MetaData' AS MetadataDatabase, N'dbo' AS MetadataSchema, N'cw_referralattachmentBase' AS MetadataTable,
           N'cw_referralattachmentId' AS MetadataIdColumn, CAST(NULL AS UNIQUEIDENTIFIER) AS ExcludedStreamId
    UNION ALL
    SELECT N'Gwent_LA_FileTable.dbo.ClientAttachment',
           N'AdvancedRBSBlob_WCCIS', N'dbo', N'ClientAttachment',
           N'Gwent_LA_FileTable', N'dbo', N'ClientAttachment',
           N'AdvancedRBS_MetaData', N'dbo', N'cw_clientattachmentBase',
           N'cw_clientattachmentId', CAST(NULL AS UNIQUEIDENTIFIER)
) AS s ON t.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TableName, SourceDatabase, SourceSchema, SourceTable, TargetDatabase, TargetSchema, TargetTable,
            MetadataDatabase, MetadataSchema, MetadataTable, MetadataIdColumn, ExcludedStreamId, IsActive)
    VALUES (s.TableName, s.SourceDatabase, s.SourceSchema, s.SourceTable, s.TargetDatabase, s.TargetSchema, s.TargetTable,
            s.MetadataDatabase, s.MetadataSchema, s.MetadataTable, s.MetadataIdColumn, s.ExcludedStreamId, 1)
WHEN MATCHED THEN
    UPDATE SET
        SourceDatabase = s.SourceDatabase, SourceSchema = s.SourceSchema, SourceTable = s.SourceTable,
        TargetDatabase = s.TargetDatabase, TargetSchema = s.TargetSchema, TargetTable = s.TargetTable,
        MetadataDatabase = s.MetadataDatabase, MetadataSchema = s.MetadataSchema, MetadataTable = s.MetadataTable,
        MetadataIdColumn = s.MetadataIdColumn, UpdatedAt = SYSDATETIME();
GO

-- -----------------------------------------------------------------------------
-- 2. Step scripts: templates with placeholders (StageName = documentation only)
-- Placeholders: [SourceTableFull], [TargetTableFull], [MetadataTableFull],
-- [MetadataIdColumn], [MaxDOP] (Step 1 only; proc replaces with numeric value).
-- Step 2 queue population: table-specific in BlobMigrationQueuePopulationScript (section 3).
-- -----------------------------------------------------------------------------

-- Step 1: Roots (parent_path_locator IS NULL). StageName = Roots.
MERGE dbo.BlobMigrationStepScript AS t
USING (SELECT 1 AS StepNumber, N'BatchInsert' AS ScriptKind, N'Roots' AS StageName, 1 AS UseParameterizedMaxDOP, N'Roots: insert top batch from source where parent_path_locator IS NULL' AS Description) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (1, N'BatchInsert', N'Roots', N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] LRA ON LRA.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NULL
  AND LRA.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
ORDER BY RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
', 1, N'Roots: insert top batch from source where parent_path_locator IS NULL')
WHEN MATCHED THEN
    UPDATE SET StageName = s.StageName, ScriptBody = N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] LRA ON LRA.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NULL
  AND LRA.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
ORDER BY RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
', Description = s.Description, UseParameterizedMaxDOP = s.UseParameterizedMaxDOP;
GO

-- Step 2: Batch insert from #Batch. StageName = MissingParents.
MERGE dbo.BlobMigrationStepScript AS t
USING (SELECT 2 AS StepNumber, N'BatchInsert' AS ScriptKind, N'MissingParents' AS StageName, 1 AS UseParameterizedMaxDOP, N'Step 2: insert batch from queue (#Batch)' AS Description) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (2, N'BatchInsert', N'MissingParents', N'
INSERT INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN #Batch B ON B.stream_id = RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
', 1, N'Step 2: insert batch from queue (#Batch)')
WHEN MATCHED THEN
    UPDATE SET StageName = s.StageName, ScriptBody = N'
INSERT INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN #Batch B ON B.stream_id = RAFT.stream_id
OPTION (MAXDOP [MaxDOP]);
', Description = s.Description;
GO

-- Step 3: Children (parent_path_locator IS NOT NULL). StageName = Children.
MERGE dbo.BlobMigrationStepScript AS t
USING (SELECT 3 AS StepNumber, N'BatchInsert' AS ScriptKind, N'Children' AS StageName, 0 AS UseParameterizedMaxDOP, N'Children: insert top batch where parent_path_locator IS NOT NULL' AS Description) AS s
ON t.StepNumber = s.StepNumber AND t.ScriptKind = s.ScriptKind
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StepNumber, ScriptKind, StageName, ScriptBody, UseParameterizedMaxDOP, Description)
    VALUES (3, N'BatchInsert', N'Children', N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] LRA ON LRA.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NOT NULL
  AND LRA.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
ORDER BY RAFT.stream_id
OPTION (MAXDOP 1);
', 0, N'Children: insert top batch where parent_path_locator IS NOT NULL')
WHEN MATCHED THEN
    UPDATE SET StageName = s.StageName, ScriptBody = N'
INSERT TOP (@BatchSize) INTO [TargetTableFull]
    ([stream_id],[file_stream],[name],[path_locator],[creation_time],[last_write_time],
     [last_access_time],[is_directory],[is_offline],[is_hidden],[is_readonly],[is_archive],[is_system],[is_temporary])
SELECT RAFT.[stream_id], RAFT.[file_stream], RAFT.[name], RAFT.[path_locator],
       RAFT.[creation_time], RAFT.[last_write_time], RAFT.[last_access_time],
       RAFT.[is_directory], RAFT.[is_offline], RAFT.[is_hidden], RAFT.[is_readonly],
       RAFT.[is_archive], RAFT.[is_system], RAFT.[is_temporary]
FROM [SourceTableFull] RAFT WITH (NOLOCK)
INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
    ON RAM.[MetadataIdColumn] = RAFT.stream_id
INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
LEFT JOIN [TargetTableFull] LRA ON LRA.stream_id = RAFT.stream_id
WHERE RAFT.parent_path_locator IS NOT NULL
  AND LRA.stream_id IS NULL
  AND RAFT.stream_id <> @ExcludedStreamId
ORDER BY RAFT.stream_id
OPTION (MAXDOP 1);
', Description = s.Description;
GO

-- -----------------------------------------------------------------------------
-- 3. Queue population scripts: one per table (ReferralAttachment, ClientAttachment)
-- Scripts remain as they are for referrals and clients; placeholders from TableConfig.
-- -----------------------------------------------------------------------------
DECLARE @QueueScript NVARCHAR(MAX) = N'
INSERT INTO dbo.BlobMigration_MissingParentsQueue (RunId, TableName, stream_id, Processed, CreatedAt)
SELECT @RunId, @TableName, Par.stream_id, 0, SYSDATETIME()
FROM (
    SELECT DISTINCT Par.stream_id
    FROM [SourceTableFull] RAFT WITH (NOLOCK)
    INNER JOIN [MetadataTableFull] RAM WITH (NOLOCK)
        ON RAM.[MetadataIdColumn] = RAFT.stream_id
    INNER JOIN Gwent_LA_FileTable.dbo.LA_BU ON businessunit = RAM.OwningBusinessUnit
    INNER JOIN [SourceTableFull] Par
        ON Par.path_locator = RAFT.parent_path_locator
    WHERE RAFT.parent_path_locator IS NOT NULL
      AND RAFT.stream_id <> @ExcludedStreamId
) Par
WHERE Par.stream_id <> @ExcludedStreamId
  AND NOT EXISTS (
      SELECT 1 FROM [TargetTableFull] T
      WHERE T.stream_id = Par.stream_id
  );
';

MERGE dbo.BlobMigrationQueuePopulationScript AS t
USING (
    SELECT N'Gwent_LA_FileTable.dbo.ReferralAttachment' AS TableName
    UNION ALL
    SELECT N'Gwent_LA_FileTable.dbo.ClientAttachment'
) AS s ON t.TableName = s.TableName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TableName, ScriptBody) VALUES (s.TableName, @QueueScript)
WHEN MATCHED THEN
    UPDATE SET ScriptBody = @QueueScript;
GO

PRINT 'V2 default table config, step scripts, and queue population scripts merged.';
GO
