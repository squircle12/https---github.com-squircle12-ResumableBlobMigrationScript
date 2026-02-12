-------------------------------------------------------------------------------
-- Destructively remove the FileStreamGroup filegroup and related objects
-- WARNING: This script drops all FILETABLEs in the target database and
--          removes the FILESTREAM filegroup/files. All FILETABLE data will
--          be permanently deleted.
-------------------------------------------------------------------------------
DECLARE @TargetDatabase sysname       = N'Primer_FileTable'; -- <== change to your DB, e.g. N'YnysMon_FileTable'
DECLARE @Sql            nvarchar(MAX);

-------------------------------------------------------------------------------
-- 1) Drop all FILETABLEs in the target database
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
DECLARE @dropSql nvarchar(MAX) = N''''; 

SELECT @dropSql = @dropSql +
       ''DROP TABLE '' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + ''.'' + QUOTENAME(t.name) + '';'' + CHAR(13) + CHAR(10)
FROM sys.tables t
WHERE t.is_filetable = 1;

IF @dropSql <> N''''
BEGIN
    PRINT ''Dropping FileTables in database ' + @TargetDatabase + N'...'';
    EXEC (@dropSql);
END;
';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- 2) Remove FILESTREAM files from the FileStreamGroup filegroup (if any)
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
DECLARE @removeFileSql nvarchar(MAX) = N'''';

SELECT @removeFileSql = @removeFileSql +
       ''ALTER DATABASE '' + QUOTENAME(DB_NAME()) + '' REMOVE FILE '' + QUOTENAME(df.name) + '';'' + CHAR(13) + CHAR(10)
FROM sys.database_files df
JOIN sys.filegroups fg ON df.data_space_id = fg.data_space_id
WHERE fg.name = N''FileStreamGroup'';

IF @removeFileSql <> N''''
BEGIN
    PRINT ''Removing FILESTREAM files from [FileStreamGroup] in database ' + @TargetDatabase + N'...'';
    EXEC (@removeFileSql);
END;
';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- 3) Remove the FileStreamGroup filegroup itself (if it exists)
-------------------------------------------------------------------------------
SET @Sql = N'IF EXISTS (
    SELECT 1 
    FROM ' + QUOTENAME(@TargetDatabase) + N'.sys.filegroups
    WHERE name = N''FileStreamGroup''
)
BEGIN
    PRINT ''Removing filegroup [FileStreamGroup] from database ' + @TargetDatabase + N'...'';
    ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N' REMOVE FILEGROUP [FileStreamGroup];
END;
';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- 4) Clear FILESTREAM database options (DIRECTORY_NAME, NON_TRANSACTED_ACCESS)
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF EXISTS (
    SELECT 1
    FROM sys.database_filestream_options
    WHERE directory_name IS NOT NULL
       OR non_transacted_access <> 0
)
BEGIN
    PRINT ''Clearing FILESTREAM options (DIRECTORY_NAME, NON_TRANSACTED_ACCESS) in database ' + @TargetDatabase + N'...'';
    ALTER DATABASE CURRENT
    SET FILESTREAM
    (
        DIRECTORY_NAME        = NULL,
        NON_TRANSACTED_ACCESS = OFF
    );
END;
';

EXEC (@Sql);

PRINT 'Cleanup of FileStreamGroup and FILETABLEs completed for database ' + QUOTENAME(@TargetDatabase) + '.';