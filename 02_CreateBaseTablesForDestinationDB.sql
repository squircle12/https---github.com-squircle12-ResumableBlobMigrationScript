-- 02_CreateBaseTablesForDestinationDB.sql
-- Purpose:
--   - Create the base lookup table and FILETABLEs required by the Blob Delta load process.
--   - LA_BU: maps business units to readable names and organisations.
--   - ReferralAttachment, ClientAttachment, Documents: FILETABLEs holding the physical blobs.
--
-- Idempotency:
--   - The FILESTREAM filegroup and file are created only if they do not already exist.
--   - Each table is only created if it does not already exist in the target database.
--   - This script can be safely re-run without failing on existing objects.
--
-- IMPORTANT:
--   - This script is designed to configure the FILESTREAM filegroup and tables for a specific
--     database (by default: the Primer database).
--   - Update the @TargetDatabase, @FileStreamDirectory and @FileTableDirectoryName variables below as needed for your
--     environment.
--   - Run this script on the SQL Server instance that will host the FILETABLE data and where
--     FILESTREAM is already enabled at the instance level.

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

DECLARE @TargetDatabase         sysname        = N'Primer_FileTable';   -- The database that will host the FILETABLEs
DECLARE @FileStreamDirectory    nvarchar(260)  = N'F:\MSSQL\MSSQL15.MSSQLSERVER\MSSQL\FileStream\' + @TargetDatabase; -- Filesystem path for FILESTREAM data
DECLARE @FileTableDirectoryName nvarchar(128)  = @TargetDatabase;       -- FILESTREAM DIRECTORY_NAME used for FileTables (must be non-NULL and unique per instance)
DECLARE @Sql                    nvarchar(max);

-------------------------------------------------------------------------------
-- Ensure FILESTREAM database options (DIRECTORY_NAME, NON_TRANSACTED_ACCESS)
-- are set for the target database. This is required before creating FileTables.
-------------------------------------------------------------------------------
SET @Sql = N'PRINT ''Setting FILESTREAM options (DIRECTORY_NAME, NON_TRANSACTED_ACCESS) for database ' + @TargetDatabase + N'...''; 
ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N'
SET FILESTREAM
(
    DIRECTORY_NAME        = N''' + @FileTableDirectoryName + N''',
    NON_TRANSACTED_ACCESS = FULL
);';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Ensure FILESTREAM filegroup and file exist in the target database
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.filegroups
    WHERE name = N''FileStreamGroup''
)
BEGIN
    ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N'
    ADD FILEGROUP [FileStreamGroup] CONTAINS FILESTREAM;
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_files df
    JOIN sys.filegroups fg ON df.data_space_id = fg.data_space_id
    WHERE fg.name = N''FileStreamGroup''
)
BEGIN
    ALTER DATABASE ' + QUOTENAME(@TargetDatabase) + N'
    ADD FILE
    (
        NAME = N''' + @TargetDatabase + N'_FS'',
        FILENAME = N''' + @FileStreamDirectory + N'''
    )
    TO FILEGROUP [FileStreamGroup];
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Create the LA_BU lookup table if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''LA_BU''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    CREATE TABLE [dbo].[LA_BU](
        [businessunit] [nvarchar](50) NOT NULL,
        [BU_Name] [nvarchar](50) NOT NULL,
        [Organisation] [nvarchar](50) NOT NULL
    ) ON [PRIMARY];
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Create the ReferralAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ReferralAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    CREATE TABLE [dbo].[ReferralAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ReferralAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Create the ClientAttachment FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''ClientAttachment''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    CREATE TABLE [dbo].[ClientAttachment] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''ClientAttachment'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END;';

EXEC (@Sql);

-------------------------------------------------------------------------------
-- Create the Documents FILETABLE if it does not already exist
-------------------------------------------------------------------------------
SET @Sql = N'USE ' + QUOTENAME(@TargetDatabase) + N';
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    WHERE t.name = N''Documents''
      AND SCHEMA_NAME(t.schema_id) = N''dbo''
)
BEGIN
    CREATE TABLE [dbo].[Documents] AS FILETABLE ON [PRIMARY] FILESTREAM_ON [FileStreamGroup]
    WITH
    (
        FILETABLE_DIRECTORY = N''Documents'',
        FILETABLE_COLLATE_FILENAME = SQL_Latin1_General_CP1_CI_AS
    );
END;';

EXEC (@Sql);
