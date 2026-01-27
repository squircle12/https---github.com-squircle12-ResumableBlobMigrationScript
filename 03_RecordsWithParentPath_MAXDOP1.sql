-- 3. Records with Parent path locator (fixed with OPTION (MAXDOP 1))

INSERT INTO Gwent_LA_FileTable.dbo.ReferralAttachment
          ([stream_id]
          ,[file_stream]
          ,[name]
          ,[path_locator]
          ,[creation_time]
          ,[last_write_time]
          ,[last_access_time]
          ,[is_directory]
          ,[is_offline]
          ,[is_hidden]
          ,[is_readonly]
          ,[is_archive]
          ,[is_system]
          ,[is_temporary])
SELECT RAFT.[stream_id]
      ,RAFT.[file_stream]
      ,RAFT.[name]
      ,RAFT.[path_locator]
      ,RAFT.[creation_time]
      ,RAFT.[last_write_time]
      ,RAFT.[last_access_time]
      ,RAFT.[is_directory]
      ,RAFT.[is_offline]
      ,RAFT.[is_hidden]
      ,RAFT.[is_readonly]
      ,RAFT.[is_archive]
      ,RAFT.[is_system]
      ,RAFT.[is_temporary]
FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT
INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK)
    ON RAM.cw_referralattachmentId = RAFT.stream_id
INNER JOIN Gwent_LA_FileTable.dbo.LA_BU
    ON businessunit = RAM.OwningBusinessUnit
WHERE RAFT.parent_path_locator IS NOT NULL
  AND RAFT.stream_id <> '7F8D53EC-B98C-F011-B86B-005056A2DD37'
OPTION (MAXDOP 1);

