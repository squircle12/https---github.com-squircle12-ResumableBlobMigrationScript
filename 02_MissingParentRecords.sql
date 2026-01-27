-- 2. Missing Parent records

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
LEFT JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK)
    ON RAM.cw_referralattachmentId = RAFT.stream_id
LEFT JOIN Gwent_LA_FileTable.dbo.ReferralAttachment LRA
    ON LRA.stream_id = RAFT.stream_id
WHERE RAFT.stream_id IN (
    SELECT DISTINCT Par.stream_id
    FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT
    INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK)
        ON RAM.cw_referralattachmentId = RAFT.stream_id
    INNER JOIN Gwent_LA_FileTable.dbo.LA_BU
        ON businessunit = RAM.OwningBusinessUnit
    INNER JOIN AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment Par
        ON Par.path_locator = RAFT.parent_path_locator
    WHERE RAFT.parent_path_locator IS NOT NULL
      AND RAFT.stream_id <> '7F8D53EC-B98C-F011-B86B-005056A2DD37'
);

--Location:        "sql\\ntdbms\\storeng\\dfs\\trans\\xact.cpp":4352
--Expression:      !m_updNestedXactCnt
--SPID:            80
--Process ID:      22164
--Description:     Trying to use the transaction while there are 1 parallel nested xacts outstanding
--Msg 21, Level 20, State 1, Line 39
--Warning:        Fatal error 3624 occurred at Jan 23 2026 10:00AM. Note the error and time, and contact your system administrator.
--Msg 596, Level 21, State 1, Line 0
--Cannot continue the execution because the session is in the kill state.
--Msg 0, Level 20, State 0, Line 0
--A severe error occurred on the current command. The results, if any, should be discarded.

