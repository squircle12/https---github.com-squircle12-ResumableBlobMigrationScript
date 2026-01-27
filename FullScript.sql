--1. Initial load of records without parent path locator

SET STATISTICS IO ON;

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

FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT WITH (NOLOCK)

INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK) on RAM.cw_referralattachmentId = RAFT.stream_id

inner join Gwent_LA_FileTable.dbo.LA_BU on businessunit = RAM.OwningBusinessUnit

where parent_path_locator is null and RAFT.stream_id <> '7F8D53EC-B98C-F011-B86B-005056A2DD37'

SET STATISTICS IO OFF;

 

 

-- Mop up only if the above fails at some point

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

INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK) on RAM.cw_referralattachmentId = RAFT.stream_id

inner join Gwent_LA_FileTable.dbo.LA_BU on businessunit = RAM.OwningBusinessUnit

left join Gwent_LA_FileTable.dbo.ReferralAttachment LRA on LRA.stream_id = RAFT.stream_id

where RAFT.parent_path_locator is null and LRA.stream_id is null

and RAFT.stream_id <> '7F8D53EC-B98C-F011-B86B-005056A2DD37'

 

 

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

SELECT  RAFT.[stream_id]

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

LEFT JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK) on RAM.cw_referralattachmentId = RAFT.stream_id

left join Gwent_LA_FileTable.dbo.ReferralAttachment LRA on LRA.stream_id = Raft.stream_id

where RAFT.stream_id in (Select distinct Par.stream_id

                                                                                                                FROM AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment RAFT

                                                                                                                INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK) on RAM.cw_referralattachmentId = RAFT.stream_id

                                                                                                                inner join Gwent_LA_FileTable.dbo.LA_BU on businessunit = RAM.OwningBusinessUnit

                                                                                                                inner join AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment Par on Par.path_locator = RAFT.parent_path_locator

                                                                                                                where NOT RAFT.parent_path_locator is null

                                                                                                                and RAFT.stream_id <> '7F8D53EC-B98C-F011-B86B-005056A2DD37')

 

 

                                --Location:        "sql\\ntdbms\\storeng\\dfs\\trans\\xact.cpp":4352

                                --Expression:    !m_updNestedXactCnt

                                --SPID:                 80

                                --Process ID:     22164

                                --Description:  Trying to use the transaction while there are 1 parallel nested xacts outstanding

                                --Msg 21, Level 20, State 1, Line 39

                                --Warning: Fatal error 3624 occurred at Jan 23 2026 10:00AM. Note the error and time, and contact your system administrator.

                                --Msg 596, Level 21, State 1, Line 0

                                --Cannot continue the execution because the session is in the kill state.

                                --Msg 0, Level 20, State 0, Line 0

                                --A severe error occurred on the current command.  The results, if any, should be discarded.

 

-- 3. Records with Parent path locator  Fixed with OPTION (MAXDOP 1)

 

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

SELECT  RAFT.[stream_id]

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

INNER JOIN AdvancedRBS_MetaData.dbo.cw_referralattachmentBase RAM WITH (NOLOCK) on RAM.cw_referralattachmentId = RAFT.stream_id

inner join Gwent_LA_FileTable.dbo.LA_BU on businessunit = RAM.OwningBusinessUnit

where NOT RAFT.parent_path_locator is null

and RAFT.stream_id <> '7F8D53EC-B98C-F011-B86B-005056A2DD37'

OPTION (MAXDOP 1)