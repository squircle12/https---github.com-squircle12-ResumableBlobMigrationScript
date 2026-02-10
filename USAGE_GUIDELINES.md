# Blob Delta Loads – Usage Guidelines

This document provides practical usage guidelines, FAQ, backup/transfer procedures, and supplier workflow for the Blob Delta Jobs (V3) system.

---

## 1. How to Use

### 1.1 Initial Setup (One-Time)

1. **Deploy the database objects** (in order, once per environment):
   - Run `03_BlobDeltaJobs_Schema.sql` – creates `BlobDeltaJobs` database and core tables.
   - Run `04_BlobDeltaJobs_Seed_Config.sql` – seeds table config and script templates.
   - Run `05_BlobDeltaJobs_Engine.sql` – deploys the engine procedures.

2. **Verify configuration** in `BlobDeltaTableConfig`:
   - Tables: `ReferralAttachment`, `ClientAttachment` (or others you’ve added).
   - Confirm `SafetyBufferMinutes` (default 240).
   - Confirm source/target/metadata database names match your environment.

3. **Optionally schedule** via SQL Server Agent:
   - Create a weekly (or other cadence) job that calls `usp_BlobDelta_RunOperator` (see below).

### 1.2 Running Delta Loads

| Scenario | Command |
|----------|---------|
| **All tables, all BUs (typical scheduled run)** | `EXEC dbo.usp_BlobDelta_RunOperator @Mode = N'AllTables', @TableName = NULL, @BatchSize = 500, @MaxDOP = 2, @BusinessUnitId = NULL` |
| **Single table (manual catch-up / testing)** | `EXEC dbo.usp_BlobDelta_RunOperator @Mode = N'SingleTable', @TableName = N'Gwent_LA_FileTable.dbo.ReferralAttachment', @BatchSize = 500, @MaxDOP = 2, @BusinessUnitId = NULL` |
| **Specific business unit only** | `EXEC dbo.usp_BlobDelta_RunOperator @Mode = N'AllTables', @TableName = NULL, @BatchSize = 500, @MaxDOP = 2, @BusinessUnitId = '<GUID>'` |

- Always run from the `BlobDeltaJobs` database.
- The engine populates the **target** tables (e.g. `Gwent_LA_FileTable.dbo.ReferralAttachment`, `ClientAttachment`) with new and updated blob records based on `[ModifiedOn]`.

---

## 2. Frequently Asked Questions

### General

**Q: How often should I run the delta job?**  
A: Typically weekly, or according to your change volume. The engine uses `ModifiedOn` windows and high-watermarks, so it is safe to run on a schedule. Overlap between runs is handled by the safety buffer; consumers apply “latest per blob” logic.

**Q: Can I run multiple delta jobs at the same time?**  
A: No. The engine uses a lease (`IsRunning`, `RunLeaseExpiresAt`) per table to prevent overlapping runs. If a run fails, the lease is cleared so a new run can proceed.

**Q: What if a run fails partway through?**  
A: High-watermarks are only advanced when all three steps (Roots, Missing Parents, Children) succeed for a table. On failure, the lease is cleared. You can re-run; the next run will reprocess from the last successful high-watermark.

### Data and Behaviour

**Q: Why might the same blob appear in more than one delta?**  
A: The safety buffer overlaps windows, and frequently updated records may appear in consecutive runs. This is intentional. Consumers should use “latest by `ModifiedOn`” semantics: if a `stream_id` exists in multiple deltas, keep the row with the latest `ModifiedOn`.

**Q: How are deletions handled?**  
A: Deletions are currently out of scope. `BlobDeltaDeletionLog` exists for future use but is not populated by the engine. If your supplier needs to reflect deletes, this will need to be added later.

**Q: What is the safety buffer and should I change it?**  
A: The default 4-hour buffer reduces the risk of missing late writes or clock drift. Adjust `SafetyBufferMinutes` in `BlobDeltaTableConfig` only if you have a strong reason (e.g. very large buffer for cross-timezone issues).

### Operations

**Q: How do I add a new table to delta loads?**  
A: Insert a row into `BlobDeltaTableConfig` with source/target/metadata details and `MetadataModifiedOnCol`, then add a matching row in `BlobDeltaHighWatermark`. The existing script templates may work; otherwise add/update scripts in `BlobDeltaStepScript` and `BlobDeltaQueuePopulationScript`.

**Q: How do I see what was processed in a run?**  
A: Use `BlobDeltaRun` and `BlobDeltaRunStep` (see [Monitoring](#3-monitoring-and-troubleshooting)). You can correlate rows with the target tables using `stream_id` and `ModifiedOn`.

---

## 3. Monitoring and Troubleshooting

### Recent runs

```sql
SELECT TOP (50)
    RunId, RunType, RequestedBy, RunStartedAt, RunCompletedAt, Status, ErrorMessage
FROM BlobDeltaJobs.dbo.BlobDeltaRun
ORDER BY RunStartedAt DESC;
```

### Per-step progress for a run

```sql
SELECT TableName, StepNumber, BatchNumber, RowsProcessed, TotalRowsProcessed,
       WindowStart, WindowEnd, BatchStartedAt, BatchCompletedAt, Status, ErrorMessage
FROM BlobDeltaJobs.dbo.BlobDeltaRunStep
WHERE RunId = @RunId
ORDER BY TableName, StepNumber, BatchNumber;
```

### High-watermarks

```sql
SELECT TableName, LastHighWaterModifiedOn, LastRunId, LastRunCompletedAt,
       IsInitialFullLoadDone, IsRunning, RunLeaseExpiresAt
FROM BlobDeltaJobs.dbo.BlobDeltaHighWatermark
ORDER BY TableName;
```

---

## 4. Backup and Transfer to Supplier

The delta engine writes to **target** tables in the FileTable database (e.g. `Gwent_LA_FileTable.dbo.ReferralAttachment`, `Gwent_LA_FileTable.dbo.ClientAttachment`). These are the tables that must be backed up and transferred to the supplier.

### 4.1 Backup Options

| Method | Description | Use case |
|--------|-------------|----------|
| **Native SQL backup** | `BACKUP DATABASE Gwent_LA_FileTable TO DISK = '...'` | Full DB backup; supplier restores entire DB. |
| **Table-level backup** | Extract target tables only (e.g. `SELECT INTO`, BCP, or custom scripts). | Transfer only `ReferralAttachment` and `ClientAttachment`. |
| **BACPAC** | Export via SSMS or `sqlpackage`. | For smaller datasets or non-native restore targets. |

> **Clarification needed:**  
> - Which backup format does the supplier expect (`.bak`, `.bacpac`, CSV, etc.)?  
> - Are you backing up only `ReferralAttachment` and `ClientAttachment`, or the whole `Gwent_LA_FileTable` database?  
> - Does the backup include FileTable file data, or only metadata rows? (FileTable rows reference `stream_id`; file content may live in filestream storage.)

### 4.2 Transfer via SFTP

1. Place the backup file(s) in a staging location after each delta run (or on schedule).
2. Use an SFTP client or script to upload to the supplier’s SFTP server.
3. Apply file naming and retention rules (e.g. `BlobDelta_ReferralAttachment_YYYYMMDD.bak`).
4. Notify the supplier that a new delta file is available (e.g. by email or shared manifest).

---

## 5. Expected Workflow for the Supplier

The supplier receives delta backup files and must merge them into their existing blob tables. Below is a recommended workflow.

### 5.1 Receive and Restore

1. **Receive** the backup file(s) via SFTP.
2. **Validate** file integrity (checksum/hash if provided).
3. **Restore** into a staging database or temporary tables:
   - If full DB backup: restore `Gwent_LA_FileTable` to a staging DB.
   - If table-level: restore/load into staging tables with the same schema as `ReferralAttachment` and `ClientAttachment`.

### 5.2 Merge into Target Tables

Apply “latest per blob” semantics: for each `stream_id`, keep the row with the latest `ModifiedOn`. Treat the delta as a set of candidate rows to merge.

**Merge logic (pseudocode):**

```
FOR each row R in the delta/staging table:
  IF R.stream_id EXISTS in target table:
    IF R.ModifiedOn > target.ModifiedOn:
      UPDATE target SET ... FROM R
  ELSE:
    INSERT R into target
```

**Example T-SQL pattern (conceptual):**

```sql
-- Merge ReferralAttachment delta into target
MERGE TargetDB.dbo.ReferralAttachment AS t
USING StagingDB.dbo.ReferralAttachment AS s
ON t.stream_id = s.stream_id
WHEN MATCHED AND s.ModifiedOn > t.ModifiedOn THEN
    UPDATE SET
        name = s.name,
        file_stream = s.file_stream,
        path_locator = s.path_locator,
        parent_path_locator = s.parent_path_locator,
        ModifiedOn = s.ModifiedOn
        -- ... other columns as needed
WHEN NOT MATCHED BY TARGET THEN
    INSERT (stream_id, name, file_stream, path_locator, parent_path_locator, ModifiedOn, ...)
    VALUES (s.stream_id, s.name, s.file_stream, s.path_locator, s.parent_path_locator, s.ModifiedOn, ...);
```

- Ensure parent rows exist before children (hierarchy: roots → missing parents → children). The delta engine uses this order; the supplier’s merge should respect parent-child relationships or process in the same order.
- The natural key for deduplication is `stream_id`; `ModifiedOn` is used to decide which version to keep.

### 5.3 Post-Merge Steps

1. **Verify** row counts and spot-check a sample of merged records.
2. **Log** the merge (e.g. rows inserted, updated, skipped) for audit.
3. **Archive or delete** the staging data and backup file according to retention policy.

### 5.4 Handling Deletes

- Deletions are **not** currently included in the delta pipeline. If the supplier needs to reflect deletes, a separate mechanism (e.g. delete manifest, `BlobDeltaDeletionLog` export) would need to be defined and implemented.

---

## 6. Items Needing Clarification

| # | Topic | Question |
|---|--------|----------|
| 1 | **Backup format** | What backup format does the supplier expect: native `.bak`, BACPAC, CSV, or something else? |
| 2 | **Scope of backup** | Are we backing up only `ReferralAttachment` and `ClientAttachment`, or the full `Gwent_LA_FileTable` database? |
| 3 | **File content vs metadata** | FileTable rows reference filestream data. Does the supplier need the actual file content, or only metadata? If both, how is filestream data included in the backup? |
| 4 | **Supplier schema** | Does the supplier’s target schema match exactly (same column names, types, constraints)? |
| 5 | **Primary key for merge** | Confirm that `stream_id` is the correct key for duplicate detection, and that `ModifiedOn` is the correct tie-breaker for updates. |
| 6 | **SFTP details** | Who provides SFTP credentials, path, and naming conventions? Is there a manifest or notification process when a new delta is ready? |
| 7 | **Retention and frequency** | How many delta files should be kept, and how often are deltas produced (weekly, daily, etc.)? |
| 8 | **Deletions** | Do supplier systems need to process deletions? If yes, a separate design for delete propagation will be needed. |

---

## 7. Quick Reference

| Object | Purpose |
|--------|---------|
| `BlobDeltaJobs` | Orchestration database for delta runs |
| `BlobDeltaTableConfig` | Per-table configuration (source, target, metadata, safety buffer) |
| `BlobDeltaHighWatermark` | Last processed `ModifiedOn` per table |
| `BlobDeltaRun` | Run headers (RunId, status, timestamps) |
| `BlobDeltaRunStep` | Per-step/batch progress |
| `usp_BlobDelta_RunOperator` | Main entry point for running delta loads |
