# Plan: Master Stored Procedure + Batched, Resumable Blob Migration

**Implementation completed.** Artifacts: `00_Schema_BlobMigration_StateAndQueue.sql`, `04_usp_BlobMigration_Run.sql`. Scripts `01`–`03` unchanged.

---

## Summary

Introduce a **state/progress table** and a **queue table** in `Gwent_LA_FileTable`, refactor the three migration steps into **batched, resumable** logic, and expose a **master stored procedure** that runs the steps sequentially. Batch size is **parameterised** (default 500 rows). The proc supports **resume** on restart and optional **reset** of progress only.

---

## Phase 1: Schema – State and Queue Tables (Gwent_LA_FileTable)

### 1.1 Progress table: `dbo.BlobMigrationProgress`

| Column | Type | Purpose |
|--------|------|---------|
| `RunId` | `uniqueidentifier` | Single run identifier (created when proc starts). |
| `RunStartedAt` | `datetime2(7)` | When the run started. |
| `Step` | `tinyint` | 1, 2, or 3. |
| `BatchNumber` | `int` | Batch index within the step. |
| `RowsInserted` | `int` | Rows inserted in this batch. |
| `TotalRowsInserted` | `int` | Cumulative rows for this run (optional but useful). |
| `BatchStartedAt` | `datetime2(7)` | Batch start. |
| `BatchCompletedAt` | `datetime2(7)` | Batch end. |
| `Status` | `varchar(20)` | `InProgress` \| `Completed` \| `Failed`. |
| `ErrorMessage` | `nvarchar(max)` | Set when `Status = Failed`. |

- **PK:** `(RunId, Step, BatchNumber)`.
- **Index:** `RunId, Step` for “current” step / resume lookups.
- One row per batch. “Current” run = latest `RunId` by `RunStartedAt` with any `Status = 'InProgress'` or last `Completed` batch.

### 1.2 Queue table (Step 2): `dbo.BlobMigration_MissingParentsQueue`

| Column | Type | Purpose |
|--------|------|---------|
| `RunId` | `uniqueidentifier` | Links to the run. |
| `stream_id` | `uniqueidentifier` | Parent to process. |
| `Processed` | `bit` | 0 = pending, 1 = done. |
| `CreatedAt` | `datetime2(7)` | When queued. |

- **PK:** `(RunId, stream_id)`.
- **Index:** `(RunId, Processed)` for `WHERE RunId = @RunId AND Processed = 0` batching.

Step 2 will **materialize** “missing parent” `stream_id`s into this table (excluding those already in target), then process in batches of `@BatchSize`. On **resume**, we continue with `Processed = 0`; no need to recompute the full set.

---

## Phase 2: Master Stored Procedure

### 2.1 Name and database

- **Name:** `dbo.usp_BlobMigration_Run`.
- **Database:** `Gwent_LA_FileTable` (state tables live here; proc reads from `AdvancedRBSBlob_WCCIS` / `AdvancedRBS_MetaData` via 3-part names).

### 2.2 Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `@BatchSize` | `int` | `500` | Rows per batch (Steps 1 & 3). Step 2 uses same size for queue batch. |
| `@MaxDOP` | `tinyint` | `2` | Max degree of parallelism for Steps 1 and 3. Step 2 **always** uses `MAXDOP 1` (nested xact issue). |
| `@Reset` | `bit` | `0` | `1` = clear progress for **current run** and restart from Step 1 Batch 1; `0` = resume. |
| `@RunId` | `uniqueidentifier` | `NULL` | If provided, **resume** this run; otherwise start a **new** run (new `RunId`). |

- **Excluded `stream_id`** remains global: `7F8D53EC-B98C-F011-B86B-005056A2DD37` in all steps.

### 2.3 High-level flow

1. **Resolve run**
   - If `@RunId` provided: use it (resume).
   - Else: create new `RunId`, insert initial progress row(s) as needed.

2. **Optional reset**
   - If `@Reset = 1`: delete from `BlobMigrationProgress` and `BlobMigration_MissingParentsQueue` for this `RunId`. Effectively “restart from Step 1” while still skipping already-inserted rows in target (idempotent inserts).

3. **Step 1 – Roots (no parent)**
   - Loop:
     - `INSERT TOP(@BatchSize)` into target from source with existing filters (`parent_path_locator IS NULL`, `LA_BU`, metadata, excluded `stream_id`), **excluding** rows already in target (e.g. `LEFT JOIN` target `ON stream_id` where `target.stream_id IS NULL`). Use `ORDER BY source.stream_id` for deterministic batching.
     - Log batch to `BlobMigrationProgress` (Step 1, `BatchNumber`, `RowsInserted`, timestamps).
     - If `RowsInserted = 0`, break.
   - Mark Step 1 `Status = 'Completed'` for the run.

4. **Step 2 – Missing parents**
   - If queue for this `RunId` is **empty** (first time or post-reset):
     - Populate `BlobMigration_MissingParentsQueue` from the **same** “missing parents” logic as current script 2 (including **LEFT JOIN**s to metadata and target), but only for `stream_id`s **not** already in target. Global exclusion applied.
   - Loop:
     - Pick up to `@BatchSize` rows from queue with `Processed = 0`.
     - `INSERT` those `stream_id`s into target from source (same columns as today), using `OPTION (MAXDOP 1)`.
     - Mark those queue rows `Processed = 1`.
     - Log batch to `BlobMigrationProgress` (Step 2).
     - If no rows processed, break.
   - Mark Step 2 `Status = 'Completed'`.

5. **Step 3 – Children (has parent)**
   - Same pattern as Step 1: `INSERT TOP(@BatchSize)` with `parent_path_locator IS NOT NULL`, exclude already in target, `ORDER BY stream_id`. Use `@MaxDOP` (default 2).
   - Log each batch to `BlobMigrationProgress` (Step 3).
   - When no rows inserted, mark Step 3 `Status = 'Completed'`.

6. **Error handling**
   - On error: set `Status = 'Failed'`, `ErrorMessage = ERROR_MESSAGE()` for the current batch, then `THROW`/`RAISE` so the caller sees the failure. Progress is persisted; next run can **resume** with `@RunId`.

---

## Phase 3: Batched Step Logic (Details)

### 3.1 Step 1 – Roots

- **Source:** `AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment` RAFT.
- **Joins:** `INNER JOIN` metadata (`cw_referralattachmentBase`) on `stream_id`, `INNER JOIN` `LA_BU` on `OwningBusinessUnit`, **LEFT JOIN** `Gwent_LA_FileTable.dbo.ReferralAttachment` LRA on `LRA.stream_id = RAFT.stream_id`.
- **Filters:** `RAFT.parent_path_locator IS NULL`, `LRA.stream_id IS NULL`, exclude global `stream_id`.
- **Batch:** `INSERT TOP(@BatchSize) ... ORDER BY RAFT.stream_id`.
- **MAXDOP:** Use `@MaxDOP` (e.g. 2–4).

### 3.2 Step 2 – Missing parents

- **Queue population (one-off per run):**  
  `INSERT INTO BlobMigration_MissingParentsQueue (RunId, stream_id, Processed, CreatedAt)`  
  From the same “missing parents” set as today (LEFT JOINs unchanged), but:
  - **Exclude** `stream_id`s already in `Gwent_LA_FileTable.dbo.ReferralAttachment`.
  - Apply global exclusion.
- **Batch processing:**  
  - `SELECT TOP(@BatchSize) stream_id FROM BlobMigration_MissingParentsQueue WHERE RunId = @RunId AND Processed = 0`.  
  - `INSERT INTO` target from source `WHERE stream_id IN (those IDs)`, `OPTION (MAXDOP 1)`.  
  - `UPDATE` queue `SET Processed = 1` for those `stream_id`s.  
- **Resume:** On restart, do **not** repopulate queue; just continue with `Processed = 0`. If queue was cleared by `@Reset = 1`, repopulate when entering Step 2.

### 3.3 Step 3 – Children

- Same as Step 1 except:
  - **Filter:** `RAFT.parent_path_locator IS NOT NULL`.
  - **MAXDOP:** Use `@MaxDOP`. (If you prefer to keep Step 3 at `MAXDOP 1` like the current script due to past issues, we can default `@MaxDOP` to 1 for Step 3 only; plan uses shared `@MaxDOP` for 1 and 3.)

---

## Phase 4: File Layout and Optional Clean-up

### 4.1 New artifacts

- **`00_Schema_BlobMigration_StateAndQueue.sql`**  
  - `CREATE TABLE` for `BlobMigrationProgress` and `BlobMigration_MissingParentsQueue` plus indexes.  
  - Idempotent where possible (e.g. `IF NOT EXISTS`).

- **`04_usp_BlobMigration_Run.sql`**  
  - `CREATE OR ALTER PROCEDURE dbo.usp_BlobMigration_Run` with full implementation.

### 4.2 Existing scripts

- **`01_InitialLoad_NoParentPath.sql`**, **`02_MissingParentRecords.sql`**, **`03_RecordsWithParentPath_MAXDOP1.sql`**  
  - **Keep as-is** for reference and one-off, non-batched runs if ever needed.  
  - No need to remove the existing “mop up” in 01.

### 4.3 Optional later improvements

- **Indexes:** Consider supporting indexes on source `ReferralAttachment` / metadata if not already present (e.g. `stream_id`, `parent_path_locator`, `path_locator`) to speed up joins and queue population.
- **Logging:** Optional separate “run log” table (e.g. run-level summary, duration) for operational visibility; can be added later without changing the core design.

---

## Phase 5: Execution and Resume Behaviour

- **Manual one-off (SSMS):**  
  - New run: `EXEC Gwent_LA_FileTable.dbo.usp_BlobMigration_Run @BatchSize = 500, @MaxDOP = 2;`  
  - Resume after failure: capture `RunId` from `BlobMigrationProgress`, then  
    `EXEC ... @RunId = '<run-id>';`

- **Reset and restart same run:**  
  `EXEC ... @RunId = '<run-id>', @Reset = 1;`  
  Progress (and queue for Step 2) for that run is cleared; inserts remain idempotent (already-present rows skipped).

- **Interruption:**  
  Stop execution (e.g. cancel in SSMS). Current batch may complete or partially run; progress up to last **fully** completed batch is stored. On next `EXEC` with same `@RunId`, proc resumes from the appropriate step/batch.

---

## Phase 6: Implementation Order

1. **Phase 1:** Create `00_Schema_BlobMigration_StateAndQueue.sql` and run it in `Gwent_LA_FileTable`.
2. **Phase 2–3:** Implement `04_usp_BlobMigration_Run.sql` (master proc + all batched logic).
3. **Phase 4:** Leave 01/02/03 unchanged; add 00 and 04 as above.
4. **Phase 5:** Test with small `@BatchSize` (e.g. 10–50) and verify resume/reset behaviour, then tune `@BatchSize` and `@MaxDOP` as needed.

---

## Summary of Design Choices

| Topic | Choice |
|-------|--------|
| Batch size | Row count, default 500, parameterised via `@BatchSize`. |
| State location | `Gwent_LA_FileTable`. |
| Run type | One-off manual (SSMS); proc supports resume and optional reset. |
| MAXDOP | Configurable `@MaxDOP` (default 2) for Steps 1 & 3; Step 2 fixed at 1. |
| Step 2 logic | LEFT JOINs retained; queue table for batching and resume. |
| Excluded `stream_id` | Global in all steps. |

---

*Once you approve this plan, implementation will proceed in the order above. After each phase, we’ll briefly confirm what was done and what remains.*
