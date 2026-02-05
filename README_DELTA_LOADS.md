### Blob Delta Loads (V3) – Design and Usage

This document describes the V3 **Blob Delta Jobs** project, which builds on the V2 migration patterns to support recurring, delta-style blob loads suitable for direct DB restore.

---

### 1. Purpose

- **Problem**: The original V2 migration was designed as a one-off/full migration of large blob tables (e.g. `ReferralAttachment`, `ClientAttachment`). We now need to **regularly send delta loads** of blob data, at scale, without re-copying everything.
- **Goal**: Provide a **resumable, script-driven engine** that:
  - Runs regularly (e.g. weekly) to capture **new and updated blobs**.
  - Uses **metadata `[ModifiedOn]`** as the reliable signal for change, not `Last_Write_Time` on the FileTable.
  - Produces deltas that can be **directly restored** as database slices, with consumers applying “latest per blob” semantics.
  - Is **per-BU aware**, reusing the existing `LA_BU` model.

---

### 2. High-level approach

- **New job DB**: `BlobDeltaJobs`
  - Owns all **orchestration** and **run metadata**:
    - `BlobDeltaTableConfig` – per logical table config (source/target/metadata, `ModifiedOn` column, safety buffer, flags).
    - `BlobDeltaHighWatermark` – per table last processed `ModifiedOn`, plus an `IsRunning` lease.
    - `BlobDeltaRun` / `BlobDeltaRunStep` – run headers and per-step/batch progress.
    - `BlobDeltaMissingParentsQueue` – queue of parent blobs to back-fill.
    - `BlobDeltaStepScript` / `BlobDeltaQueuePopulationScript` – script templates (Roots / MissingParents / Children) with placeholders.
    - `BlobDeltaDeletionLog` – optional log for rare deletions.
  - References the existing **blob/FileTable DB** (`Gwent_LA_FileTable`) and **metadata DB** (`AdvancedRBS_MetaData`) via three-part names.

- **Change detection via `[ModifiedOn]`**
  - Each run computes a **time window** per table:
    - `WindowEnd = RunStart - SafetyBufferMinutes`
    - `WindowStart = (LastHighWater - SafetyBufferMinutes)` or an initial baseline if no high-watermark exists yet.
  - The window is applied to the **metadata** table’s `[ModifiedOn]` column, not the FileTable’s `Last_Write_Time`.
  - A configurable **safety buffer** (default **4 hours**) reduces the risk of missing records due to late writes, scheduling jitter, or minor clock issues. Records may appear in multiple deltas; consumers should pick the latest by `ModifiedOn`.

- **Three-step pattern per table (reusing V2 concepts)**
  - **Step 1 – Roots**: insert roots where `parent_path_locator IS NULL`, delta-windowed, BU-aware, and not already in the target.
  - **Step 2 – Missing Parents**:
    - Populate a queue of “missing parents” for the current window, then
    - Process the queue in batches using a `#Batch` table.
  - **Step 3 – Children**: insert children where `parent_path_locator IS NOT NULL`, delta-windowed, BU-aware, and not already in the target.
  - Each step is driven by **scripts stored in tables**, so new tables or logic variations can be added without altering the engine procedure.

- **High-watermarks and resumability**
  - For each table, `BlobDeltaHighWatermark.LastHighWaterModifiedOn` is advanced to `WindowEnd` only after all three steps succeed.
  - Runs are **resumable** by `RunId`: `BlobDeltaRunStep` logs per-batch status, and queues are keyed by `(RunId, TableName, stream_id)`.

---

### 3. Key database objects

- **Schema script**: `03_BlobDeltaJobs_Schema.sql`
  - Creates the `BlobDeltaJobs` database and core tables:
    - `BlobDeltaTableConfig`
    - `BlobDeltaHighWatermark`
    - `BlobDeltaRun`
    - `BlobDeltaRunStep`
    - `BlobDeltaMissingParentsQueue`
    - `BlobDeltaStepScript`
    - `BlobDeltaQueuePopulationScript`
    - `BlobDeltaDeletionLog`

- **Seed script**: `04_BlobDeltaJobs_Seed_Config.sql`
  - Seeds `BlobDeltaTableConfig` for:
    - `Gwent_LA_FileTable.dbo.ReferralAttachment`
    - `Gwent_LA_FileTable.dbo.ClientAttachment`
  - Initializes matching `BlobDeltaHighWatermark` rows.
  - Populates `BlobDeltaStepScript` with delta-windowed, BU-aware templates:
    - Step 1 `Roots`
    - Step 2 `MissingParentsBatch`
    - Step 3 `Children`
  - Populates `BlobDeltaQueuePopulationScript` with a shared delta-aware queue population template.

- **Engine script**: `05_BlobDeltaJobs_Engine.sql`
  - `usp_BlobDelta_ResolveTableConfig`
    - Helper proc that resolves full table names, metadata column names, and safety buffer for a given `TableName`.
  - `usp_BlobDelta_Run`
    - Core engine that:
      - Creates/updates a `BlobDeltaRun` row.
      - Selects active tables from `BlobDeltaTableConfig` (or a single table, if specified).
      - For each table:
        - Computes `WindowStart` / `WindowEnd` from `BlobDeltaHighWatermark` + `SafetyBufferMinutes`.
        - Acquires a simple **lease** (`IsRunning`, `RunLeaseExpiresAt`) to prevent overlapping runs.
        - Executes Steps 1–3 using `BlobDeltaStepScript` and `BlobDeltaQueuePopulationScript`, passing:
          - `@BatchSize`, `@ExcludedStreamId`, `@WindowStart`, `@WindowEnd`, `@BusinessUnitId`.
        - Logs per-batch progress in `BlobDeltaRunStep`.
        - Advances `LastHighWaterModifiedOn` to `WindowEnd` and clears the lease on success.
      - On error:
        - Logs a `Failed` step row per table.
        - Clears the lease for that table.
        - Marks the overall run as `Failed`.
  - `usp_BlobDelta_RunOperator`
    - Thin, operator-friendly wrapper that supports:
      - `@Mode = 'AllTables'`: run deltas for all active tables.
      - `@Mode = 'SingleTable'`: run deltas for a specific `@TableName`.
      - Optional `@BusinessUnitId` to scope to a single BU.
      - Returns the `RunId` for inspection.

---

### 4. Usage patterns

#### 4.1 One-time setup

1. **Deploy schema and seed scripts** (once per environment):
   - Run `03_BlobDeltaJobs_Schema.sql`.
   - Run `04_BlobDeltaJobs_Seed_Config.sql`.
2. **Review/extend configuration**:
   - Confirm `BlobDeltaTableConfig` rows for `ReferralAttachment` / `ClientAttachment`:
     - Source/target/metadata DB/schema/table names.
     - `MetadataModifiedOnCol` (typically `ModifiedOn`).
     - `SafetyBufferMinutes` (default 240).
   - Add additional tables by inserting rows into `BlobDeltaTableConfig` and referencing the existing script templates (or new ones, if needed).
3. **Deploy engine**:
   - Run `05_BlobDeltaJobs_Engine.sql`.

#### 4.2 Scheduled delta load (all tables)

Typical SQL Agent job step (T-SQL) to run deltas for all active tables (all BUs):

```sql
USE BlobDeltaJobs;
GO

DECLARE @RunId uniqueidentifier;

EXEC dbo.usp_BlobDelta_RunOperator
    @Mode          = N'AllTables',
    @TableName     = NULL,
    @BatchSize     = 500,
    @MaxDOP        = 2,
    @BusinessUnitId = NULL;  -- all BUs
```

- This will:
  - Create a new `BlobDeltaRun` row.
  - Process all active tables sequentially using the configured windows.
  - Record per-batch progress into `BlobDeltaRunStep`.
  - Advance high-watermarks on success.

#### 4.3 Running for a single table (e.g. manual catch-up)

```sql
USE BlobDeltaJobs;
GO

DECLARE @RunId uniqueidentifier;

EXEC dbo.usp_BlobDelta_RunOperator
    @Mode          = N'SingleTable',
    @TableName     = N'Gwent_LA_FileTable.dbo.ReferralAttachment',
    @BatchSize     = 500,
    @MaxDOP        = 2,
    @BusinessUnitId = NULL;  -- or a specific BU GUID
```

- Use this when:
  - Testing changes for one table.
  - Catching up a specific table without touching others.

#### 4.4 Running for a specific BU

```sql
USE BlobDeltaJobs;
GO

DECLARE @RunId uniqueidentifier;
DECLARE @BU uniqueidentifier = '<BUSINESS-UNIT-ID-HERE>';

EXEC dbo.usp_BlobDelta_RunOperator
    @Mode          = N'AllTables',
    @TableName     = NULL,
    @BatchSize     = 500,
    @MaxDOP        = 2,
    @BusinessUnitId = @BU;
```

- The underlying scripts will apply `@BusinessUnitId` when joining to `Gwent_LA_FileTable.dbo.LA_BU`.

---

### 5. Monitoring and troubleshooting

- **Find recent runs**

```sql
SELECT TOP (50)
    RunId,
    RunType,
    RequestedBy,
    RunStartedAt,
    RunCompletedAt,
    Status,
    ErrorMessage
FROM dbo.BlobDeltaRun
ORDER BY RunStartedAt DESC;
```

- **Inspect per-table / per-step progress for a run**

```sql
SELECT
    TableName,
    StepNumber,
    BatchNumber,
    RowsProcessed,
    TotalRowsProcessed,
    WindowStart,
    WindowEnd,
    BatchStartedAt,
    BatchCompletedAt,
    Status,
    ErrorMessage
FROM dbo.BlobDeltaRunStep
WHERE RunId = @RunId
ORDER BY TableName, StepNumber, BatchNumber;
```

- **Check high-watermarks**

```sql
SELECT
    h.TableName,
    h.LastHighWaterModifiedOn,
    h.LastRunId,
    h.LastRunCompletedAt,
    h.IsInitialFullLoadDone,
    h.IsRunning,
    h.RunLeaseExpiresAt
FROM dbo.BlobDeltaHighWatermark h
ORDER BY h.TableName;
```

- **Investigate missing parents**

```sql
SELECT
    RunId,
    TableName,
    stream_id,
    BusinessUnit,
    Processed,
    CreatedAt
FROM dbo.BlobDeltaMissingParentsQueue
WHERE RunId = @RunId
ORDER BY TableName, CreatedAt;
```

---

### 6. Behavioural notes and trade-offs

- **Updates vs duplicates**
  - By design, **frequently updated records** may appear in multiple delta runs, due to the safety buffer overlapping windows.
  - This is acceptable because:
    - Consumers are expected to treat deltas as **“latest by ModifiedOn” snapshots** per blob ID.
    - The primary guarantee is **no missed changes**, not strict uniqueness across deltas.

- **Deletions**
  - Deletions are currently **very rare**, so the first implementation:
    - Provides `BlobDeltaDeletionLog` as a place to record deletions if/when there is a reliable source.
    - Does **not** yet integrate deletes into the main delta pipeline.
  - This keeps the design simpler while leaving room for future evolution when downstream delete requirements are clearer.

- **Clock and safety buffer**
  - Job, metadata, and blob DBs are on the **same SQL Server instance**, so `[ModifiedOn]` timestamps are broadly aligned.
  - The safety buffer (default 4 hours) provides a **“belt and braces”** guard against:
    - Slight clock discrepancies.
    - Delayed writes.
    - Scheduling jitter.

---

### 7. Extending the design

- **Adding a new table to deltas**
  - Add a row to `BlobDeltaTableConfig` with:
    - Correct source/target/metadata DB/schema/table names.
    - `MetadataIdColumn` and `MetadataModifiedOnCol`.
    - `SafetyBufferMinutes` and flags.
  - Optionally seed a specific `BlobDeltaQueuePopulationScript` row if the standard template does not fit.

- **Changing safety buffer or windows**
  - Update `SafetyBufferMinutes` per table in `BlobDeltaTableConfig`.
  - If needed, adjust the initial baseline logic in `usp_BlobDelta_Run` (for `WindowStart` when there is no high-watermark).

- **Incorporating deletions**
  - Once a reliable delete signal exists (e.g. status flag, audit table, trigger):
    - Populate `BlobDeltaDeletionLog` as part of the existing jobs, or
    - Extend the engine to write delete events into a dedicated delta stream for consumers.

This design aims to be **scalable for large blob volumes**, **safe against missed changes**, and **maintainable** by keeping the orchestration logic in a dedicated DB, driven primarily by configuration and script templates rather than hard-coded SQL per table.

