# Blob Migration V2: Script-in-Table Approach

## 1. Approach review

### 1.1 Goal

- **Single master procedure** that runs blob migration for any configured table (ReferralAttachment, ClientAttachment, future tables).
- **Scripts stored in tables** so new tables or step changes can be added by inserting/updating data instead of altering the proc.
- **Stages and steps** clearly modelled so the engine runs: Stage/Step 1 (Roots) → Step 2 (Missing parents: queue + batch) → Step 3 (Children).

### 1.2 Current pattern (ReferralAttachment vs ClientAttachment)

| Aspect | ReferralAttachment | ClientAttachment |
|--------|--------------------|-------------------|
| Source table | `AdvancedRBSBlob_WCCIS.dbo.ReferralAttachment` | `AdvancedRBSBlob_WCCIS.dbo.ClientAttachment` |
| Target table | `Gwent_LA_FileTable.dbo.ReferralAttachment` | `Gwent_LA_FileTable.dbo.ClientAttachment` |
| Metadata table | `AdvancedRBS_MetaData.dbo.cw_referralattachmentBase` | `AdvancedRBS_MetaData.dbo.cw_clientattachmentBase` |
| Metadata PK column | `cw_referralattachmentId` | `cw_clientattachmentId` |
| BU join | Same: `LA_BU`, `businessunit`, `OwningBusinessUnit` |

Steps are identical in shape: (1) Roots INSERT/SELECT, (2) Queue population + batched INSERT from queue, (3) Children INSERT/SELECT. Only object names and metadata column names differ.

### 1.3 Proposed model: stages and steps

- **Stage** = logical phase of migration (e.g. Roots, MissingParents, Children). Used for ordering and documentation.
- **Step** = the numeric step the proc runs (1, 2, 3). Step 2 has two script “kinds”: queue population (run once) and batch insert (run in a loop).

Suggested storage:

- **BlobMigrationTableConfig**: one row per migrated table (TableName, source/target/metadata identifiers). Used for placeholder replacement and for scoping progress/queue by `@TableName`.
- **BlobMigrationStepScript**: one row per (Step, ScriptKind) with a script template. ScriptKind distinguishes Step 2 queue vs Step 2 batch. Step 1 and Step 3 have a single script each. Templates use placeholders that are replaced at runtime from **BlobMigrationTableConfig** plus proc parameters.

---

## 2. Variables in SQL when scripts are stored in a table

### 2.1 Two kinds of “variables”

| Kind | Examples | How to handle |
|------|----------|----------------|
| **Runtime parameters** (safe, typed) | `@BatchSize`, `@ExcludedStreamId` | Pass via `sp_executesql @Sql, N'@BatchSize INT, @ExcludedStreamId UNIQUEIDENTIFIER', @BatchSize, @ExcludedStreamId`. The script body in the table must use the same parameter names. |
| **Object names** (table/schema/DB, column names) | Source/target/metadata table, metadata PK column | **Cannot** be passed as `sp_executesql` parameters (SQL Server does not allow parameterising object names). Must be injected into the script string. |

### 2.2 Options for object names

1. **Placeholder replacement**
   - Store script with tokens e.g. `[TargetTable]`, `[SourceTable]`, `[MetadataTableFull]`, `[MetadataIdColumn]`.
   - At runtime: look up the row in **BlobMigrationTableConfig** for the current `@TableName`, then `REPLACE(script, '[TargetTable]', config.TargetTable)` (and similar for other tokens).
   - **Pros**: One template per step; add new tables by adding config rows (and reusing same scripts). **Cons**: If config is wrong or compromised, concatenation could be abused (injection). Mitigation: config is DBA-controlled; optional validation that values match a strict pattern (e.g. `[Db].[Schema].[Table]`).

2. **Fully dynamic SQL in the proc (no script table)**
   - Proc builds the entire SQL string from config columns (source/target/metadata names) and fixed logic.
   - **Pros**: No script storage; no placeholder parsing. **Cons**: Any change to the shape of the query (e.g. extra filter, new join) requires a proc change.

3. **Hybrid**
   - Script table holds **templates** with placeholders; proc replaces placeholders from **BlobMigrationTableConfig** and then executes with `sp_executesql` for `@BatchSize`, `@ExcludedStreamId`. **Recommended** for V2: add tables without proc changes, and keep a single place (script table) to adjust query text.

### 2.3 Recommended placeholder set

- **From table config** (per table): `[SourceTableFull]`, `[TargetTableFull]`, `[MetadataTableFull]`, `[MetadataIdColumn]`. Optional: `[SourceAlias]`, `[MetadataAlias]` if we ever need different aliases.
- **From proc / global**: `[MaxDOP]` for Step 1 (replaced with numeric value when building the batch script). Step 3 can use fixed `MAXDOP 1` in the template.
- **Parameters (not replaced)**: Script uses `@BatchSize` and `@ExcludedStreamId` literally; proc passes them with `sp_executesql`.

Config table can store either full three-part names or separate Database, Schema, Table columns; proc (or a view) can build `[SourceTableFull]` etc. for replacement.

### 2.4 Step 2 special case

Step 2 has two parts:

- **Queue population**: run once per run (when queue for that RunId/TableName is empty). INSERT into BlobMigration_MissingParentsQueue SELECT … FROM source/metadata/target with “missing parent” logic. Script template uses same placeholders (source, target, metadata, metadata ID column).
- **Batch insert**: run in a loop. INSERT into target FROM source INNER JOIN #Batch. Template needs `[TargetTableFull]`, `[SourceTableFull]` only (no metadata join). Queue and progress handling (e.g. #Batch, Processed flag) stay in the proc; only the INSERT…SELECT is driven by the script table.

So we need two script rows for Step 2: e.g. `ScriptKind = 'QueuePopulation'` and `ScriptKind = 'BatchInsert'`.

### 2.5 MAXDOP

- Step 1: configurable (e.g. `@MaxDOP`). Template contains `OPTION (MAXDOP [MaxDOP])`; proc replaces `[MaxDOP]` with the numeric value (concatenated into the string, not as a parameter).
- Step 2: always MAXDOP 1 (hardcoded in template or in proc).
- Step 3: MAXDOP 1 (hardcoded in template) to avoid FileTable constraint issues.

---

## 3. Decisions (answers to clarifying questions)


| # | Question | Decision |
|---|----------|----------|
| 1 | Stages vs steps in the schema | **StepNumber + StageName on script table for documentation only.** No separate Stage table; `BlobMigrationStepScript` has a `StageName` column (e.g. Roots, MissingParents, Children). |
| 2 | Primary key for progress table | **PK updated to `(RunId, TableName, Step, BatchNumber)`** so one RunId can safely track multiple tables. Schema and migration block added in `00_Schema_V2.sql`. |
| 3 | Excluded stream ID | **Global for all tables.** Proc uses a single global excluded `stream_id`; config column is unused. |
| 4 | Step 2 queue population script | **Queue population scripts for ReferralAttachment and ClientAttachment remain as they are.** Table-specific scripts stored in **BlobMigrationQueuePopulationScript** (one row per table); same placeholder template for both, replaced from TableConfig at runtime. |
| 5 | Validation of config values | **DBA trust for now.** No validation proc; config values are trusted when doing REPLACE. |
| 6 | Order of tables | **One table to completion (Steps 1→2→3) then next.** Master proc accepts a single `@TableName` and runs that table to completion; caller loops over table names for "all" tables. |

