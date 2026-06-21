# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

This repository contains Databricks notebooks and PySpark code for incrementally ingesting data from an Oracle (Protheus) ERP system into a Databricks Delta Lake. The project is refactoring a previous full-overwrite ingestion strategy into an incremental load pattern using watermark columns.

## Execution Protocol (MANDATORY)

Before implementing anything, you must:

1. Explain what you are going to do
2. Highlight any risks
3. Ask for missing information (especially unique/business keys per table)
4. Wait for explicit user approval

Only proceed after approval is given.

## Incremental Load Strategy

Watermark columns used to detect new/updated records:

- `S_T_A_M_P_` — last modification timestamp
- `I_N_S_D_T_` — insertion timestamp

All incremental loads must:

- Use `MERGE INTO` (not overwrite) to handle inserts and updates
- Be idempotent (safe to rerun without duplicates)
- Support reprocessing (allow re-ingesting a time window)
- Identify new and updated records via the watermark columns before merging

## Data Target

- **Catalog:** `ihara_datalake_incremental`
- **Schema:** `raw`
- All tables are created and written exclusively in `ihara_datalake_incremental.raw`

## Security & Governance Rules (STRICT)

- **NEVER** read from, write to, or modify anything in catalog `ihara_datalake`
- **NEVER** create or modify: Jobs, Clusters, Pipelines, or Permissions
- **ONLY** use catalog `ihara_datalake_incremental`

## Unique Key Protocol

- **Always ask** the user for the unique/business keys before implementing a MERGE for any table
- Attempt to infer candidate keys from column names and table structure, then propose them with trade-off explanations
- Never assume keys — ambiguity must be resolved with the user before writing code

## Technical Stack & Preferences

- **Platform:** Databricks (Databricks CLI assumed configured)
- **Storage:** Delta Lake
- **Language:** PySpark (Python notebooks)
- **Pattern:** MERGE INTO over full overwrite
- **Style:** Simple, production-ready code — avoid unnecessary frameworks (no pydantic, no heavy abstractions)
- **Performance:** Design for large datasets; partition and Z-order Delta tables on watermark columns where appropriate

## Code Conventions

- Notebook cells should be clearly separated by concern: configuration, extraction, transformation, merge
- Watermark state (last processed `S_T_A_M_P_` / `I_N_S_D_T_`) should be persisted in a Delta control table or read from the target table's max value
- MERGE conditions must use the validated business key(s) for matching, and update all non-key columns on match
