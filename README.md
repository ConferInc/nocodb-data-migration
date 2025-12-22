# NocoDB Data Migration
###This is broken markdown


This repository contains Python scripts to **clone a table schema + data** between NocoDB bases/tables.

## Which file should you use?

- **Use this (recommended):** `nocodb_clone_any_table.py`
  - Reusable for *any* source/target table
  - Configure via **CLI arguments** or **environment variables**
  - Copies:
    - Column names + data types
    - SingleSelect/MultiSelect options (including option colors)
    - All rows (batch insert)

- **Example only:** `nocodb_clone_table.py`
  - Same logic but uses an inline `CONFIG` block
  - Kept mainly as a reference/example

## Requirements

- Python 3.9+
- `requests`

Install:

```bash
pip install -r requirements.txt
```

## Usage (recommended script)

### Option A: CLI arguments

```bash
python nocodb_clone_any_table.py \
  --base-url "http://YOUR_NOCODB_HOST" \
  --token "YOUR_NOCODB_TOKEN" \
  --source-base-id "SOURCE_BASE_ID" \
  --source-table-id "SOURCE_TABLE_ID" \
  --target-base-id "TARGET_BASE_ID" \
  --target-table-id "TARGET_TABLE_ID"
```

### Option B: Environment variables

Set:

- `NOCODB_URL`
- `NOCODB_TOKEN`
- `SOURCE_BASE_ID`
- `SOURCE_TABLE_ID`
- `TARGET_BASE_ID`
- `TARGET_TABLE_ID`

Then run:

```bash
python nocodb_clone_any_table.py
```

## Safety / behavior notes

- The **source table is never modified**.
- By default the script:
  - Deletes all rows in the target table
  - Deletes all non-system, non-PK columns in the target table
  - Recreates the schema, then inserts all rows

To keep target rows/columns, use:

- `--no-delete-rows`
- `--no-delete-columns`

## About CreatedAt / UpdatedAt

NocoDB system fields `CreatedAt` / `UpdatedAt` are **auto-generated** and generally **cannot be set** via the public API.

If you need to preserve the source timestamps, you can copy them into custom columns using:

```bash
python nocodb_clone_any_table.py ... --preserve-source-timestamps \
  --created-at-title "Created At1" \
  --updated-at-title "Source UpdatedAt"
```
