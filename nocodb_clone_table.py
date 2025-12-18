#!/usr/bin/env python3
"""nocodb_clone_table.py

Clone a NocoDB table schema + data from a SOURCE table into an existing TARGET table.

What it does (TARGET only):
- Deletes all rows
- Deletes all non-system, non-PK columns
- Recreates all SOURCE columns in the same order
  - Includes SingleSelect/MultiSelect options (including option colors)
- Copies all SOURCE rows into TARGET (batch insert with fallback)

What it does NOT do:
- It never modifies the SOURCE table.

Requirements:
- Python 3
- requests

Tested endpoints (NocoDB v1 meta + v1 data):
- GET  /api/v1/db/meta/tables/{tableId}
- POST /api/v1/db/meta/tables/{tableId}/columns
- PATCH/GET/DELETE /api/v1/db/meta/columns/{columnId}
- GET  /api/v1/db/data/v1/{baseId}/{tableId}
- POST /api/v1/db/data/v1/{baseId}/{tableId}
- POST /api/v1/db/data/bulk/v1/{baseId}/{tableId} (if supported)
- DELETE /api/v1/db/data/v1/{baseId}/{tableId}/{rowId}

NOTE about “colors”:
- In NocoDB, the visible “colors” for status-like fields are usually the
  Select-option colors. This script preserves those by recreating select options
  with their color hex codes.
"""

import json
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# ==========================
# CONFIGURATION (edit here)
# ==========================
CONFIG: Dict[str, Any] = {
    "BASE_URL": "http://YOUR_NOCODB_HOST",
    "API_TOKEN": "YOUR_NOCODB_TOKEN_HERE",

    # Source (read-only)
    "SOURCE_BASE_ID": "SOURCE_BASE_ID",
    "SOURCE_TABLE_ID": "SOURCE_TABLE_ID",

    # Target (will be overwritten)
    "TARGET_BASE_ID": "TARGET_BASE_ID",
    "TARGET_TABLE_ID": "TARGET_TABLE_ID",

    "BATCH_SIZE": 50,
    "REQUEST_TIMEOUT_SEC": 60,
    "SLEEP_BETWEEN_REQUESTS_SEC": 0.0,

    # Timestamp preservation
    # NocoDB system fields (CreatedAt/UpdatedAt) are auto-generated and cannot be set via API.
    # When enabled, we copy the source timestamps into these dedicated target columns.
    "PRESERVE_SOURCE_TIMESTAMPS": False,
    "TARGET_SOURCE_CREATED_AT_TITLE": "Created At1",
    "TARGET_SOURCE_UPDATED_AT_TITLE": "Source UpdatedAt",

    # Safety switches
    "DELETE_TARGET_ROWS_FIRST": True,
    "DELETE_TARGET_COLUMNS_FIRST": True,
}


AUTO_KEYS_TO_STRIP = {
    "Id",
    "id",
    "CreatedAt",
    "UpdatedAt",
    "created_at",
    "updated_at",
}

# Keys that often appear in meta objects and must NOT be sent back on create.
META_KEYS_STRIP = {
    "id",
    "fk_column_id",
    "base_id",
    "created_at",
    "updated_at",
    "createdAt",
    "updatedAt",
}


class NocoDBCloneError(RuntimeError):
    pass


class NocoDBCloner:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.base_url = cfg["BASE_URL"].rstrip("/")
        self.s = requests.Session()
        self.s.headers.update(
            {
                "xc-token": cfg["API_TOKEN"],
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def _sleep(self) -> None:
        delay = float(self.cfg.get("SLEEP_BETWEEN_REQUESTS_SEC", 0.0) or 0.0)
        if delay > 0:
            time.sleep(delay)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        ok_statuses: Tuple[int, ...] = (200, 201),
    ) -> Any:
        url = f"{self.base_url}{path}"
        self._sleep()
        r = self.s.request(
            method,
            url,
            params=params,
            json=json_body,
            timeout=int(self.cfg.get("REQUEST_TIMEOUT_SEC", 60)),
        )
        if r.status_code not in ok_statuses:
            msg = r.text
            raise NocoDBCloneError(f"{method} {url} -> {r.status_code}: {msg}")
        if not r.content:
            return None
        try:
            return r.json()
        except Exception:
            return r.text

    def _get_table_meta(self, table_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v1/db/meta/tables/{table_id}")

    def _sanitize_meta_obj(self, obj: Any) -> Any:
        """Recursively remove server-generated keys from meta payloads."""
        if isinstance(obj, list):
            return [self._sanitize_meta_obj(x) for x in obj]
        if isinstance(obj, dict):
            out: Dict[str, Any] = {}
            for k, v in obj.items():
                if k in META_KEYS_STRIP:
                    continue
                out[k] = self._sanitize_meta_obj(v)
            return out
        return obj

    def _delete_target_rows(self) -> None:
        if not self.cfg.get("DELETE_TARGET_ROWS_FIRST", True):
            print("[INFO] Skipping target row deletion (DELETE_TARGET_ROWS_FIRST=False)")
            return

        base_id = self.cfg["TARGET_BASE_ID"]
        table_id = self.cfg["TARGET_TABLE_ID"]

        print("[INFO] Deleting all TARGET rows...")
        deleted = 0
        offset = 0
        limit = 200

        while True:
            data = self._request(
                "GET",
                f"/api/v1/db/data/v1/{base_id}/{table_id}",
                params={"offset": offset, "limit": limit},
            )
            rows = (data or {}).get("list", [])
            if not rows:
                break

            for row in rows:
                row_id = row.get("Id") or row.get("id")
                if row_id is None:
                    continue
                # DELETE expects /{rowId}
                self._request(
                    "DELETE",
                    f"/api/v1/db/data/v1/{base_id}/{table_id}/{row_id}",
                    ok_statuses=(200, 204),
                )
                deleted += 1
                if deleted % 25 == 0:
                    print(f"[INFO] Deleted {deleted} rows...")

            # After deletions, start from offset 0 again because rows shifted.
            offset = 0

        print(f"[INFO] Deleted total {deleted} TARGET rows")

    def _delete_target_columns(self) -> None:
        if not self.cfg.get("DELETE_TARGET_COLUMNS_FIRST", True):
            print("[INFO] Skipping target column deletion (DELETE_TARGET_COLUMNS_FIRST=False)")
            return

        table_id = self.cfg["TARGET_TABLE_ID"]
        tgt_meta = self._get_table_meta(table_id)
        cols = tgt_meta.get("columns", [])

        print("[INFO] Deleting TARGET non-system, non-PK columns...")
        deleted = 0
        for c in cols:
            if c.get("system"):
                continue
            if c.get("pk"):
                continue
            col_id = c.get("id")
            if not col_id:
                continue
            try:
                self._request("DELETE", f"/api/v1/db/meta/columns/{col_id}", ok_statuses=(200, 204))
                deleted += 1
            except NocoDBCloneError as e:
                # Some instances may block deletion of certain default columns.
                print(f"[WARN] Could not delete column '{c.get('title')}' ({col_id}): {e}")

        print(f"[INFO] Deleted {deleted} TARGET columns")

    def _build_create_column_payload(self, src_col: Dict[str, Any]) -> Dict[str, Any]:
        """Build a safe column creation payload for /tables/{id}/columns."""
        payload: Dict[str, Any] = {}

        # Core identity
        payload["column_name"] = src_col.get("column_name")
        payload["title"] = src_col.get("title") or src_col.get("column_name")

        # Data/UI type hints
        for k in [
            "uidt",
            "dt",
            "np",
            "ns",
            "clen",
            "dtx",
            "dtxp",
            "dtxs",
            "un",
            "ai",
            "unique",
            "rqd",
            "cdf",
            "cc",
        ]:
            if k in src_col and src_col.get(k) is not None:
                payload[k] = src_col.get(k)

        # Column order if available
        if "order" in src_col and src_col.get("order") is not None:
            payload["order"] = src_col.get("order")

        # Preserve meta if present
        if isinstance(src_col.get("meta"), dict):
            payload["meta"] = self._sanitize_meta_obj(src_col.get("meta"))

        # Preserve select options (and other colOptions) but sanitize IDs
        if isinstance(src_col.get("colOptions"), dict):
            col_options = self._sanitize_meta_obj(src_col.get("colOptions"))
            # For select columns, ensure options carry only allowed keys
            if src_col.get("uidt") in ("SingleSelect", "MultiSelect"):
                opts = (col_options or {}).get("options")
                if isinstance(opts, list):
                    sanitized_opts: List[Dict[str, Any]] = []
                    for o in opts:
                        if not isinstance(o, dict):
                            continue
                        so: Dict[str, Any] = {}
                        # Keep only stable fields that matter
                        if "title" in o:
                            so["title"] = o.get("title")
                        if "color" in o:
                            so["color"] = o.get("color")
                        if "order" in o:
                            so["order"] = o.get("order")
                        sanitized_opts.append(so)
                    col_options["options"] = sanitized_opts
            payload["colOptions"] = col_options

        return payload

    def _create_target_columns_from_source(self) -> None:
        src_table_id = self.cfg["SOURCE_TABLE_ID"]
        tgt_table_id = self.cfg["TARGET_TABLE_ID"]

        src_meta = self._get_table_meta(src_table_id)
        src_cols = src_meta.get("columns", [])

        print(f"[INFO] SOURCE has {len(src_cols)} columns")
        print("[INFO] Creating TARGET columns from SOURCE schema...")

        created = 0
        failed: List[str] = []

        for col in src_cols:
            if col.get("system"):
                continue
            if col.get("pk"):
                continue

            payload = self._build_create_column_payload(col)
            try:
                self._request("POST", f"/api/v1/db/meta/tables/{tgt_table_id}/columns", json_body=payload)
                created += 1
                if created % 10 == 0:
                    print(f"[INFO] Created {created} columns...")
            except NocoDBCloneError as e:
                title = payload.get("title")
                failed.append(f"{title}: {e}")

        print(f"[INFO] Created {created} columns")
        if failed:
            print("[ERROR] Some columns failed to create (first 10 shown):")
            for line in failed[:10]:
                print("  -", line)
            raise NocoDBCloneError(
                f"Failed to create {len(failed)} columns. Fix those errors first before copying rows."
            )

    def _ensure_source_timestamp_columns(self) -> None:
        """Ensure the target has dedicated columns to store source CreatedAt/UpdatedAt."""
        if not self.cfg.get("PRESERVE_SOURCE_TIMESTAMPS", False):
            return

        tgt_table_id = self.cfg["TARGET_TABLE_ID"]
        tgt_meta = self._get_table_meta(tgt_table_id)
        existing_titles = {c.get("title") for c in tgt_meta.get("columns", [])}

        created_title = self.cfg.get("TARGET_SOURCE_CREATED_AT_TITLE", "Source CreatedAt")
        updated_title = self.cfg.get("TARGET_SOURCE_UPDATED_AT_TITLE", "Source UpdatedAt")

        to_create: List[Dict[str, Any]] = []
        if created_title not in existing_titles:
            to_create.append(
                {
                    "column_name": "source_created_at",
                    "title": created_title,
                    "uidt": "DateTime",
                }
            )
        if updated_title not in existing_titles:
            to_create.append(
                {
                    "column_name": "source_updated_at",
                    "title": updated_title,
                    "uidt": "DateTime",
                }
            )

        for payload in to_create:
            self._request("POST", f"/api/v1/db/meta/tables/{tgt_table_id}/columns", json_body=payload)
            print(f"[INFO] Created timestamp copy column: {payload['title']}")

    def _fetch_all_rows(self, base_id: str, table_id: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        offset = 0
        limit = 200

        while True:
            data = self._request(
                "GET",
                f"/api/v1/db/data/v1/{base_id}/{table_id}",
                params={"offset": offset, "limit": limit},
            )
            batch = (data or {}).get("list", [])
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        return rows

    def _insert_rows_bulk_or_fallback(self, base_id: str, table_id: str, rows: List[Dict[str, Any]]) -> None:
        batch_size = int(self.cfg.get("BATCH_SIZE", 50))
        total = len(rows)
        if total == 0:
            print("[INFO] No SOURCE rows to copy")
            return

        preserve_ts = bool(self.cfg.get("PRESERVE_SOURCE_TIMESTAMPS", False))
        created_title = self.cfg.get("TARGET_SOURCE_CREATED_AT_TITLE", "Source CreatedAt")
        updated_title = self.cfg.get("TARGET_SOURCE_UPDATED_AT_TITLE", "Source UpdatedAt")

        # Prepare clean rows
        cleaned_rows: List[Dict[str, Any]] = []
        for r in rows:
            payload = {k: v for k, v in r.items() if (k not in AUTO_KEYS_TO_STRIP and not str(k).startswith("nc_"))}
            if preserve_ts:
                src_created = r.get("CreatedAt") or r.get("created_at")
                src_updated = r.get("UpdatedAt") or r.get("updated_at")
                if src_created is not None:
                    payload[created_title] = src_created
                if src_updated is not None:
                    payload[updated_title] = src_updated
            cleaned_rows.append(payload)

        # Try bulk endpoint first
        bulk_path = f"/api/v1/db/data/bulk/v1/{base_id}/{table_id}"
        data_path = f"/api/v1/db/data/v1/{base_id}/{table_id}"

        def try_bulk(payload_list: List[Dict[str, Any]]):
            return self._request("POST", bulk_path, json_body=payload_list, ok_statuses=(200, 201))

        def try_single(payload_obj: Dict[str, Any]):
            return self._request("POST", data_path, json_body=payload_obj, ok_statuses=(200, 201))

        print(f"[INFO] Copying {total} rows into TARGET...")

        # Probe bulk support with 1 row
        bulk_supported = True
        try:
            try_bulk([cleaned_rows[0]])
        except Exception as e:
            print(f"[WARN] Bulk insert not available (will fallback to per-row). Reason: {e}")
            bulk_supported = False

        inserted = 0
        if bulk_supported:
            # We already inserted 1 row during probe
            inserted = 1
            start_index = 1
            for i in range(start_index, total, batch_size):
                batch = cleaned_rows[i : i + batch_size]
                try:
                    try_bulk(batch)
                    inserted += len(batch)
                except Exception as e:
                    raise NocoDBCloneError(f"Bulk insert failed at offset {i}: {e}")
                if inserted % 50 == 0 or inserted == total:
                    print(f"[INFO] Inserted {inserted}/{total} rows")
        else:
            for i, row in enumerate(cleaned_rows, 1):
                try:
                    try_single(row)
                    inserted += 1
                except Exception as e:
                    raise NocoDBCloneError(f"Row insert failed at row {i}: {e}")
                if inserted % 50 == 0 or inserted == total:
                    print(f"[INFO] Inserted {inserted}/{total} rows")

        print(f"[INFO] Done. Inserted {inserted}/{total} rows")

    def run(self) -> None:
        # 0) Read meta quickly to show what we are about to do
        print("[INFO] SOURCE (read-only):", self.cfg["SOURCE_TABLE_ID"])
        print("[INFO] TARGET (will be overwritten):", self.cfg["TARGET_TABLE_ID"])

        # 1) Clear target rows
        self._delete_target_rows()

        # 2) Clear target columns
        self._delete_target_columns()

        # 3) Create target columns from source (incl. select options/colors)
        self._create_target_columns_from_source()

        # 3.1) Add dedicated columns to preserve source timestamps (CreatedAt/UpdatedAt)
        self._ensure_source_timestamp_columns()

        # 4) Copy data rows
        src_rows = self._fetch_all_rows(self.cfg["SOURCE_BASE_ID"], self.cfg["SOURCE_TABLE_ID"])
        print(f"[INFO] Fetched {len(src_rows)} SOURCE rows")

        # Ensure target empty again (some APIs auto-create rows on schema ops)
        self._delete_target_rows()

        self._insert_rows_bulk_or_fallback(self.cfg["TARGET_BASE_ID"], self.cfg["TARGET_TABLE_ID"], src_rows)

        # 5) Quick verify
        verify = self._request(
            "GET",
            f"/api/v1/db/data/v1/{self.cfg['TARGET_BASE_ID']}/{self.cfg['TARGET_TABLE_ID']}",
            params={"limit": 1},
        )
        total = (verify or {}).get("pageInfo", {}).get("totalRows")
        print(f"[INFO] VERIFY: TARGET totalRows = {total}")
        sample = (verify or {}).get("list", [])
        if sample:
            print("[INFO] VERIFY sample keys:", list(sample[0].keys())[:15], "...")


def main() -> None:
    try:
        NocoDBCloner(CONFIG).run()
    except NocoDBCloneError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("[WARN] Interrupted", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
