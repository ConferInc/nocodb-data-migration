#!/usr/bin/env python3
import argparse
import os
import sys
import 
from typing import Any, Dict, List, Optional, Tuple

import requests


AUTO_KEYS_TO_STRIP = {
    "Id",
    "id",
    "CreatedAt",
    "UpdatedAt",
    "created_at",
    "updated_at",
}

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
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        source_base_id: str,
        source_table_id: str,
        target_base_id: str,
        target_table_id: str,
        batch_size: int,
        request_timeout_sec: int,
        sleep_between_requests_sec: float,
        delete_target_rows_first: bool,
        delete_target_columns_first: bool,
        preserve_source_timestamps: bool,
        target_source_created_at_title: str,
        target_source_updated_at_title: str,
    ):
        self.base_url = base_url.rstrip("/")
        self.source_base_id = source_base_id
        self.source_table_id = source_table_id
        self.target_base_id = target_base_id
        self.target_table_id = target_table_id
        self.batch_size = batch_size
        self.request_timeout_sec = request_timeout_sec
        self.sleep_between_requests_sec = sleep_between_requests_sec
        self.delete_target_rows_first = delete_target_rows_first
        self.delete_target_columns_first = delete_target_columns_first
        self.preserve_source_timestamps = preserve_source_timestamps
        self.target_source_created_at_title = target_source_created_at_title
        self.target_source_updated_at_title = target_source_updated_at_title

        self.s = requests.Session()
        self.s.headers.update(
            {
                "xc-token": token,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def _sleep(self) -> None:
        if self.sleep_between_requests_sec > 0:
            time.sleep(self.sleep_between_requests_sec)

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
            timeout=self.request_timeout_sec,
        )
        if r.status_code not in ok_statuses:
            raise NocoDBCloneError(f"{method} {url} -> {r.status_code}: {r.text}")
        if not r.content:
            return None
        try:
            return r.json()
        except Exception:
            return r.text

    def _get_table_meta(self, table_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v1/db/meta/tables/{table_id}")

    def _sanitize_meta_obj(self, obj: Any) -> Any:
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
        if not self.delete_target_rows_first:
            return

        print("[INFO] Deleting all TARGET rows...")
        deleted = 0
        while True:
            data = self._request(
                "GET",
                f"/api/v1/db/data/v1/{self.target_base_id}/{self.target_table_id}",
                params={"offset": 0, "limit": 200},
            )
            rows = (data or {}).get("list", [])
            if not rows:
                break

            for row in rows:
                row_id = row.get("Id") or row.get("id")
                if row_id is None:
                    continue
                self._request(
                    "DELETE",
                    f"/api/v1/db/data/v1/{self.target_base_id}/{self.target_table_id}/{row_id}",
                    ok_statuses=(200, 204),
                )
                deleted += 1
                if deleted % 50 == 0:
                    print(f"[INFO] Deleted {deleted} rows...")

        print(f"[INFO] Deleted total {deleted} TARGET rows")

    def _delete_target_columns(self) -> None:
        if not self.delete_target_columns_first:
            return

        tgt_meta = self._get_table_meta(self.target_table_id)
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
                print(f"[WARN] Could not delete column '{c.get('title')}' ({col_id}): {e}")

        print(f"[INFO] Deleted {deleted} TARGET columns")

    def _build_create_column_payload(self, src_col: Dict[str, Any]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        payload["column_name"] = src_col.get("column_name")
        payload["title"] = src_col.get("title") or src_col.get("column_name")

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

        if "order" in src_col and src_col.get("order") is not None:
            payload["order"] = src_col.get("order")

        if isinstance(src_col.get("meta"), dict):
            payload["meta"] = self._sanitize_meta_obj(src_col.get("meta"))

        if isinstance(src_col.get("colOptions"), dict):
            col_options = self._sanitize_meta_obj(src_col.get("colOptions"))
            if src_col.get("uidt") in ("SingleSelect", "MultiSelect"):
                opts = (col_options or {}).get("options")
                if isinstance(opts, list):
                    sanitized_opts: List[Dict[str, Any]] = []
                    for o in opts:
                        if not isinstance(o, dict):
                            continue
                        so: Dict[str, Any] = {}
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
        src_meta = self._get_table_meta(self.source_table_id)
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
                self._request(
                    "POST",
                    f"/api/v1/db/meta/tables/{self.target_table_id}/columns",
                    json_body=payload,
                )
                created += 1
                if created % 10 == 0:
                    print(f"[INFO] Created {created} columns...")
            except NocoDBCloneError as e:
                failed.append(f"{payload.get('title')}: {e}")

        print(f"[INFO] Created {created} columns")
        if failed:
            print("[ERROR] Some columns failed to create (first 10 shown):")
            for line in failed[:10]:
                print("  -", line)
            raise NocoDBCloneError(f"Failed to create {len(failed)} columns.")

    def _ensure_source_timestamp_columns(self) -> None:
        if not self.preserve_source_timestamps:
            return

        tgt_meta = self._get_table_meta(self.target_table_id)
        existing_titles = {c.get("title") for c in tgt_meta.get("columns", [])}

        to_create: List[Dict[str, Any]] = []
        if self.target_source_created_at_title not in existing_titles:
            to_create.append(
                {
                    "column_name": "source_created_at",
                    "title": self.target_source_created_at_title,
                    "uidt": "DateTime",
                }
            )
        if self.target_source_updated_at_title not in existing_titles:
            to_create.append(
                {
                    "column_name": "source_updated_at",
                    "title": self.target_source_updated_at_title,
                    "uidt": "DateTime",
                }
            )

        for payload in to_create:
            self._request(
                "POST",
                f"/api/v1/db/meta/tables/{self.target_table_id}/columns",
                json_body=payload,
            )
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
        total = len(rows)
        if total == 0:
            print("[INFO] No SOURCE rows to copy")
            return

        cleaned_rows: List[Dict[str, Any]] = []
        for r in rows:
            payload = {k: v for k, v in r.items() if (k not in AUTO_KEYS_TO_STRIP and not str(k).startswith("nc_"))}
            if self.preserve_source_timestamps:
                src_created = r.get("CreatedAt") or r.get("created_at")
                src_updated = r.get("UpdatedAt") or r.get("updated_at")
                if src_created is not None:
                    payload[self.target_source_created_at_title] = src_created
                if src_updated is not None:
                    payload[self.target_source_updated_at_title] = src_updated
            cleaned_rows.append(payload)

        bulk_path = f"/api/v1/db/data/bulk/v1/{base_id}/{table_id}"
        data_path = f"/api/v1/db/data/v1/{base_id}/{table_id}"

        def try_bulk(payload_list: List[Dict[str, Any]]):
            return self._request("POST", bulk_path, json_body=payload_list, ok_statuses=(200, 201))

        def try_single(payload_obj: Dict[str, Any]):
            return self._request("POST", data_path, json_body=payload_obj, ok_statuses=(200, 201))

        print(f"[INFO] Copying {total} rows into TARGET...")

        bulk_supported = True
        try:
            try_bulk([cleaned_rows[0]])
        except Exception as e:
            print(f"[WARN] Bulk insert not available (will fallback to per-row). Reason: {e}")
            bulk_supported = False

        inserted = 0
        if bulk_supported:
            inserted = 1
            for i in range(1, total, self.batch_size):
                batch = cleaned_rows[i : i + self.batch_size]
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
        print("[INFO] SOURCE (read-only):", self.source_table_id)
        print("[INFO] TARGET (will be overwritten):", self.target_table_id)

        self._delete_target_rows()
        self._delete_target_columns()
        self._create_target_columns_from_source()
        self._ensure_source_timestamp_columns()

        src_rows = self._fetch_all_rows(self.source_base_id, self.source_table_id)
        print(f"[INFO] Fetched {len(src_rows)} SOURCE rows")

        self._delete_target_rows()
        self._insert_rows_bulk_or_fallback(self.target_base_id, self.target_table_id, src_rows)

        verify = self._request(
            "GET",
            f"/api/v1/db/data/v1/{self.target_base_id}/{self.target_table_id}",
            params={"limit": 1},
        )
        total = (verify or {}).get("pageInfo", {}).get("totalRows")
        print(f"[INFO] VERIFY: TARGET totalRows = {total}")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default=_env("NOCODB_URL"), required=_env("NOCODB_URL") is None)
    p.add_argument("--token", default=_env("NOCODB_TOKEN"), required=_env("NOCODB_TOKEN") is None)

    p.add_argument("--source-base-id", default=_env("SOURCE_BASE_ID"), required=_env("SOURCE_BASE_ID") is None)
    p.add_argument("--source-table-id", default=_env("SOURCE_TABLE_ID"), required=_env("SOURCE_TABLE_ID") is None)
    p.add_argument("--target-base-id", default=_env("TARGET_BASE_ID"), required=_env("TARGET_BASE_ID") is None)
    p.add_argument("--target-table-id", default=_env("TARGET_TABLE_ID"), required=_env("TARGET_TABLE_ID") is None)

    p.add_argument("--batch-size", type=int, default=int(_env("BATCH_SIZE", "50")))
    p.add_argument("--timeout", type=int, default=int(_env("REQUEST_TIMEOUT_SEC", "60")))
    p.add_argument("--sleep", type=float, default=float(_env("SLEEP_BETWEEN_REQUESTS_SEC", "0")))

    p.add_argument("--no-delete-rows", action="store_true")
    p.add_argument("--no-delete-columns", action="store_true")

    p.add_argument("--preserve-source-timestamps", action="store_true")
    p.add_argument("--created-at-title", default=_env("TARGET_SOURCE_CREATED_AT_TITLE", "Created At1"))
    p.add_argument("--updated-at-title", default=_env("TARGET_SOURCE_UPDATED_AT_TITLE", "Source UpdatedAt"))

    args = p.parse_args()

    try:
        NocoDBCloner(
            base_url=args.base_url,
            token=args.token,
            source_base_id=args.source_base_id,
            source_table_id=args.source_table_id,
            target_base_id=args.target_base_id,
            target_table_id=args.target_table_id,
            batch_size=args.batch_size,
            request_timeout_sec=args.timeout,
            sleep_between_requests_sec=args.sleep,
            delete_target_rows_first=(not args.no_delete_rows),
            delete_target_columns_first=(not args.no_delete_columns),
            preserve_source_timestamps=bool(args.preserve_source_timestamps),
            target_source_created_at_title=args.created_at_title,
            target_source_updated_at_title=args.updated_at_title,
        ).run()
    except NocoDBCloneError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("[WARN] Interrupted", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
