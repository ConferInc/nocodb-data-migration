#!/usr/bin/env python3
"""
Universal NocoDB Upload Script
==============================

One-stop solution for cleaning XLSX data and uploading it to NocoDB.

Features:
- Robust Header Detection
- Nested Data Flattening (JSON/Dict in cells)
- Automatic Column Mapping (Coalesces 'Subitems' -> 'Name' if empty)
- Batch Upload (Bulk API)
- Rate Limiting & Retry Logic

Usage:
  python universal_nocodb_upload.py --file "data.xlsx" --table "TargetTable" ...
"""

import argparse
import ast
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests


# ==========================================
# DATA CLEANER LOGIC
# ==========================================

class NocoDBCleaner
    """Handles cleaning, flattening, and standardizing of dataframe."""
    
    @staticmethod
    def flatten_value(key: str, value: Any) -> Dict[str, Any]:
        """Recursively flatten dict/list/JSON string into {key: value} map."""
        res = {}
        
        # 1. Try string parsing
        if isinstance(value, str):
            s_val = value.strip()
            # Heuristic for JSON/Dict string
            if (s_val.startswith('{') and s_val.endswith('}')) or \
               (s_val.startswith('[') and s_val.endswith(']')):
                try:
                    parsed = json.loads(s_val)
                    return NocoDBCleaner.flatten_obj(key, parsed)
                except:
                    try:
                        parsed = ast.literal_eval(s_val)
                        return NocoDBCleaner.flatten_obj(key, parsed)
                    except:
                        pass
        
        # 2. Direct structure
        if isinstance(value, (dict, list)):
            return NocoDBCleaner.flatten_obj(key, value)
            
        # 3. Scalar
        res[key] = value
        return res

    @staticmethod
    def flatten_obj(prefix: str, obj: Any) -> Dict[str, Any]:
        """Helper to flatten structured objects."""
        res = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{prefix}_{k}" if prefix else str(k)
                res.update(NocoDBCleaner.flatten_value(new_key, v))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                new_key = f"{prefix}_{i+1}" if prefix else str(i+1)
                res.update(NocoDBCleaner.flatten_value(new_key, v))
        else:
            res[prefix] = obj
        return res

    @classmethod
    def process_file(cls, file_path: str) -> List[Dict[str, Any]]:
        """Main cleaning pipeline."""
        print(f"Reading {file_path}...")
        
        # 1. Header Detection
        # Read first few rows without header to inspect
        df_raw = pd.read_excel(file_path, header=None)
        
        header_idx = 0
        max_text_cols = -1
        
        # Scan first 20 rows for likely header
        for idx, row in df_raw.iloc[:20].iterrows():
            # Count columns that are purely text (no numbers, no JSON-like)
            vals = [str(x).strip() for x in row if pd.notna(x)]
            if not vals:
                continue
                
            text_score = 0
            for v in vals:
                # Check if it looks like a header label (not too long, no special chars)
                if len(v) < 60 and not any(c in v for c in '{}[]'):
                    text_score += 1
            
            if len(vals) > 1 and text_score > max_text_cols:
                max_text_cols = text_score
                header_idx = idx
                
        print(f"Detected header row at index {header_idx}")
        
        # Re-read with header
        df = pd.read_excel(file_path, header=header_idx)
        
        # 2. Row Processing
        cleaned_records = []
        original_cols = [str(c).strip() for c in df.columns]
        
        # Helper to check if a row repeats the header
        def is_repeated_header(row_vals):
            matches = sum(1 for v in row_vals if v in original_cols)
            return matches > (len(original_cols) * 0.5)

        for idx, row in df.iterrows():
            if row.isna().all():
                continue
                
            # Convert row to simple dict for checking
            row_dict = {str(k).strip(): v for k, v in row.items()}
            
            # Check repeated header
            vals_str = [str(v).strip() for v in row.values if pd.notna(v)]
            if is_repeated_header(vals_str):
                # print(f"Skipping repeated header at row {idx}")
                continue
                
            # --- FILTER SPARSE ROWS ---
            # User requested to skip rows with only 1 column of data
            non_empty_count = sum(1 for v in row.values if pd.notna(v) and str(v).strip() != '')
            if non_empty_count <= 1:
                # print(f"Skipping sparse row at index {idx} (only {non_empty_count} value)")
                continue

            # --- FIX FOR MISSING NAMES REMOVED ---
            # User requested to keep Name and Subitems separate.
            # We will just upload what is in the row.
            
            # Flatten Record
            flat_record = {}
            for col, val in row_dict.items():
                if pd.isna(val) or str(val).strip() == '':
                    continue
                # Handle unnamed columns
                if str(col).startswith('Unnamed:'):
                     continue # Skip unnamed garbage columns usually
                     
                flat_record.update(cls.flatten_value(col, val))
                
            if flat_record:
                cleaned_records.append(flat_record)
        
        return cleaned_records


# ==========================================
# UPLOADER LOGIC
# ==========================================

class NocoDBUploader:
    def __init__(self, base_url, token, base_id):
        self.base_url = base_url.rstrip('/')
        self.base_id = base_id
        self.session = requests.Session()
        self.session.headers.update({
            'xc-token': token,
            'Content-Type': 'application/json'
        })
    
    def _request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}{endpoint}"
        for i in range(3):
            try:
                r = self.session.request(method, url, **kwargs)
                r.raise_for_status()
                return r.json() if r.content else {}
            except Exception as e:
                if i == 2:
                     if 'r' in locals() and r is not None:
                         print(f"API Error Response: {r.text}")
                     raise e
                time.sleep(1)

    def create_table(self, name: str, records: List[Dict]) -> str:
        # 1. Infer Columns from all records
        all_keys = set()
        for r in records:
            all_keys.update(r.keys())
            
        columns = []
        for key in all_keys:
            col_def = {
                'title': key,
                'column_name': key.lower().replace(' ', '_').replace('-', '_')[:50], # Sanitize
                'uidt': 'SingleLineText'
            }
            # Heuristic type check
            sample_val = next((r[key] for r in records if key in r and r[key] is not None), None)
            if isinstance(sample_val, (int, float)) and not isinstance(sample_val, bool):
                col_def['uidt'] = 'Number'
            elif isinstance(sample_val, bool):
                col_def['uidt'] = 'Checkbox'
                
            columns.append(col_def)
            
        # Explicit Primary Key to ensure editability
        # Check if 'id' already exists in columns to avoid conflict
        if not any(c['column_name'] == 'id' for c in columns):
            pk_col = {
                'title': 'Id',
                'column_name': 'id',
                'uidt': 'ID',
                'pk': True
            }
            columns.insert(0, pk_col)
            
        # Ensure 'Title' or 'Name' is first if possible? NocoDB handles this.
        
        # 2. Create
        print(f"Creating table '{name}' with {len(columns)} columns...")
        payload = {
            'table_name': name,
            'title': name,
            'columns': columns
        }
        res = self._request('POST', f'/api/v1/db/meta/projects/{self.base_id}/tables', json=payload)
        return res['id']
        
    def upload_bulk(self, table_id: str, records: List[Dict]):
        # Batch upload
        batch_size = 100
        total = len(records)
        bulk_url = f'/api/v1/db/data/bulk/v1/{self.base_id}/{table_id}'
        single_url = f'/api/v1/db/data/noco/{self.base_id}/{table_id}'
        
        # Helper to make records JSON safe
        def simple_json_safe(obj):
            import datetime
            if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
                return str(obj)
            if isinstance(obj, (datetime.date, datetime.time, datetime.datetime)):
                return str(obj)
            if pd.isna(obj):
                return None
            return obj
            
        # Pre-process all records to be safe
        safe_records = []
        for r in records:
            safe_r = {k: simple_json_safe(v) for k, v in r.items()}
            safe_records.append(safe_r)
            
        uploaded = 0
        for i in range(0, total, batch_size):
            batch = safe_records[i:i+batch_size]
            
            # Ensure None for missing keys in this batch (pandas vs dict difference)
            # Actually API handles missing keys fine usually.
            
            try:
                self._request('POST', bulk_url, json=batch)
                uploaded += len(batch)
                print(f"Uploaded {uploaded}/{total}")
            except Exception as e:
                print(f"Bulk failed at batch {i}: {e}. Fallback to single...")
                for r in batch:
                    try:
                        self._request('POST', single_url, json=r)
                        uploaded += 1
                    except Exception as inner:
                        print(f"Row fail: {inner}")
            
            time.sleep(0.5)

# ==========================================
# MAIN
# ==========================================

def main():
    parser = argparse.ArgumentParser(description="Universal NocoDB Uploader")
    parser.add_argument('--file', required=True)
    parser.add_argument('--table-name', required=True)
    parser.add_argument('--base-url', required=True)
    parser.add_argument('--token', required=True)
    parser.add_argument('--base-id', required=True)
    
    args = parser.parse_args()
    
    # 1. Clean
    print(f"Cleaning {args.file}...")
    try:
        records = NocoDBCleaner.process_file(args.file)
    except Exception as e:
        print(f"Cleaning failed: {e}")
        sys.exit(1)
        
    if not records:
        print("No records found after cleaning.")
        sys.exit(1)
        
    print(f"Ready to upload {len(records)} records.")
    
    # 2. Upload
    uploader = NocoDBUploader(args.base_url, args.token, args.base_id)
    try:
        table_id = uploader.create_table(args.table_name, records)
        print(f"Table created: {table_id}")
        time.sleep(2) # propagation
        uploader.upload_bulk(table_id, records)
        print("Success!")
    except Exception as e:
        print(f"Upload failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
