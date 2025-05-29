# app/services/converter.py
import os
import pandas as pd
import pyodbc
import pyreadstat
import numpy as np
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict
from io import BytesIO
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import tempfile
import logging
import time
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Tuple
from app.core.config import settings
# Import configuration

# Import database utilities
from app.db.session import ConnectionPool

logger = logging.getLogger("sas_importer")

class ProjectRequest(BaseModel):
    project_name: str

def process_file(schema_name, table_name, tmp_path):
    """Process SAS file with optimized database operations and connection management"""
    start_time = time.time()
    logger.info(f"Starting processing: {schema_name}.{table_name}")
    try:
        # Read SAS file
        read_start = time.time()
        df, meta = pyreadstat.read_sas7bdat(tmp_path)
        logger.info(f"Read SAS file {table_name} in {time.time() - read_start:.2f}s, rows={len(df)}, cols={len(df.columns)}")
        # Data conversion
        conv_start = time.time()
        df, date_cols = convert_sas_date(df, meta)
        df = df.replace([np.inf, -np.inf], np.nan)
        logger.info(f"Data conversion completed in {time.time() - conv_start:.2f}s")
        # Prepare column definitions
        formats = getattr(meta, 'column_formats', [None] * len(df.columns))
        col_defs, type_map = [], {}
        for col in df.columns:
            col_idx = df.columns.get_loc(col)
            sql_type = get_sql_type(col, df[col].dtype, formats[col_idx])
            type_map[col] = sql_type
            col_defs.append(f'[{col}] {sql_type} NULL')
        # Database operations with connection management
        conn = None
        try:
            # Get connection with timeout handling
            conn = ConnectionPool.get_connection(settings.MAIN_DB_NAME)
            cursor = conn.cursor()
            # Create table if not exists
            create_start = time.time()
            cursor.execute(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = ? AND t.name = ?
                )
                BEGIN
                    CREATE TABLE [{schema_name}].[{table_name}] ({', '.join(col_defs)})
                END
            """, schema_name, table_name)
            logger.info(f"Table creation check for {table_name} took {time.time() - create_start:.2f}s")
            # Prepare insert query
            columns = [f'[{col}]' for col in df.columns]
            insert_sql = f"""
                INSERT INTO [{schema_name}].[{table_name}]
                ({', '.join(columns)})
                VALUES ({', '.join(['?'] * len(columns))})
            """
            # Configure input types
            type_info = [
                pyodbc.SQL_TYPE_TIMESTAMP if type_map[col].startswith('DATETIME') else
                pyodbc.SQL_DECIMAL if 'DECIMAL' in type_map[col] else
                pyodbc.SQL_REAL if 'FLOAT' in type_map[col] else None
                for col in df.columns
            ]
            cursor.setinputsizes(type_info)
            cursor.fast_executemany = True
            # Process chunks with optimized insertion
            total_rows = len(df)
            chunk_count = (total_rows + settings.CHUNK_SIZE - 1) // settings.CHUNK_SIZE
            logger.info(f"Inserting {total_rows} rows in {chunk_count} chunks")
            insert_start = time.time()
            total_inserted = 0
            for i in range(0, total_rows, settings.CHUNK_SIZE):
                chunk_end = min(i + settings.CHUNK_SIZE, total_rows)
                chunk_df = df.iloc[i:chunk_end]
                # Prepare data using vectorized operations
                data_chunk = []
                for row in chunk_df.itertuples(index=False):
                    row_data = []
                    for val, col in zip(row, df.columns):
                        if pd.isna(val):
                            row_data.append(None)
                        elif type_map[col].startswith('DATETIME') and isinstance(val, pd.Timestamp):
                            row_data.append(val.to_pydatetime())
                        else:
                            row_data.append(val)
                    data_chunk.append(tuple(row_data))
                # Execute chunk insert
                cursor.executemany(insert_sql, data_chunk)
                inserted = len(data_chunk)
                total_inserted += inserted
                chunk_num = (i // settings.CHUNK_SIZE) + 1
                logger.info(f"Inserted chunk {chunk_num}/{chunk_count} ({inserted} rows) for {table_name}")
            conn.commit()
            logger.info(f"Inserted {total_inserted} rows in {time.time() - insert_start:.2f}s")
        except Exception as e:
            logger.error(f"Database error in {table_name}: {str(e)}", exc_info=True)
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                ConnectionPool.return_connection(conn, settings.MAIN_DB_NAME)
        total_time = time.time() - start_time
        logger.info(f"✅ Completed {schema_name}.{table_name} in {total_time:.2f}s")
        return f"{schema_name}.{table_name}"
    except Exception as e:
        logger.error(f"🚫 Failed {schema_name}.{table_name}: {str(e)}", exc_info=True)
        return None
    finally:
        try:
            os.remove(tmp_path)
        except:
            logger.warning(f"Could not delete temporary file: {tmp_path}")

def download_blob(blob_client):
    """Optimized blob download with Azure SDK compatibility"""
    try:
        start_time = time.time()
        blob_name = blob_client.blob_name
        # Get blob properties first
        blob_props = blob_client.get_blob_properties()
        blob_size = blob_props.size
        # Download with timeout handling
        download_stream = blob_client.download_blob(timeout=settings.AZURE_DOWNLOAD_TIMEOUT)
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix=".sas7bdat", delete=False) as tmp_file:
            # Download in 4MB chunks for better memory management
            chunk_size = 4 * 1024 * 1024
            downloaded = 0
            chunks = download_stream.chunks()
            while True:
                try:
                    chunk = next(chunks)
                    tmp_file.write(chunk)
                    downloaded += len(chunk)
                    # Log progress every 20%
                    if blob_size > 0:
                        progress = (downloaded / blob_size) * 100
                        if progress >= 20 and int(progress) % 20 == 0:
                            logger.info(f"Downloading {blob_name}: {progress:.0f}% complete")
                except StopIteration:
                    break
            tmp_path = tmp_file.name
        duration = time.time() - start_time
        speed = blob_size / (1024 * 1024 * duration) if duration > 0 else 0
        logger.info(f"✅ Downloaded {blob_name} ({blob_size/1024/1024:.2f} MB) in {duration:.2f}s ({speed:.2f} MB/s)")
        return tmp_path
    except Exception as e:
        logger.error(f"🚫 Download failed for {blob_name}: {str(e)}", exc_info=True)
        raise

def convert_sas_date(df, meta):
    date_cols = []
    for idx, fmt in enumerate(getattr(meta, 'column_formats', [])):
        if fmt and any(f in fmt.upper() for f in ['DATE', 'DATETIME', 'YYMMDD', 'HHMM']):
            col = df.columns[idx]
            df[col] = pd.to_datetime(df[col].where(pd.notnull(df[col]), None), unit='s', origin='1960-01-01', errors='coerce')
            date_cols.append(col)
    return df, date_cols

def get_sql_type(col_name, pandas_dtype, sas_format=None):
    if sas_format:
        sas_format = sas_format.upper()
        if any(fmt in sas_format for fmt in ['DATETIME', 'DATE', 'TIME', 'YYMMDD', 'HHMM']):
            return 'DATETIME2'
        if any(fmt in sas_format for fmt in ['BEST', 'COMMA', 'DOLLAR', 'PERCENT', 'NUMERIC']):
            if '.' in sas_format:
                parts = sas_format.split('.')
                if len(parts) == 2 and parts[1].isdigit():
                    return f'DECIMAL({sum(map(len, parts))},{len(parts[1])})'
            return 'FLOAT'
        return 'NVARCHAR(255)'
    dtype_map = {
        'int': 'BIGINT',
        'float': 'FLOAT',
        'bool': 'BIT',
        'object': 'NVARCHAR(255)',
        'datetime64[ns]': 'DATETIME2'
    }
    return dtype_map.get(str(pandas_dtype), 'NVARCHAR(255)')

def create_schema(schema_name: str):
    try:
        logger.info(f"Creating schema: {schema_name}")
        with ConnectionPool.get_connection(settings.MAIN_DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema_name)
            if not cursor.fetchone():
                cursor.execute(f"CREATE SCHEMA [{schema_name}]")
            conn.commit()
        logger.info(f"Schema {schema_name} created/verified")
    except Exception as e:
        logger.error(f"Schema creation failed for {schema_name}: {str(e)}", exc_info=True)
        raise

def upload_sas_files(req: ProjectRequest):
    start_time = datetime.now()
    inserted_tables = []
    project_name = req.project_name
    logger.info(f"🚀 Starting SAS import for project: {project_name}")
    try:
        project_prefix = f"{settings.BASE_BLOB_PATH}/{project_name}"
        blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(settings.AZURE_STORAGE_CONTAINER_NAME)
        # Ensure database exists
        try:
            with ConnectionPool.get_connection() as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(f"""
                    IF DB_ID('{settings.MAIN_DB_NAME}') IS NULL
                    CREATE DATABASE [{settings.MAIN_DB_NAME}]
                """)
            logger.info(f"Database {settings.MAIN_DB_NAME} verified")
        except Exception as e:
            logger.error(f"Database verification failed: {str(e)}")
            return {"status": "error", "message": f"Database setup failed: {str(e)}"}
        # Create schemas
        schema_names = [f"{project_name}_ADAM", f"{project_name}_SDTM"]
        for schema_name in schema_names:
            try:
                create_schema(schema_name)
            except Exception as e:
                return {"status": "error", "message": f"Schema creation failed: {str(e)}"}
        # Find all SAS files
        file_tasks = []
        for domain in ['ADAM', 'SDTM']:
            schema_name = f"{project_name}_{domain}"
            domain_prefix = f"{project_prefix}/{domain}/"
            blobs = list(container_client.list_blobs(name_starts_with=domain_prefix))
            logger.info(f"Found {len(blobs)} blobs in {domain_prefix}")
            for blob in blobs:
                if blob.name.lower().endswith('.sas7bdat'):
                    table_name = os.path.splitext(os.path.basename(blob.name))[0].lower()
                    blob_client = container_client.get_blob_client(blob)
                    file_tasks.append((schema_name, table_name, blob_client))
        logger.info(f"📁 Found {len(file_tasks)} SAS files for processing")
        # Process files in two stages: download then processing
        with ThreadPoolExecutor(max_workers=settings.DOWNLOAD_WORKERS) as download_executor, \
             ThreadPoolExecutor(max_workers=settings.PROCESSING_WORKERS) as processing_executor:
            # Submit download tasks
            download_futures = {}
            for task in file_tasks:
                schema_name, table_name, blob_client = task
                future = download_executor.submit(download_blob, blob_client)
                download_futures[future] = (schema_name, table_name, blob_client.blob_name)
                logger.info(f"Submitted download: {blob_client.blob_name}")
            # Process downloads as they complete
            processing_futures = []
            for future in as_completed(download_futures):
                schema_name, table_name, blob_name = download_futures[future]
                try:
                    tmp_path = future.result()
                    logger.info(f"✅ Download completed: {blob_name}")
                    # Submit processing task
                    p_future = processing_executor.submit(
                        process_file, 
                        schema_name, 
                        table_name, 
                        tmp_path
                    )
                    processing_futures.append((p_future, blob_name))
                    logger.info(f"Submitted processing: {blob_name}")
                except Exception as e:
                    logger.error(f"🚫 Download failed for {blob_name}: {str(e)}")
            # Process results as they complete
            for p_future, blob_name in processing_futures:
                try:
                    result = p_future.result()
                    if result:
                        inserted_tables.append(result)
                        logger.info(f"✅ Successfully processed: {blob_name}")
                    else:
                        logger.error(f"🚫 Processing failed for {blob_name}")
                except Exception as e:
                    logger.error(f"🚫 Processing failed for {blob_name}: {str(e)}")
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"🏁 Completed project {project_name} in {duration:.2f} seconds")
        return {
            "status": "success",
            "tables_inserted": inserted_tables,
            "duration_seconds": duration,
            "files_processed": len(inserted_tables),
            "total_files": len(file_tasks)
        }
    except Exception as e:
        logger.error(f"🔥 Project processing failed: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}