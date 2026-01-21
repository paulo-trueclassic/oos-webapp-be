import os
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

from core.logger import get_logger
from core.config import (
    BIGQUERY_DATASET,
    BIGQUERY_STORD_DETAILS_TABLE,
    BIGQUERY_SHIPBOB_DETAILS_TABLE,
)

logger = get_logger(__name__)


class BigQueryClientError(Exception):
    """Custom exception for BigQuery client initialization errors"""
    pass

class BigQueryService:
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.dataset_id = BIGQUERY_DATASET
        self._client = None  # Lazy initialization

        if not self.project_id:
            logger.warning("GOOGLE_CLOUD_PROJECT environment variable is not set. BigQuery operations will fail.")
            self.stord_details_table_id = None
            self.shipbob_details_table_id = None
        else:
            self.stord_details_table_id = f"{self.project_id}.{self.dataset_id}.{BIGQUERY_STORD_DETAILS_TABLE}"
            self.shipbob_details_table_id = f"{self.project_id}.{self.dataset_id}.{BIGQUERY_SHIPBOB_DETAILS_TABLE}"
        
        # Schemas for raw flattened JSON tables
        self._stord_details_schema = [
            bigquery.SchemaField("order_number", "STRING", mode="REQUIRED"),  # Stord primary ID
            bigquery.SchemaField("raw_json", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("first_seen_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("last_seen_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("is_currently_in_exception", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("resolved_timestamp", "TIMESTAMP", mode="NULLABLE"),
        ]
        self._shipbob_details_schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),  # Shipbob primary ID
            bigquery.SchemaField("raw_json", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("first_seen_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("last_seen_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("is_currently_in_exception", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("resolved_timestamp", "TIMESTAMP", mode="NULLABLE"),
        ]

    @property
    def client(self):
        """Lazy initialization of BigQuery client"""
        if self._client is None:
            try:
                self._client = self._get_bigquery_client()
            except (RuntimeError, ValueError) as e:
                # Convert to custom exception for easier handling in endpoints
                raise BigQueryClientError(str(e)) from e
        return self._client

    def _get_bigquery_client(self):
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT is not set. Cannot initialize BigQuery client.")
        
        credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        
        # Debug logging (without exposing sensitive data)
        if credentials_json:
            cred_length = len(credentials_json)
            cred_preview = credentials_json[:50] if cred_length > 50 else credentials_json
            logger.debug(f"GOOGLE_CREDENTIALS_JSON found: length={cred_length}, preview={cred_preview}...")
        else:
            logger.debug("GOOGLE_CREDENTIALS_JSON is not set or is empty")
        
        if credentials_json and credentials_json.strip():
            # Remove surrounding quotes if present (common .env file issue)
            credentials_json = credentials_json.strip()
            if credentials_json.startswith('"') and credentials_json.endswith('"'):
                credentials_json = credentials_json[1:-1]
            if credentials_json.startswith("'") and credentials_json.endswith("'"):
                credentials_json = credentials_json[1:-1]
            
            try:
                logger.info("Initializing BigQuery client with credentials from GOOGLE_CREDENTIALS_JSON.")
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                return bigquery.Client(credentials=credentials, project=self.project_id)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON as JSON: {e}")
                logger.error(f"JSON error at line {e.lineno}, column {e.colno}: {e.msg}")
                logger.error("Make sure GOOGLE_CREDENTIALS_JSON contains valid JSON (not quoted, no extra escaping)")
                logger.warning("Falling back to default BigQuery client initialization.")
                # Fall through to default initialization
            except (ValueError, KeyError) as e:
                logger.error(f"Failed to use GOOGLE_CREDENTIALS_JSON: {e}")
                logger.error("The JSON may be missing required fields for a service account key")
                logger.warning("Falling back to default BigQuery client initialization.")
                # Fall through to default initialization
            except Exception as e:
                logger.error(f"Unexpected error initializing BigQuery client with credentials: {e}")
                logger.warning("Falling back to default BigQuery client initialization.")
                # Fall through to default initialization
        
        # Default initialization (either no credentials or fallback)
        logger.warning("Using default BigQuery client initialization.")
        try:
            return bigquery.Client(project=self.project_id)
        except Exception as e:
            logger.error(f"Failed to initialize default BigQuery client: {e}")
            logger.error("BigQuery client initialization failed.")
            logger.error("Please ensure GOOGLE_CREDENTIALS_JSON is set correctly or default credentials are available.")
            # Store the error to be raised when client is actually accessed
            raise RuntimeError(f"BigQuery client initialization failed: {e}. Please check your credentials configuration.")

    def create_tables_if_not_exists(self):
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset '{self.dataset_id}' already exists.")
        except NotFound:
            logger.info(f"Creating dataset '{self.dataset_id}'...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            logger.info(f"Dataset '{self.dataset_id}' created.")

        # Create both Stord and Shipbob details tables
        self._create_table_if_not_exists(self.stord_details_table_id, self._stord_details_schema)
        self._create_table_if_not_exists(self.shipbob_details_table_id, self._shipbob_details_schema)

    def _create_table_if_not_exists(self, table_id: str, schema: List[bigquery.SchemaField]):
        try:
            self.client.get_table(table_id)
            logger.info(f"Table '{table_id}' already exists.")
        except NotFound:
            logger.info(f"Creating table '{table_id}'...")
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            logger.info(f"Table '{table_id}' created.")

    def sync_raw_order_data(
        self,
        source: str,
        raw_orders_data: List[Dict[str, Any]],
        timestamp: datetime
    ):
        logger.info(f"Starting BigQuery MERGE sync for source: {source}")

        if source == "stord":
            target_table_id = self.stord_details_table_id
            id_field = "order_number"
            schema = self._stord_details_schema
        elif source == "shipbob":
            target_table_id = self.shipbob_details_table_id
            id_field = "id"
            schema = self._shipbob_details_schema
        else:
            raise ValueError(f"Invalid source: {source}")

        # Staging table for the current batch of data
        staging_table_id = f"{self.project_id}.{self.dataset_id}.staging_{source}_{timestamp.strftime('%Y%m%d%H%M%S')}"
        
        try:
            # 1. Prepare and load data into a temporary staging table
            if raw_orders_data:
                rows_to_load = []
                for order_data in raw_orders_data:
                    order_id_value = str(order_data.get(id_field))
                    if not order_id_value:
                        logger.warning(f"Skipping order due to missing {id_field} for source {source}: {order_data}")
                        continue
                    rows_to_load.append({
                        id_field: order_id_value,
                        "raw_json": json.dumps(order_data),
                    })
                
                if not rows_to_load:
                    logger.info(f"No valid rows to load for source {source}. Processing potential resolutions.")
                else:
                    # Create a simple schema for the staging table
                    staging_schema = [
                        bigquery.SchemaField(id_field, "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("raw_json", "JSON", mode="REQUIRED"),
                    ]
                    staging_table = bigquery.Table(staging_table_id, schema=staging_schema)
                    self.client.create_table(staging_table)
                    logger.info(f"Created staging table {staging_table_id}")

                    job_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        autodetect=False,
                        schema=staging_schema
                    )
                    load_job = self.client.load_table_from_json(rows_to_load, staging_table_id, job_config=job_config)
                    load_job.result()
                    logger.info(f"Loaded {len(rows_to_load)} rows into staging table {staging_table_id}")
            else:
                logger.info(f"No raw order data provided for source {source}. The MERGE will treat all existing exceptions as resolved.")

            # 2. Execute the MERGE statement
            merge_sql = f"""
            MERGE `{target_table_id}` AS T
            USING `{staging_table_id if raw_orders_data else f"(SELECT * FROM `{target_table_id}` WHERE 1=0)"}` AS S
            ON T.{id_field} = S.{id_field} AND T.source = @source

            -- Case 1: New exception order. INSERT it.
            WHEN NOT MATCHED BY TARGET THEN
              INSERT (
                  {id_field}, raw_json, source, first_seen_timestamp, 
                  last_seen_timestamp, is_currently_in_exception, resolved_timestamp
              )
              VALUES (
                  S.{id_field}, S.raw_json, @source, @timestamp, 
                  @timestamp, TRUE, NULL
              )

            -- Case 2: Order is still in exception. Update it.
            -- Also handles cases where a resolved order reappears in exceptions.
            WHEN MATCHED THEN
              UPDATE SET
                T.raw_json = S.raw_json,
                T.last_seen_timestamp = @timestamp,
                T.is_currently_in_exception = TRUE,
                T.resolved_timestamp = NULL

            -- Case 3: Order is no longer in the exception source data. Mark it as resolved.
            -- It only marks currently active exceptions as resolved.
            WHEN NOT MATCHED BY SOURCE AND T.is_currently_in_exception = TRUE AND T.source = @source THEN
              UPDATE SET
                T.is_currently_in_exception = FALSE,
                T.resolved_timestamp = @timestamp
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                    bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
                ]
            )
            
            logger.info("Executing MERGE statement...")
            merge_job = self.client.query(merge_sql, job_config=job_config)
            merge_job.result()
            logger.info(f"MERGE statement completed successfully for source: {source}")

        finally:
            # 3. Drop the staging table
            if raw_orders_data:
                self.client.delete_table(staging_table_id, not_found_ok=True)
                logger.info(f"Dropped staging table {staging_table_id}")

    def get_oos_orders(self, source: str) -> List[Dict[str, Any]]:
        try:
            target_table_id = ""
            if source == "stord":
                target_table_id = self.stord_details_table_id
            elif source == "shipbob":
                target_table_id = self.shipbob_details_table_id
            else:
                raise ValueError(f"Invalid source: {source}")

            if not target_table_id:
                raise BigQueryClientError("BigQuery is not configured. GOOGLE_CLOUD_PROJECT is not set.")

            query = f"SELECT raw_json FROM `{target_table_id}` WHERE source = @source AND is_currently_in_exception = TRUE ORDER BY last_seen_timestamp DESC"
            job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("source", "STRING", source)])
            query_job = self.client.query(query, job_config=job_config)
            
            results = []
            for row in query_job.result():
                results.append(json.loads(row.raw_json)) # Parse the raw JSON string back to dict
            return results
        except BigQueryClientError:
            raise
        except Exception as e:
            logger.error(f"Error fetching OOS orders from BigQuery: {e}")
            raise BigQueryClientError(f"Failed to fetch orders from BigQuery: {e}") from e

    def get_historical_oos_orders_by_date(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Retrieves all orders that first went into an OOS state within the given date range,
        including those that have since been resolved.
        """
        try:
            if not self.stord_details_table_id or not self.shipbob_details_table_id:
                raise BigQueryClientError("BigQuery is not configured. GOOGLE_CLOUD_PROJECT is not set.")

            query = f"""
                SELECT raw_json
                FROM (
                    SELECT raw_json, first_seen_timestamp, is_currently_in_exception, resolved_timestamp FROM `{self.stord_details_table_id}`
                    UNION ALL
                    SELECT raw_json, first_seen_timestamp, is_currently_in_exception, resolved_timestamp FROM `{self.shipbob_details_table_id}`
                )
                WHERE 
                    (is_currently_in_exception = TRUE OR resolved_timestamp IS NOT NULL)
                    AND first_seen_timestamp BETWEEN @start_date AND @end_date
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                    bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            
            results = []
            for row in query_job.result():
                if row.raw_json:
                    results.append(json.loads(row.raw_json))
            return results
        except BigQueryClientError:
            raise
        except Exception as e:
            logger.error(f"Error fetching historical OOS orders from BigQuery: {e}")
            raise BigQueryClientError(f"Failed to fetch historical orders from BigQuery: {e}") from e

    def get_order_details(self, order_id: str, source: str) -> Optional[Dict[str, Any]]:
        try:
            target_table_id = ""
            id_field = ""
            if source == "stord":
                target_table_id = self.stord_details_table_id
                id_field = "order_number"
            elif source == "shipbob":
                target_table_id = self.shipbob_details_table_id
                id_field = "id"
            else:
                raise ValueError(f"Invalid source: {source}")

            if not target_table_id:
                raise BigQueryClientError("BigQuery is not configured. GOOGLE_CLOUD_PROJECT is not set.")

            query = f"SELECT raw_json FROM `{target_table_id}` WHERE {id_field} = @order_id AND source = @source LIMIT 1"
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("order_id", "STRING", order_id),
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            rows = query_job.result()

            for row in rows:
                return json.loads(row.raw_json) # Parse the raw JSON string back to dict
            return None
        except BigQueryClientError:
            raise
        except Exception as e:
            logger.error(f"Error fetching order details from BigQuery: {e}")
            raise BigQueryClientError(f"Failed to fetch order details from BigQuery: {e}") from e

    def get_last_refresh_time(self) -> Optional[datetime]:
        """
        Retrieves the most recent 'last_updated_at' timestamp from both Stord and Shipbob tables.
        """
        try:
            if not self.stord_details_table_id or not self.shipbob_details_table_id:
                raise BigQueryClientError("BigQuery is not configured. GOOGLE_CLOUD_PROJECT is not set.")

            query = f"""
                SELECT MAX(last_seen_timestamp) as last_refresh_time
                FROM (
                    SELECT last_seen_timestamp FROM `{self.stord_details_table_id}`
                    UNION ALL
                    SELECT last_seen_timestamp FROM `{self.shipbob_details_table_id}`
                )
            """
            query_job = self.client.query(query)
            results = query_job.result()
            for row in results:
                return row.last_refresh_time
        except BigQueryClientError:
            raise
        except Exception as e:
            logger.error(f"Failed to retrieve last refresh time: {e}")
            raise BigQueryClientError(f"Failed to retrieve last refresh time: {e}") from e
        return None

# Initialize BigQuery service
# The service uses lazy client initialization, so the app can start even if credentials are missing
# BigQuery operations will fail when actually attempted if credentials are not configured
try:
    bigquery_service = BigQueryService()
    logger.info("BigQueryService initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize BigQueryService during import: {e}")
    logger.warning("Application will start, but BigQuery operations will fail.")
    # Create a minimal service instance - client will be None and will fail when accessed
    # This allows the app to start but BigQuery endpoints will return errors
    class _FailedBigQueryService:
        def __getattr__(self, name):
            raise RuntimeError(f"BigQueryService not properly initialized. Original error: {e}")
    bigquery_service = _FailedBigQueryService()
