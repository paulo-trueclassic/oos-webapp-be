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
            bigquery.SchemaField("order_number", "STRING", mode="REQUIRED"), # Stord primary ID
            bigquery.SchemaField("raw_json", "JSON", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_updated_at", "TIMESTAMP"),
        ]
        self._shipbob_details_schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"), # Shipbob primary ID
            bigquery.SchemaField("raw_json", "JSON", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_updated_at", "TIMESTAMP"),
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
        raw_orders_data: List[Dict[str, Any]], # Now accepts raw dicts
        timestamp: datetime
    ):
        logger.info(f"Starting BigQuery data sync for source: {source}")

        target_table_id = ""
        if source == "stord":
            target_table_id = self.stord_details_table_id
            # For Stord, use 'order_number' as the primary ID field for BigQuery operations
            id_field = "order_number"
        elif source == "shipbob":
            target_table_id = self.shipbob_details_table_id
            # For Shipbob, use 'id' as the primary ID field for BigQuery operations
            id_field = "id"
        else:
            raise ValueError(f"Invalid source: {source}")

        rows_to_load = []
        for order_data in raw_orders_data:
            # Extract the ID based on the source's primary ID field
            order_id_value = str(order_data.get(id_field))
            if not order_id_value:
                logger.warning(f"Skipping order due to missing {id_field} for source {source}: {order_data}")
                continue

            rows_to_load.append({
                id_field: order_id_value, # Use source-specific ID field
                "raw_json": json.dumps(order_data), # Store the entire raw order as JSON string
                "source": source,
                "last_updated_at": timestamp.isoformat()
            })

        if rows_to_load:
            delete_job = self.client.query(f"DELETE FROM `{target_table_id}` WHERE source = '{source}'")
            delete_job.result() 
            logger.info(f"Deleted existing data for source: {source} in {target_table_id}")

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=False,
                schema=self._stord_details_schema if source == "stord" else self._shipbob_details_schema
            )
            load_job = self.client.load_table_from_json(rows_to_load, target_table_id, job_config=job_config)
            load_job.result() 
            logger.info(f"Successfully loaded {len(rows_to_load)} raw order rows for {source} into {target_table_id}.")
        else:
            logger.info(f"No raw order rows to load for {source}.")
            delete_job = self.client.query(f"DELETE FROM `{target_table_id}` WHERE source = '{source}'")
            delete_job.result()
            logger.info(f"Cleared existing data for {source} (no new data to load) in {target_table_id}.")

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

            query = f"SELECT raw_json FROM `{target_table_id}` WHERE source = @source ORDER BY last_updated_at DESC"
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
                SELECT MAX(last_updated_at) as last_refresh_time
                FROM (
                    SELECT last_updated_at FROM `{self.stord_details_table_id}`
                    UNION ALL
                    SELECT last_updated_at FROM `{self.shipbob_details_table_id}`
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
