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

class BigQueryService:
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.dataset_id = BIGQUERY_DATASET
        self.client = self._get_bigquery_client()

        if not self.project_id:
            logger.error("GOOGLE_CLOUD_PROJECT environment variable is not set.")
            raise ValueError("Google Cloud Project ID is not set.")

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

    def _get_bigquery_client(self):
        credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if credentials_json:
            logger.info("Initializing BigQuery client with credentials from GOOGLE_CREDENTIALS_JSON.")
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            return bigquery.Client(credentials=credentials, project=self.project_id)
        else:
            logger.warning("GOOGLE_CREDENTIALS_JSON not found. Attempting default BigQuery client initialization.")
            return bigquery.Client(project=self.project_id)

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
        target_table_id = ""
        if source == "stord":
            target_table_id = self.stord_details_table_id
        elif source == "shipbob":
            target_table_id = self.shipbob_details_table_id
        else:
            raise ValueError(f"Invalid source: {source}")

        query = f"SELECT raw_json FROM `{target_table_id}` WHERE source = @source ORDER BY last_updated_at DESC"
        job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("source", "STRING", source)])
        query_job = self.client.query(query, job_config=job_config)
        
        results = []
        for row in query_job.result():
            results.append(json.loads(row.raw_json)) # Parse the raw JSON string back to dict
        return results

    def get_order_details(self, order_id: str, source: str) -> Optional[Dict[str, Any]]:
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

    def get_last_refresh_time(self) -> Optional[datetime]:
        """
        Retrieves the most recent 'last_updated_at' timestamp from both Stord and Shipbob tables.
        """
        query = f"""
            SELECT MAX(last_updated_at) as last_refresh_time
            FROM (
                SELECT last_updated_at FROM `{self.stord_details_table_id}`
                UNION ALL
                SELECT last_updated_at FROM `{self.shipbob_details_table_id}`
            )
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            for row in results:
                return row.last_refresh_time
        except Exception as e:
            logger.error(f"Failed to retrieve last refresh time: {e}")
            return None
        return None

bigquery_service = BigQueryService()
