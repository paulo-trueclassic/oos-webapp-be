import json
from datetime import datetime, timezone
from typing import List, Dict, Any

from core.logger import get_logger
from core.stord_service import StordService
from core.shipbob_service import ShipbobService
from core.config import STORD_CHANNEL_IDS, STORD_STATUS
from core.bigquery_service import bigquery_service
from core.data_models import OutOfStockSKU, OrderDetails, convert_stord_order_to_model, convert_shipbob_order_to_model

logger = get_logger(__name__)

def process_stord_data():
    """Fetches Stord OOS orders and syncs their raw details to BigQuery."""
    logger.info("Starting Stord OOS data refresh in background.")
    stord_service = StordService()
    
    try:
        stord_raw_orders = stord_service.get_sales_orders(
            single_page=False,
            limit=100,
            channel_ids=STORD_CHANNEL_IDS,
            status=STORD_STATUS,
            fields=[
                "order_number", "status", "priority", "facility", "channel", 
                "channel_category", "shipment_type", "shipped_at", "customer", 
                "sales_order_lines", "custom_reference", "order_id", "external_posted_at", "facility_activities"
            ],
        )
        logger.info(f"Fetched {len(stord_raw_orders)} raw Stord orders.")

        stord_filtered_oos_orders: List[Dict[str, Any]] = []
        for order_data in stord_raw_orders:
            # The conversion function returns a list with a single order model.
            model_order_list = convert_stord_order_to_model(order_data)
            if not model_order_list:
                continue

            model_order = model_order_list[0]  # Get the single order from the list
            is_oos = any(
                line.status == "backordered"
                for line in model_order.line_items
            )
            if is_oos:
                stord_filtered_oos_orders.append(order_data)  # Store original raw data
        
        logger.info(f"Found {len(stord_filtered_oos_orders)} Stord OOS orders.")

        current_timestamp = datetime.now(timezone.utc)
        bigquery_service.sync_raw_order_data(
            source="stord",
            raw_orders_data=stord_filtered_oos_orders,
            timestamp=current_timestamp
        )
        logger.info("Stord OOS raw data sync with BigQuery completed successfully.")

    except Exception as e:
        logger.error(f"Error processing Stord data: {e}", exc_info=True)

def process_shipbob_data():
    """Fetches Shipbob OOS orders and syncs their raw details to BigQuery."""
    logger.info("Starting Shipbob OOS data refresh in background (long-running).")
    shipbob_service = ShipbobService()

    try:
        shipbob_raw_orders = shipbob_service.get_orders(
            single_page=False, limit=250, max_pages=25
        )
        logger.info(f"Fetched {len(shipbob_raw_orders)} raw Shipbob orders.")

        filtered_shipbob_orders = shipbob_service._filter_oos_orders(
            shipbob_raw_orders, save_to_file=False
        )
        logger.info(f"Found {len(filtered_shipbob_orders)} Shipbob OOS orders.")

        current_timestamp = datetime.now(timezone.utc)
        bigquery_service.sync_raw_order_data(
            source="shipbob",
            raw_orders_data=filtered_shipbob_orders,
            timestamp=current_timestamp
        )
        logger.info("Shipbob OOS raw data sync with BigQuery completed successfully.")

    except Exception as e:
        logger.error(f"Error processing Shipbob data: {e}", exc_info=True)

def trigger_full_refresh():
    """Triggers a full refresh for both Stord and Shipbob data."""
    logger.info("Initiating full data refresh for Stord and Shipbob.")
    try:
        bigquery_service.create_tables_if_not_exists()
        process_stord_data()
        process_shipbob_data()
        logger.info("Full data refresh process completed.")
    except Exception as e:
        logger.error(f"Full data refresh failed: {e}", exc_info=True)


def trigger_source_refresh(source: str):
    """Triggers a refresh for a single source."""
    logger.info(f"Initiating data refresh for source: {source}.")
    try:
        bigquery_service.create_tables_if_not_exists()
        if source == "stord":
            process_stord_data()
        elif source == "shipbob":
            process_shipbob_data()
        else:
            logger.warning(f"Invalid source specified for refresh: {source}")
            return
        logger.info(f"Data refresh for source '{source}' completed.")
    except Exception as e:
        logger.error(f"Data refresh for source '{source}' failed: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Running background tasks directly for testing...")
    trigger_full_refresh()
    logger.info("Test run finished.")
