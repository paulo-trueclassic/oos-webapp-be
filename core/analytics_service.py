from collections import defaultdict
from typing import Dict, Any, List, Set
from datetime import datetime, timezone

from core.bigquery_service import bigquery_service, BigQueryClientError
from core.logger import get_logger

logger = get_logger(__name__)

# --- Helper functions to correctly identify OOS SKUs ---

def get_shipbob_oos_skus(raw_order: Dict[str, Any]) -> Set[str]:
    """Parses a ShipBob raw_order to find SKUs explicitly marked as OutOfStock."""
    oos_skus = set()
    shipments = raw_order.get("shipments", [])
    for shipment in shipments:
        # Check for OutOfStock based on status_details first
        oos_inventory_ids = {
            detail.get("inventory_id")
            for detail in shipment.get("status_details", [])
            if detail.get("name") == "OutOfStock" and detail.get("inventory_id")
        }
        
        if not oos_inventory_ids:
            continue

        products = shipment.get("products", [])
        for product in products:
            if product.get("sku") and product.get("inventory_items"):
                for item in product["inventory_items"]:
                    if item.get("id") in oos_inventory_ids:
                        oos_skus.add(product["sku"])
                        break # Move to the next product once one OOS item is found
    return oos_skus

def get_stord_oos_skus(raw_order: Dict[str, Any]) -> Set[str]:
    """Parses a Stord raw_order to find SKUs in 'backordered' sales order lines."""
    oos_skus = set()
    sales_order_lines = raw_order.get("sales_order_lines", [])
    for sol in sales_order_lines:
        if sol.get("status") == "backordered":
            for oli in sol.get("order_line_items", []):
                if oli.get("item_sku"):
                    oos_skus.add(oli["item_sku"])
    return oos_skus


class AnalyticsService:
    def __init__(self):
        self.bq_service = bigquery_service

    def get_oos_orders_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        try:
            return self.bq_service.get_historical_oos_orders_by_date(
                start_date, end_date
            )
        except BigQueryClientError:
            raise

    def get_full_analytics(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        """
        Performs all analytics calculations in Python, correctly identifying
        only the SKUs that are actually out of stock.
        """
        try:
            oos_orders = self.get_oos_orders_by_date_range(start_date, end_date)
            logger.info(f"Processing analytics for {len(oos_orders)} orders with available raw_json.")

            sku_frequency = defaultdict(int)
            customer_impact = defaultdict(int)
            facility_hotspots_counts = defaultdict(int)
            partner_performance_counts = defaultdict(int)
            resolution_times = defaultdict(list)
            
            for raw_order in oos_orders:
                if not raw_order: continue

                source = "stord" if "sales_order_lines" in raw_order else "shipbob"
                
                # --- Correctly identify and count ONLY OOS SKUs ---
                oos_skus_in_order = set()
                if source == "stord":
                    oos_skus_in_order = get_stord_oos_skus(raw_order)
                else: # shipbob
                    oos_skus_in_order = get_shipbob_oos_skus(raw_order)

                # Now, iterate through the order's items to get the quantity for the OOS SKUs
                if source == "stord":
                    for sol in raw_order.get("sales_order_lines", []):
                        for item in sol.get("order_line_items", []):
                            sku = item.get("item_sku")
                            if sku in oos_skus_in_order:
                                quantity_str = item.get("item_quantity")
                                if quantity_str:
                                    try: sku_frequency[sku] += float(quantity_str)
                                    except (ValueError, TypeError): pass
                else: # shipbob
                    for item in raw_order.get("products", []):
                        sku = item.get("sku")
                        if sku in oos_skus_in_order:
                            quantity = item.get("quantity")
                            if quantity is not None:
                                try: sku_frequency[sku] += int(quantity)
                                except (ValueError, TypeError): pass
                
                # --- Customer, Fulfillment, and Operational Analytics (remain the same) ---
                partner_performance_counts[source] += 1
                identifier = None
                if source == "stord":
                    identifier = raw_order.get("destination_address", {}).get("name")
                    if not identifier:
                        order_id = raw_order.get("order_id") or raw_order.get("order_number")
                        identifier = f"stord_order_{order_id}" if order_id else None
                else: # Shipbob
                    identifier = raw_order.get("recipient", {}).get("email")
                    if not identifier:
                        order_id = raw_order.get("id")
                        identifier = f"shipbob_order_{order_id}" if order_id else None
                if identifier:
                    customer_impact[identifier.lower()] += 1

                facility = None
                if source == "stord":
                    if raw_order.get("facility_activities"):
                         facility = raw_order.get("facility_activities")[0].get("facility_alias")
                else: # shipbob
                    if raw_order.get("shipments"):
                        facility = raw_order.get("shipments")[0].get("location", {}).get("name")
                if facility:
                    facility_hotspots_counts[f"{facility} ({source})"] += 1

                resolved_timestamp_float = raw_order.get("resolved_timestamp")
                first_seen_timestamp_float = raw_order.get("first_seen_timestamp")
                if resolved_timestamp_float and first_seen_timestamp_float:
                    resolved_dt = datetime.fromtimestamp(resolved_timestamp_float, tz=timezone.utc)
                    first_seen_dt = datetime.fromtimestamp(first_seen_timestamp_float, tz=timezone.utc)
                    duration_hours = (resolved_dt - first_seen_dt).total_seconds() / 3600
                    resolution_times[source].append(duration_hours)

            # --- Format Final Analytics Objects ---
            sku_analytics = {"sku_frequency": dict(sorted(sku_frequency.items(), key=lambda item: item[1], reverse=True)[:10]), "sku_co_occurrence": {}}
            
            repeat_customers = {k: v for k, v in customer_impact.items() if v > 1}
            customer_analytics = {
                "total_customers_affected": len(customer_impact),
                "repeat_customers_affected": len(repeat_customers),
                "top_repeat_customers": dict(sorted(repeat_customers.items(), key=lambda item: item[1], reverse=True)[:5])
            }

            fulfillment_analytics = {
                "facility_hotspots": [{"facility": k, "oos_count": v} for k, v in sorted(facility_hotspots_counts.items(), key=lambda item: item[1], reverse=True)[:5]],
                "partner_performance": {
                    "stord_oos_count": partner_performance_counts.get("stord", 0),
                    "shipbob_oos_count": partner_performance_counts.get("shipbob", 0),
                    "total_oos_count": sum(partner_performance_counts.values())
                }
            }
            
            avg_resolution_hours = {source: round(sum(times) / len(times), 2) if times else 0 for source, times in resolution_times.items()}
            operational_analytics = {"average_resolution_time_hours": avg_resolution_hours}

            return {
                "sku_analytics": sku_analytics,
                "customer_analytics": customer_analytics,
                "fulfillment_analytics": fulfillment_analytics,
                "operational_analytics": operational_analytics,
            }

        except Exception as e:
            logger.error(f"Error in get_full_analytics (Python processing): {e}")
            raise BigQueryClientError(f"Failed to process analytics data: {e}")

analytics_service = AnalyticsService()