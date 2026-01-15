import csv
import requests
import json
import dotenv
import asyncio
from typing import Optional, Dict, Any

from core.logger import get_logger
from core.config import SHIPBOB_BASE_URL, SHIPBOB_API_TOKEN

dotenv.load_dotenv()
logger = get_logger(__name__)


class ShipbobService:
    def __init__(self):
        self.base_url = SHIPBOB_BASE_URL
        self.api_token = SHIPBOB_API_TOKEN
        if not all([self.base_url, self.api_token]):
            logger.warning("Some ShipbobHelper environment variables are missing")
        else:
            logger.debug("ShipbobHelper initialized successfully")

    def get_inventory_by_fulfillment_center(
        self, output_format: str = "json", single_page: bool = False, limit: int = 100
    ):
        url = f"{self.base_url}/inventory-level/locations"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        params = {"PageSize": limit, "IsActive": True}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()["items"]

        page_count = 1
        logger.debug(f"Fetched page {page_count}, total items: {len(data)}")
        if single_page:
            pass
        else:
            while response.json()["next"]:
                page_count += 1
                next_url = response.json()["next"]
                url = f"{self.base_url}{next_url}"
                params["next"] = next_url
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data.extend(response.json()["items"])
                logger.debug(f"Fetched page {page_count}, total items: {len(data)}")
            logger.info(
                f"Successfully fetched inventory data across {page_count} page(s)"
            )

        return data

    def get_orders(
        self,
        single_page: bool = False,
        limit: int = 250,
        max_pages: int = 100,
        has_tracking: bool = False,
    ):
        logger.info(
            f"Fetching orders (single_page={single_page}, limit={limit}, max_pages={max_pages})"
        )
        page = 1
        output = []
        max_pages_reached = False

        try:
            while True:
                if page > max_pages:
                    max_pages_reached = True
                    logger.warning(
                        f"Maximum page limit ({max_pages}) reached, stopping pagination"
                    )
                    break

                url = f"{self.base_url}/order?page={page}&limit={limit}&HasTracking={has_tracking}"
                headers = {"Authorization": f"Bearer {self.api_token}"}
                logger.debug(f"Making request to: {url}")
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()

                if len(data) == 0:
                    logger.debug(f"Page {page} returned no data, stopping pagination")
                    break

                output.extend(data)
                logger.debug(
                    f"Fetched page {page}, received {len(data)} items, total items: {len(output)}"
                )

                if single_page:
                    logger.info("Single page mode, stopping after first page")
                    break

                if len(data) < limit:
                    logger.debug(
                        f"Received fewer items ({len(data)}) than limit ({limit}), might be last page"
                    )

                page += 1

            if max_pages_reached:
                logger.warning(
                    f"Stopped at maximum page limit. Fetched {page - 1} page(s), total items: {len(output)}"
                )
            else:
                logger.info(
                    f"Successfully fetched orders across {page} page(s), total items: {len(output)}"
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching orders: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise


        return output

    def _filter_oos_orders(self, orders: list, save_to_file: bool = True):
        output = []
        seen_orders = set()  # Track order IDs to avoid duplicates

        for order in orders:
            # Skip if order type is not DTC
            if order.get("type") != "DTC":
                continue

            # Skip if order status is not Exception
            if order.get("status") != "Exception":
                continue

            has_oos = any(
                shipment.get("status") == "Exception"
                and any(
                    status_detail.get("name") == "OutOfStock"
                    for status_detail in shipment.get("status_details", [])
                )
                for shipment in order.get("shipments", [])
            )

            # Add order if it has OutOfStock and we haven't seen it before
            if has_oos:
                order_id = order.get("id") or id(
                    order
                )  # Use order ID if available, otherwise use object id
                if order_id not in seen_orders:
                    seen_orders.add(order_id)
                    # Extract shipments[0].location.name if available
                    shipments = order.get("shipments")
                    if shipments and isinstance(shipments, list) and len(shipments) > 0:
                        first_shipment = shipments[0]
                        if first_shipment and isinstance(first_shipment, dict):
                            location = first_shipment.get("location")
                            if location and isinstance(location, dict):
                                order["shipments.location.name"] = location.get("name")
                            else:
                                order["shipments.location.name"] = None
                        else:
                            order["shipments.location.name"] = None
                    else:
                        order["shipments.location.name"] = None
                    output.append(order)


        logger.info(f"Exception orders: {len(output)} items")
        return output

    def get_order_by_id(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Fetches a single Shipbob order by its ID directly from the API."""
        logger.info(f"Fetching Shipbob order details for order_id: {order_id}")
        url = f"{self.base_url}/order/{order_id}"
        headers = {"Authorization": f"Bearer {self.api_token}"}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"Shipbob order {order_id} not found (404).")
                return None
            logger.error(f"Error fetching Shipbob order {order_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching Shipbob order {order_id}: {e}")
            raise


    async def get_inventory_from_shipbob_api(self, sku: str) -> tuple[int, int]:
        """
        Fetches inventory for a given SKU from the Shipbob API, separating Fontana (ID 250)
        stock from other locations. Returns (fontana_stock, other_stock).
        Returns (0, 0) if the SKU is not found or an error occurs.
        """
        def _fetch():
            logger.info(f"SHIPBOB INVENTORY DEBUG: Requesting inventory for SKU: '{sku}'")
            url = f"{self.base_url}/inventory-level/locations"
            headers = {"Authorization": f"Bearer {self.api_token}"}
            params = {"SearchBy": sku}

            fontana_stock = 0
            other_stock = 0

            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                response_data = response.json()
                logger.info(f"SHIPBOB INVENTORY DEBUG: Raw API response for SKU '{sku}': {response_data}")

                if response_data and response_data.get("items"):
                    item = response_data["items"][0]
                    locations = item.get("locations", [])

                    for location in locations:
                        on_hand = location.get("on_hand_quantity", 0)
                        if location.get("location_id") == 250:
                            fontana_stock += on_hand
                        else:
                            other_stock += on_hand
                    logger.info(f"Shipbob inventory for SKU {sku}: Fontana={fontana_stock}, Other={other_stock}")
                    return fontana_stock, other_stock
                else:
                    logger.info(f"Shipbob inventory for SKU {sku} not found in API response.")
                    return 0, 0
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching Shipbob inventory for SKU {sku}: {e}")
                return 0, 0
            except (KeyError, IndexError) as e:
                logger.error(f"Unexpected response format for Shipbob inventory for SKU {sku}: {e}")
                return 0, 0
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching Shipbob inventory for SKU {sku}: {e}")
                return 0, 0
        
        return await asyncio.to_thread(_fetch)


if __name__ == "__main__":
    shipbob_service = ShipbobService()
    orders = shipbob_service.get_orders(
        single_page=True, limit=10
    )
    filtered_orders = shipbob_service._filter_oos_orders(orders, save_to_file=False)
    logger.info(f"Filtered orders: {len(filtered_orders)} items")

    # Example usage: fetching a single order by ID
    # Replace with a real Shipbob Order ID for testing
    test_order_id = "a_shipbob_order_id_for_testing"
    try:
        order_detail = shipbob_service.get_order_by_id(test_order_id)
        if order_detail:
            print(f"Fetched Shipbob Order {test_order_id}: {json.dumps(order_detail, indent=2)}")
        else:
            print(f"Shipbob Order {test_order_id} not found.")
    except Exception as e:
        print(f"Error during test fetch for Shipbob Order {test_order_id}: {e}")

    # Example usage: fetching inventory by SKU
    test_sku = "TCT4000SEPIAWDL" # Replace with a real SKU for testing
    try:
        fontana_stock, other_stock = shipbob_service.get_inventory_from_shipbob_api(test_sku)
        print(f"Shipbob Inventory for SKU {test_sku}: Fontana={fontana_stock}, Other={other_stock}")
    except Exception as e:
        print(f"Error during test fetch for Shipbob Inventory {test_sku}: {e}")
