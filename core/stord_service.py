import requests
import json
import math
import asyncio
from typing import Optional, Dict, Any

from collections import OrderedDict
import dotenv

try:
    from core.logger import get_logger
    from core.config import (
        STORD_BASE_URL,
        STORD_API_TOKEN,
        STORD_ORG_ID,
        STORD_NETWORK_ID,
        STORD_CHANNEL_IDS,
        STORD_STATUS,
    )
except ModuleNotFoundError:
    from logger import get_logger
    from config import (
        STORD_BASE_URL,
        STORD_API_TOKEN,
        STORD_ORG_ID,
        STORD_NETWORK_ID,
        STORD_CHANNEL_IDS,
        STORD_STATUS,
    )

dotenv.load_dotenv()

logger = get_logger(__name__)


class StordService:
    def __init__(self):
        logger.info("Initializing StordService")
        self.base_url = STORD_BASE_URL
        self.api_token = STORD_API_TOKEN
        self.org_id = STORD_ORG_ID
        self.network_id = STORD_NETWORK_ID

        if not all([self.base_url, self.api_token, self.org_id, self.network_id]):
            logger.warning("Some StordService environment variables are missing")
        else:
            logger.debug("StordService initialized successfully")

    def get_network_inventory(
        self, single_page: bool = False, limit: int = 100, output_format: str = "json"
    ):
        logger.info(f"Fetching network inventory (single_page={single_page})")
        url = f"{self.base_url}/organizations/{self.org_id}/oms/networks/{self.network_id}/inventory/reports/network"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        params = {"limit": limit}

        output = []
        try:
            logger.debug(f"Making request to: {url}")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            response_data = response.json()
            output.extend(response_data["data"])

            total_count = response_data["metadata"].get("total_count", 0)
            total_api_calls = math.ceil(total_count / limit) if total_count > 0 else 1
            logger.info(
                f"Total items: {total_count}, Limit per page: {limit}, Total API calls needed: {total_api_calls}"
            )

            logger.debug(f"Received {len(response_data['data'])} items in first page")

            if single_page:
                logger.info("Single page mode, returning first page only")
                flattened_output = [item for page in output for item in page]
                return flattened_output

            page_count = 1
            while response_data["metadata"]["after"]:
                page_count += 1
                params["after"] = response_data["metadata"]["after"]
                logger.debug(f"Fetching page {page_count} of {total_api_calls}")
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                response_data = response.json()
                output.extend(response_data["data"])
                logger.debug(
                    f"Received {len(response_data['data'])} items in page {page_count}"
                )

            logger.info(
                f"Successfully fetched network inventory data across {page_count} page(s)"
            )

            flattened_output = [item for page in output for item in page]

            return flattened_output
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching network inventory data: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected response format: {e}")
            raise

    def get_inventory_by_facility(
        self, single_page: bool = False, limit: int = 100, out_of_stock: bool = False
    ):
        logger.info(f"Fetching inventory by facility (single_page={single_page})")
        url = f"{self.base_url}/organizations/{self.org_id}/oms/networks/{self.network_id}/inventory/reports/facilities"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        params = {"limit": limit, "out_of_stock": out_of_stock}

        output = []
        try:
            logger.debug(f"Making request to: {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            output.extend(response_data["data"])

            total_count = response_data["metadata"].get("total_count", 0)
            total_api_calls = math.ceil(total_count / limit) if total_count > 0 else 1
            logger.info(
                f"Total items: {total_count}, Limit per page: {limit}, Total API calls needed: {total_api_calls}"
            )

            logger.debug(f"Received {len(response_data['data'])} items in first page")

            if single_page:
                logger.info("Single page mode, returning first page only")
                flattened_output = [item for page in output for item in page]
                return flattened_output

            page_count = 1
            while response_data["metadata"]["after"]:
                page_count += 1
                params["after"] = response_data["metadata"]["after"]
                logger.debug(f"Fetching page {page_count} of {total_api_calls}")
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                response_data = response.json()
                output.extend(response_data["data"])
                logger.debug(
                    f"Received {len(response_data['data'])} items in page {page_count}"
                )

            logger.info(
                f"Successfully fetched inventory data across {page_count} page(s)"
            )

            flattened_output = [item for page in output for item in page]

            return flattened_output
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching inventory data: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected response format: {e}")
            raise

    def get_sales_orders(
        self,
        single_page: bool = False,
        limit: int = 100,
        channel_ids: list = None,
        status: list = None,
        fields: list = None,
    ):
        logger.info(f"Fetching sales orders (single_page={single_page}, limit={limit})")

        params = [f"limit={limit}"]
        if channel_ids:
            params.extend(f"channel_id[]={cid}" for cid in channel_ids)
        if status:
            params.extend(f"status[]={s}" for s in status)

        base_params_str = "&".join(params)
        base_url = f"{self.base_url}/organizations/{self.org_id}/oms/networks/{self.network_id}/orders/sales"
        headers = {"Authorization": f"Bearer {self.api_token}"}

        response_data = []
        try:
            url = f"{base_url}?{base_params_str}"
            logger.debug(f"Making request to: {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            response_data.extend(response_json["data"])

            total_count = response_json["metadata"].get("total_count", 0)
            total_api_calls = math.ceil(total_count / limit) if total_count > 0 else 1
            logger.info(
                f"Total items: {total_count}, Limit per page: {limit}, Total API calls needed: {total_api_calls}"
            )
            logger.debug(f"Received {len(response_json['data'])} items in first page")

            if single_page:
                logger.info("Single page mode, returning first page only")
            else:
                logger.info("Multiple page mode, fetching all pages")
                page_count = 1
                while response_json["metadata"].get("after"):
                    page_count += 1
                    after = response_json["metadata"]["after"]
                    params_str = f"{base_params_str}&after={after}"
                    url = f"{base_url}?{params_str}"
                    logger.debug(f"Fetching page {page_count} of {total_api_calls}")
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    response_json = response.json()
                    response_data.extend(response_json["data"])
                    logger.debug(
                        f"Fetched page {page_count}, total items: {len(response_data)}"
                    )
                logger.info(
                    f"Successfully fetched sales orders across {page_count} page(s)"
                )

            if fields:
                response_data = [
                    OrderedDict((field, item.get(field, None)) for field in fields)
                    for item in response_data
                ]

            return response_data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching sales orders: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected response format: {e}")
            raise

    def get_order_by_id(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Fetches a single Stord order by its ID directly from the API."""
        logger.info(f"Fetching Stord order details for order_id: {order_id}")
        # Use the provided curl command structure
        url = f"{self.base_url}/organizations/{self.org_id}/oms/networks/{self.network_id}/orders/sales"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        params = {
            "limit": 1,  # We only need one order
            "search_field": "order_id",
            "search_term": order_id,
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            response_json = response.json()

            if response_json and response_json.get("data"):
                # Return the first matching order's data
                return response_json["data"][0]
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Stord order {order_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching Stord order {order_id}: {e}")
            raise

    async def get_inventory_from_stord_api(self, sku: str) -> int:
        """
        Fetches the on-hand inventory quantity for a given SKU from the Stord API.
        Returns 0 if the SKU is not found or an error occurs.
        """
        def _fetch():
            logger.info(f"STORD INVENTORY DEBUG: Requesting inventory for SKU: '{sku}'")
            url = f"{self.base_url}/inventory-levels"
            headers = {
                "Authorization": f"Bearer {self.api_token}",
                "Stord-Organization-Id": self.org_id,
            }
            params = {"sku": sku}

            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                response_data = response.json()
                logger.info(f"STORD INVENTORY DEBUG: Raw API response for SKU '{sku}': {response_data}")

                if response_data and response_data.get("content"):
                    # Stord API returns a list of inventory levels for different facilities
                    total_on_hand = sum(
                        item.get("on_hand_quantity", 0)
                        for item in response_data["content"]
                    )
                    logger.info(f"Stord inventory for SKU {sku}: Total On Hand={total_on_hand}")
                    return total_on_hand
                else:
                    logger.info(f"Stord inventory for SKU {sku} not found in API response.")
                    return 0
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching Stord inventory for SKU {sku}: {e}")
                return 0
            except (KeyError, IndexError) as e:
                logger.error(f"Unexpected response format for Stord inventory for SKU {sku}: {e}")
                return 0
            except Exception as e:
                logger.error(f"An unexpected error occurred while fetching Stord inventory for SKU {sku}: {e}")
                return 0
        
        return await asyncio.to_thread(_fetch)

if __name__ == "__main__":
    # Example usage
    stord_service = StordService()
    orders = stord_service.get_sales_orders(limit=10)
    if orders:
        print(f"Fetched {len(orders)} orders. First order: {json.dumps(orders[0], indent=2)}")

    # Example of fetching a single sales order
    test_order_number = "SO-000-000-000" # Replace with a real order number for testing
    try:
        order_detail = stord_service.get_sales_order_by_number(test_order_number)
        if order_detail:
            print(f"Fetched Stord Order {test_order_number}: {json.dumps(order_detail, indent=2)}")
        else:
            print(f"Stord Order {test_order_number} not found.")
    except Exception as e:
        print(f"Error during test fetch for Stord Order {test_order_number}: {e}")


if __name__ == "__main__":
    stord_service = StordService()
    sales_orders = stord_service.get_sales_orders(
        single_page=False,
        limit=100,
        channel_ids=STORD_CHANNEL_IDS,
        status=STORD_STATUS,
        fields=["order_number", "status", "sales_order_lines"],
    )
    # print(f"Fetched {len(sales_orders)} sample sales orders.")

    # # Example usage: fetching a single order by ID
    # # Replace with a real Stord Order ID for testing
    # test_order_id = "a_stord_order_id_for_testing"
    # try:
    #     order_detail = stord_service.get_order_by_id(test_order_id)
    #     if order_detail:
    #         print(f"Fetched Stord Order {test_order_id}: {json.dumps(order_detail, indent=2)}")
    #     else:
    #         print(f"Stord Order {test_order_id} not found.")
    # except Exception as e:
    #     print(f"Error during test fetch for Stord Order {test_order_id}: {e}")

    # # Example usage: fetching inventory by SKU
    # test_sku = "TCT4000SEABRZXL" # Replace with a real SKU for testing
    # try:
    #     inventory = stord_service.get_inventory_from_stord_api(test_sku)
    #     print(f"Stord Inventory for SKU {test_sku}: {inventory}")
    # except Exception as e:
    #     print(f"Error during test fetch for Stord Inventory {test_sku}: {e}")
