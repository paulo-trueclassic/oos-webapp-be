from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid # Added for UUID fallback

from core.logger import get_logger # Added logger import

logger = get_logger(__name__)

# --- Pydantic Models ---

class OutOfStockSKU(BaseModel):
    sku: str
    count_affected_orders: int
    last_updated: datetime

class OrderLineItem(BaseModel):
    sku: Optional[str]
    quantity: Optional[int]
    status: Optional[str]

class OrderDetails(BaseModel):
    order_id: str # Standardize on order_id, populated from source-specific fields
    order_number: Optional[str]
    status: Optional[str] # Made optional
    source: str # 'stord' or 'shipbob'
    purchase_date: Optional[datetime]
    priority: Optional[str]
    facility: Optional[str]
    channel: Optional[str]
    channel_category: Optional[str]
    shipment_type: Optional[str]
    shipped_at: Optional[datetime]
    customer: Optional[Dict[str, Any]]
    custom_reference: Optional[str]
    line_items: List[OrderLineItem]
    raw_data: Optional[Dict[str, Any]] # To store the original API response
    last_updated_at: Optional[datetime] # Timestamp from BigQuery for summary, or current for live

# --- Conversion Functions ---

def convert_stord_order_to_model(order_data: Dict[str, Any], include_raw: bool = False) -> OrderDetails:
    line_items = []
    for sol in order_data.get("sales_order_lines", []) or []:
        for oli in sol.get("order_line_items", []) or []:
            line_items.append(OrderLineItem(
                sku=oli.get("item_sku"),
                quantity=int(float(oli.get("item_quantity"))) if oli.get("item_quantity") else None,
                status=sol.get("status") # Use sales order line status
            ))

    customer_data = order_data.get("customer")
    customer_name = customer_data if isinstance(customer_data, str) else customer_data.get("name") if isinstance(customer_data, dict) else None

    # Attempt to parse datetime strings safely
    shipped_at_str = order_data.get("shipped_at") or order_data.get("external_posted_at")
    shipped_at_dt = None
    if shipped_at_str:
        try:
            shipped_at_dt = datetime.fromisoformat(shipped_at_str.replace('Z', '+00:00'))
        except ValueError:
            logger.warning(f"Could not parse datetime for Stord shipped_at: {shipped_at_str}")

    # Robust facility extraction
    facility_alias = None
    facility_activities = order_data.get("facility_activities")
    if facility_activities and isinstance(facility_activities, list) and len(facility_activities) > 0:
        first_activity = facility_activities[0]
        if isinstance(first_activity, dict):
            facility_alias = first_activity.get("facility_alias")

    return OrderDetails(
        order_id=order_data.get("order_number") or order_data.get("order_id") or str(uuid.uuid4()), # Fallback to a UUID if no ID found
        order_number=order_data.get("order_number"),
        status=order_data.get("status"),
        source="stord",
        priority=order_data.get("priority"),
        facility=facility_alias,
        channel=order_data.get("channel"),
        channel_category=order_data.get("channel_category"),
        shipment_type=order_data.get("shipment_type"),
        shipped_at=shipped_at_dt,
        purchase_date=shipped_at_dt,
        customer={"name": customer_name, "email": None},
        custom_reference=order_data.get("custom_reference"),
        line_items=line_items,
        raw_data=order_data if include_raw else None,
        last_updated_at=datetime.now()
    )

def convert_shipbob_order_to_model(order_data: Dict[str, Any], include_raw: bool = False) -> OrderDetails:
    line_items = [] 
    for shipment in order_data.get("shipments", []) or []:
        for product_item in shipment.get("products", []) or []:
            line_items.append(OrderLineItem(
                sku=product_item.get("sku"),
                quantity=product_item.get("quantity"),
                status=shipment.get("status") # Using shipment status as line status for now
            ))

    recipient = order_data.get("recipient", {})
    customer_name = recipient.get("name")
    customer_email = recipient.get("email")

    created_date_str = order_data.get("created_date")
    created_date_dt = None
    if created_date_str:
        try:
            created_date_dt = datetime.fromisoformat(created_date_str.replace('Z', '+00:00'))
        except ValueError:
            logger.warning(f"Could not parse datetime for Shipbob created_date: {created_date_str}")
    
    return OrderDetails(
        order_id=str(order_data.get("id") or uuid.uuid4()), # Use Shipbob ID as unique ID, fallback to UUID
        order_number=order_data.get("order_number"), # Corrected from 'orderNumber'
        status=order_data.get("status"),
        source="shipbob",
        priority=None, 
        facility=order_data.get("shipments", [])[0].get("location", {}).get("name"),
        channel=order_data.get("channel", {}).get("name"),
        channel_category=order_data.get("type"), 
        shipment_type=order_data.get("shipping_method"),
        shipped_at=created_date_dt, 
        purchase_date=created_date_dt, 
        customer={"name": customer_name, "email": customer_email},
        custom_reference=order_data.get("reference_id"), # Corrected from 'referenceId'
        line_items=line_items,
        raw_data=order_data if include_raw else None,
        last_updated_at=datetime.now()
    )