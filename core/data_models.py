from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid # Added for UUID fallback

from core.logger import get_logger # Added logger import

logger = get_logger(__name__)

# --- Pydantic Models ---

class SkuInventory(BaseModel):
    sku: str
    stord_stock: int
    shipbob_fontana_stock: int
    shipbob_other_stock: int


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
    # Add other fields as necessary, ensuring they match the converted data structure

class CommentBase(BaseModel):
    order_id: str
    sku: str
    facility: str
    comment: str

class CommentCreate(CommentBase):
    pass

class CommentRead(CommentBase):
    author: str
    created_at: datetime

# --- Conversion Functions ---

def convert_stord_order_to_model(order_data: Dict[str, Any], include_raw: bool = False) -> List[OrderDetails]:
    """
    Converts a single Stord order into a list containing one OrderDetails object,
    aggregating all line items.
    """
    order_id = order_data.get("order_number") or order_data.get("order_id") or str(uuid.uuid4())
    
    line_items = []
    for sol in order_data.get("sales_order_lines", []):
        for oli in sol.get("order_line_items", []):
            line_items.append(OrderLineItem(
                sku=oli.get("item_sku"),
                quantity=int(float(oli.get("item_quantity"))) if oli.get("item_quantity") else None,
                status=sol.get("status")
            ))

    # Centralize parsing logic
    customer_data = order_data.get("customer")
    customer_details = {"name": customer_data if isinstance(customer_data, str) else customer_data.get("name") if isinstance(customer_data, dict) else None, "email": None}
    
    shipped_at, purchase_date = None, None
    shipped_at_str = order_data.get("shipped_at") or order_data.get("external_posted_at")
    if shipped_at_str:
        try:
            shipped_at = datetime.fromisoformat(shipped_at_str.replace('Z', '+00:00'))
            purchase_date = shipped_at
        except (ValueError, TypeError):
            logger.warning(f"Could not parse datetime for Stord shipped_at: {shipped_at_str}")

    facility = None
    facility_activities = order_data.get("facility_activities")
    if facility_activities and isinstance(facility_activities, list) and len(facility_activities) > 0:
        facility = facility_activities[0].get("facility_alias")

    order_details = OrderDetails(
        order_id=order_id,
        order_number=order_data.get("order_number"),
        status=order_data.get("status"),
        source="stord",
        priority=order_data.get("priority"),
        channel=order_data.get("channel"),
        channel_category=order_data.get("channel_category"),
        shipment_type=order_data.get("shipment_type"),
        custom_reference=order_data.get("custom_reference"),
        raw_data=order_data if include_raw else None,
        last_updated_at=datetime.now(),
        customer=customer_details,
        shipped_at=shipped_at,
        purchase_date=purchase_date,
        facility=facility,
        line_items=line_items
    )

    return [order_details]


def convert_shipbob_order_to_model(order_data: Dict[str, Any], include_raw: bool = False) -> List[OrderDetails]:
    """
    Converts a single Shipbob order into a list containing one OrderDetails object,
    aggregating all line items.
    """
    order_id = str(order_data.get("id") or uuid.uuid4())
    
    line_items = []
    for product in order_data.get("products", []):
        line_items.append(OrderLineItem(
            sku=product.get("sku"),
            quantity=product.get("quantity"),
            status=order_data.get("status") # Main order status
        ))

    # Centralize parsing logic
    recipient = order_data.get("recipient", {})
    customer_details = {"name": recipient.get("name"), "email": recipient.get("email")}

    purchase_date, shipped_at = None, None
    created_date_str = order_data.get("created_date")
    if created_date_str:
        try:
            purchase_date = datetime.fromisoformat(created_date_str.replace('Z', '+00:00'))
            shipped_at = purchase_date
        except (ValueError, TypeError):
            logger.warning(f"Could not parse datetime for Shipbob created_date: {created_date_str}")
    
    facility = None
    shipments = order_data.get("shipments", [])
    if shipments:
        facility = shipments[0].get("location", {}).get("name")

    order_details = OrderDetails(
        order_id=order_id,
        order_number=order_data.get("order_number"),
        status=order_data.get("status"),
        source="shipbob",
        priority=None,
        channel=order_data.get("channel", {}).get("name"),
        channel_category=order_data.get("type"),
        shipment_type=order_data.get("shipping_method"),
        custom_reference=order_data.get("reference_id"),
        raw_data=order_data if include_raw else None,
        last_updated_at=datetime.now(),
        customer=customer_details,
        purchase_date=purchase_date,
        shipped_at=shipped_at,
        facility=facility,
        line_items=line_items
    )
    
    return [order_details]