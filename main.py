from fastapi import (
    FastAPI,
    BackgroundTasks,
    HTTPException,
    Depends,
    status,
    Request,
    Response,
)
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
import json
import asyncio

from core.logger import get_logger
from core.bigquery_service import bigquery_service, BigQueryClientError
from core.background_tasks import trigger_full_refresh, trigger_source_refresh
from core.data_models import (
    OrderDetails,
    convert_stord_order_to_model,
    convert_shipbob_order_to_model,
    SkuInventory,
)
from core.stord_service import StordService
from core.shipbob_service import ShipbobService
from core.analytics_service import analytics_service
from core.security import get_current_user, User
from routers import auth as auth_router, users as users_router

logger = get_logger(__name__)

app = FastAPI(
    title="OOS Workflow API",
    description="API for processing and serving out-of-stock order data",
    version="1.0.0",
)

@app.on_event("startup")
async def startup_event():
    """
    On startup, check if the required BigQuery tables exist and create them if they don't.
    """
    try:
        logger.info("Application startup: Verifying BigQuery tables...")
        bigquery_service.create_tables_if_not_exists()
        logger.info("BigQuery table verification complete.")
    except BigQueryClientError as e:
        logger.error(f"FATAL: Could not connect to BigQuery on startup: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during startup: {e}")

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Initialize services
stord_service = StordService()
shipbob_service = ShipbobService()

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    lambda request, exc: Response(
        content=f"Rate limit exceeded: {exc.detail}",
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
    ),
)

# CORS configuration
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://100.108.61.13:3000",
    "http://100.108.61.13",
    "https://oos-webapp-fe-999385730987.us-west2.run.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Include Routers ---
app.include_router(auth_router.router)
app.include_router(users_router.router)


@app.get("/")
async def root():
    return {
        "message": "OOS Workflow API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/api/{source}/oos-orders", response_model=List[OrderDetails])
@limiter.limit("100/minute")
async def get_oos_orders(
    request: Request,
    source: str,
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves a list of all out-of-stock orders for the given source from BigQuery.
    """
    if source.lower() not in ["stord", "shipbob"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid source specified. Must be 'stord' or 'shipbob'.",
        )

    try:
        raw_orders_data = bigquery_service.get_oos_orders(source=source.lower())
    except BigQueryClientError as e:
        raise HTTPException(
            status_code=503,
            detail=f"BigQuery service unavailable: {str(e)}. Please check your BigQuery credentials configuration.",
        )

    converted_orders = []
    for item in raw_orders_data:
        if source.lower() == "stord":
            converted_orders.extend(
                convert_stord_order_to_model(item, include_raw=True)
            )
        else:
            converted_orders.extend(
                convert_shipbob_order_to_model(item, include_raw=True)
            )
    return converted_orders


@app.get("/api/{source}/order-details/{order_id}", response_model=Optional[OrderDetails])
@limiter.limit("100/minute")
async def get_order_details(
    request: Request,
    order_id: str,
    source: str,
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves full order details for a specific order_id and source from BigQuery.
    """
    if source.lower() not in ["stord", "shipbob"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid source specified. Must be 'stord' or 'shipbob'.",
        )

    try:
        raw_order_data = bigquery_service.get_order_details(
            order_id=order_id, source=source.lower()
        )
    except BigQueryClientError as e:
        raise HTTPException(
            status_code=503,
            detail=f"BigQuery service unavailable: {str(e)}.",
        )

    if not raw_order_data:
        raise HTTPException(
            status_code=404, detail=f"Order {order_id} from {source} not found."
        )

    if source.lower() == "stord":
        return convert_stord_order_to_model(raw_order_data, include_raw=True)[0]
    else:
        return convert_shipbob_order_to_model(raw_order_data, include_raw=True)[0]


@app.post("/api/trigger-refresh", status_code=status.HTTP_202_ACCEPTED)
@limiter.limit("10/minute")
async def trigger_full_refresh_endpoint(
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
):
    """
    Triggers a full refresh of Stord and Shipbob OOS data in the background.
    """
    background_tasks.add_task(trigger_full_refresh)
    logger.info(f"User '{current_user.username}' triggered a full data refresh.")
    return {"message": "Full data refresh initiated in the background."}


@app.post("/api/trigger-refresh/{source}", status_code=status.HTTP_202_ACCEPTED)
@limiter.limit("10/minute")
async def trigger_source_refresh_endpoint(
    request: Request,
    source: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
):
    """
    Triggers a refresh of OOS data for a single source ('stord' or 'shipbob') in the background.
    """
    source = source.lower()
    if source not in ["stord", "shipbob"]:
        raise HTTPException(status_code=400, detail="Invalid source specified.")
    
    background_tasks.add_task(trigger_source_refresh, source)
    logger.info(f"User '{current_user.username}' triggered a data refresh for source '{source}'.")
    return {"message": f"Data refresh for source '{source}' initiated in the background."}


@app.get("/api/last-refresh-time")
@limiter.limit("100/minute")
async def get_last_refresh_time(
    request: Request, current_user: User = Depends(get_current_user)
):
    """
    Retrieves the most recent timestamp of a data refresh.
    """
    try:
        last_refresh = bigquery_service.get_last_refresh_time()
        if last_refresh:
            return {"last_refresh_time": last_refresh.isoformat()}
        else:
            raise HTTPException(status_code=404, detail="Refresh time not available.")
    except BigQueryClientError as e:
        raise HTTPException(
            status_code=503,
            detail=f"BigQuery service unavailable: {str(e)}.",
        )

@app.get("/api/analytics/summary", status_code=status.HTTP_200_OK)
@limiter.limit("30/minute")
async def get_analytics_summary(
    request: Request,
    current_user: User = Depends(get_current_user),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    Retrieves a consolidated summary of historical OOS analytics for a given date range.
    """
    try:
        now_utc = datetime.now(timezone.utc)
        if start_date:
            start_dt = datetime.fromisoformat(start_date).replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
            )
        else:
            start_dt = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if end_date:
            end_dt = datetime.fromisoformat(end_date).replace(
                hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
            )
        else:
            end_dt = now_utc

        analytics_data = analytics_service.get_full_analytics(start_dt, end_dt)
        analytics_data["last_updated"] = now_utc.isoformat()
        analytics_data["date_range"] = {
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
        }
        return analytics_data
    except BigQueryClientError as e:
        raise HTTPException(status_code=503, detail=f"BigQuery service unavailable: {str(e)}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_analytics_summary: {e}")
        raise HTTPException(status_code=500, detail="An internal error occurred.")


@app.post("/api/inventory/bulk", response_model=Dict[str, SkuInventory])
@limiter.limit("100/minute")
async def get_bulk_inventory(
    request: Request, current_user: User = Depends(get_current_user)
):
    """
    Fetches live inventory data for a list of SKUs from Stord and Shipbob APIs concurrently.
    """
    try:
        request_body = await request.json()
        skus = list(set(request_body.get("skus", [])))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not skus:
        return {}

    inventory_results = {}
    batch_size = 100
    
    for i in range(0, len(skus), batch_size):
        batch_skus = skus[i:i + batch_size]
        logger.info(f"Processing inventory batch {i//batch_size + 1} with {len(batch_skus)} SKUs.")
        
        tasks = [stord_service.get_inventory_from_stord_api(sku) for sku in batch_skus]
        tasks += [shipbob_service.get_inventory_from_shipbob_api(sku) for sku in batch_skus]

        batch_api_results = await asyncio.gather(*tasks, return_exceptions=True)

        for j, sku in enumerate(batch_skus):
            stord_result = batch_api_results[j]
            shipbob_result = batch_api_results[j + len(batch_skus)]

            stord_stock = stord_result if not isinstance(stord_result, Exception) else 0
            if isinstance(stord_result, Exception):
                logger.error(f"Error fetching Stord inventory for SKU {sku}: {stord_result}")

            shipbob_fontana_stock, shipbob_other_stock = shipbob_result if not isinstance(shipbob_result, Exception) else (0, 0)
            if isinstance(shipbob_result, Exception):
                logger.error(f"Error fetching Shipbob inventory for SKU {sku}: {shipbob_result}")

            inventory_results[sku.strip().lower()] = SkuInventory(
                sku=sku,
                stord_stock=stord_stock,
                shipbob_fontana_stock=shipbob_fontana_stock,
                shipbob_other_stock=shipbob_other_stock
            )
            
        if i + batch_size < len(skus):
            logger.info(f"Batch {i//batch_size + 1} complete. Waiting 60 seconds.")
            await asyncio.sleep(60)

    logger.info("All inventory batches processed successfully.")
    return inventory_results


if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
