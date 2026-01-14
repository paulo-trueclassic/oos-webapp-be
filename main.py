from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends, status, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import base64

from core.logger import get_logger
from core.config import APP_USERNAME, APP_PASSWORD
from core.bigquery_service import bigquery_service
from core.background_tasks import trigger_full_refresh
from core.data_models import OrderDetails, convert_stord_order_to_model, convert_shipbob_order_to_model

logger = get_logger(__name__)

app = FastAPI(
    title="OOS Workflow API",
    description="API for processing and serving out-of-stock order data",
    version="1.0.0",
)

# CORS configuration
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://100.108.61.13:3000",
    "http://100.108.61.13",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Basic authentication
security = HTTPBasic()

# Login request model for JSON body authentication
class LoginRequest(BaseModel):
    username: str
    password: str

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    # Check if credentials are configured
    if not APP_USERNAME or not APP_PASSWORD:
        logger.error("APP_USERNAME or APP_PASSWORD not configured in environment variables")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication not configured",
        )
    
    # Log received credentials (username only for security)
    logger.info(f"Authentication attempt for username: {credentials.username}")
    
    # Compare credentials (case-sensitive)
    if credentials.username != APP_USERNAME or credentials.password != APP_PASSWORD:
        logger.warning(f"Authentication failed for username: {credentials.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    logger.info(f"Authentication successful for username: {credentials.username}")
    return credentials

def verify_credentials_from_json(username: str, password: str):
    """Helper function to verify credentials from JSON body"""
    # Check if credentials are configured
    if not APP_USERNAME or not APP_PASSWORD:
        logger.error("APP_USERNAME or APP_PASSWORD not configured in environment variables")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication not configured",
        )
    
    # Log received credentials (username only for security)
    logger.info(f"Authentication attempt for username: {username}")
    
    # Compare credentials (case-sensitive)
    if username != APP_USERNAME or password != APP_PASSWORD:
        logger.warning(f"Authentication failed for username: {username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    
    logger.info(f"Authentication successful for username: {username}")
    return {"username": username}

async def get_optional_basic_auth(request: Request) -> Optional[HTTPBasicCredentials]:
    """Try to get Basic Auth credentials from header, return None if not present"""
    authorization = request.headers.get("Authorization")
    if not authorization or not authorization.startswith("Basic "):
        return None
    
    try:
        import base64
        credentials = authorization.replace("Basic ", "")
        decoded = base64.b64decode(credentials).decode("utf-8")
        username, password = decoded.split(":", 1)
        return HTTPBasicCredentials(username=username, password=password)
    except Exception:
        return None

@app.get("/")
async def root():
    return {
        "message": "OOS Workflow API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "oos_orders": "/api/{source}/oos-orders",
            "order_details": "/api/{source}/order-details/{order_id}",
            "trigger_refresh": "/api/trigger-refresh (POST)",
            "token": "/token (POST)"
        },
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/token")
async def login(login_data: LoginRequest, response: Response):
    """
    Login endpoint that accepts credentials via a JSON body.
    """
    verify_credentials_from_json(login_data.username, login_data.password)

    # Create the Basic Auth header value
    auth_string = f"{login_data.username}:{login_data.password}"
    encoded_auth_string = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
    auth_header = f"Basic {encoded_auth_string}"

    # Set the header on the response
    response.headers["Authorization"] = auth_header
    
    return {"message": "Login successful", "username": login_data.username}

@app.get("/api/{source}/oos-orders", response_model=List[OrderDetails])
async def get_oos_orders(
    source: str,
    credentials: HTTPBasicCredentials = Depends(verify_credentials)
):
    """
    Retrieves a list of all out-of-stock orders for the given source from BigQuery.
    """
    if source.lower() not in ["stord", "shipbob"]:
        raise HTTPException(status_code=400, detail="Invalid source specified. Must be 'stord' or 'shipbob'.")
    
    raw_orders_data = bigquery_service.get_oos_orders(source=source.lower())
    
    # Convert raw data to OrderDetails models for consistent frontend consumption
    converted_orders = []
    for item in raw_orders_data:
        if source.lower() == "stord":
            converted_orders.append(convert_stord_order_to_model(item, include_raw=True))
        else: # shipbob
            converted_orders.append(convert_shipbob_order_to_model(item, include_raw=True))

    return converted_orders

@app.get("/api/{source}/order-details/{order_id}", response_model=Optional[OrderDetails])
async def get_order_details(
    order_id: str,
    source: str,
    credentials: HTTPBasicCredentials = Depends(verify_credentials)
):
    """
    Retrieves full order details for a specific order_id and source from BigQuery.
    """
    if source.lower() not in ["stord", "shipbob"]:
        raise HTTPException(status_code=400, detail="Invalid source specified. Must be 'stord' or 'shipbob'.")

    raw_order_data = bigquery_service.get_order_details(order_id=order_id, source=source.lower())
    
    if not raw_order_data:
        raise HTTPException(status_code=404, detail=f"Order {order_id} from {source} not found.")

    # Convert raw data to OrderDetails model for consistent frontend consumption
    if source.lower() == "stord":
        return convert_stord_order_to_model(raw_order_data, include_raw=True)
    else: # shipbob
        return convert_shipbob_order_to_model(raw_order_data, include_raw=True)

@app.post("/api/trigger-refresh", status_code=status.HTTP_202_ACCEPTED)
async def trigger_refresh(
    background_tasks: BackgroundTasks,
    credentials: HTTPBasicCredentials = Depends(verify_credentials) 
):
    """
    Triggers a full refresh of Stord and Shipbob OOS data in the background.
    """
    background_tasks.add_task(trigger_full_refresh)
    logger.info("Data refresh task added to background.")
    return {"message": "Data refresh initiated in the background."}


@app.get("/api/last-refresh-time")
async def get_last_refresh_time(credentials: HTTPBasicCredentials = Depends(verify_credentials)):
    """
    Retrieves the most recent timestamp of a data refresh.
    """
    last_refresh = bigquery_service.get_last_refresh_time()
    if last_refresh:
        return {"last_refresh_time": last_refresh.isoformat()}
    else:
        raise HTTPException(status_code=404, detail="Refresh time not available.")

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
