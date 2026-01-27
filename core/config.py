"""
Configuration module for the shortage-workflow project.

This module centralizes all environment variable access.
All configuration values are loaded from .env file via python-dotenv.
"""

import os
import dotenv
from pathlib import Path

project_root = Path(__file__).parent.parent
dotenv_path = project_root / ".env"

if dotenv_path.exists():
    dotenv.load_dotenv(dotenv_path=dotenv_path)
    print(f"Loaded .env file from: {dotenv_path}")
else:
    print(f"Warning: .env file not found at {dotenv_path}. Using system environment variables.")

# ============================================================================
# Stord API Configuration
# ============================================================================
STORD_BASE_URL = os.getenv("STORD_BASE_URL")
STORD_API_TOKEN = os.getenv("STORD_API_TOKEN")
STORD_ORG_ID = os.getenv("STORD_ORG_ID")
STORD_NETWORK_ID = os.getenv("STORD_NETWORK_ID")
channel_ids_str = os.getenv("STORD_CHANNEL_IDS")
STORD_CHANNEL_IDS = channel_ids_str.split(",") if channel_ids_str else []
status_str = os.getenv("STORD_STATUS")
STORD_STATUS = status_str.split(",") if status_str else []

# ============================================================================
# Shipbob API Configuration
# ============================================================================
SHIPBOB_BASE_URL = os.getenv("SHIPBOB_BASE_URL")
SHIPBOB_API_TOKEN = os.getenv("SHIPBOB_API_TOKEN")

# ============================================================================
# BigQuery Configuration
# ============================================================================
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") # Added
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "oos_reporting")
BIGQUERY_STORD_DETAILS_TABLE = os.getenv("BIGQUERY_STORD_DETAILS_TABLE", "stord_order_details")
BIGQUERY_SHIPBOB_DETAILS_TABLE = os.getenv("BIGQUERY_SHIPBOB_DETAILS_TABLE", "shipbob_order_details")
BIGQUERY_COMMENTS_TABLE = os.getenv("BIGQUERY_COMMENTS_TABLE", "oos_comments")

# ============================================================================
# Application Configuration
# ============================================================================
ENV = os.getenv("ENV", "").lower()
TOKEN_VALIDITY = os.getenv("TOKEN_VALIDITY", 1440)
TOKEN_VALIDITY = int(TOKEN_VALIDITY)