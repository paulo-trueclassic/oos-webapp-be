import os
from core.config import TOKEN_VALIDITY

# BigQuery Table for Users
BIGQUERY_USERS_TABLE = os.getenv("BIGQUERY_TABLE_USERS", "users")

# JWT Configuration
# IMPORTANT: This secret key should be overridden in a production environment
# using a securely generated key (e.g., openssl rand -hex 32).
SECRET_KEY = os.getenv("SECRET_KEY", "a-very-insecure-default-secret-key-for-development")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = TOKEN_VALIDITY
