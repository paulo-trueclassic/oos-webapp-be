import os
import asyncio
from google.cloud import bigquery
from passlib.context import CryptContext

from core.logger import get_logger
from core.bigquery_service import BigQueryService

logger = get_logger(__name__)

# --- Configuration ---
ADMIN_USERNAME = "paulo"
ADMIN_PASSWORD = "trueclassictees"
ADMIN_ROLE = "admin"

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_bigquery_client() -> bigquery.Client:
    """Initializes and returns a BigQuery client."""
    try:
        # We instantiate BigQueryService to reuse its client initialization logic
        bq_service = BigQueryService()
        return bq_service.client
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        logger.error("Please ensure your GOOGLE_APPLICATION_CREDENTIALS or other GCP auth is configured.")
        raise

async def seed_admin_user():
    """
    Checks for the existence of the admin user and creates it if it does not exist.
    """
    logger.info("--- Starting Admin User Seeding Script ---")
    
    try:
        client = get_bigquery_client()
        bq_service = BigQueryService() # To get table_id
        users_table_id = bq_service.users_table_id

        if not users_table_id:
            logger.error("Users table ID is not configured in BigQueryService. Aborting.")
            return

        # 1. Hash the password
        hashed_password = pwd_context.hash(ADMIN_PASSWORD)
        logger.info(f"Successfully hashed password for user '{ADMIN_USERNAME}'.")

        # 2. Check if the admin user already exists
        query = f"SELECT username FROM `{users_table_id}` WHERE username = @username"
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("username", "STRING", ADMIN_USERNAME)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)

        results = list(query_job.result())
        
        if len(results) > 0:
            logger.warning(f"Admin user '{ADMIN_USERNAME}' already exists in the database. No action taken.")
            return

        logger.info(f"Admin user '{ADMIN_USERNAME}' not found. Proceeding with creation.")

        # 3. Insert the new admin user
        rows_to_insert = [{
            "username": ADMIN_USERNAME,
            "hashed_password": hashed_password,
            "role": ADMIN_ROLE,
        }]

        errors = client.insert_rows_json(users_table_id, rows_to_insert)
        
        if not errors:
            logger.info(f"Successfully created admin user '{ADMIN_USERNAME}' in table '{users_table_id}'.")
        else:
            logger.error("Failed to insert admin user into BigQuery.")
            for error in errors:
                logger.error(f"Error details: {error}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during the seeding process: {e}")
    finally:
        logger.info("--- Admin User Seeding Script Finished ---")


if __name__ == "__main__":
    # Ensure the script can be run from the command line
    # This setup allows the script to find the 'core' module
    import sys
    sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
    
    # Load .env file for local development
    from dotenv import load_dotenv
    load_dotenv()
    
    asyncio.run(seed_admin_user())