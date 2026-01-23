from typing import Optional, Dict, Any, List
from google.cloud import bigquery

from core.bigquery_service import BigQueryService, BigQueryClientError
from core.logger import get_logger

logger = get_logger(__name__)

class UserService:
    def __init__(self):
        self.bq_service = BigQueryService()
        self.client = self.bq_service.client
        self.users_table_id = self.bq_service.users_table_id

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a user's details from BigQuery by their username.
        """
        if not self.users_table_id:
            raise BigQueryClientError("Users table is not configured.")

        query = f"SELECT * FROM `{self.users_table_id}` WHERE username = @username"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("username", "STRING", username)
            ]
        )
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job.result())
            if not results:
                return None
            return dict(results[0])
        except Exception as e:
            logger.error(f"Error fetching user '{username}' from BigQuery: {e}")
            raise BigQueryClientError(f"Failed to fetch user: {e}")

    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Retrieves all users from the BigQuery users table.
        """
        if not self.users_table_id:
            raise BigQueryClientError("Users table is not configured.")

        query = f"SELECT username, role FROM `{self.users_table_id}` ORDER BY username"
        try:
            query_job = self.client.query(query)
            users = [dict(row) for row in query_job.result()]
            return users
        except Exception as e:
            logger.error(f"Error fetching all users from BigQuery: {e}")
            raise BigQueryClientError(f"Failed to fetch users: {e}")

    def create_user(self, username: str, hashed_password: str, role: str) -> Dict[str, Any]:
        """
        Creates a new user in the BigQuery users table.
        """
        if not self.users_table_id:
            raise BigQueryClientError("Users table is not configured.")

        # First, check if user already exists
        if self.get_user_by_username(username):
            raise ValueError(f"User '{username}' already exists.")

        row_to_insert = {
            "username": username,
            "hashed_password": hashed_password,
            "role": role,
        }
        try:
            errors = self.client.insert_rows_json(self.users_table_id, [row_to_insert])
            if errors:
                logger.error(f"Failed to create user '{username}': {errors}")
                raise BigQueryClientError(f"Failed to insert user: {errors}")
            
            logger.info(f"Successfully created user '{username}'.")
            return {"username": username, "role": role}
        except Exception as e:
            logger.error(f"Error creating user '{username}' in BigQuery: {e}")
            raise BigQueryClientError(f"Failed to create user: {e}")

    def update_password(self, username: str, new_hashed_password: str) -> bool:
        """
        Updates a user's hashed password in BigQuery.
        """
        if not self.users_table_id:
            raise BigQueryClientError("Users table is not configured.")

        query = f"""
            UPDATE `{self.users_table_id}`
            SET hashed_password = @new_hashed_password
            WHERE username = @username
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("new_hashed_password", "STRING", new_hashed_password),
                bigquery.ScalarQueryParameter("username", "STRING", username),
            ]
        )
        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for the job to complete
            
            if query_job.num_dml_affected_rows > 0:
                logger.info(f"Successfully updated password for user '{username}'.")
                return True
            else:
                logger.warning(f"Attempted to update password for non-existent user '{username}'.")
                return False
        except Exception as e:
            logger.error(f"Error updating password for user '{username}': {e}")
            raise BigQueryClientError(f"Failed to update password: {e}")
            
    def delete_user(self, username: str) -> bool:
        """
        Deletes a user from the BigQuery users table.
        """
        if not self.users_table_id:
            raise BigQueryClientError("Users table is not configured.")

        query = f"DELETE FROM `{self.users_table_id}` WHERE username = @username"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("username", "STRING", username)
            ]
        )
        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result() # Wait for job to complete
            
            if query_job.num_dml_affected_rows > 0:
                logger.info(f"Successfully deleted user '{username}'.")
                return True
            else:
                logger.warning(f"Attempted to delete non-existent user '{username}'.")
                return False
        except Exception as e:
            logger.error(f"Error deleting user '{username}': {e}")
            raise BigQueryClientError(f"Failed to delete user: {e}")

# Singleton instance
user_service = UserService()
