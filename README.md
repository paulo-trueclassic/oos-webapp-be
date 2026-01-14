# OOS Webapp Backend (FastAPI)

This repository contains the backend service for the Out-Of-Stock (OOS) Web Application, built using FastAPI. It provides APIs for user authentication, fetching out-of-stock order data from Google BigQuery, and triggering manual data refresh tasks.

## Features

*   **User Authentication:** Secure login functionality using basic authentication.
*   **BigQuery Integration:** Fetches real-time out-of-stock order data from specified BigQuery tables.
*   **Data Refresh Endpoint:** Allows authenticated users to trigger an immediate data refresh from the source systems into BigQuery.
*   **Last Refresh Timestamp:** Provides an endpoint to retrieve the timestamp of the last successful data refresh.
*   **Containerized (Dockerfile):** Ready for deployment in containerized environments like Google Cloud Run.

## Technologies

*   **Framework:** FastAPI
*   **ASGI Server:** Uvicorn (for development), Gunicorn (for production)
*   **Database:** Google BigQuery
*   **Data Validation:** Pydantic
*   **Environment Variables:** python-dotenv
*   **Cloud Client Libraries:** google-cloud-bigquery, google-api-core, google-auth

## Setup

### Prerequisites

*   Python 3.9+
*   `pip` (Python package installer)
*   `gcloud` CLI (for Google Cloud BigQuery authentication if running locally outside a GCP environment)

### 1. Clone the repository

```bash
git clone <repository_url>
cd oos-webapp-be
```

### 2. Create and Activate Virtual Environment

```bash
python -m venv .venv
.\.venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment Variables

Create a `.env` file in the `oos-webapp-be` directory with the following structure:

```dotenv
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/google-cloud-keyfile.json
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET_ID=your-bigquery-dataset-id
BIGQUERY_TABLE_STORD_OOS=your-stord-oos-table-id
BIGQUERY_TABLE_SHIPBOB_OOS=your-shipbob-oos-table-id
BIGQUERY_TABLE_LAST_REFRESH=your-last-refresh-table-id
BASIC_AUTH_USERNAME=your-username
BASIC_AUTH_PASSWORD=your-password
```

*   `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud service account key file (JSON).
*   `BIGQUERY_PROJECT_ID`: Your Google Cloud Project ID where BigQuery is located.
*   `BIGQUERY_DATASET_ID`: The BigQuery dataset containing your OOS tables.
*   `BIGQUERY_TABLE_STORD_OOS`: The table ID for Stord OOS orders.
*   `BIGQUERY_TABLE_SHIPBOB_OOS`: The table ID for Shipbob OOS orders.
*   `BIGQUERY_TABLE_LAST_REFRESH`: The table ID storing the last refresh timestamp.
*   `BASIC_AUTH_USERNAME`: Username for API basic authentication.
*   `BASIC_AUTH_PASSWORD`: Password for API basic authentication.

### 5. Running the Application Locally

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`.

## API Endpoints

### Authentication

*   **POST `/token`**
    *   **Description:** Authenticates a user and returns a token.
    *   **Request Body (JSON):**
        ```json
        {
            "username": "your-username",
            "password": "your-password"
        }
        ```
    *   **Response (Headers):** `Authorization: Basic <base64_encoded_credentials>`

### Data Endpoints

*   **GET `/api/stord/oos-orders`**
    *   **Description:** Retrieves a list of out-of-stock orders from Stord.
    *   **Authentication:** Requires `Authorization` header.
*   **GET `/api/shipbob/oos-orders`**
    *   **Description:** Retrieves a list of out-of-stock orders from Shipbob.
    *   **Authentication:** Requires `Authorization` header.
*   **GET `/api/last-refresh-time`**
    *   **Description:** Returns the UTC timestamp of the last data refresh.
    *   **Authentication:** Requires `Authorization` header.
    *   **Response (JSON):**
        ```json
        {
            "last_refresh_time": "2026-01-15T10:30:00+00:00"
        }
        ```

### Data Refresh

*   **POST `/api/trigger-refresh`**
    *   **Description:** Triggers a background task to refresh OOS data from source systems into BigQuery.
    *   **Authentication:** Requires `Authorization` header.

## Deployment (Google Cloud Run)

This application is designed to be deployed on Google Cloud Run. Ensure that the environment variables (especially `GOOGLE_APPLICATION_CREDENTIALS` or appropriate service account permissions) are correctly configured in your Cloud Run service settings.
