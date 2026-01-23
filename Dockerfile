# Use an official Python runtime as a parent image
FROM python:3.12-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire backend application into the container at /app
COPY . .

# Expose the port that FastAPI will run on
EXPOSE 8080

# Run the FastAPI application using Gunicorn with Uvicorn workers
# This is a more robust setup for production than running uvicorn directly.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--worker-class", "uvicorn.workers.UvicornWorker", "main:app"]
