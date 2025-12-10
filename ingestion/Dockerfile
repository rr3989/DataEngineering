# Use a Python base image
FROM python:3.11-slim

# Set environment variables for non-interactive commands
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the dependency file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY trade_publisher.py .

# Command to run the publisher script.
# This runs constantly and is what Cloud Run executes.
CMD ["python", "trade_publisher.py"]