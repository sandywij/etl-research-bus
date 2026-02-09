# Use a lightweight Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# 1. Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# 2. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the rest of your application code
# This includes pipeline.py and locations.csv
COPY . .

# 4. Create the data directory for the SQLite database
# The Fly volume will be mounted to this path
RUN mkdir -p /data

# 5. Command to run your script
CMD ["python", "pipeline.py"]