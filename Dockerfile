FROM python:3.11-slim

WORKDIR /app

# Install tzdata so the OS understands timezones
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /data

CMD ["python", "pipeline.py"]