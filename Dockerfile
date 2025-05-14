# 1. Base image
FROM python:3.11-slim-bullseye

# 2. Set environment variables to optimize Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# 3. Working directory at /app
WORKDIR /app

# 4. Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir kafka-python

RUN apt-get update && \
apt-get install -y postgresql-client && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# 5. Copy source code to /app
COPY . .

# 6. Run server
CMD ["bash", "-c", "cd /app/src && python manage.py migrate && python manage.py runserver 0.0.0.0:8000"]