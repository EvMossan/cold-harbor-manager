FROM python:3.11-slim

# Prevent .pyc files and ensure stdout/err are unbuffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000 \
    PYTHONPATH=/app/src

WORKDIR /app

# System deps for psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

# Run via gunicorn; Cloud Run passes $PORT
# Use a shell so $PORT is expanded at runtime
CMD ["sh", "-c", "gunicorn -k gevent -w 1 -b 0.0.0.0:${PORT} cold_harbour.account_web:app"]
