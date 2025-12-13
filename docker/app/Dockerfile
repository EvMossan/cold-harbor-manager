# docker/app/Dockerfile
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV PORT=5000

# Install curl to download cloudflared and clean up apt lists.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Fetch the most recent Linux amd64 cloudflared binary.
RUN curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
    -o /usr/local/bin/cloudflared \
    && chmod +x /usr/local/bin/cloudflared

# Update path to requirements file (moved to requirements/)
COPY requirements/requirements_dashboard.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src

# Update path to entrypoint (moved to docker/app/)
COPY docker/app/entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE ${PORT}

ENTRYPOINT ["./entrypoint.sh"]
