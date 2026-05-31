# Slim Python with build essentials for yfinance / lxml / numpy wheels.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8080 \
    CORTEX_DATA_DIR=/data

WORKDIR /app

# Install deps first so changes to source code don't bust the layer cache.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy the app last.
COPY server.py index.html ./

EXPOSE 8080
CMD ["python", "server.py", "serve"]
