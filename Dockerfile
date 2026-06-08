# ============================================
# Python App Base Image
# Untuk Producer, Processor, dan Dashboard
# ============================================
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy semua source code
COPY data/ ./data/
COPY producer/ ./producer/
COPY processor/ ./processor/
COPY dashboard/ ./dashboard/
COPY .streamlit/ ./.streamlit/

# Default command (di-override oleh docker-compose)
CMD ["python", "--version"]
