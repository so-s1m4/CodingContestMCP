FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8000 \
    CCC_DATA_DIR=/app/data

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
COPY ccc_mcp ./ccc_mcp
COPY container_entrypoint.py .
RUN useradd --uid 10001 --create-home mcp && mkdir -p /app/data/inbox && chown -R mcp:mcp /app/data
VOLUME ["/app/data"]

EXPOSE 8000
CMD ["python", "container_entrypoint.py"]
