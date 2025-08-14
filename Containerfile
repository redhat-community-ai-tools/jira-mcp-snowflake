FROM registry.redhat.io/ubi9/python-311

LABEL konflux.additional-tags="latest"
# Set default MCP transport if not provided
ENV MCP_TRANSPORT=stdio

COPY . . 
# Install dependencies

RUN pip install --no-cache-dir .

# Environment variables (set these when running the container)
# Required:
# SNOWFLAKE_TOKEN - Snowflake authentication token (required)
#
# Optional Configuration:
# SNOWFLAKE_BASE_URL - Snowflake API base URL (optional, defaults to Red Hat's instance)
# SNOWFLAKE_DATABASE - Snowflake database name (optional)
# SNOWFLAKE_SCHEMA - Snowflake schema name (optional)
# MCP_TRANSPORT - MCP transport type (optional)
#
# Performance Tuning:
# ENABLE_CACHING - Enable response caching (default: true)
# CACHE_TTL_SECONDS - Cache time-to-live in seconds (default: 300)
# CACHE_MAX_SIZE - Maximum cache entries (default: 1000)
# MAX_HTTP_CONNECTIONS - HTTP connection pool size (default: 20)
# HTTP_TIMEOUT_SECONDS - HTTP request timeout (default: 60)
# THREAD_POOL_WORKERS - Thread pool size for CPU tasks (default: 10)
# RATE_LIMIT_PER_SECOND - API rate limit per second (default: 50)
# CONCURRENT_QUERY_BATCH_SIZE - Batch size for concurrent queries (default: 5)
#
# Monitoring:
# ENABLE_METRICS - Enable Prometheus metrics (default: false)
# METRICS_PORT - Metrics server port (default: 8000)

# Expose metrics port
EXPOSE 8000

CMD ["python", "src/mcp_server.py"]
