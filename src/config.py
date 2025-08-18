import os
import logging

# Configure logging to stderr so it doesn't interfere with MCP protocol
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# MCP Configuration
MCP_TRANSPORT = os.environ.get("MCP_TRANSPORT", "stdio")
FASTMCP_HOST = os.environ.get("FASTMCP_HOST", "0.0.0.0")
FASTMCP_PORT = os.environ.get("FASTMCP_PORT", "8000")

# Snowflake API configuration from environment variables
SNOWFLAKE_BASE_URL = os.environ.get("SNOWFLAKE_BASE_URL")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "DEFAULT")
INTERNAL_GATEWAY = os.environ.get("INTERNAL_GATEWAY", "false")

# Snowflake token handling - for stdio transport, get from environment
# For other transports, it will be retrieved from request context in tools layer
if MCP_TRANSPORT == "stdio":
    SNOWFLAKE_TOKEN = os.environ.get("SNOWFLAKE_TOKEN")
else:
    # For non-stdio transports, token will be passed from tools layer
    SNOWFLAKE_TOKEN = None

# Prometheus metrics configuration
ENABLE_METRICS = os.environ.get("ENABLE_METRICS", "false").lower() == "true"
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))

# Performance configuration
ENABLE_CACHING = os.environ.get("ENABLE_CACHING", "true").lower() == "true"
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "300"))  # 5 minutes
CACHE_MAX_SIZE = int(os.environ.get("CACHE_MAX_SIZE", "1000"))
MAX_HTTP_CONNECTIONS = int(os.environ.get("MAX_HTTP_CONNECTIONS", "20"))
HTTP_TIMEOUT_SECONDS = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "60"))
THREAD_POOL_WORKERS = int(os.environ.get("THREAD_POOL_WORKERS", "10"))
RATE_LIMIT_PER_SECOND = int(os.environ.get("RATE_LIMIT_PER_SECOND", "50"))
CONCURRENT_QUERY_BATCH_SIZE = int(os.environ.get("CONCURRENT_QUERY_BATCH_SIZE", "5"))

# Check if Prometheus is available
try:
    # These imports are used in metrics.py
    from prometheus_client import Counter, Histogram, Gauge, CONTENT_TYPE_LATEST, generate_latest  # noqa: F401
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
