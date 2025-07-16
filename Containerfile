FROM registry.redhat.io/ubi9/python-311

# Set default MCP transport if not provided
ENV MCP_TRANSPORT=stdio

COPY . . 
# Install dependencies

RUN pip install --no-cache-dir .

# Environment variables (set these when running the container)
# SNOWFLAKE_BASE_URL - Snowflake API base URL (optional, defaults to Red Hat's instance)
# SNOWFLAKE_TOKEN - Snowflake authentication token (required)
# SNOWFLAKE_DATABASE - Snowflake database name (optional)
# SNOWFLAKE_SCHEMA - Snowflake schema name (optional)
# MCP_TRANSPORT - MCP transport type (optional)

# Expose metrics port
EXPOSE 8000

CMD ["python", "src/mcp_server.py"]
