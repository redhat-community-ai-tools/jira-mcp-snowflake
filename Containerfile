FROM ghcr.io/astral-sh/uv:alpine

# Set default MCP transport if not provided
ENV MCP_TRANSPORT=stdio

# Install Python 3.11 and required build dependencies
RUN apk add --no-cache python3 python3-dev py3-pip build-base

# Copy project files to user's home directory (no permission issues)
COPY pyproject.toml ./
COPY .python-version ./
COPY uv.lock ./
COPY README.md ./
# Copy application files (needed for editable install)
COPY ./src/ ./

# Install dependencies - no permission changes needed
RUN uv sync --no-cache --locked

# Environment variables (set these when running the container)
# SNOWFLAKE_BASE_URL - Snowflake API base URL (optional, defaults to Red Hat's instance)
# SNOWFLAKE_TOKEN - Snowflake authentication token (required)
# SNOWFLAKE_DATABASE - Snowflake database name (optional)
# SNOWFLAKE_SCHEMA - Snowflake schema name (optional)
# MCP_TRANSPORT - MCP transport type (optional)

# Expose metrics port
EXPOSE 8000

CMD ["uv", "run","--no-cache", "python", "mcp_server.py"]
