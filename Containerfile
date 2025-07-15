FROM registry.access.redhat.com/ubi9/python-311

WORKDIR /app

# Set default MCP transport if not provided
ENV MCP_TRANSPORT=stdio

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy project files needed for uv sync
COPY pyproject.toml ./
COPY .python-version ./
COPY uv.lock ./
COPY README.md ./
# Copy application files (needed for editable install)
COPY ./src/ ./src/

# Install dependencies using UV
RUN uv sync --locked
# Environment variables (set these when running the container)
# SNOWFLAKE_BASE_URL - Snowflake API base URL (optional, defaults to Red Hat's instance)
# SNOWFLAKE_TOKEN - Snowflake authentication token (required)
# SNOWFLAKE_DATABASE - Snowflake database name (optional)
# SNOWFLAKE_SCHEMA - Snowflake schema name (optional)
# MCP_TRANSPORT - MCP transport type (optional)


# Expose metrics port
EXPOSE 8000

CMD ["uv", "run", "python", "src/mcp_server.py"]
