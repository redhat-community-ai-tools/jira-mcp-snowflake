FROM quay.io/centos/centos:stream9

# Set default MCP transport if not provided
ENV MCP_TRANSPORT=stdio

# Install Python 3.11, pip, and build dependencies
RUN dnf -y install --setopt=install_weak_deps=False --setopt=tsflags=nodocs \
    python3.12 python3.12-devel python3.12-pip gcc make git && \
    dnf clean all

# Install uv (universal virtualenv/dependency manager)
RUN pip3.12 install --no-cache-dir  --upgrade pip && \
    pip3.12 install --no-cache-dir uv

# Copy project files to working directory
WORKDIR /app

# Set ownership to the user we created. Group 0 (root) is important for OpenShift compatibility.
RUN chown -R 1001:0 /app && \
    chgrp -R 0 /app && \
    chmod -R g+rwX /app

# Switch to the non-root user *before* copying files and installing dependencies
USER 1001

COPY pyproject.toml ./
COPY .python-version ./
COPY uv.lock ./
COPY README.md ./
# Copy application files (needed for editable install)
COPY ./src/ ./

# Install dependencies
RUN uv sync --no-cache --locked

# Environment variables (set these when running the container)
# SNOWFLAKE_BASE_URL - Snowflake API base URL (optional, defaults to Red Hat's instance)
# SNOWFLAKE_TOKEN - Snowflake authentication token (required)
# SNOWFLAKE_DATABASE - Snowflake database name (optional)
# SNOWFLAKE_SCHEMA - Snowflake schema name (optional)
# MCP_TRANSPORT - MCP transport type (optional)

# Expose metrics port
EXPOSE 8000

CMD ["uv", "run", "--no-cache", "python", "mcp_server.py"]
