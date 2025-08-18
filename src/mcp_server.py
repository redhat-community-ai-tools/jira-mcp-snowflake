#!/usr/bin/env python3
"""
JIRA MCP Server for Snowflake

A Model Context Protocol (MCP) server that provides access to JIRA data stored in Snowflake.
Includes optional Prometheus metrics for monitoring tool usage and performance.
"""

import logging

from mcp.server.fastmcp import FastMCP

from config import MCP_TRANSPORT
from metrics import start_metrics_thread, set_active_connections
from tools import register_tools
from database import cleanup_resources

# Get logger
logger = logging.getLogger(__name__)


async def async_cleanup():
    """Async cleanup function"""
    try:
        await cleanup_resources()
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def main():
    """Main entry point for the MCP server"""
    # Initialize FastMCP server
    mcp = FastMCP("jira-mcp-snowflake", host="0.0.0.0")

    # Register all tools
    register_tools(mcp)

    # Start metrics server in background thread if enabled
    start_metrics_thread()

    # Run the MCP server
    try:
        logger.info("Starting JIRA MCP Server for Snowflake")
        mcp.run(transport=MCP_TRANSPORT)
    except KeyboardInterrupt:
        logger.info("Shutting down MCP server...")
    except Exception as e:
        logger.error(f"Error running MCP server: {e}")
        raise
    finally:
        # Clean up resources
        set_active_connections(0)
        import asyncio
        try:
            asyncio.run(async_cleanup())
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        logger.info("MCP server shutdown complete")


if __name__ == "__main__":
    main()
