# Jira MCP Server

A Model Context Protocol (MCP) server that provides access to JIRA issue data stored in Snowflake. This server enables AI assistants to query, filter, and analyze JIRA issues through a standardized interface.

## Overview

This MCP server connects to Snowflake to query JIRA data and provides three main tools for interacting with the data:

- **`list_issues`** - Query and filter JIRA issues with various criteria
- **`get_issue_details`** - Get detailed information for a specific issue by key
- **`get_project_summary`** - Get statistics and summaries for all projects

## Features

### Data Sources
The server connects to Snowflake and queries the following tables:
- `JIRA_ISSUE_NON_PII` - Main issue data (non-personally identifiable information)
- `JIRA_LABEL_RHAI` - Issue labels and tags

**Note**: Table names are expected to exist in your configured Snowflake database and schema.

### Available Tools

#### 1. List Issues (`list_issues`)
Query JIRA issues with optional filtering:
- **Project filtering** - Filter by project key (e.g., 'SMQE', 'OSIM')
- **Issue type filtering** - Filter by issue type ID
- **Status filtering** - Filter by issue status ID  
- **Priority filtering** - Filter by priority ID
- **Text search** - Search in summary and description fields
- **Result limiting** - Control number of results returned (default: 50)

#### 2. Get Issue Details (`get_issue_details`)
Retrieve comprehensive information for a specific JIRA issue by its key (e.g., 'SMQE-1280'), including:
- Basic issue information (summary, description, status, priority)
- Timestamps (created, updated, due date, resolution date)
- Time tracking (original estimate, current estimate, time spent)
- Metadata (votes, watches, environment, components)
- Associated labels

#### 3. Get Project Summary (`get_project_summary`)
Generate statistics across all projects:
- Total issue counts per project
- Status distribution per project
- Priority distribution per project
- Overall statistics

## Prerequisites

- Python 3.8+
- Podman or Docker
- Access to Snowflake with appropriate credentials

## Environment Variables

The following environment variables are used to configure the Snowflake connection:

### Required
- **`SNOWFLAKE_TOKEN`** - Your Snowflake authentication token (Bearer token)
- **`SNOWFLAKE_BASE_URL`** - Snowflake API base URL (e.g., `https://your-account.snowflakecomputing.com/api/v2`)
- **`SNOWFLAKE_DATABASE`** - Snowflake database name containing your JIRA data
- **`SNOWFLAKE_SCHEMA`** - Snowflake schema name containing your JIRA tables

### Optional
- **`MCP_TRANSPORT`** - Transport protocol for MCP communication  
  - Default: `stdio`


## Installation & Setup

### Local Development

1. Clone the repository:
```bash
git clone <repository-url>
cd jira-mcp-snowflake
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables (see Environment Variables section above)

4. Run the server:
```bash
python mcp_server.py
```

### Container Deployment

## Building locally

To build the container image locally using Podman, run:

```sh
podman build -t jira-mcp-snowflake:latest .
```

This will create a local image named `jira-mcp-snowflake:latest` that you can use to run the server.

## Running with Podman or Docker

Example configuration for running with Podman:

```json
{
  "mcpServers": {
    "jira-mcp-snowflake": {
      "command": "podman",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e", "SNOWFLAKE_TOKEN=your_token_here",
        "-e", "SNOWFLAKE_BASE_URL=https://your-account.snowflakecomputing.com/api/v2",
        "-e", "SNOWFLAKE_DATABASE=your_database_name",
        "-e", "SNOWFLAKE_SCHEMA=your_schema_name",
        "-e", "MCP_TRANSPORT=stdio",
        "localhost/jira-mcp-snowflake:latest"
      ]
    }
  }
}
```

## Connecting to a remote instance

Example configuration for connecting to a remote instance:

```json
{
  "mcpServers": {
    "jira-mcp-snowflake": {
      "url": "https://jira-mcp-snowflake.example.com/sse",
      "headers": {
        "X-Snowflake-Token": "your_token_here"
      }
    }
  }
}
```

### VS Code Continue Integration

Example configuration to add to VS Code Continue:

```json
{
  "experimental": {
    "modelContextProtocolServers": [
      {
        "name": "jira-mcp-snowflake",
        "transport": {
          "type": "stdio",
          "command": "podman",
          "args": [
            "run",
            "-i",
            "--rm",
            "-e", "SNOWFLAKE_TOKEN=your_token_here",
            "-e", "SNOWFLAKE_BASE_URL=https://your-account.snowflakecomputing.com/api/v2",
            "-e", "SNOWFLAKE_DATABASE=your_database_name",
            "-e", "SNOWFLAKE_SCHEMA=your_schema_name",
            "-e", "MCP_TRANSPORT=stdio",
            "localhost/jira-mcp-snowflake:latest"
          ]
        }
      }
    ]
  }
}
```

## Usage Examples

### Query Issues by Project
```python
# List all issues from the SMQE project
result = await list_issues(project="SMQE", limit=10)
```

### Search Issues by Text
```python
# Search for issues containing "authentication" in summary or description
result = await list_issues(search_text="authentication", limit=20)
```

### Get Specific Issue Details
```python
# Get detailed information for a specific issue
result = await get_issue_details(issue_key="SMQE-1280")
```

### Get Project Overview
```python
# Get statistics for all projects
result = await get_project_summary()
```

## Data Privacy

This server is designed to work with non-personally identifiable information (non-PII) data only. The Snowflake tables should contain sanitized data with any sensitive personal information removed.

## Security Considerations

- **Environment Variables**: Store sensitive information like `SNOWFLAKE_TOKEN` in environment variables, never in code
- **Token Security**: Ensure your Snowflake token is kept secure and rotated regularly
- **Network Security**: Use HTTPS endpoints and secure network connections
- **Access Control**: Follow principle of least privilege for Snowflake database access

## Dependencies

- `httpx` - HTTP client library
- `fastmcp` - Fast MCP server framework

