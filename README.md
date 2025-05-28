# Jira MCP Server

A Model Context Protocol (MCP) server that provides access to JIRA issue data stored in CSV files exported from Snowflake. This server enables AI assistants to query, filter, and analyze JIRA issues through a standardized interface.

## Overview

This MCP server reads JIRA data from CSV files and provides three main tools for interacting with the data:

- **`list_issues`** - Query and filter JIRA issues with various criteria
- **`get_issue_details`** - Get detailed information for a specific issue by key
- **`get_project_summary`** - Get statistics and summaries for all projects

## Features

### Data Sources
The server reads from the following CSV files in the `Snowflake_CSV/` directory:
- `JIRA_ISSUE_NON_PII.csv` - Main issue data (non-personally identifiable information)
- `JIRA_LABEL_RHAI.csv` - Issue labels and tags
- `JIRA_COMPONENT_RHAI.csv` - Component information
- `JIRA_COMMENT_NON_PII.csv` - Issue comments (non-PII)

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
- CSV data files in `Snowflake_CSV/` directory

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

3. Ensure CSV data files are present in `Snowflake_CSV/` directory

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
        "-e", "MCP_TRANSPORT=stdio",
        "localhost/jira-mcp-snowflake:latest"
      ]
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

## Environment Variables

- `MCP_TRANSPORT` - Transport protocol for MCP communication (default: "stdio")

## Data Privacy

This server is designed to work with non-personally identifiable information (non-PII) data only. The CSV files should be sanitized to remove any sensitive personal information before use.

## Dependencies

- `httpx` - HTTP client library
- `fastmcp` - Fast MCP server framework
- `aiofiles` - Asynchronous file operations

