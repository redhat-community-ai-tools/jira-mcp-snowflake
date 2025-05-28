# mcp-server template

MCP (ModelContextProvider) server template

---

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
        "-e", "API_BASE_URL",
        "-e", "API_KEY",
        "-e", "MCP_TRANSPORT",
        "localhost/jira-mcp-snowflake:latest"
      ]
    }
  }
}
```

example to add to VSCODE continue
```json
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
            "-e", "API_BASE_URL",
            "-e", "API_KEY",
            "-e", "MCP_TRANSPORT",
            "localhost/jira-mcp-snowflake:latest"
          ]
        }
      }
    ]
  }
```
