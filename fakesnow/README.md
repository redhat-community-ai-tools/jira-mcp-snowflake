# Fakesnowflake instance

[Fakesnow repo](https://github.com/tekumara/fakesnow)

1. Build the container image with podman

```bash
podman build -t fakesnow:latest -f Containerfile .
```

2. Run the container with local podman

```bash
# Optional: For data persistence to a host directory:
# Create 'my_fakesnow_data_on_host' directory first on your host if you use this.
# -e FAKESNOW_DB_PATH_CONFIG="/data/db" -v ./my_fakesnow_data_on_host:/data/db \
podman run -d --name local-fakesnow-server -p 8080:8080 fakesnow-server:latest
```
