# Fakesnowflake instance

[Fakesnow repo](https://github.com/tekumara/fakesnow)

## Run localy

```bash
pip3 install -r requirements.txt #or 
pip install -r requirements.txt
```

```bash
python3 wrapper.py #or 
python warpper.py
```

## Container

> There is a known bug to run the pod with 0.0.0.0 as the bind address. [Github Issue](https://github.com/tekumara/fakesnow/issues/201)

1. Build the container image with podman

```bash
podman build --no-cache -t fakesnow-server:latest . 
```

2. Run the container with local podman

```bash
# Optional: For data persistence to a host directory:
# Create 'my_fakesnow_data_on_host' directory first on your host if you use this.
# -e FAKESNOW_DB_PATH_CONFIG="/data/db" -v ./my_fakesnow_data_on_host:/data/db \
podman run -d --name local-fakesnow-server -p 8080:8080 fakesnow-server:latest
```

Access via http://localhost:8080