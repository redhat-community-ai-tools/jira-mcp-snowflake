# wrapper.py
import os
import signal
import time
import threading
import sys

print(f"--- FakeSnow Server Wrapper (for v0.9.38): Starting (Python {sys.version.splitlines()[0]}) ---")
print(f"--- Python Executable: {sys.executable} ---")

try:
    from fakesnow.patch import UvicornServer
    from fakesnow.server import app as fastapi_app
    from fakesnow import instance as fakesnow_instance_module
    from fakesnow.patch import sessions # Global sessions dictionary
    print("INFO: Successfully imported UvicornServer, fakesnow.server.app, fakesnow.instance, and fakesnow.patch.sessions.")
except ImportError as e:
    print(f"CRITICAL ERROR: Failed to import necessary components from fakesnow. Error: {e}", file=sys.stderr)
    print(f"CRITICAL ERROR: Ensure fakesnow and its server dependencies (fastapi, uvicorn, starlette) are correctly installed.", file=sys.stderr)
    sys.exit(1)

def main():
    host = os.getenv("FAKESNOW_HOST", "0.0.0.0")
    port = int(os.getenv("FAKESNOW_PORT", 8080))
    db_path_config = os.getenv("FAKESNOW_DB_PATH_CONFIG", None)

    fakesnow_db_path = None
    if db_path_config and db_path_config != ":isolated:":
        fakesnow_db_path = db_path_config
        print(f"INFO: Configuring FakeSnow instance with database path: {fakesnow_db_path}")
    # ... (rest of db_path_config handling as in the previously successful wrapper) ...
    else:
        print("INFO: Configuring FakeSnow instance with default in-memory database.")

    try:
        fs_instance = fakesnow_instance_module.FakeSnow(db_path=fakesnow_db_path)
        fastapi_app.state.fakesnow_instance = fs_instance
        sessions[0] = fs_instance # Mimic storing the default session
        print(f"INFO: FakeSnow instance created. State prepared for FastAPI app.")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to create or configure FakeSnow instance: {e}", file=sys.stderr)
        sys.exit(1)

    server_run_kwargs = {"host": host, "port": port, "app_module": "fakesnow.server:app"}
    print(f"INFO: Attempting to start UvicornServer with arguments: {server_run_kwargs}")

    try:
        server_instance = UvicornServer(host=host, port=port, app_module="fakesnow.server:app")
        server_instance.start()
    except Exception as e:
        print(f"CRITICAL ERROR: Error during UvicornServer instantiation or start: {e}", file=sys.stderr)
        if hasattr(fastapi_app.state, "fakesnow_instance"): del fastapi_app.state.fakesnow_instance
        if 0 in sessions: del sessions[0]
        sys.exit(1)

    print(f"INFO: UvicornServer for FakeSnow started. Listening on {host}:{port}.")
    shutdown_event = threading.Event()

    def handle_signal(signum, frame):
        signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') and isinstance(signum, int) else f"Signal {signum}"
        print(f"INFO: Received {signal_name}, initiating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print("INFO: Main thread active. Send SIGINT (Ctrl+C) or SIGTERM to stop.")
    try:
        while not shutdown_event.is_set(): time.sleep(1)
    except KeyboardInterrupt: print("INFO: KeyboardInterrupt received.")
    finally:
        print("INFO: Stopping UvicornServer...")
        if 'server_instance' in locals() and hasattr(server_instance, 'stop'): server_instance.stop()
        if hasattr(fastapi_app.state, "fakesnow_instance"): del fastapi_app.state.fakesnow_instance
        if 0 in sessions: del sessions[0]
        print("INFO: FakeSnow server resources released.")

if __name__ == "__main__":
    main()