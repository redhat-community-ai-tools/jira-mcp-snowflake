# wrapper.py (version to test TypeError fix)
import os
import signal
import time
import threading
import sys

print(f"--- FakeSnow Server Wrapper (using fakesnow.server(), no host kwarg): Starting (Python {sys.version.splitlines()[0]}) ---")
print(f"--- Python Executable: {sys.executable} ---")

try:
    import fakesnow
    print("INFO: Successfully imported 'fakesnow' package.")
    if not hasattr(fakesnow, 'server'):
        print("CRITICAL ERROR: 'fakesnow' module does not have a 'server' attribute/function.", file=sys.stderr)
        sys.exit(1)
    print("INFO: 'fakesnow.server' attribute/function found.")
except ImportError as e:
    print(f"CRITICAL ERROR: Failed to import 'fakesnow' package. Error: {e}", file=sys.stderr)
    sys.exit(1)

def main():
    global server_context_active

    host_to_listen_on = os.getenv("FAKESNOW_HOST", "0.0.0.0")
    port = int(os.getenv("FAKESNOW_PORT", 8080))
    db_path_config = os.getenv("FAKESNOW_DB_PATH_CONFIG", None)

    server_options = {"port": port} # REMOVED "host"

    session_params = {}
    if db_path_config:
        session_params["FAKESNOW_DB_PATH"] = db_path_config
    
    if session_params:
        server_options["session_parameters"] = session_params # fakesnow.server() should handle this
    
    print(f"INFO: Preparing to start fakesnow.server() with options: {server_options}")

    shutdown_event = threading.Event()

    def handle_signal(signum, frame):
        signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') and isinstance(signum, int) else f"Signal {signum}"
        print(f"INFO: Received {signal_name}, setting shutdown event...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        with fakesnow.server(**server_options) as conn_kwargs: # 'host' is NOT passed here
            server_context_active = True
            print(f"INFO: FakeSnow server started via fakesnow.server() context manager.")
            print(f"INFO: Server yielding connection kwargs: {conn_kwargs}")
            # conn_kwargs['host'] will likely be 'localhost'.
            # We need to check if the server is actually accessible from outside on port 8080.
            # If UvicornServer inside fakesnow.server() binds to localhost, it won't be.
            
            print("INFO: Main thread will now sleep. Server runs in a separate thread.")
            while not shutdown_event.is_set():
                time.sleep(1)
            print("INFO: Shutdown event received, exiting 'with fakesnow.server()' block...")

    except Exception as e:
        print(f"CRITICAL ERROR: Exception during fakesnow.server() lifecycle: {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        server_context_active = False
        print("INFO: Exited fakesnow.server() context. Server should be stopped by context manager.")
        print("INFO: FakeSnow server wrapper script is terminating.")

if __name__ == "__main__":
    main()