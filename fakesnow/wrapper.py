# src/wrapper.py
import os
import signal
import time
import threading
import sys

print(f"--- FakeSnow Server Wrapper (using fakesnow.server(), no host kwarg): Starting (Python {sys.version.splitlines()[0]}) ---")
print(f"--- Python Executable: {sys.executable} ---")

try:
    # Attempt to import the 'fakesnow' package containing our application's server logic.
    import fakesnow
    print("INFO: Successfully imported 'fakesnow' package.")

    # Check if the 'server' function (our application entry point) is available within 'fakesnow'.
    if not hasattr(fakesnow, 'server'):
        print("CRITICAL ERROR: 'fakesnow' module does not contain a 'server' attribute/function.", file=sys.stderr)
        sys.exit(1)
    print("INFO: 'fakesnow.server' attribute/function found.")
except ImportError as e:
    # In case 'fakesnow' cannot be imported, terminate with an error message.
    print(f"CRITICAL ERROR: Failed to import 'fakesnow' package. Error: {e}", file=sys.stderr)
    sys.exit(1)

def main():
    global server_context_active

    # Retrieve and prepare application configuration from environment variables.
    port = int(os.getenv("FAKESNOW_PORT", 8080))
    db_path_config = os.getenv("FAKESNOW_DB_PATH_CONFIG", None)
    server_options = {"port": port}
    session_params = {}

    # If a database path is provided through the environment variable, set it within 'session_params'.
    if db_path_config:
        session_params["FAKESNOW_DB_PATH"] = db_path_config
    
    # Merge 'session_params' into 'server_options' if it's not empty (optional parameters).
    if session_params:
        server_options["session_parameters"] = session_params
    
    # Print the configured options for the server.
    print(f"INFO: Preparing to start fakesnow.server() with options: {server_options}")

    # Set up a Flag to signify the server's context is active.
    shutdown_event = threading.Event()

    def handle_signal(signum, frame):
        # Print a signal received message which should help in monitoring the script's execution.
        signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') and isinstance(signum, int) else f"Signal {signum}"
        print(f"INFO: Received {signal_name}, setting shutdown event...")
        shutdown_event.set()

    # Register signal handlers for graceful shutdown using SIGINT and SIGTERM.
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        # Start the 'fakesnow.server' using the configured options.
        with fakesnow.server(**server_options) as conn_kwargs:
            server_context_active = True
            print(f"INFO: FakeSnow server started via fakesnow.server() context manager.")
            print(f"INFO: Server yielding connection kwargs: {conn_kwargs}")
            
            # The server runs independently in a separate thread.
            print("INFO: Main thread will now sleep. Server runs in a separate thread.")
            while not shutdown_event.is_set():
                time.sleep(1)
            print("INFO: Shutdown event received, exiting 'with fakesnow.server()' block...")
    except Exception as e:
        # In case of any exception during server lifecycle, log the error and exit.
        print(f"CRITICAL ERROR: Exception during fakesnow.server() lifecycle: {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        # Ensure the server's context flag is set to False after completion or exit.
        server_context_active = False
        print("INFO: Exited fakesnow.server() context. Server should be stopped by context manager.")
        print("INFO: FakeSnow server wrapper script is terminating.")

if __name__ == "__main__":
    # Trigger the main function when this script is run directly.
    main()