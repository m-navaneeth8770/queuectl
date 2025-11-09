import subprocess
import time
import os

# Define the base command
# This assumes 'python' is in your path and 'queuectl.py' is in the same dir
BASE_CMD = ["python", "queuectl.py"]

# --- Helper Functions ---

def run_cmd(args_list):
    """Helper to run a queuectl command and return its output."""
    try:
        cmd = BASE_CMD + args_list
        print(f"\n> {' '.join(cmd)}")
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            check=True, 
            timeout=10
        )
        print(result.stdout.strip())
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return None
    except subprocess.TimeoutExpired:
        print("Error: Command timed out.")
        return None

def run_windows_cmd(cmd_str):
    """
    Special helper for Windows to handle the tricky JSON quotes.
    We run this in 'cmd.exe /C ...'
    """
    try:
        full_cmd = f"cmd.exe /C python queuectl.py {cmd_str}"
        print(f"\n> {full_cmd}")
        result = subprocess.run(
            full_cmd, 
            capture_output=True, 
            text=True, 
            check=True, 
            timeout=10
        )
        print(result.stdout.strip())
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return None

def start_worker():
    """Starts a worker in a new background process."""
    print("\n> Starting worker in background...")
    # Use Popen to start a non-blocking background process
    worker_process = subprocess.Popen(
        BASE_CMD + ["worker", "start"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    print(f"Worker started with PID: {worker_process.pid}")
    return worker_process

def stop_worker(process):
    """Stops the background worker process."""
    print(f"\n> Stopping worker (PID: {process.pid})...")
    process.terminate() # Send SIGTERM
    try:
        # Wait for the process to terminate
        stdout, stderr = process.communicate(timeout=5)
        print("Worker stdout:")
        print(stdout)
        print("Worker stderr:")
        print(stderr)
    except subprocess.TimeoutExpired:
        print("Worker did not terminate, forcing kill.")
        process.kill()
    print("Worker stopped.")

# --- Main Test Flow ---

def main_test():
    print("--- 🧪 Starting Core Flow Test ---")

    # 1. Clean up and Initialize
    if os.path.exists("queue.db"):
        os.remove("queue.db")
        print("Removed old queue.db")

    run_cmd(["initdb"])

    # 2. Set Config
    run_cmd(["config", "set", "max-retries", "2"])

    # 3. Enqueue Jobs
    # Use the special Windows helper
    print("Enqueuing jobs...")
    run_windows_cmd('enqueue "{\\"id\\": \\"job-pass\\", \\"command\\": \\"echo Test Pass\\"}"')
    run_windows_cmd('enqueue "{\\"id\\": \\"job-fail\\", \\"command\\": \\"notarealcommand\\"}"')

    # 4. Check Status (should be 2 pending)
    status_output = run_cmd(["status"])
    if "PENDING: 2" not in status_output:
        print("TEST FAILED: Did not find 2 pending jobs.")
        return

    # 5. Run Worker
    worker = start_worker()

    print("\nWaiting for worker to process jobs (10 seconds)...")
    time.sleep(10) # Give worker time to retry and move to DLQ

    stop_worker(worker)

    # 6. Check Final Status
    print("\nChecking final job states...")
    final_status = run_cmd(["status"])

    if "COMPLETED: 1" not in final_status:
        print("TEST FAILED: 'job-pass' did not complete.")
        return

    if "DEAD: 1" not in final_status:
        print("TEST FAILED: 'job-fail' did not move to DLQ.")
        return

    print("\nJob-pass and Job-fail processed correctly.")

    # 7. Check DLQ
    dlq_output = run_cmd(["dlq", "list"])
    if "job-fail" not in dlq_output:
        print("TEST FAILED: 'job-fail' not found in DLQ list.")
        return

    print("\n--- ✅ Test Completed Successfully ---")

if __name__ == "__main__":
    main_test()