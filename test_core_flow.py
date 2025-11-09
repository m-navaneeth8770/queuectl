import subprocess
import time
import os
BASE_CMD = ["python", "queuectl.py"]



def run_cmd(args_list):
  
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
    
    print(f"\n> Stopping worker (PID: {process.pid})...")
    process.terminate() # Send SIGTERM
    try:
        stdout, stderr = process.communicate(timeout=5)
        print("Worker stdout:")
        print(stdout)
        print("Worker stderr:")
        print(stderr)
    except subprocess.TimeoutExpired:
        print("Worker did not terminate, forcing kill.")
        process.kill()
    print("Worker stopped.")



def main_test():
    print("--- 🧪 Starting Core Flow Test ---")

   
    if os.path.exists("queue.db"):
        os.remove("queue.db")
        print("Removed old queue.db")

    run_cmd(["initdb"])

 
    run_cmd(["config", "set", "max-retries", "2"])

 
    print("Enqueuing jobs...")
    run_windows_cmd('enqueue "{\\"id\\": \\"job-pass\\", \\"command\\": \\"echo Test Pass\\"}"')
    run_windows_cmd('enqueue "{\\"id\\": \\"job-fail\\", \\"command\\": \\"notarealcommand\\"}"')

   
    status_output = run_cmd(["status"])
    if "PENDING: 2" not in status_output:
        print("TEST FAILED: Did not find 2 pending jobs.")
        return


    worker = start_worker()

    print("\nWaiting for worker to process jobs (10 seconds)...")
    time.sleep(10) # Give worker time to retry and move to DLQ

    stop_worker(worker)

   
    print("\nChecking final job states...")
    final_status = run_cmd(["status"])

    if "COMPLETED: 1" not in final_status:
        print("TEST FAILED: 'job-pass' did not complete.")
        return

    if "DEAD: 1" not in final_status:
        print("TEST FAILED: 'job-fail' did not move to DLQ.")
        return

    print("\nJob-pass and Job-fail processed correctly.")


    dlq_output = run_cmd(["dlq", "list"])
    if "job-fail" not in dlq_output:
        print("TEST FAILED: 'job-fail' not found in DLQ list.")
        return

    print("\n--- ✅ Test Completed Successfully ---")

if __name__ == "__main__":
    main_test()
