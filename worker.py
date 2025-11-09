import time
import subprocess
import signal
from collections import namedtuple
from db import get_db_connection, log_job_success, record_job_failure
from models import JobState


def run_worker_process(worker_id):
  
    w = Worker(worker_id)
    
   
    def shutdown(sig, frame):
        print(f"Worker {w.id} received signal {sig}, stopping...")
        w.stop()
        
    signal.signal(signal.SIGINT, shutdown)  # Handle Ctrl+C
    signal.signal(signal.SIGTERM, shutdown) # Handle termination
    
    try:
        w.run()
    except Exception as e:
        print(f"Worker {w.id} crashed with error: {e}")
    finally:
        print(f"Worker {w.id} shut down.")


JobResult = namedtuple("JobResult", ["success", "output", "error"])



def execute_job(command: str, timeout: int) -> JobResult:
   
    print(f"Executing command: '{command}' (Timeout: {timeout}s)")
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            check=False,
            timeout=timeout  # <-- Pass timeout to subprocess
        )

        output = result.stdout.strip()
        error = result.stderr.strip()

        if result.returncode == 0:
            print(f"Command success. Output: {output}")
            return JobResult(success=True, output=output, error=error)
        else:
            print(f"Command failed. Error: {error}")
            return JobResult(success=False, output=output, error=error)

    except subprocess.TimeoutExpired:
        error_msg = f"Job timed out after {timeout} seconds."
        print(error_msg)
        return JobResult(success=False, output="", error=error_msg)
   

    except Exception as e:
        error_msg = f"An error occurred during command execution: {e}"
        print(error_msg)
        return JobResult(success=False, output="", error=error_msg)


def find_next_job():
   atetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    sql = """
    UPDATE jobs
    SET state = ?, updated_at = ?, retry_at = NULL
    WHERE id = (
        SELECT id
        FROM jobs
        WHERE 
            -- Is 'pending' and ready to run
            (state = ? AND run_at <= ?) 
            OR 
            -- Is 'failed' and ready to retry
            (state = ? AND retry_at <= ?)

        ORDER BY priority DESC, created_at ASC

        LIMIT 1
    )
    RETURNING id, command, attempts, max_retries,timeout;
    """

    conn = get_db_connection()
    try:
        with conn: # Begin transaction
            cursor = conn.execute(sql, [
                JobState.PROCESSING.value, 
                now, 
                JobState.PENDING.value,
                now,  # For run_at
                JobState.FAILED.value,
                now   # For retry_at
            ])
            job_row = cursor.fetchone()

        if job_row:
            print(f"Worker found job: {job_row['id']} (attempts: {job_row['attempts']})")
            return job_row
        else:
            return None
    except Exception as e:
        print(f"An error occurred while finding a job: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

class Worker:
    def __init__(self, id):
        self.id = id
        self.running = True
        print(f"Worker {self.id} starting...")

    def stop(self):
        
        self.running = False
        print(f"Worker {self.id} stopping...")

    def run(self):
       
        while self.running:
            job = find_next_job()
            
            if job:
                job_id = job['id']
                command = job['command']
                timeout = job['timeout']
                
                print(f"Worker {self.id} processing job: {job_id} ('{command}')")
                
                result = execute_job(command,timeout)
                
                if result.success:
             
                    log_job_success(job_id, result.output)
                    print(f"Job {job_id} marked as completed.")
                else:
                    
                    record_job_failure(job_id, result.error)
                    print(f"Job {job_id} marked as failed.")
            else:
             
                print(f"Worker {self.id} waiting for jobs...")
                time.sleep(5)


if __name__ == "__main__":
    print("Starting a single worker for testing...")
    run_worker_process(1)
