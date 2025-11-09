import click
import json
import multiprocessing  # <-- 1. ADD THIS
import time             # <-- 2. ADD THIS
import signal           # <-- 3. ADD THIS
from datetime import datetime
from db import initialize_db, create_job, get_status_summary, list_jobs_by_state, retry_dead_job, set_config, get_metrics
from worker import run_worker_process
from models import JobState # <-- Also import JobState
from dashboard import run_dashboard

@click.group()
def cli():
    """
    queuectl - A CLI for managing the background job queue.
    """
    # This function acts as the main entry point for the CLI group
    pass

@cli.command()
def initdb():
    """
    Initializes the job queue database.
    """
    # This command calls the function we defined in db.py
    initialize_db()
# REPLACE this function in queuectl.py
@cli.command()
@click.argument('job_payload', type=str)
def enqueue(job_payload):
    """
    Adds a new job to the queue.

    JOB_PAYLOAD: A JSON string with 'id', 'command',
    and optional 'run_at', 'priority', and 'timeout' (int seconds).

    e.g., '{"id": "job1", "command": "sleep 10", "timeout": 5}'
    """
    try:
        data = json.loads(job_payload)
        job_id = data.get('id')
        command = data.get('command')
        run_at = data.get('run_at')
        priority = data.get('priority', 0)
        # --- ADDED THIS LINE ---
        timeout = data.get('timeout', 60) # Default to 60 seconds

        if not job_id or not command:
            click.echo("Error: JSON payload must include 'id' and 'command'.")
            return

        if run_at:
            try:
                datetime.fromisoformat(run_at.replace('Z', '+00:00'))
                click.echo(f"Job '{job_id}' will be scheduled for {run_at}")
            except ValueError:
                click.echo("Error: Invalid 'run_at' format. Must be ISO 8601.")
                return

        try:
            priority = int(priority)
        except ValueError:
            click.echo("Error: 'priority' must be an integer.")
            return

        # --- ADDED THIS TRY/EXCEPT ---
        try:
            timeout = int(timeout)
            if timeout <= 0:
                click.echo("Error: 'timeout' must be a positive integer.")
                return
        except ValueError:
            click.echo("Error: 'timeout' must be an integer.")
            return

        # --- PASS 'timeout' ---
        if create_job(job_id, command, run_at, priority, timeout):
            click.echo(f"Job '{job_id}' enqueued successfully (Priority: {priority}, Timeout: {timeout}s).")

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON payload.")
    except Exception as e:
        click.echo(f"An unexpected error occurred: {e}")

@click.group()
def worker():
    """Manages worker processes."""
    pass

# ... (after the @click.group() def worker():) ...

# REPLACE your old 'start' function with this:
@worker.command()
@click.option('--count', default=1, help='Number of workers to start.')
def start(count):
    """Starts one or more worker processes."""

    if count <= 0:
        click.echo("Error: --count must be 1 or greater.")
        return

    click.echo(f"Starting {count} worker process(es)...")
    click.echo("Press CTRL+C to stop all workers.")

    processes = []
    for i in range(count):
        worker_id = i + 1
        # Create a new process
        p = multiprocessing.Process(
            target=run_worker_process, 
            args=(worker_id,)
        )
        p.start()
        processes.append(p)

    # --- Graceful shutdown for the main process ---
    def shutdown_main(sig, frame):
        click.echo("\nMain process received signal, terminating all workers...")
        for p in processes:
            p.terminate() # Send SIGTERM to all child processes
        click.echo("Waiting for workers to shut down...")

    signal.signal(signal.SIGINT, shutdown_main)
    signal.signal(signal.SIGTERM, shutdown_main)

    # Wait for all processes to finish
    try:
        for p in processes:
            p.join() # Wait for this process to exit
    except KeyboardInterrupt:
        # This is handled by the signal handler, but we keep it
        # just in case to avoid a messy traceback.
        pass

    click.echo("All workers have shut down.")

# ... (rest of the file remains the same) ...

# 3. Add the new 'worker' group to the main 'cli'
cli.add_command(worker)
@cli.command()
def status():
    """
    Show summary of all job states & execution stats.
    """
    click.echo(" Job Status Summary:")
    summary = get_status_summary()

    if not summary:
        click.echo("  No jobs found.")
    else:
        all_states = [s.value for s in JobState]
        for state in all_states:
            count = summary.get(state, 0)
            click.echo(f"  - {state.upper()}: {count}")

    click.echo("\n Execution Metrics:")
    metrics = get_metrics()

    if not metrics:
        click.echo("  No metrics found.")
    else:
        click.echo(f"  - Total Jobs Completed: {metrics.get('jobs_completed', 0)}")
        click.echo(f"  - Total Jobs Failed (1+ attempts): {metrics.get('jobs_failed', 0)}")

@cli.command(name="list") # Use 'name=' to avoid conflict with Python 'list'
@click.option('--state', 
              type=click.Choice([s.value for s in JobState], case_sensitive=False), 
              required=True, 
              help='List jobs by their state.')
def list_cmd(state):
    """
    List jobs by state.
    """
    # Convert the string 'state' back into our JobState Enum
    state_enum = JobState(state.lower())
    
    jobs = list_jobs_by_state(state_enum)
    
    click.echo(f"Jobs in '{state.upper()}' state:")
    
    if not jobs:
        click.echo("  No jobs found in this state.")
        return
        
    for job in jobs:
        # 'job' is a sqlite3.Row object, we can access by key
        click.echo(f"  - ID: {job['id']}")
        click.echo(f"    Command: {job['command']}")
        click.echo(f"    Attempts: {job['attempts']}")
        click.echo(f"    Updated: {job['updated_at']}")

        if job['output']:
            click.echo(f"    Output: {job['output']}")
        if job['error']:
            click.echo(f"    Error: {job['error']}")
        click.echo("-" * 20)

@click.group()
def dlq():
    """View or retry jobs in the Dead Letter Queue (DLQ)."""
    pass

@dlq.command(name="list")
def dlq_list():
    """Lists all jobs in the DLQ (state = 'dead')."""
    jobs = list_jobs_by_state(JobState.DEAD)
    
    click.echo("Jobs in Dead Letter Queue (DLQ):")
    if not jobs:
        click.echo("  DLQ is empty.")
        return
        
    for job in jobs:
        click.echo(f"  - ID: {job['id']}")
        click.echo(f"    Command: {job['command']}")
        click.echo(f"    Attempts: {job['attempts']}")
        click.echo(f"    Updated: {job['updated_at']}")
        click.echo("-" * 20)

@dlq.command(name="retry")
@click.argument('job_id')
def dlq_retry(job_id):
    """Retries a specific job from the DLQ by ID."""
    if retry_dead_job(job_id):
        click.echo(f"Job '{job_id}' moved from DLQ back to 'pending'.")
    else:
        click.echo(f"Error: Could not retry job '{job_id}'. (Is it in the DLQ?)")

# 3. Add the new 'dlq' group to the main 'cli'
cli.add_command(dlq)

# ... (just before if __name__ == "__main__":)

# --- ADD ALL THE CODE BELOW ---
@click.group()
def config():
    """Manage configuration (retry, backoff, etc.)."""
    pass

# REPLACE the logic inside the config_set function

@config.command(name="set")
@click.argument('key')
@click.argument('value')
def config_set(key, value):
    """
    Sets a configuration value.

    Example: queuectl config set max-retries 5
    Example: queuectl config set backoff-base 3
    """

    # --- NEW LOGIC ---
    valid_keys = {
        "max-retries": "max_retries",
        "backoff-base": "backoff_base"
    }

    if key not in valid_keys:
        click.echo(f"Error: Unknown config key '{key}'. Valid keys are: {list(valid_keys.keys())}")
        return

    # Check that value is a positive integer
    try:
        int_value = int(value)
        if int_value <= 0:
            click.echo(f"Error: {key} must be a positive integer.")
            return
    except ValueError:
        click.echo(f"Error: {key} must be an integer.")
        return

    # Save to DB
    db_key = valid_keys[key]
    if set_config(db_key, str(int_value)):
        click.echo(f"Config updated: {key} = {int_value}")
    else:
        click.echo("Error: Failed to update config.")
    # --- END OF NEW LOGIC ---

# Add the new 'config' group to the main 'cli'
cli.add_command(config)

# ... (just before if __name__ == "__main__":)

# --- ADD THIS NEW COMMAND ---
@cli.command()
def dashboard():
    """
    Runs a minimal web dashboard to monitor the queue.
    """
    try:
        run_dashboard()
    except ImportError:
        click.echo("Error: Flask is not installed.")
        click.echo("Please run 'pip install flask' to use the dashboard.")
# --- END OF NEW COMMAND ---

if __name__ == "__main__":
    cli()