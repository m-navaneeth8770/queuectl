import click
import json
import multiprocessing  
import time            
import signal           
from datetime import datetime
from db import initialize_db, create_job, get_status_summary, list_jobs_by_state, retry_dead_job, set_config, get_metrics
from worker import run_worker_process
from models import JobState 
from dashboard import run_dashboard

@click.group()
def cli():
    pass

@cli.command()
def initdb():
   
   
    initialize_db()

@cli.command()
@click.argument('job_payload', type=str)
def enqueue(job_payload):
 
    try:
        data = json.loads(job_payload)
        job_id = data.get('id')
        command = data.get('command')
        run_at = data.get('run_at')
        priority = data.get('priority', 0)
       
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

        
        try:
            timeout = int(timeout)
            if timeout <= 0:
                click.echo("Error: 'timeout' must be a positive integer.")
                return
        except ValueError:
            click.echo("Error: 'timeout' must be an integer.")
            return

       
        if create_job(job_id, command, run_at, priority, timeout):
            click.echo(f"Job '{job_id}' enqueued successfully (Priority: {priority}, Timeout: {timeout}s).")

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON payload.")
    except Exception as e:
        click.echo(f"An unexpected error occurred: {e}")

@click.group()
def worker():
    
    pass



@worker.command()
@click.option('--count', default=1, help='Number of workers to start.')
def start(count):
    

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

   
    def shutdown_main(sig, frame):
        click.echo("\nMain process received signal, terminating all workers...")
        for p in processes:
            p.terminate() 
        click.echo("Waiting for workers to shut down...")

    signal.signal(signal.SIGINT, shutdown_main)
    signal.signal(signal.SIGTERM, shutdown_main)

    
    try:
        for p in processes:
            p.join() # Wait for this process to exit
    except KeyboardInterrupt:
    
        pass

    click.echo("All workers have shut down.")


cli.add_command(worker)
@cli.command()
def status():
   
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
     
  

    state_enum = JobState(state.lower())
    
    jobs = list_jobs_by_state(state_enum)
    
    click.echo(f"Jobs in '{state.upper()}' state:")
    
    if not jobs:
        click.echo("  No jobs found in this state.")
        return
        
    for job in jobs:
    
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
    
    pass

@dlq.command(name="list")
def dlq_list():

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
    
    if retry_dead_job(job_id):
        click.echo(f"Job '{job_id}' moved from DLQ back to 'pending'.")
    else:
        click.echo(f"Error: Could not retry job '{job_id}'. (Is it in the DLQ?)")

cli.add_command(dlq)



@click.group()
def config():
    """Manage configuration (retry, backoff, etc.)."""
    pass



@config.command(name="set")
@click.argument('key')
@click.argument('value')
def config_set(key, value):

    valid_keys = {
        "max-retries": "max_retries",
        "backoff-base": "backoff_base"
    }

    if key not in valid_keys:
        click.echo(f"Error: Unknown config key '{key}'. Valid keys are: {list(valid_keys.keys())}")
        return


    try:
        int_value = int(value)
        if int_value <= 0:
            click.echo(f"Error: {key} must be a positive integer.")
            return
    except ValueError:
        click.echo(f"Error: {key} must be an integer.")
        return

    db_key = valid_keys[key]
    if set_config(db_key, str(int_value)):
        click.echo(f"Config updated: {key} = {int_value}")
    else:
        click.echo("Error: Failed to update config.")

cli.add_command(config)




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


if __name__ == "__main__":
    cli()
