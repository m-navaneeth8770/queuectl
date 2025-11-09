import sqlite3 
from datetime import datetime, timezone
from models import JobState 


DB_FILE = "queue.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    retry_at TEXT,
    run_at TEXT,
    output TEXT, 
    error TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    timeout INTEGER NOT NULL DEFAULT 60
);


-- This index will speed up finding 'pending' jobs later
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs (state);


CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- *** ADDED THIS NEW TABLE AND STATS ***
CREATE TABLE IF NOT EXISTS metrics (
    stat_key TEXT PRIMARY KEY,
    stat_value INTEGER NOT NULL DEFAULT 0
);

-- Initialize the stats if they don't exist
INSERT INTO metrics (stat_key, stat_value) VALUES ('jobs_completed', 0)
    ON CONFLICT(stat_key) DO NOTHING;
INSERT INTO metrics (stat_key, stat_value) VALUES ('jobs_failed', 0)
    ON CONFLICT(stat_key) DO NOTHING;
"""

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db():
    """Initializes the database and creates the 'jobs' table."""
    try:
        with get_db_connection() as conn:
           
            conn.executescript(SCHEMA)
        print("Database initialized successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred while initializing the database: {e}")


def create_job(job_id: str, command: str, run_at: str = None, priority: int = 0, timeout: int = 60) -> bool:
    """
    Inserts a new job into the database with default values
    and optional 'run_at', 'priority', and 'timeout'.
    """

    from datetime import datetime, timezone
    from models import JobState

    now = datetime.now(timezone.utc).isoformat()

   
    default_retries = int(get_config("max_retries", "3"))

 
    effective_run_at = run_at if run_at else now

    job_data = (
        job_id,
        command,
        JobState.PENDING.value,
        0,                       # attempts
        default_retries,         # max_retries
        now,                     # created_at
        now,                     # updated_at
        effective_run_at,        # run_at
        priority,                # priority
        timeout                  # timeout
    )

    sql = """
    INSERT INTO jobs (
        id, command, state, attempts, max_retries, 
        created_at, updated_at, run_at, priority, timeout
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        with get_db_connection() as conn:
            conn.execute(sql, job_data)
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        print(f"Error: Job with ID '{job_id}' already exists.")
        return False
    except sqlite3.Error as e:
        print(f"An error occurred while creating the job: {e}")
        return False

def update_job_state(job_id: str, state: JobState):
    """Updates a job's state and 'updated_at' timestamp."""
    
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    
    sql = "UPDATE jobs SET state = ?, updated_at = ? WHERE id = ?"
    
    try:
        with get_db_connection() as conn:
            conn.execute(sql, [state.value, now, job_id])
            conn.commit()
    except sqlite3.Error as e:
        print(f"An error occurred while updating job {job_id}: {e}")


def record_job_failure(job_id: str, error_output: str):
    """
    Increments attempt count and logs the error output.
    Moves to 'dead' (DLQ) if max_retries is met.
    Schedules a retry with exponential backoff if not.
    *** Also increments the 'jobs_failed' metric. ***
    """

    from datetime import datetime, timezone, timedelta
    from models import JobState

    BACKOFF_BASE_SECONDS = int(get_config("backoff_base", "2"))

    conn = get_db_connection()
    try:
        with conn: 
            cursor = conn.execute("SELECT attempts, max_retries FROM jobs WHERE id = ?", [job_id])
            job = cursor.fetchone()

            if not job:
                print(f"Error: Could not find job {job_id} to record failure.")
                return

            new_attempts = job['attempts'] + 1
            now = datetime.now(timezone.utc)
            
            
            conn.execute("UPDATE metrics SET stat_value = stat_value + 1 WHERE stat_key = 'jobs_failed'")
           

            if new_attempts >= job['max_retries']:
                new_state = JobState.DEAD.value
                retry_at = None
                print(f"Job {job_id} failed {new_attempts} times, moving to DLQ (dead).")
            else:
                new_state = JobState.FAILED.value
                delay_seconds = BACKOFF_BASE_SECONDS ** new_attempts
                retry_at_time = now + timedelta(seconds=delay_seconds)
                retry_at = retry_at_time.isoformat()
                print(f"Job {job_id} failed, retrying in {delay_seconds} seconds...")

            sql = """
            UPDATE jobs
            SET state = ?, attempts = ?, updated_at = ?, retry_at = ?, error = ?
            WHERE id = ?
            """
            conn.execute(sql, [
                new_state, 
                new_attempts, 
                now.isoformat(), 
                retry_at, 
                error_output,
                job_id
            ])

    except sqlite3.Error as e:
        print(f"An error occurred while recording failure for job {job_id}: {e}")
        conn.rollback()
    finally:
        conn.close()


def get_status_summary():
    """
    Returns a count of jobs in each state.
    """
    sql = "SELECT state, COUNT(*) as count FROM jobs GROUP BY state"
    try:
        with get_db_connection() as conn:
            cursor = conn.execute(sql)
            summary = {row['state']: row['count'] for row in cursor.fetchall()}
            return summary
    except sqlite3.Error as e:
        print(f"An error occurred while getting status summary: {e}")
        return None


def list_jobs_by_state(state: JobState):
    """
    Returns all jobs that are in the specified state.
    (Now includes output and error columns)
    """
    sql = """
    SELECT id, command, state, attempts, created_at, updated_at, output, error 
    FROM jobs 
    WHERE state = ?
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.execute(sql, [state.value])
            jobs = cursor.fetchall() 
            return jobs
    except sqlite3.Error as e:
        print(f"An error occurred while listing jobs: {e}")
        return None

    
def retry_dead_job(job_id: str):
    """
    Resets a 'dead' job's state to 'pending' and clears its attempts.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    
    sql = """
    UPDATE jobs
    SET state = ?, attempts = 0, updated_at = ?, retry_at = NULL
    WHERE id = ? AND state = ?
    """
    
    try:
        with get_db_connection() as conn:
            from models import JobState
            cursor = conn.execute(sql, [
                JobState.PENDING.value,
                now,
                job_id,
                JobState.DEAD.value
            ])
            conn.commit()
            
            if cursor.rowcount > 0:
                return True
            else:
                return False
    except sqlite3.Error as e:
        print(f"An error occurred while retrying job {job_id}: {e}")
        return False


def set_config(key: str, value: str):
    """Sets a configuration key-value pair."""
    sql = "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)"
    try:
        with get_db_connection() as conn:
            conn.execute(sql, [key, value])
            conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred while setting config {key}: {e}")
        return False

def get_config(key: str, default: str = None) -> str:
    """Gets a configuration value by key."""
    sql = "SELECT value FROM config WHERE key = ?"
    try:
        with get_db_connection() as conn:
            cursor = conn.execute(sql, [key])
            row = cursor.fetchone()
            if row:
                return row['value']
            else:
                return default
    except sqlite3.Error as e:
        print(f"An error occurred while getting config {key}: {e}")
        return default

    
def log_job_success(job_id: str, output: str):
    """
    Updates a job's state to 'completed', logs its output,
    and increments the 'jobs_completed' metric.
    """

    from datetime import datetime, timezone
    from models import JobState
    now = datetime.now(timezone.utc).isoformat()

    sql = "UPDATE jobs SET state = ?, output = ?, updated_at = ? WHERE id = ?"

    try:
        with get_db_connection() as conn:
            conn.execute(sql, [JobState.COMPLETED.value, output, now, job_id])
            
            
            conn.execute("UPDATE metrics SET stat_value = stat_value + 1 WHERE stat_key = 'jobs_completed'")
           
            
            conn.commit()
    except sqlite3.Error as e:
        print(f"An error occurred while logging success for job {job_id}: {e}")


def get_metrics():
    """Returns all metrics as a dictionary."""
    sql = "SELECT stat_key, stat_value FROM metrics"
    try:
        with get_db_connection() as conn:
            cursor = conn.execute(sql)
            metrics = {row['stat_key']: row['stat_value'] for row in cursor.fetchall()}
            return metrics
    except sqlite3.Error as e:
        print(f"An error occurred while getting metrics: {e}")
        return None