import enum
from dataclasses import dataclass
from datetime import datetime

# These are the 5 states from the 'Job Lifecycle' image
class JobState(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"

# A dataclass is a clean way to represent the 'Job Specification'
@dataclass
class Job:
    id: str
    command: str
    state: JobState
    attempts: int
    max_retries: int
    created_at: datetime
    updated_at: datetime