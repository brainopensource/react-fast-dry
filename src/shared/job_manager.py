import json
import os
import time
from enum import Enum
from typing import Dict, Optional
from dataclasses import dataclass, asdict
import asyncio
import logging

logger = logging.getLogger(__name__)

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class Job:
    id: str
    status: JobStatus
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    progress: int = 0
    total_records: Optional[int] = None
    new_records: Optional[int] = None
    duplicate_records: Optional[int] = None

class JobManager:
    def __init__(self, jobs_file: str = "jobs.json"):
        self.jobs_file = jobs_file
        self._lock = asyncio.Lock()
        self._load_jobs()

    def _load_jobs(self):
        try:
            if os.path.exists(self.jobs_file):
                with open(self.jobs_file, 'r') as f:
                    try:
                        data = json.load(f)
                        self.jobs = {k: Job(**{**v, 'status': JobStatus(v['status'])}) 
                                   for k, v in data.items()}
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in {self.jobs_file}, initializing empty jobs")
                        self.jobs = {}
            else:
                self.jobs = {}
        except Exception as e:
            logger.error(f"Error loading jobs file: {str(e)}")
            self.jobs = {}

    def _save_jobs(self):
        with open(self.jobs_file, 'w') as f:
            # Convert Job objects to dict and handle JobStatus enum serialization
            jobs_dict = {}
            for k, v in self.jobs.items():
                job_dict = asdict(v)
                job_dict['status'] = job_dict['status'].value  # Convert enum to string
                jobs_dict[k] = job_dict
            json.dump(jobs_dict, f, indent=2)

    async def create_job(self) -> Optional[str]:
        """Create a new job if no job is running"""
        async with self._lock:
            # Check if any job is running
            running_job = self._get_running_job()
            if running_job:
                return None

            # Create new job
            job_id = f"import_{int(time.time())}"
            job = Job(
                id=job_id,
                status=JobStatus.PENDING,
                created_at=time.time()
            )
            self.jobs[job_id] = job
            self._save_jobs()
            return job_id

    def _get_running_job(self) -> Optional[Job]:
        """Get currently running job if any"""
        for job in self.jobs.values():
            if job.status == JobStatus.RUNNING:
                return job
        return None

    async def update_job(self, job_id: str, **kwargs):
        """Update job status and progress"""
        async with self._lock:
            if job_id not in self.jobs:
                return

            job = self.jobs[job_id]
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)

            if 'status' in kwargs:
                if kwargs['status'] == JobStatus.RUNNING:
                    job.started_at = time.time()
                elif kwargs['status'] in (JobStatus.COMPLETED, JobStatus.FAILED):
                    job.completed_at = time.time()

            self._save_jobs()

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID"""
        return self.jobs.get(job_id)

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get job status and progress"""
        job = self.get_job(job_id)
        if not job:
            return None
        return asdict(job) 