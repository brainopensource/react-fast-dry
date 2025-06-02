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
    TIMEOUT = "timeout"

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
    last_updated_at: float = 0.0

class JobManager:
    def __init__(self, jobs_file: str = "jobs.json", job_timeout_seconds: int = 3600):
        self.jobs_file = jobs_file
        self._lock = asyncio.Lock()
        self.job_timeout_seconds = job_timeout_seconds
        self._load_jobs()
        self._cleanup_stale_jobs()

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

    def _cleanup_stale_jobs(self):
        """Clean up stale jobs that have been running too long"""
        current_time = time.time()
        for job_id, job in list(self.jobs.items()):
            # Check for running jobs that haven't been updated
            if job.status == JobStatus.RUNNING:
                if current_time - job.last_updated_at > self.job_timeout_seconds:
                    logger.warning(f"Job {job_id} timed out after {self.job_timeout_seconds} seconds")
                    job.status = JobStatus.TIMEOUT
                    job.error = "Job timed out due to inactivity"
                    job.completed_at = current_time
                # Check for jobs that have been running too long
                elif job.started_at and current_time - job.started_at > self.job_timeout_seconds:
                    logger.warning(f"Job {job_id} exceeded maximum runtime of {self.job_timeout_seconds} seconds")
                    job.status = JobStatus.TIMEOUT
                    job.error = "Job exceeded maximum runtime"
                    job.completed_at = current_time

        # Save changes after cleanup
        self._save_jobs()

    async def create_job(self) -> Optional[str]:
        """Create a new job if no job is running"""
        async with self._lock:
            # Clean up stale jobs before creating new one
            self._cleanup_stale_jobs()
            
            # Check if any job is running
            running_job = self._get_running_job()
            if running_job:
                return None

            # Create new job
            job_id = f"import_{int(time.time())}"
            current_time = time.time()
            job = Job(
                id=job_id,
                status=JobStatus.PENDING,
                created_at=current_time,
                last_updated_at=current_time
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
            current_time = time.time()
            
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)

            if 'status' in kwargs:
                if kwargs['status'] == JobStatus.RUNNING:
                    job.started_at = current_time
                elif kwargs['status'] in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.TIMEOUT):
                    job.completed_at = current_time

            # Update last_updated_at timestamp
            job.last_updated_at = current_time
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

    def cleanup_old_jobs(self, max_age_days: int = 7):
        """Clean up old completed/failed jobs"""
        current_time = time.time()
        max_age_seconds = max_age_days * 24 * 3600
        
        for job_id, job in list(self.jobs.items()):
            if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.TIMEOUT):
                if current_time - job.completed_at > max_age_seconds:
                    del self.jobs[job_id]
        
        self._save_jobs() 