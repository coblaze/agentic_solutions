"""
Batch state tracking models
Manages the execution state of evaluation batches
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum
import json


class BatchStatus(str, Enum):
    """Batch execution status"""
    PENDING = "pending"          # Not yet processed
    RUNNING = "running"          # Currently being processed
    COMPLETED = "completed"      # Successfully completed
    FAILED = "failed"           # Failed with errors
    PARTIAL = "partial"         # Partially completed
    SKIPPED = "skipped"         # Skipped (no data)
    CANCELLED = "cancelled"     # Cancelled by system
    
    def is_terminal(self) -> bool:
        """Check if this is a terminal state"""
        return self in [self.COMPLETED, self.SKIPPED, self.CANCELLED]
    
    def can_retry(self) -> bool:
        """Check if batch can be retried"""
        return self in [self.FAILED, self.PARTIAL, self.PENDING]


class BatchRecoveryStrategy(str, Enum):
    """Recovery strategies for missed batches"""
    SKIP = "skip"                    # Skip missed batches
    RECOVER_ALL = "recover_all"      # Process all missed batches
    RECOVER_LAST = "recover_last"    # Only process most recent missed
    ALERT_ONLY = "alert_only"        # Just alert, don't process
    
    def should_recover(self) -> bool:
        """Check if this strategy involves recovery"""
        return self in [self.RECOVER_ALL, self.RECOVER_LAST]


class BatchState(BaseModel):
    """
    Complete state tracking for a batch execution
    Enables recovery and monitoring
    """
    # Core identifiers
    batch_date: datetime = Field(..., description="Date this batch is for (normalized)")
    batch_id: Optional[str] = Field(None, description="Unique batch ID once processing starts")
    
    # Status tracking
    status: BatchStatus = Field(default=BatchStatus.PENDING)
    previous_status: Optional[BatchStatus] = Field(None, description="Previous status before update")
    
    # Timing information
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(None, description="When processing started")
    completed_at: Optional[datetime] = Field(None, description="When processing completed")
    failed_at: Optional[datetime] = Field(None, description="When processing failed")
    
    # Retry tracking
    retry_count: int = Field(default=0, ge=0)
    max_retries: int = Field(default=3, ge=0)
    last_retry_at: Optional[datetime] = Field(None)
    retry_after: Optional[datetime] = Field(None, description="Don't retry before this time")
    
    # Error tracking
    error_message: Optional[str] = Field(None, max_length=5000)
    error_type: Optional[str] = Field(None, description="Type of error encountered")
    error_details: Dict[str, Any] = Field(default_factory=dict)
    
    # Progress tracking
    total_pairs: int = Field(default=0, ge=0)
    processed_pairs: int = Field(default=0, ge=0)
    successful_evaluations: int = Field(default=0, ge=0)
    failed_evaluations: int = Field(default=0, ge=0)
    error_evaluations: int = Field(default=0, ge=0)
    
    # Results
    passed: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    accuracy: Optional[float] = Field(None, ge=0.0, le=1.0)
    
    # Output tracking
    report_path: Optional[str] = Field(None, description="Path to generated report")
    report_generated: bool = Field(default=False)
    email_sent: bool = Field(default=False)
    email_recipients: List[str] = Field(default_factory=list)
    
    # Recovery tracking
    is_recovery: bool = Field(default=False, description="Is this a recovery run")
    recovery_triggered_by: Optional[str] = Field(None, description="What triggered recovery")
    recovery_attempted: bool = Field(default=False)
    recovery_count: int = Field(default=0, description="Number of recovery attempts")
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    tags: List[str] = Field(default_factory=list, description="Tags for filtering/grouping")
    
    @validator('batch_date', pre=True)
    def normalize_batch_date(cls, v):
        """Ensure batch_date is normalized to start of day"""
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace('Z', '+00:00'))
        if isinstance(v, datetime):
            return v.replace(hour=0, minute=0, second=0, microsecond=0)
        return v
    
    @validator('updated_at', pre=True, always=True)
    def set_updated_at(cls, v):
        """Always update the updated_at timestamp"""
        return datetime.utcnow()
    
    def can_retry(self) -> bool:
        """Check if this batch can be retried"""
        if self.retry_count >= self.max_retries:
            return False
        if self.retry_after and datetime.utcnow() < self.retry_after:
            return False
        return self.status.can_retry()
    
    def increment_retry(self):
        """Increment retry count and update timestamps"""
        self.retry_count += 1
        self.last_retry_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def set_error(self, error: Exception, error_type: Optional[str] = None):
        """Set error information from exception"""
        self.error_message = str(error)[:5000]  # Limit length
        self.error_type = error_type or type(error).__name__
        self.failed_at = datetime.utcnow()
        self.status = BatchStatus.FAILED
        
        # Store additional error details
        self.error_details = {
            "error_class": type(error).__name__,
            "error_module": type(error).__module__,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    def get_progress_percentage(self) -> float:
        """Get processing progress as percentage"""
        if self.total_pairs == 0:
            return 0.0
        return round((self.processed_pairs / self.total_pairs) * 100, 2)
    
    def get_duration(self) -> Optional[float]:
        """Get processing duration in seconds"""
        if self.started_at and (self.completed_at or self.failed_at):
            end_time = self.completed_at or self.failed_at
            return (end_time - self.started_at).total_seconds()
        return None
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Get summary dictionary for logging/reporting"""
        return {
            "batch_date": self.batch_date.strftime("%Y-%m-%d"),
            "batch_id": self.batch_id,
            "status": self.status.value,
            "progress": f"{self.processed_pairs}/{self.total_pairs}",
            "progress_percentage": f"{self.get_progress_percentage()}%",
            "accuracy": f"{self.accuracy:.2%}" if self.accuracy else "N/A",
            "retry_count": f"{self.retry_count}/{self.max_retries}",
            "duration_seconds": self.get_duration(),
            "is_recovery": self.is_recovery
        }
    
    class Config:
        """Pydantic configuration"""
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            BatchStatus: lambda v: v.value
        }
        use_enum_values = True
        schema_extra = {
            "example": {
                "batch_date": "2024-01-15T00:00:00",
                "status": "completed",
                "total_pairs": 150,
                "processed_pairs": 150,
                "passed": 140,
                "failed": 10,
                "accuracy": 0.9333,
                "retry_count": 0
            }
        }
