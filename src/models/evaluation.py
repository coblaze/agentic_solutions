"""
Evaluation data models
Defines structures for evaluation inputs, outputs, and results
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum
import uuid


class EvaluationStatus(str, Enum):
    """Evaluation result status"""
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"  # Added for evaluation errors
    
    def __str__(self):
        return self.value


class TranscriptSummaryPair(BaseModel):
    """
    Input data structure for evaluation
    Represents a transcript and its AI-generated summary
    """
    # Identifiers
    interaction_id: str = Field(..., description="Unique interaction ID")
    ivr_call_id: str = Field(..., description="IVR system call ID")
    customer_id: str = Field(..., description="Customer identifier")
    transcript_ref_id: str = Field(..., description="Reference to transcript")
    
    # Agent and call details
    lob: str = Field(..., description="Line of Business")
    agent_id: str = Field(..., description="Agent identifier")
    start_timestamp: datetime = Field(..., description="Call start time")
    
    # Content
    transcript: str = Field(..., description="Full call transcript")
    post_call_summary: str = Field(..., description="AI-generated summary")
    
    # Optional fields
    account_number: Optional[str] = Field(None, description="Customer account number")
    
    @validator('transcript', 'post_call_summary')
    def validate_not_empty(cls, v, field):
        """Ensure transcript and summary are not empty"""
        if not v or not v.strip():
            raise ValueError(f"{field.name} cannot be empty")
        return v.strip()
    
    @validator('start_timestamp', pre=True)
    def parse_timestamp(cls, v):
        """Handle various timestamp formats"""
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except:
                return datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
        return v
    
    class Config:
        """Pydantic configuration"""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "interaction_id": "INT-123456",
                "ivr_call_id": "IVR-789012",
                "customer_id": "CUST-345678",
                "transcript_ref_id": "TRANS-901234",
                "lob": "Sales",
                "agent_id": "AGENT-567890",
                "start_timestamp": "2024-01-15T10:30:00Z",
                "transcript": "Customer: I need help with my account...",
                "post_call_summary": "Customer called regarding account assistance...",
                "account_number": "ACC-123456"
            }
        }


class EvaluationResult(BaseModel):
    """
    Individual evaluation result
    Contains the evaluation outcome and all related metadata
    """
    # Unique identifiers
    evaluation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    batch_id: str = Field(..., description="Batch this evaluation belongs to")
    interaction_id: str = Field(..., description="Original interaction ID")
    
    # Evaluation outcome
    status: EvaluationStatus = Field(..., description="Pass/Fail status")
    reason: str = Field(..., description="Detailed reason for the evaluation result")
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="LLM confidence score")
    
    # Original data (for traceability)
    transcript_ref_id: str
    ivr_call_id: str
    customer_id: str
    lob: str
    agent_id: str
    start_timestamp: datetime
    transcript: str
    post_call_summary: str
    account_number: Optional[str] = None
    
    # Metadata
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)
    evaluation_duration_seconds: Optional[float] = Field(None, description="Time taken to evaluate")
    llm_model_used: Optional[str] = Field(None, description="LLM model version used")
    
    @validator('reason')
    def clean_reason(cls, v):
        """Clean up evaluation reason text"""
        # Remove common prefixes if present
        import re
        cleaned = re.sub(r'^(Pass|Fail|Error):\s*', '', v, flags=re.IGNORECASE)
        return cleaned.strip()
    
    def to_report_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for reporting"""
        return {
            "IVRCallId": self.ivr_call_id,
            "interactionId": self.interaction_id,
            "customerId": self.customer_id,
            "LOB": self.lob,
            "agentId": self.agent_id,
            "startTimestamp": self.start_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "message": self.transcript,
            "transcriptRefId": self.transcript_ref_id,
            "postCallSummary": self.post_call_summary,
            "accountNumber": self.account_number or "",
            "evaluationStatus": self.status.value,
            "evaluationReason": self.reason,
            "confidenceScore": f"{self.confidence_score:.2f}" if self.confidence_score else "N/A",
            "evaluatedAt": self.evaluated_at.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    class Config:
        """Pydantic configuration"""
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            EvaluationStatus: lambda v: v.value
        }
        use_enum_values = True


class BatchEvaluation(BaseModel):
    """
    Batch-level evaluation summary
    Contains aggregated statistics for a batch of evaluations
    """
    # Identifiers
    batch_id: str = Field(default_factory=lambda: f"BATCH-{uuid.uuid4().hex[:8]}")
    evaluation_date: datetime = Field(..., description="Date being evaluated")
    
    # Statistics
    total_evaluations: int = Field(..., ge=0)
    passed: int = Field(..., ge=0)
    failed: int = Field(..., ge=0)
    errors: int = Field(default=0, ge=0, description="Evaluations that errored")
    
    # Percentages
    pass_percentage: float = Field(..., ge=0.0, le=100.0)
    fail_percentage: float = Field(..., ge=0.0, le=100.0)
    error_percentage: float = Field(default=0.0, ge=0.0, le=100.0)
    
    # Overall metrics
    accuracy: float = Field(..., ge=0.0, le=1.0, description="Overall accuracy (0-1)")
    average_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    
    # References
    evaluation_ids: List[str] = Field(default_factory=list)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processing_time_seconds: Optional[float] = Field(None, description="Total processing time")
    
    @validator('pass_percentage', 'fail_percentage', 'error_percentage')
    def validate_percentages(cls, v, values):
        """Ensure percentages are valid"""
        return round(v, 2)
    
    @validator('accuracy')
    def validate_accuracy(cls, v, values):
        """Ensure accuracy is calculated correctly"""
        if 'total_evaluations' in values and values['total_evaluations'] > 0:
            if 'passed' in values:
                calculated = values['passed'] / values['total_evaluations']
                return round(calculated, 4)
        return v
    
    def is_below_threshold(self, threshold: float) -> bool:
        """Check if accuracy is below threshold"""
        return self.accuracy < threshold
    
    def get_summary_text(self) -> str:
        """Get human-readable summary"""
        return (
            f"Batch {self.batch_id} - {self.evaluation_date.strftime('%Y-%m-%d')}\n"
            f"Total: {self.total_evaluations} | "
            f"Passed: {self.passed} ({self.pass_percentage}%) | "
            f"Failed: {self.failed} ({self.fail_percentage}%) | "
            f"Accuracy: {self.accuracy:.2%}"
        )
    
    class Config:
        """Pydantic configuration"""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "batch_id": "BATCH-abc123",
                "evaluation_date": "2024-01-15T00:00:00",
                "total_evaluations": 150,
                "passed": 140,
                "failed": 10,
                "pass_percentage": 93.33,
                "fail_percentage": 6.67,
                "accuracy": 0.9333,
                "evaluation_ids": ["eval-1", "eval-2", "..."],
                "created_at": "2024-01-16T00:15:00"
            }
        }
