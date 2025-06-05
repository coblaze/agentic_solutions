"""
Helper utility functions
Common functions used across the pipeline
"""
import re
import uuid
from typing import Tuple, Optional, Any, List, Dict
from datetime import datetime, timedelta
import logging
from src.models.evaluation import EvaluationStatus

logger = logging.getLogger(__name__)


def parse_evaluation_response(score: float, 
                            reason: str, 
                            threshold: float) -> Tuple[EvaluationStatus, str]:
    """
    Parse evaluation response to extract status and reason
    
    Args:
        score: Evaluation score (0-1)
        reason: Raw evaluation reason from LLM
        threshold: Accuracy threshold
        
    Returns:
        Tuple of (status, cleaned_reason)
    """
    # Determine status based on score
    status = EvaluationStatus.PASS if score >= threshold else EvaluationStatus.FAIL
    
    # Clean up reason - remove Pass:/Fail: prefix if present
    cleaned_reason = reason
    if reason:
        # Remove common prefixes
        cleaned_reason = re.sub(r'^(Pass|Fail|Error):\s*', '', reason, flags=re.IGNORECASE)
        cleaned_reason = cleaned_reason.strip()
        
        # Remove extra whitespace
        cleaned_reason = ' '.join(cleaned_reason.split())
        
        # Ensure reason is not empty
        if not cleaned_reason:
            cleaned_reason = f"Evaluation score: {score:.2f} ({'above' if status == EvaluationStatus.PASS else 'below'} threshold {threshold:.2f})"
            
    else:
        # Provide default reason if none given
        cleaned_reason = f"Evaluation completed with score {score:.2f}"
        
    return status, cleaned_reason


def format_timestamp(timestamp: Any, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
    """
    Format timestamp for display
    
    Args:
        timestamp: Timestamp to format (datetime, string, or None)
        format_str: Desired output format
        
    Returns:
        Formatted timestamp string
    """
    if timestamp is None:
        return "N/A"
        
    if isinstance(timestamp, str):
        try:
            # Try to parse ISO format
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.strftime(format_str)
        except:
            return timestamp
            
    if hasattr(timestamp, 'strftime'):
        return timestamp.strftime(format_str)
        
    return str(timestamp)


def calculate_duration(start_time: Optional[datetime], 
                      end_time: Optional[datetime] = None) -> Optional[float]:
    """
    Calculate duration between two timestamps in seconds
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp (defaults to current time)
        
    Returns:
        Duration in seconds or None if start_time is None
    """
    if start_time is None:
        return None
        
    if end_time is None:
        end_time = datetime.utcnow()
        
    # Handle string timestamps
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
    if isinstance(end_time, str):
        end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
    duration = (end_time - start_time).total_seconds()
    return max(0, duration)  # Ensure non-negative


def sanitize_text(text: str, max_length: Optional[int] = None) -> str:
    """
    Sanitize text for safe storage and display
    
    Args:
        text: Text to sanitize
        max_length: Maximum allowed length
        
    Returns:
        Sanitized text
    """
    if not text:
        return ""
        
    # Remove control characters except newlines and tabs
    sanitized = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F-\x9F]', '', text)
    
    # Normalize whitespace
    sanitized = re.sub(r'[ \t]+', ' ', sanitized)  # Multiple spaces/tabs to single space
    sanitized = re.sub(r'\n\s*\n\s*\n', '\n\n', sanitized)  # Max 2 consecutive newlines
    
    # Trim
    sanitized = sanitized.strip()
    
    # Apply length limit if specified
    if max_length and len(sanitized) > max_length:
        sanitized = sanitized[:max_length-3] + "..."
        
    return sanitized


def create_batch_id(prefix: str = "BATCH", date: Optional[datetime] = None) -> str:
    """
    Create a unique batch ID
    
    Args:
        prefix: Prefix for the ID
        date: Optional date to include in ID
        
    Returns:
        Unique batch ID
    """
    if date:
        date_str = date.strftime('%Y%m%d')
        return f"{prefix}-{date_str}-{uuid.uuid4().hex[:8]}"
    else:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"


def format_duration(seconds: Optional[float]) -> str:
    """
    Format duration in seconds to human-readable string
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Human-readable duration string
    """
    if seconds is None:
        return "N/A"
        
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def parse_bool_env(env_value: Any, default: bool = False) -> bool:
    """
    Parse boolean environment variable
    
    Args:
        env_value: Environment variable value
        default: Default value if parsing fails
        
    Returns:
        Boolean value
    """
    if env_value is None:
        return default
        
    if isinstance(env_value, bool):
        return env_value
        
    if isinstance(env_value, str):
        return env_value.lower() in ['true', '1', 'yes', 'on', 'enabled']
        
    return default


def chunk_list(items: list, chunk_size: int) -> list:
    """
    Split a list into chunks of specified size
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
        
    Yields:
        Chunks of the list
    """
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division by zero
        
    Returns:
        Division result or default
    """
    if denominator == 0:
        return default
    return numerator / denominator


def extract_error_info(error: Exception) -> dict:
    """
    Extract detailed information from an exception
    
    Args:
        error: Exception object
        
    Returns:
        Dictionary with error information
    """
    import traceback
    
    return {
        "error_type": type(error).__name__,
        "error_module": type(error).__module__,
        "error_message": str(error),
        "error_args": error.args if hasattr(error, 'args') else [],
        "traceback": traceback.format_exc()
    }
