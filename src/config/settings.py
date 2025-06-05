"""
Centralized configuration management
Loads settings from environment variables
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional
# Load environment variables from .env file
env_path = Path(__file__).parent.parent.parent / '.env'

if env_path.exists():
    load_dotenv(env_path)
else:
    print(f"Warning: .env file not found at {env_path}")

class Settings:
    """Application settings loaded from environment variables"""
    # MongoDB Configuration
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "evaluation_db")
    MONGODB_COLLECTION_TRANSCRIPTS: str = os.getenv("MONGODB_COLLECTION_TRANSCRIPTS", "transcripts")
    MONGODB_COLLECTION_SUMMARIES: str = os.getenv("MONGODB_COLLECTION_SUMMARIES", "summaries")
    MONGODB_COLLECTION_EVALUATIONS: str = os.getenv("MONGODB_COLLECTION_EVALUATIONS", "evaluations")
    MONGODB_COLLECTION_BATCHES: str = os.getenv("MONGODB_COLLECTION_BATCHES", "evaluation_batches")
    # Email Configuration - Multiple Providers
    EMAIL_PROVIDER: str = os.getenv("EMAIL_PROVIDER", "smtp") # Options: smtp, sendgrid, aws
    FROM_EMAIL: str = os.getenv("FROM_EMAIL", "noreply@company.com")
    ALERT_EMAIL: str = os.getenv("ALERT_EMAIL", "plum.alerts@plum.com")
    # SMTP Settings (Primary or Fallback)
    SMTP_SERVER: Optional[str] = os.getenv("SMTP_SERVER")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME: Optional[str] = os.getenv("SMTP_USERNAME")
    SMTP_PASSWORD: Optional[str] = os.getenv("SMTP_PASSWORD")
    # SendGrid (Recommended for production)
    SENDGRID_API_KEY: Optional[str] = os.getenv("SENDGRID_API_KEY")
    # AWS SES
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    # Evaluation Configuration
    ACCURACY_THRESHOLD: float = float(os.getenv("ACCURACY_THRESHOLD", "0.85"))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    # Recovery Configuration
    BATCH_RECOVERY_STRATEGY: str = os.getenv("BATCH_RECOVERY_STRATEGY", "recover_all")
    RECOVERY_LOOKBACK_DAYS: int = int(os.getenv("RECOVERY_LOOKBACK_DAYS", "7"))
    EXPECTED_MIN_DAILY_CALLS: int = int(os.getenv("EXPECTED_MIN_DAILY_CALLS", "100"))
    # Scheduling
    TIMEZONE: str = os.getenv("TIMEZONE", "America/Chicago")
    RUN_TIME: str = os.getenv("RUN_TIME", "00:01") # 12:01 AM
    # Google Vertex AI
    GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    GOOGLE_CLOUD_LOCATION: str = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    VERTEX_AI_MODEL: str = os.getenv("VERTEX_AI_MODEL", "gemini-2.0-flash-001")
    # Report Configuration
    REPORT_PASSWORD: str = os.getenv("REPORT_PASSWORD", "EvalReport2024")

    def validate(self):
        """Validate required settings"""
        errors = []
        # Check MongoDB
        if not self.MONGODB_URI:
            errors.append("MONGODB_URI is required")
            # Check email configuration
            if self.EMAIL_PROVIDER == "smtp":
                if not all([self.SMTP_SERVER, self.SMTP_USERNAME, self.SMTP_PASSWORD]):
                    errors.append("SMTP configuration incomplete")
                elif self.EMAIL_PROVIDER == "sendgrid":
                    if not self.SENDGRID_API_KEY:
                        errors.append("SENDGRID_API_KEY is required for SendGrid provider")
                    elif self.EMAIL_PROVIDER == "aws":
                        if not all([self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY]):
                            errors.append("AWS credentials required for SES provider")
            # Check Google Cloud
            if not self.GOOGLE_CLOUD_PROJECT:
                errors.append("GOOGLE_CLOUD_PROJECT is required")
            if errors:
                raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        def __repr__(self):
            """String representation hiding sensitive values"""
            return (
            f"Settings("
            f"MONGODB_DATABASE={self.MONGODB_DATABASE}, "
            f"EMAIL_PROVIDER={self.EMAIL_PROVIDER}, "
            f"ACCURACY_THRESHOLD={self.ACCURACY_THRESHOLD}, "
            f"TIMEZONE={self.TIMEZONE})"
            )
        
# Create singleton instance
settings = Settings()
# Validate on import (can be disabled for testing)
if os.getenv("SKIP_CONFIG_VALIDATION") != "true":
    try:
        settings.validate()
    except ValueError as e:
        print(f"Configuration Error: {e}")
        print("Please check your .env file")
