"""
Multi-Provider Email Service
Supports SMTP, SendGrid, and AWS SES with automatic fallback
"""
import os
import logging
from typing import List, Optional, Dict, Any
from abc import ABC, abstractmethod
import asyncio
import aiofiles
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import ssl
from datetime import datetime

# Optional imports for additional providers
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False
    
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

from src.config.settings import settings
from src.models.evaluation import BatchEvaluation

logger = logging.getLogger(__name__)


class EmailProvider(ABC):
    """Abstract base class for email providers"""
    
    @abstractmethod
    async def send_email(self, 
                        to_email: str, 
                        subject: str, 
                        body: str, 
                        attachments: Optional[List[str]] = None,
                        cc_emails: Optional[List[str]] = None,
                        priority: str = "normal") -> bool:
        """Send email through the provider"""
        pass
        
    @abstractmethod
    def is_available(self) -> bool:
        """Check if this provider is available and configured"""
        pass


class SMTPProvider(EmailProvider):
    """Standard SMTP email provider"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'server name')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.username = os.getenv('SMTP_USERNAME', '')
        self.password = os.getenv('SMTP_PASSWORD', '')
        self.from_email = os.getenv('FROM_EMAIL', 'plum.cart@plum.com')
        
    async def send_email(self, to_email: str, subject: str, body: str,
                        attachments: Optional[List[str]] = None,
                        cc_emails: Optional[List[str]] = None,
                        priority: str = "normal") -> bool:
        try:
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = to_email if isinstance(to_email, str) else ', '.join(to_email)
            msg['Subject'] = subject
            
            if '<html>' in body:
                msg.attach(MIMEText(body, 'html'))
            else:
                msg.attach(MIMEText(body, 'plain'))
            
            if attachments and os.getenv('ENABLE_ATTACHMENTS', 'true') == 'true':
                for file_path in attachments:
                    if os.path.exists(file_path):
                        with open(file_path, 'rb') as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename="{os.path.basename(file_path)}"'
                            )
                            msg.attach(part)
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            
            if self.username and self.password:
                server.login(self.username, self.password)
            
            # Handle recipients
            recipients = [to_email] if isinstance(to_email, str) else to_email
            if cc_emails:
                recipients.extend(cc_emails)
            
            server.sendmail(self.from_email, recipients, msg.as_string())
            server.quit()
            
            logger.info(f"Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False



class EmailService:
    """
    Main email service with multi-provider support and fallback
    Automatically tries multiple providers until success
    """
    
    def __init__(self):
        self.providers = self._initialize_providers()
        self.alert_email = settings.ALERT_EMAIL
        
        if not self.providers:
            logger.warning("No email providers configured!")
            
    def _initialize_providers(self) -> List[EmailProvider]:
        """Initialize available email providers in priority order"""
        providers = []
        
        # Primary provider based on configuration
        primary = settings.EMAIL_PROVIDER.lower()
        
        # Initialize primary provider first
        if primary == "smtp":
            provider = SMTPProvider()
            if provider.is_available():
                providers.append(provider)
                logger.info("SMTP configured as primary email provider")
                
        # Add fallback providers
        # SMTP as fallback
        if primary != "smtp":
            smtp_provider = SMTPProvider()
            if smtp_provider.is_available():
                providers.append(smtp_provider)

                
        return providers
        
    async def send_evaluation_report(self,
                                   batch: BatchEvaluation,
                                   report_path: str,
                                   send_alert: bool = False,
                                   is_recovery: bool = False):
        """Send evaluation report email"""
        # Generate subject
        recovery_note = " (Recovery Batch)" if is_recovery else ""
        date_str = batch.evaluation_date.strftime('%Y-%m-%d')
        
        if send_alert:
            subject = (f"ALERT: Low Accuracy ({batch.accuracy:.2%}) - "
                      f"Evaluation Report {date_str}{recovery_note}")
        else:
            subject = f"Evaluation Report - {date_str}{recovery_note}"
            
        # Generate body
        body = self._generate_report_body(batch, send_alert, is_recovery)
        
        # Determine priority
        priority = "high" if send_alert else "normal"
        
        # Try to send with attachments
        attachments = [report_path] if os.path.exists(report_path) else None
        
        success = await self._send_with_fallback(
            self.alert_email,
            subject,
            body,
            attachments,
            priority=priority
        )
        
        if not success:
            logger.error("Failed to send evaluation report email!")
            raise Exception("All email providers failed")
            
    async def send_alert(self, 
                        subject: str, 
                        body: str, 
                        priority: str = "normal",
                        attachments: Optional[List[str]] = None):
        """Send alert email"""
        success = await self._send_with_fallback(
            self.alert_email,
            subject,
            body,
            attachments,
            priority=priority
        )
        
        if not success:
            logger.critical(f"Failed to send critical alert: {subject}")
            
    async def _send_with_fallback(self,
                                 to_email: str,
                                 subject: str,
                                 body: str,
                                 attachments: Optional[List[str]] = None,
                                 cc_emails: Optional[List[str]] = None,
                                 priority: str = "normal") -> bool:
        """Try sending email through available providers with fallback"""
        if not self.providers:
            logger.error("No email providers available")
            return False
            
        for i, provider in enumerate(self.providers):
            try:
                provider_name = type(provider).__name__
                logger.info(f"Attempting to send email via {provider_name} "
                          f"(attempt {i + 1}/{len(self.providers)})")
                
                success = await provider.send_email(
                    to_email,
                    subject,
                    body,
                    attachments,
                    cc_emails,
                    priority
                )
                
                if success:
                    logger.info(f"Email sent successfully via {provider_name}")
                    return True
                else:
                    logger.warning(f"{provider_name} returned failure status")
                    
            except Exception as e:
                logger.error(f"Provider {type(provider).__name__} failed with exception: {e}")
                
            # If not the last provider, wait a bit before trying next
            if i < len(self.providers) - 1:
                await asyncio.sleep(1)
                
        logger.error("All email providers failed!")
        return False
        
    def _generate_report_body(self, 
                             batch: BatchEvaluation, 
                             send_alert: bool, 
                             is_recovery: bool) -> str:
        """Generate HTML email body for evaluation report"""
        recovery_banner = """
        <div style="background-color: #fff3cd; border: 1px solid #ffc107; padding: 10px; margin-bottom: 20px; border-radius: 5px;">
            <strong> Recovery Batch:</strong> This evaluation was run as part of automatic recovery for a previously missed or failed batch.
        </div>
        """ if is_recovery else ""
        
        if send_alert:
            accuracy_color = "#dc3545"  # Red
            status_emoji = ""
            status_text = "Below Threshold"
        else:
            accuracy_color = "#28a745"  # Green
            status_emoji = ""
            status_text = "Within Threshold"
            
        return f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .metrics {{ background-color: #ffffff; border: 1px solid #dee2e6; padding: 20px; border-radius: 5px; }}
                .metric-row {{ display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #f0f0f0; }}
                .metric-label {{ font-weight: bold; color: #495057; }}
                .metric-value {{ color: #212529; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #dee2e6; color: #6c757d; font-size: 0.9em; }}
                .alert-box {{ background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                .success-box {{ background-color: #d4edda; border: 1px solid #c3e6cb; padding: 15px; border-radius: 5px; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="container">
                {recovery_banner}
                
                <div class="header">
                    <h2 style="margin: 0; color: #212529;">LLM Evaluation Report</h2>
                    <p style="margin: 10px 0 0 0; color: #6c757d;">
                        {batch.evaluation_date.strftime('%B %d, %Y')}
                    </p>
                </div>
                
                <div class="{'alert-box' if send_alert else 'success-box'}">
                    <h3 style="margin: 0 0 10px 0;">
                        {status_emoji} Accuracy Status: {status_text}
                    </h3>
                    <p style="margin: 0;">
                        Overall accuracy: <strong style="color: {accuracy_color};">{batch.accuracy:.2%}</strong>
                        (Threshold: {settings.ACCURACY_THRESHOLD:.0%})
                    </p>
                </div>
                
                <div class="metrics">
                    <h3 style="margin-top: 0;">Evaluation Summary</h3>
                    
                    <div class="metric-row">
                        <span class="metric-label">Batch ID:</span>
                        <span class="metric-value">{batch.batch_id}</span>
                    </div>
                    
                    <div class="metric-row">
                        <span class="metric-label">Total Evaluations:</span>
                        <span class="metric-value">{batch.total_evaluations:,}</span>
                    </div>
                    
                    <div class="metric-row">
                        <span class="metric-label">Passed:</span>
                        <span class="metric-value" style="color: #28a745;">
                            {batch.passed:,} ({batch.pass_percentage:.2f}%)
                        </span>
                    </div>
                    
                    <div class="metric-row">
                        <span class="metric-label">Failed:</span>
                        <span class="metric-value" style="color: #dc3545;">
                            {batch.failed:,} ({batch.fail_percentage:.2f}%)
                        </span>
                    </div>
                    
                    {"" if batch.errors == 0 else f'''
                    <div class="metric-row">
                        <span class="metric-label">Errors:</span>
                        <span class="metric-value" style="color: #ffc107;">
                            {batch.errors:,} ({batch.error_percentage:.2f}%)
                        </span>
                    </div>
                    '''}
                    
                    <div class="metric-row">
                        <span class="metric-label">Processing Time:</span>
                        <span class="metric-value">
                            {batch.processing_time_seconds:.1f} seconds
                        </span>
                    </div>
                    
                    {f'''
                    <div class="metric-row">
                        <span class="metric-label">Average Confidence:</span>
                        <span class="metric-value">{batch.average_confidence:.2%}</span>
                    </div>
                    ''' if batch.average_confidence else ""}
                </div>
                
                <div style="margin-top: 20px;">
                    <p><strong>📎 Attachment:</strong> Detailed evaluation report (Excel)</p>
                    <p>Please review the attached Excel file for:</p>
                    <ul>
                        <li>Detailed metrics breakdown</li>
                        <li>Individual evaluation results</li>
                        <li>Performance statistics by dimension</li>
                    </ul>
                </div>
                
                {"" if not send_alert else '''
                <div style="background-color: #fff3cd; border: 1px solid #ffc107; padding: 15px; border-radius: 5px; margin-top: 20px;">
                    <h4 style="margin: 0 0 10px 0; color: #856404;">⚠️ Action Required</h4>
                    <p style="margin: 0; color: #856404;">
                        The evaluation accuracy has fallen below the acceptable threshold. 
                        Please review the detailed report and investigate the cause of the accuracy drop.
                    </p>
                </div>
                '''}
                
                <div class="footer">
                    <p>This is an automated report from the LLM Evaluation Pipeline.</p>
                    <p>Generated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
                </div>
            </div>
        </body>
        </html>
        """
    
