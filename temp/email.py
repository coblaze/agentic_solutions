import smtplib
import os
from typing import List, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime
import pandas as pd

class EmailingService:
    def __init__(
        self,
        smtp_server: str = 'dfjkfoteee',
        smtp_port: int = 300,
        sender_email: str = "plum.cart@plum.com",
        accuracy_threshold: float = 0.85  # 85% threshold
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.accuracy_threshold = accuracy_threshold
    
    def send_email_with_attachment(
        self,
        receiver_emails: List[str],
        subject: str,
        body: str,
        attachment_path: Optional[str] = None
    ) -> bool:
        """Send email with optional Excel attachment"""
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = ','.join(receiver_emails)
        msg['Subject'] = subject
        
        # Add body
        msg.attach(MIMEText(body, 'plain'))
        
        # Add attachment if provided
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, 'rb') as file:
                attach = MIMEApplication(file.read(), _subtype="xlsx")
                attach.add_header(
                    'Content-Disposition',
                    'attachment',
                    filename=os.path.basename(attachment_path)
                )
                msg.attach(attach)
        
        try:
            # Connect to server and send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.sendmail(self.sender_email, receiver_emails, msg.as_string())
            server.quit()
            print(f"Email sent successfully to {', '.join(receiver_emails)}")
            return True
        except Exception as e:
            print(f"Error sending email: {e}")
            return False
    
    def send_evaluation_results(
        self,
        receiver_emails: List[str],
        excel_file_path: str,
        accuracy: float,
        metrics_data: dict
    ):
        """Send evaluation results based on accuracy threshold"""
        
        # Format metrics for email body
        metrics_summary = f"""
Evaluation Results Summary:
==========================
Total Processed: {metrics_data['total_count']}
Passed: {metrics_data['pass_count']} ({metrics_data['pass_percentage']:.2f}%)
Failed: {metrics_data['fail_count']} ({metrics_data['fail_percentage']:.2f}%)
Overall Accuracy: {accuracy:.2%}
"""
        
        if accuracy < self.accuracy_threshold:
            # Below threshold - send ALERT with Excel attachment
            subject = f"ALERT: Evaluation Accuracy Below Threshold ({accuracy:.1%})"
            body = f"""
ATTENTION: The latest evaluation batch has fallen below the accuracy threshold.

{metrics_summary}
Accuracy Threshold: {self.accuracy_threshold:.0%}
Status: BELOW THRESHOLD 

The evaluation results Excel file is attached for your review.
Please investigate the failed cases to identify issues.

File: {os.path.basename(excel_file_path)}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Action Required: Review failed evaluations and determine root cause.
"""
            # Send WITH attachment for review
            self.send_email_with_attachment(
                receiver_emails=receiver_emails,
                subject=subject,
                body=body,
                attachment_path=excel_file_path  # Include Excel for investigation
            )
        else:
            # Above threshold - send normally with Excel attachment (no alert language)
            subject = f"Evaluation Results - {datetime.now().strftime('%Y-%m-%d')}"
            body = f"""
Evaluation batch completed.

{metrics_summary}

The detailed evaluation results are attached.
- Sheet 'Results': Contains all evaluated records
- Sheet 'Metrics': Contains summary statistics

File: {os.path.basename(excel_file_path)}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            # Send with attachment (normal email, no alert)
            self.send_email_with_attachment(
                receiver_emails=receiver_emails,
                subject=subject,
                body=body,
                attachment_path=excel_file_path
            )
    
    def send_error_notification(
        self,
        receiver_emails: List[str],
        error_message: str
    ):
        """Send error notification if processing fails"""
        subject = "Evaluation Process Error"
        body = f"""
An error occurred during the evaluation process:

Error Details:
{error_message}

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Please check the logs for more details.
"""
        self.send_email_with_attachment(
            receiver_emails=receiver_emails,
            subject=subject,
            body=body,
            attachment_path=None
        )


# Standalone function for integration with existing scheduler
def send_batch_evaluation_email(
    excel_file_path: str,
    accuracy: float,
    metrics_data: dict,
    receiver_emails: List[str] = ["plum.checkout@plum.com"],
    accuracy_threshold: float = 0.85
):
    """
    Convenience function for scheduled email sending
    
    Args:
        excel_file_path: Path to the evaluation results Excel file
        accuracy: The accuracy value (0-1)
        metrics_data: Dictionary with evaluation metrics
        receiver_emails: List of recipient emails
        accuracy_threshold: Threshold for alerts (default 0.85 = 85%)
    """
    email_service = EmailingService(accuracy_threshold=accuracy_threshold)
    email_service.send_evaluation_results(
        receiver_emails=receiver_emails,
        excel_file_path=excel_file_path,
        accuracy=accuracy,
        metrics_data=metrics_data
    )