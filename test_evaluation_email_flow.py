"""
Test script to verify email flow using dev lead's approach
"""
import asyncio
import os
from datetime import datetime, timedelta
import sys

import smtplib
from typing import List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

def send_email_notification(sender_email: str,
                           receiver_email: List[str], 
                           subject: str, 
                           body: str,
                           attachment_path: str = None):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    
    # Add attachment if provided
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="{os.path.basename(attachment_path)}"'
            )
            msg.attach(part)
    
    try:
        smtp_server = os.getenv('SMTP_SERVER', 'server name')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        
        # Add auth if needed
        smtp_user = os.getenv('SMTP_USERNAME')
        smtp_pass = os.getenv('SMTP_PASSWORD')
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)
            
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("Email sent successfully!")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False


def test_evaluation_email_flow(accuracy: float = 0.83):
    """Test the email flow with configurable accuracy"""
    sender = "plum.cart@plum.com"
    receivers = ["plum.checkout@plum.com"]
    evaluation_date = datetime.now() - timedelta(days=1)
    
    print(f"\n{'='*60}")
    print(f"Testing email flow with accuracy: {accuracy:.2%}")
    print(f"Threshold: 0.85")
    print(f"{'='*60}\n")
    
    # Step 1: Send alert if accuracy < 0.85
    if accuracy < 0.85:
        print("1. Accuracy below threshold - Sending ALERT first...")
        
        alert_subject = f"URGENT ALERT: Low Accuracy ({accuracy:.2%}) - {evaluation_date.strftime('%Y-%m-%d')}"
        alert_body = f"""URGENT: Accuracy Below Threshold

The evaluation accuracy has fallen below the acceptable threshold.

Quick Summary:
- Date: {evaluation_date.strftime('%Y-%m-%d')}
- Accuracy: {accuracy:.2%} (BELOW THRESHOLD)
- Threshold: 85%
- Status: IMMEDIATE ATTENTION REQUIRED

A detailed report will follow shortly.

Action Required: Please investigate the cause of the accuracy drop immediately.
"""
        
        success = send_email_notification(sender, receivers, alert_subject, alert_body)
        if success:
            print("Alert sent successfully!")
        else:
            print("Alert failed to send!")
            
        # Small delay between emails
        print("   Waiting 2 seconds before sending report...")
        import time
        time.sleep(2)
    else:
        print("1. Accuracy above threshold - No alert needed")
    
    # Step 2: ALWAYS send the report
    print("\n2. Sending evaluation report (always sent)...")
    
    if accuracy < 0.85:
        report_subject = f"ALERT: Low Accuracy ({accuracy:.2%}) - Evaluation Report {evaluation_date.strftime('%Y-%m-%d')}"
    else:
        report_subject = f"Evaluation Report - {evaluation_date.strftime('%Y-%m-%d')}"
    
    report_body = f"""Evaluation Report for {evaluation_date.strftime('%Y-%m-%d')}

Summary:
- Total Evaluations: 150
- Passed: {int(150 * accuracy)}
- Failed: {150 - int(150 * accuracy)}
- Accuracy: {accuracy:.2%}
- Status: {'BELOW THRESHOLD - ACTION REQUIRED' if accuracy < 0.85 else '✅ WITHIN THRESHOLD'}

Detailed Results:
- See attached Excel report for complete evaluation data
- All individual evaluation results included
- Performance breakdown by dimension

{'ALERT: This batch requires immediate attention due to low accuracy.' if accuracy < 0.85 else 'No issues detected - accuracy within acceptable range.'}

This is an automated report from the LLM Evaluation Pipeline.
Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    # In real scenario, would attach Excel file
    # For testing, we'll create a dummy file
    dummy_report_path = f"test_report_{evaluation_date.strftime('%Y%m%d')}.txt"
    with open(dummy_report_path, 'w') as f:
        f.write("This is a test report file")
    
    success = send_email_notification(sender, receivers, report_subject, report_body, dummy_report_path)
    if success:
        print("Report sent successfully!")
    else:
        print("Report failed to send!")
    
    # Cleanup
    if os.path.exists(dummy_report_path):
        os.remove(dummy_report_path)
    
    print(f"\n{'='*60}")
    print("Email flow test complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Test both scenarios
    print("\nTEST 1: Low accuracy (should send alert then report)")
    test_evaluation_email_flow(accuracy=0.75)
    
    print("\n" + "="*80 + "\n")
    
    print("TEST 2: Good accuracy (should only send report)")
    test_evaluation_email_flow(accuracy=0.92)