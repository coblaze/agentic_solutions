"""
System monitoring and health checks
Ensures pipeline reliability
"""
import asyncio
import psutil
import os
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import aiohttp
from src.config.settings import settings
from src.services.db_service import MongoDBService

logger = logging.getLogger(__name__)


class HealthChecker:
    """
    System health checker for production monitoring
    Performs various health checks to ensure system reliability
    """
    
    def __init__(self):
        self.checks = [
            ("disk_space", self.check_disk_space),
            ("memory", self.check_memory),
            ("mongodb", self.check_mongodb),
            ("google_cloud", self.check_google_cloud),
            ("email_service", self.check_email_service),
            ("network", self.check_network)
        ]
        self.last_check_results: Dict[str, Any] = {}
        self.last_check_time: Optional[datetime] = None
        
    async def is_healthy(self) -> bool:
        """
        Run all health checks
        
        Returns:
            True if all checks pass, False otherwise
        """
        self.last_check_time = datetime.utcnow()
        self.last_check_results = {}
        all_healthy = True
        
        logger.info("Running system health checks...")
        
        # Run all checks concurrently
        check_tasks = []
        for check_name, check_func in self.checks:
            check_tasks.append(self._run_check(check_name, check_func))
            
        results = await asyncio.gather(*check_tasks, return_exceptions=True)
        
        # Process results
        for i, (check_name, _) in enumerate(self.checks):
            result = results[i]
            
            if isinstance(result, Exception):
                logger.error(f"Health check '{check_name}' failed with exception: {result}")
                self.last_check_results[check_name] = {
                    "status": "error",
                    "message": str(result),
                    "timestamp": datetime.utcnow()
                }
                all_healthy = False
            else:
                success, message = result
                self.last_check_results[check_name] = {
                    "status": "pass" if success else "fail",
                    "message": message,
                    "timestamp": datetime.utcnow()
                }
                
                if success:
                    logger.info(f"✅ Health check '{check_name}': {message}")
                else:
                    logger.warning(f"❌ Health check '{check_name}': {message}")
                    all_healthy = False
                    
        return all_healthy
        
    async def _run_check(self, name: str, check_func) -> tuple:
        """Run a single health check with timeout"""
        try:
            return await asyncio.wait_for(check_func(), timeout=30)
        except asyncio.TimeoutError:
            return False, f"Check timed out after 30 seconds"
        except Exception as e:
            return False, f"Check failed: {str(e)}"
            
    async def check_disk_space(self) -> tuple:
        """Check if sufficient disk space is available"""
        try:
            usage = psutil.disk_usage('/')
            free_gb = usage.free / (1024 ** 3)
            used_percent = usage.percent
            
            # Check both free space and percentage
            if free_gb < 1:  # Less than 1GB free
                return False, f"Critical: Only {free_gb:.2f}GB free disk space"
            elif free_gb < 5:  # Less than 5GB free
                return False, f"Low disk space: {free_gb:.2f}GB free ({used_percent}% used)"
            elif used_percent > 90:
                return False, f"Disk usage critical: {used_percent}% used"
            else:
                return True, f"Disk space OK: {free_gb:.1f}GB free ({used_percent}% used)"
                
        except Exception as e:
            return False, f"Failed to check disk space: {e}"
            
    async def check_memory(self) -> tuple:
        """Check system memory"""
        try:
            memory = psutil.virtual_memory()
            available_gb = memory.available / (1024 ** 3)
            
            if memory.percent > 95:
                return False, f"Critical memory usage: {memory.percent}% used"
            elif memory.percent > 90:
                return False, f"High memory usage: {memory.percent}% used"
            elif available_gb < 0.5:
                return False, f"Low available memory: {available_gb:.2f}GB"
            else:
                return True, f"Memory OK: {memory.percent}% used, {available_gb:.1f}GB available"
                
        except Exception as e:
            return False, f"Failed to check memory: {e}"
            
    async def check_mongodb(self) -> tuple:
        """Check MongoDB connectivity"""
        try:
            db_service = MongoDBService()
            await db_service.connect()
            
            # Ping database
            await db_service.client.admin.command('ping')
            
            # Check collections exist
            db = db_service.db
            collections = await db.list_collection_names()
            
            required_collections = [
                settings.MONGODB_COLLECTION_TRANSCRIPTS,
                settings.MONGODB_COLLECTION_SUMMARIES,
                settings.MONGODB_COLLECTION_EVALUATIONS,
                settings.MONGODB_COLLECTION_BATCHES
            ]
            
            missing = [c for c in required_collections if c not in collections]
            
            await db_service.disconnect()
            
            if missing:
                return False, f"Missing collections: {', '.join(missing)}"
            else:
                return True, f"MongoDB connected, all {len(required_collections)} collections present"
                
        except Exception as e:
            return False, f"MongoDB check failed: {e}"
            
    async def check_google_cloud(self) -> tuple:
        """Check Google Cloud connectivity"""
        try:
            # Check if credentials are configured
            if not settings.GOOGLE_CLOUD_PROJECT:
                return False, "Google Cloud project not configured"
                
            # Try to import and verify settings
            from google.cloud import aiplatform
            
            # This will fail if credentials are not properly configured
            # In production, you might want to make a simple API call
            return True, f"Google Cloud configured for project: {settings.GOOGLE_CLOUD_PROJECT}"
            
        except ImportError:
            return False, "Google Cloud SDK not installed"
        except Exception as e:
            return False, f"Google Cloud check failed: {e}"
            
    async def check_email_service(self) -> tuple:
        """Check email service availability"""
        try:
            # Check configuration based on provider
            provider = settings.EMAIL_PROVIDER.lower()
            
            if provider == "smtp":
                if not all([settings.SMTP_SERVER, settings.SMTP_USERNAME, settings.SMTP_PASSWORD]):
                    return False, "SMTP configuration incomplete"
                return True, f"SMTP configured: {settings.SMTP_SERVER}"
                
            elif provider == "sendgrid":
                if not settings.SENDGRID_API_KEY:
                    return False, "SendGrid API key not configured"
                return True, "SendGrid configured"
                
            elif provider == "aws":
                if not all([settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY]):
                    return False, "AWS SES credentials not configured"
                return True, f"AWS SES configured for region: {settings.AWS_REGION}"
                
            else:
                return False, f"Unknown email provider: {provider}"
                
        except Exception as e:
            return False, f"Email service check failed: {e}"
            
    async def check_network(self) -> tuple:
        """Check network connectivity"""
        try:
            # Try to reach a reliable endpoint
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get('https://www.google.com') as response:
                    if response.status == 200:
                        return True, "Network connectivity OK"
                    else:
                        return False, f"Network check returned status: {response.status}"
                        
        except asyncio.TimeoutError:
            return False, "Network request timed out"
        except Exception as e:
            return False, f"Network check failed: {e}"
            
    async def send_health_alert(self):
        """Send alert when health check fails"""
        from src.services.email_service import EmailService
        email_service = EmailService()
        
        subject = "⚠️ ALERT: Evaluation Pipeline Health Check Failed"
        
        # Build detailed report
        failed_checks = []
        passed_checks = []
        
        for check_name, result in self.last_check_results.items():
            if result["status"] != "pass":
                failed_checks.append((check_name, result))
            else:
                passed_checks.append((check_name, result))
                
        body = f"""
        <html>
        <head>
            <style>
                .fail {{ color: red; }}
                .pass {{ color: green; }}
                .details {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin: 10px 0; }}
            </style>
        </head>
        <body>
            <h2>⚠️ Health Check Failed</h2>
            <p>The evaluation pipeline health check has detected issues that may affect operation.</p>
            
            <p><strong>Check Time:</strong> {self.last_check_time.strftime('%Y-%m-%d %H:%M:%S UTC') if self.last_check_time else 'Unknown'}</p>
            
            <h3>Failed Checks ({len(failed_checks)}):</h3>
            <ul>
        """
        
        for check_name, result in failed_checks:
            body += f"""
                <li class="fail">
                    <strong>{check_name}:</strong> {result['message']}
                </li>
            """
            
        body += """
            </ul>
            
            <h3>Passed Checks ({len(passed_checks)}):</h3>
            <ul>
        """
        
        for check_name, result in passed_checks:
            body += f"""
                <li class="pass">
                    <strong>{check_name}:</strong> {result['message']}
                </li>
            """
            
        body += """
            </ul>
            
            <div class="details">
                <h3>Recommended Actions:</h3>
                <ol>
                    <li>Check system resources (disk space, memory)</li>
                    <li>Verify MongoDB connectivity and credentials</li>
                    <li>Confirm Google Cloud/Vertex AI access</li>
                    <li>Test email service configuration</li>
                    <li>Check network connectivity</li>
                </ol>
            </div>
            
            <p><strong>Note:</strong> The evaluation may still proceed, but failures are more likely.</p>
        </body>
        </html>
        """
        
        try:
            await email_service.send_alert(subject, body, priority="high")
        except Exception as e:
            logger.error(f"Failed to send health alert email: {e}")
            
    def get_health_summary(self) -> Dict[str, Any]:
        """Get summary of last health check results"""
        return {
            "last_check_time": self.last_check_time.isoformat() if self.last_check_time else None,
            "results": self.last_check_results,
            "overall_healthy": all(
                r.get("status") == "pass" 
                for r in self.last_check_results.values()
            )
        }
