"""Alert management system for RS Systems Health Monitor."""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid
from collections import deque
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import aiohttp
from slack_sdk.webhook.async_client import AsyncWebhookClient

from .config import settings
from .models.django_models import Alert

logger = logging.getLogger(__name__)


class AlertManager:
    """Manage system alerts and notifications."""

    def __init__(self):
        self.config = settings.alerts
        self.alert_history = deque(maxlen=1000)
        self.active_alerts = {}
        self.cooldown_tracker = {}

        # Initialize Slack client if configured
        self.slack_client = None
        if self.config.slack_webhook_url:
            self.slack_client = AsyncWebhookClient(url=self.config.slack_webhook_url)

    def _should_alert(self, component: str, alert_type: str) -> bool:
        """Check if an alert should be sent based on cooldown."""
        if not self.config.enabled:
            return False

        key = f"{component}:{alert_type}"
        now = datetime.now()

        if key in self.cooldown_tracker:
            last_alert = self.cooldown_tracker[key]
            cooldown_end = last_alert + timedelta(minutes=self.config.cooldown_minutes)

            if now < cooldown_end:
                return False

        self.cooldown_tracker[key] = now
        return True

    async def create_alert(
        self,
        severity: str,
        component: str,
        title: str,
        message: str,
        threshold_value: Optional[float] = None,
        actual_value: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Alert:
        """Create a new alert."""
        alert = Alert(
            id=str(uuid.uuid4()),
            severity=severity,
            component=component,
            title=title,
            message=message,
            threshold_value=threshold_value,
            actual_value=actual_value,
            metadata=metadata or {}
        )

        # Add to active alerts
        self.active_alerts[alert.id] = alert

        # Add to history
        self.alert_history.append(alert)

        # Send notifications if conditions are met
        if self._should_alert(component, title):
            await self._send_notifications(alert)

        logger.info(f"Alert created: {alert.title} ({alert.severity})")
        return alert

    async def _send_notifications(self, alert: Alert):
        """Send alert notifications to configured channels."""
        tasks = []

        if self.slack_client:
            tasks.append(self._send_slack_notification(alert))

        if self.config.email_enabled:
            tasks.append(self._send_email_notification(alert))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_slack_notification(self, alert: Alert):
        """Send alert to Slack."""
        try:
            # Format Slack message
            color = {
                "critical": "danger",
                "warning": "warning",
                "info": "good"
            }.get(alert.severity, "default")

            attachment = {
                "color": color,
                "title": f"{alert.severity.upper()}: {alert.title}",
                "text": alert.message,
                "fields": [
                    {
                        "title": "Component",
                        "value": alert.component,
                        "short": True
                    },
                    {
                        "title": "Time",
                        "value": alert.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "short": True
                    }
                ],
                "footer": "RS Systems Health Monitor",
                "ts": int(alert.created_at.timestamp())
            }

            if alert.threshold_value is not None and alert.actual_value is not None:
                attachment["fields"].extend([
                    {
                        "title": "Actual Value",
                        "value": str(alert.actual_value),
                        "short": True
                    },
                    {
                        "title": "Threshold",
                        "value": str(alert.threshold_value),
                        "short": True
                    }
                ])

            response = await self.slack_client.send(
                text=f"Alert: {alert.title}",
                attachments=[attachment]
            )

            if response.status_code != 200:
                logger.error(f"Failed to send Slack notification: {response.body}")

        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

    async def _send_email_notification(self, alert: Alert):
        """Send alert via email."""
        try:
            # Create email message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[RS Systems Alert] {alert.severity.upper()}: {alert.title}"
            msg["From"] = self.config.email_from
            msg["To"] = ", ".join(self.config.email_to)

            # Create HTML body
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="background-color: {'#dc3545' if alert.severity == 'critical' else '#ffc107' if alert.severity == 'warning' else '#28a745'};
                            color: white; padding: 10px; border-radius: 5px;">
                    <h2>{alert.severity.upper()}: {alert.title}</h2>
                </div>

                <div style="padding: 20px;">
                    <p><strong>Component:</strong> {alert.component}</p>
                    <p><strong>Message:</strong> {alert.message}</p>
                    <p><strong>Time:</strong> {alert.created_at.strftime("%Y-%m-%d %H:%M:%S")}</p>

                    {"<p><strong>Actual Value:</strong> " + str(alert.actual_value) + "</p>" if alert.actual_value else ""}
                    {"<p><strong>Threshold:</strong> " + str(alert.threshold_value) + "</p>" if alert.threshold_value else ""}

                    <hr>
                    <p style="font-size: 12px; color: #666;">
                        This alert was generated by RS Systems Health Monitor
                    </p>
                </div>
            </body>
            </html>
            """

            msg.attach(MIMEText(html_body, "html"))

            # Send email
            with smtplib.SMTP(self.config.email_smtp_host, self.config.email_smtp_port) as server:
                server.starttls()
                if self.config.email_smtp_user and self.config.email_smtp_password:
                    server.login(self.config.email_smtp_user, self.config.email_smtp_password)
                server.send_message(msg)

            logger.info(f"Email alert sent to {', '.join(self.config.email_to)}")

        except Exception as e:
            logger.error(f"Error sending email notification: {e}")

    async def resolve_alert(self, alert_id: str):
        """Mark an alert as resolved."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.is_resolved = True
            alert.resolved_at = datetime.now()

            # Remove from active alerts
            del self.active_alerts[alert_id]

            logger.info(f"Alert resolved: {alert.title}")

    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        return list(self.active_alerts.values())

    def get_alert_history(self, limit: int = 100) -> List[Alert]:
        """Get recent alert history."""
        alerts = list(self.alert_history)
        alerts.reverse()
        return alerts[:limit]

    async def process_monitor_results(self, results: Dict[str, Any]):
        """Process monitoring results and generate alerts."""
        alerts_created = []

        def safe_get(obj, key, default=None):
            """Safely get a value from a dict, returning default if obj is not a dict."""
            if isinstance(obj, dict):
                return obj.get(key, default)
            return default

        # Process database monitoring results
        if "database" in results:
            db_results = results["database"]
            if isinstance(db_results, dict) and db_results.get("has_issues"):
                for issue in db_results.get("issues", []):
                    alert = await self.create_alert(
                        severity="warning",
                        component="database",
                        title="Database Performance Issue",
                        message=issue if isinstance(issue, str) else safe_get(issue, "message", str(issue)),
                        metadata={"monitor": "database", "issue": issue}
                    )
                    alerts_created.append(alert)

        # Process API monitoring results
        if "api" in results:
            api_results = results["api"]
            if isinstance(api_results, dict) and api_results.get("has_issues"):
                for issue in api_results.get("issues", []):
                    severity = safe_get(issue, "severity", "warning")
                    alert = await self.create_alert(
                        severity=severity,
                        component="api",
                        title=safe_get(issue, "type", "API Issue"),
                        message=safe_get(issue, "message", "API performance issue detected"),
                        threshold_value=safe_get(issue, "threshold"),
                        actual_value=safe_get(issue, "value"),
                        metadata=issue if isinstance(issue, dict) else {"issue": issue}
                    )
                    alerts_created.append(alert)

        # Process queue monitoring results
        if "queue" in results:
            queue_results = results["queue"]
            if isinstance(queue_results, dict) and queue_results.get("has_issues"):
                for issue in queue_results.get("issues", []):
                    severity = safe_get(issue, "severity", "warning")
                    alert = await self.create_alert(
                        severity=severity,
                        component="queue",
                        title=safe_get(issue, "type", "Queue Issue"),
                        message=safe_get(issue, "message", "Queue processing issue detected"),
                        threshold_value=safe_get(issue, "threshold"),
                        actual_value=safe_get(issue, "value"),
                        metadata=issue if isinstance(issue, dict) else {"issue": issue}
                    )
                    alerts_created.append(alert)

        # Process storage monitoring results
        if "storage" in results:
            storage_results = results["storage"]
            if isinstance(storage_results, dict) and storage_results.get("has_issues"):
                for issue in storage_results.get("issues", []):
                    severity = safe_get(issue, "severity", "warning")
                    alert = await self.create_alert(
                        severity=severity,
                        component="storage",
                        title=safe_get(issue, "type", "Storage Issue"),
                        message=safe_get(issue, "message", "Storage issue detected"),
                        threshold_value=safe_get(issue, "threshold"),
                        actual_value=safe_get(issue, "value"),
                        metadata=issue if isinstance(issue, dict) else {"issue": issue}
                    )
                    alerts_created.append(alert)

        # Process activity monitoring results
        if "activity" in results:
            activity_results = results["activity"]
            if isinstance(activity_results, dict) and activity_results.get("has_issues"):
                for issue in activity_results.get("issues", []):
                    alert = await self.create_alert(
                        severity=safe_get(issue, "severity", "info"),
                        component="activity",
                        title=safe_get(issue, "type", "Activity Issue"),
                        message=safe_get(issue, "message", "Activity pattern issue detected"),
                        metadata=issue if isinstance(issue, dict) else {"issue": issue}
                    )
                    alerts_created.append(alert)

        return alerts_created

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alert statistics."""
        active = self.get_active_alerts()
        history = list(self.alert_history)

        # Count by severity
        severity_counts = {
            "critical": 0,
            "warning": 0,
            "info": 0
        }

        for alert in active:
            severity_counts[alert.severity] = severity_counts.get(alert.severity, 0) + 1

        # Count by component
        component_counts = {}
        for alert in active:
            component_counts[alert.component] = component_counts.get(alert.component, 0) + 1

        # Recent trends (last 24 hours)
        cutoff = datetime.now() - timedelta(hours=24)
        recent_alerts = [a for a in history if a.created_at > cutoff]

        return {
            "active_alerts_count": len(active),
            "severity_breakdown": severity_counts,
            "component_breakdown": component_counts,
            "alerts_last_24h": len(recent_alerts),
            "most_recent_alert": active[0].dict() if active else None,
            "timestamp": datetime.now().isoformat()
        }