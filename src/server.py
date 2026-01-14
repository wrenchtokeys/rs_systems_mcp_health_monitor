"""Main MCP server for RS Systems Health Monitor."""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from mcp.server import NotificationOptions, Server, stdio_server
from mcp.server.models import InitializationOptions
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource, ServerCapabilities, ToolsCapability

from .config import settings
from .monitors.database import DatabaseMonitor
from .monitors.api import APIMonitor
from .monitors.queue import QueueMonitor
from .monitors.storage import StorageMonitor
from .monitors.activity_simple import ActivityMonitor
from .alerts import AlertManager
from .models.django_models import MonitoringQuery

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.logging.level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RSHealthMonitorServer:
    """RS Systems Health Monitor MCP Server."""

    def __init__(self):
        self.server = Server(settings.mcp.server_name)

        # Initialize monitors with error handling
        self.db_monitor = None
        if settings.features.enable_database_monitoring:
            try:
                self.db_monitor = DatabaseMonitor()
                logger.info("Database monitor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize database monitor: {e}")
                self.db_monitor = None

        self.api_monitor = None
        if settings.features.enable_api_monitoring:
            try:
                self.api_monitor = APIMonitor()
                logger.info("API monitor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize API monitor: {e}")
                self.api_monitor = None

        self.queue_monitor = None
        if settings.features.enable_queue_monitoring and self.db_monitor:
            try:
                self.queue_monitor = QueueMonitor(self.db_monitor)
                logger.info("Queue monitor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize queue monitor: {e}")
                self.queue_monitor = None
        elif settings.features.enable_queue_monitoring:
            logger.warning("Queue monitoring is enabled but database monitor is not available")

        self.storage_monitor = None
        if settings.features.enable_s3_monitoring:
            try:
                self.storage_monitor = StorageMonitor()
                logger.info("Storage monitor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize storage monitor: {e}")
                logger.info("S3 monitoring disabled due to initialization error")
                self.storage_monitor = None

        self.activity_monitor = None
        if settings.features.enable_activity_monitoring and self.db_monitor:
            try:
                self.activity_monitor = ActivityMonitor(self.db_monitor)
                logger.info("Activity monitor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize activity monitor: {e}")
                self.activity_monitor = None
        elif settings.features.enable_activity_monitoring:
            logger.warning("Activity monitoring is enabled but database monitor is not available")

        try:
            self.alert_manager = AlertManager()
            logger.info("Alert manager initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize alert manager: {e}")
            self.alert_manager = None

        # Background monitoring task
        self.monitoring_task = None
        self.is_monitoring = False

        self._register_tools()

    def _register_tools(self):
        """Register MCP tools."""

        @self.server.list_tools()
        async def handle_list_tools() -> list[Tool]:
            """List available monitoring tools."""
            return [
                Tool(
                    name="system_health_summary",
                    description="Get comprehensive system health summary for RS Systems",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "include_details": {
                                "type": "boolean",
                                "description": "Include detailed metrics in response",
                                "default": False
                            },
                            "components": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "enum": ["database", "api", "queue", "storage", "activity"]
                                },
                                "description": "Specific components to check",
                                "default": ["database", "api", "queue", "storage", "activity"]
                            }
                        }
                    }
                ),
                Tool(
                    name="check_database_performance",
                    description="Monitor PostgreSQL database performance for RS Systems",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "include_slow_queries": {
                                "type": "boolean",
                                "description": "Include slow query details",
                                "default": True
                            },
                            "threshold_ms": {
                                "type": "integer",
                                "description": "Slow query threshold in milliseconds",
                                "default": 500
                            }
                        }
                    }
                ),
                Tool(
                    name="monitor_repair_queue",
                    description="Monitor repair queue status and health",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "include_stuck_repairs": {
                                "type": "boolean",
                                "description": "Include details of stuck repairs",
                                "default": True
                            },
                            "include_technician_load": {
                                "type": "boolean",
                                "description": "Include technician workload analysis",
                                "default": True
                            }
                        }
                    }
                ),
                Tool(
                    name="check_api_performance",
                    description="Monitor API endpoint performance and health",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "reset_metrics": {
                                "type": "boolean",
                                "description": "Reset API metrics after check",
                                "default": False
                            },
                            "test_endpoints": {
                                "type": "boolean",
                                "description": "Perform live endpoint tests",
                                "default": True
                            }
                        }
                    }
                ),
                Tool(
                    name="analyze_s3_usage",
                    description="Monitor AWS S3 storage usage and costs",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "include_large_files": {
                                "type": "boolean",
                                "description": "Include large file analysis",
                                "default": True
                            },
                            "include_cost_estimate": {
                                "type": "boolean",
                                "description": "Include cost estimation",
                                "default": True
                            }
                        }
                    }
                ),
                Tool(
                    name="track_user_activity",
                    description="Monitor user and technician activity patterns",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "days": {
                                "type": "integer",
                                "description": "Number of days to analyze",
                                "default": 30,
                                "minimum": 1,
                                "maximum": 90
                            },
                            "include_patterns": {
                                "type": "boolean",
                                "description": "Include login pattern analysis",
                                "default": True
                            }
                        }
                    }
                ),
                Tool(
                    name="get_active_alerts",
                    description="Get current active system alerts",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "severity": {
                                "type": "string",
                                "enum": ["critical", "warning", "info"],
                                "description": "Filter by alert severity"
                            },
                            "component": {
                                "type": "string",
                                "enum": ["database", "api", "queue", "storage", "activity"],
                                "description": "Filter by component"
                            }
                        }
                    }
                ),
                Tool(
                    name="start_monitoring",
                    description="Start continuous background monitoring",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "interval_seconds": {
                                "type": "integer",
                                "description": "Monitoring interval in seconds",
                                "default": 60,
                                "minimum": 10
                            }
                        }
                    }
                ),
                Tool(
                    name="stop_monitoring",
                    description="Stop continuous background monitoring",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                ),
                Tool(
                    name="resolve_alert",
                    description="Resolve an active alert",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "alert_id": {
                                "type": "string",
                                "description": "ID of the alert to resolve"
                            }
                        },
                        "required": ["alert_id"]
                    }
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
            """Handle tool calls."""
            try:
                if name == "system_health_summary":
                    return await self._system_health_summary(arguments)
                elif name == "check_database_performance":
                    return await self._check_database_performance(arguments)
                elif name == "monitor_repair_queue":
                    return await self._monitor_repair_queue(arguments)
                elif name == "check_api_performance":
                    return await self._check_api_performance(arguments)
                elif name == "analyze_s3_usage":
                    return await self._analyze_s3_usage(arguments)
                elif name == "track_user_activity":
                    return await self._track_user_activity(arguments)
                elif name == "get_active_alerts":
                    return await self._get_active_alerts(arguments)
                elif name == "start_monitoring":
                    return await self._start_monitoring(arguments)
                elif name == "stop_monitoring":
                    return await self._stop_monitoring(arguments)
                elif name == "resolve_alert":
                    return await self._resolve_alert(arguments)
                else:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]

            except Exception as e:
                logger.error(f"Error handling tool call {name}: {e}")
                return [TextContent(type="text", text=f"Error: {str(e)}")]

    def _get_health_score(self, result: Any) -> int:
        """Safely extract health score from monitor result."""
        if not isinstance(result, dict):
            return 50  # Unknown/error state
        if result.get("error"):
            return 50  # Error state
        return 100 if not result.get("has_issues") else 75

    async def _system_health_summary(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Get comprehensive system health summary."""
        include_details = arguments.get("include_details", False)
        components = arguments.get("components", ["database", "api", "queue", "storage", "activity"])

        results = {}
        health_scores = {}

        # Run enabled monitors
        if "database" in components and self.db_monitor and settings.features.enable_database_monitoring:
            results["database"] = await self.db_monitor.monitor()
            health_scores["database"] = self._get_health_score(results["database"])

        if "api" in components and self.api_monitor and settings.features.enable_api_monitoring:
            results["api"] = await self.api_monitor.monitor()
            health_scores["api"] = self._get_health_score(results["api"])

        if "queue" in components and self.queue_monitor and settings.features.enable_queue_monitoring:
            results["queue"] = await self.queue_monitor.monitor()
            health_scores["queue"] = self._get_health_score(results["queue"])

        if "storage" in components and self.storage_monitor and settings.features.enable_s3_monitoring:
            results["storage"] = await self.storage_monitor.monitor()
            health_scores["storage"] = self._get_health_score(results["storage"])

        if "activity" in components and self.activity_monitor and settings.features.enable_activity_monitoring:
            results["activity"] = await self.activity_monitor.monitor()
            health_scores["activity"] = self._get_health_score(results["activity"])

        # Calculate overall health score
        if health_scores:
            overall_score = sum(health_scores.values()) / len(health_scores)
        else:
            overall_score = 100

        # Get active alerts
        alerts = self.alert_manager.get_active_alerts()
        alert_summary = self.alert_manager.get_alert_summary()

        # Process results for alert generation
        await self.alert_manager.process_monitor_results(results)

        # Create summary response
        summary = {
            "overall_health_score": round(overall_score, 1),
            "status": "healthy" if overall_score >= 90 else "degraded" if overall_score >= 70 else "unhealthy",
            "active_alerts_count": len(alerts),
            "components_checked": list(health_scores.keys()),
            "timestamp": datetime.now().isoformat()
        }

        if include_details:
            summary["detailed_results"] = results
            summary["alert_summary"] = alert_summary

        # Format response
        response_text = f"""# RS Systems Health Summary

**Overall Health Score:** {summary['overall_health_score']}/100 ({summary['status'].upper()})
**Active Alerts:** {summary['active_alerts_count']}
**Last Check:** {summary['timestamp']}

## Component Status:
"""

        for component, score in health_scores.items():
            status_icon = "✅" if score >= 90 else "⚠️" if score >= 70 else "❌"
            response_text += f"- {component.capitalize()}: {score}/100 {status_icon}\n"

        if alerts:
            response_text += "\n## Active Alerts:\n"
            for alert in alerts[:5]:  # Show first 5 alerts
                severity_icon = "🚨" if alert.severity == "critical" else "⚠️" if alert.severity == "warning" else "ℹ️"
                response_text += f"- {severity_icon} {alert.title} ({alert.component})\n"

        if include_details:
            response_text += f"\n## Detailed Results:\n```json\n{results}\n```"

        return [TextContent(type="text", text=response_text)]

    async def _check_database_performance(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Check database performance."""
        if not self.db_monitor:
            return [TextContent(type="text", text="Database monitoring is disabled.")]

        include_slow_queries = arguments.get("include_slow_queries", True)
        threshold_ms = arguments.get("threshold_ms", 500)

        results = await self.db_monitor.monitor()

        response = f"""# Database Performance Report

**Status:** {results.get('health', {}).get('status', 'unknown').upper()}
**Connection Pool Usage:** {results.get('connection_stats', {}).get('pool_usage_pct', 0)}%
**Active Connections:** {results.get('connection_stats', {}).get('active_connections', 0)}
**Slow Queries Found:** {len(results.get('slow_queries', []))}
"""

        if include_slow_queries and results.get('slow_queries'):
            response += "\n## Slow Queries:\n"
            for query in results['slow_queries'][:5]:
                response += f"- **Duration:** {query.get('duration_ms', 0)}ms\n"
                response += f"  **Query:** {query.get('query', 'N/A')[:100]}...\n\n"

        if results.get('issues'):
            response += "\n## Issues Detected:\n"
            for issue in results['issues']:
                response += f"- ⚠️ {issue}\n"

        return [TextContent(type="text", text=response)]

    async def _monitor_repair_queue(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Monitor repair queue status."""
        if not self.queue_monitor:
            return [TextContent(type="text", text="Queue monitoring is disabled.")]

        results = await self.queue_monitor.monitor()

        response = f"""# Repair Queue Status

**Queue Health:** {results.get('health', {}).get('status', 'unknown').upper()}
**Stuck Repairs:** {len(results.get('stuck_repairs', []))}
**Completion Rate:** {results.get('throughput', {}).get('completion_rate_pct', 0)}%

## Status Distribution:
"""

        for status, data in results.get('queue_status', {}).items():
            response += f"- **{status}:** {data.get('count', 0)} repairs (avg age: {data.get('average_age_hours', 0)}h)\n"

        if results.get('stuck_repairs'):
            response += "\n## Stuck Repairs (sample):\n"
            for repair in results['stuck_repairs'][:5]:
                response += f"- Repair #{repair.get('repair_id')} - {repair.get('status')} for {repair.get('stuck_hours')}h\n"

        return [TextContent(type="text", text=response)]

    async def _check_api_performance(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Check API performance."""
        if not self.api_monitor:
            return [TextContent(type="text", text="API monitoring is disabled.")]

        reset_metrics = arguments.get("reset_metrics", False)
        test_endpoints = arguments.get("test_endpoints", True)

        if test_endpoints:
            results = await self.api_monitor.monitor()
        else:
            results = {"metrics": self.api_monitor.calculate_metrics()}

        if reset_metrics:
            self.api_monitor.reset_metrics()

        response = f"""# API Performance Report

**Overall Error Rate:** {results.get('metrics', {}).get('summary', {}).get('error_rate_pct', 0)}%
**Average Response Time:** {results.get('metrics', {}).get('summary', {}).get('average_response_time_ms', 0)}ms
**Total Requests:** {results.get('metrics', {}).get('summary', {}).get('total_requests', 0)}

## Endpoint Details:
"""

        for endpoint, data in results.get('metrics', {}).get('endpoints', {}).items():
            response += f"- **{endpoint}:** {data.get('average_response_time_ms', 0)}ms (error rate: {data.get('error_rate_pct', 0)}%)\n"

        return [TextContent(type="text", text=response)]

    async def _analyze_s3_usage(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Analyze S3 storage usage."""
        if not self.storage_monitor:
            return [TextContent(type="text", text="S3 storage monitoring is disabled.")]

        results = await self.storage_monitor.monitor()

        if results.get('error'):
            return [TextContent(type="text", text=f"S3 monitoring error: {results['error']}")]

        response = f"""# S3 Storage Analysis

**Total Storage:** {results.get('bucket_size', {}).get('total_size_gb', 0)}GB
**Object Count:** {results.get('bucket_size', {}).get('object_count', 0)}
**Estimated Monthly Cost:** ${results.get('estimated_costs', {}).get('total_estimated', 0)}

## Storage by Category:
"""

        for prefix, data in results.get('bucket_size', {}).get('by_prefix', {}).items():
            response += f"- **{prefix}:** {data.get('size_gb', 0)}GB ({data.get('object_count', 0)} files)\n"

        if results.get('large_files'):
            response += "\n## Largest Files:\n"
            for file in results['large_files'][:5]:
                response += f"- {file.get('key', 'N/A')} - {file.get('size_mb', 0)}MB\n"

        return [TextContent(type="text", text=response)]

    async def _track_user_activity(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Track user activity."""
        if not self.activity_monitor:
            return [TextContent(type="text", text="Activity monitoring is disabled.")]

        results = await self.activity_monitor.monitor()

        response = f"""# User Activity Report

**Active Users (30d):** {results.get('user_activity', {}).get('active_users_30d', 0)}
**Active Today:** {results.get('user_activity', {}).get('active_today', 0)}
**Active Technicians Today:** {results.get('user_activity', {}).get('active_technicians_today', 0)}
**Customer Engagement Rate:** {results.get('customer_activity', {}).get('engagement_rate_pct', 0)}%

## Top Performing Technicians:
"""

        for tech in results.get('technician_performance', [])[:5]:
            response += f"- **{tech.get('username')}:** {tech.get('total_repairs')} repairs ({tech.get('completion_rate_pct')}% completion)\n"

        return [TextContent(type="text", text=response)]

    async def _get_active_alerts(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Get active alerts."""
        severity_filter = arguments.get("severity")
        component_filter = arguments.get("component")

        alerts = self.alert_manager.get_active_alerts()

        # Apply filters
        if severity_filter:
            alerts = [a for a in alerts if a.severity == severity_filter]
        if component_filter:
            alerts = [a for a in alerts if a.component == component_filter]

        if not alerts:
            return [TextContent(type="text", text="No active alerts matching criteria.")]

        response = f"# Active Alerts ({len(alerts)})\n\n"
        for alert in alerts:
            severity_icon = "🚨" if alert.severity == "critical" else "⚠️" if alert.severity == "warning" else "ℹ️"
            response += f"**{severity_icon} {alert.title}**\n"
            response += f"- Component: {alert.component}\n"
            response += f"- Message: {alert.message}\n"
            response += f"- Created: {alert.created_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            response += f"- ID: {alert.id}\n\n"

        return [TextContent(type="text", text=response)]

    async def _start_monitoring(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Start continuous monitoring."""
        if self.is_monitoring:
            return [TextContent(type="text", text="Monitoring is already running.")]

        interval = arguments.get("interval_seconds", 60)
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop(interval))

        return [TextContent(type="text", text=f"Started continuous monitoring with {interval}s interval.")]

    async def _stop_monitoring(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Stop continuous monitoring."""
        if not self.is_monitoring:
            return [TextContent(type="text", text="Monitoring is not running.")]

        self.is_monitoring = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            self.monitoring_task = None

        return [TextContent(type="text", text="Stopped continuous monitoring.")]

    async def _resolve_alert(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """Resolve an alert."""
        alert_id = arguments.get("alert_id")
        if not alert_id:
            return [TextContent(type="text", text="Alert ID is required.")]

        await self.alert_manager.resolve_alert(alert_id)
        return [TextContent(type="text", text=f"Alert {alert_id} has been resolved.")]

    async def _monitoring_loop(self, interval: int):
        """Background monitoring loop."""
        logger.info(f"Starting monitoring loop with {interval}s interval")

        while self.is_monitoring:
            try:
                # Run all monitors
                results = {}

                if self.db_monitor and settings.features.enable_database_monitoring:
                    results["database"] = await self.db_monitor.monitor()

                if self.api_monitor and settings.features.enable_api_monitoring:
                    results["api"] = await self.api_monitor.monitor()

                if self.queue_monitor and settings.features.enable_queue_monitoring:
                    results["queue"] = await self.queue_monitor.monitor()

                if self.storage_monitor and settings.features.enable_s3_monitoring:
                    results["storage"] = await self.storage_monitor.monitor()

                if self.activity_monitor and settings.features.enable_activity_monitoring:
                    results["activity"] = await self.activity_monitor.monitor()

                # Process results for alerts
                await self.alert_manager.process_monitor_results(results)

                logger.debug("Monitoring cycle completed")

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")

            # Wait for next cycle
            await asyncio.sleep(interval)

        logger.info("Monitoring loop stopped")

    async def cleanup(self):
        """Cleanup resources."""
        if self.is_monitoring:
            await self._stop_monitoring({})

        if self.db_monitor:
            self.db_monitor.close()

        logger.info("RS Health Monitor server cleanup completed")


async def main():
    """Main entry point for the MCP server."""
    if not settings.validate():
        logger.error("Configuration validation failed")
        return

    server = RSHealthMonitorServer()
    logger.info(f"Starting RS Systems Health Monitor MCP Server v{settings.mcp.server_version}")

    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name=settings.mcp.server_name,
                    server_version=settings.mcp.server_version,
                    capabilities=ServerCapabilities(
                        tools=ToolsCapability()
                    )
                )
            )
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await server.cleanup()


if __name__ == "__main__":
    asyncio.run(main())