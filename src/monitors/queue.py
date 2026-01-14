"""Repair queue monitoring for RS Systems."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import psycopg2

from ..config import settings
from ..models.django_models import RepairStatus, HealthCheckResult

logger = logging.getLogger(__name__)


class QueueMonitor:
    """Monitor repair queue health and performance."""

    def __init__(self, db_monitor):
        self.db_monitor = db_monitor  # Reuse database connection from DatabaseMonitor
        self.thresholds = settings.thresholds

    async def get_queue_status(self) -> Dict[str, Any]:
        """Get current repair queue status."""
        # Check database type
        is_sqlite = 'sqlite' in settings.database.database_url.lower()

        if is_sqlite:
            # SQLite-compatible query
            query = """
            SELECT
                queue_status,
                COUNT(*) as count,
                AVG((julianday('now') - julianday(repair_date)) * 24) as avg_age_hours,
                MAX((julianday('now') - julianday(repair_date)) * 24) as max_age_hours,
                MIN((julianday('now') - julianday(repair_date)) * 24) as min_age_hours
            FROM technician_portal_repair
            WHERE queue_status != 'COMPLETED'
            GROUP BY queue_status
            """
        else:
            # PostgreSQL query
            query = """
            SELECT
                queue_status,
                COUNT(*) as count,
                AVG(EXTRACT(EPOCH FROM (now() - created_at)) / 3600) as avg_age_hours,
                MAX(EXTRACT(EPOCH FROM (now() - created_at)) / 3600) as max_age_hours,
                MIN(EXTRACT(EPOCH FROM (now() - created_at)) / 3600) as min_age_hours
            FROM technician_portal_repair
            WHERE queue_status != 'COMPLETED'
            GROUP BY queue_status
            """

        queue_status = {}
        try:
            with self.db_monitor.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    for row in rows:
                        status = row[0]
                        queue_status[status] = {
                            "count": row[1],
                            "average_age_hours": round(row[2] or 0, 2),
                            "max_age_hours": round(row[3] or 0, 2),
                            "min_age_hours": round(row[4] or 0, 2)
                        }
        except Exception as e:
            logger.error(f"Failed to get queue status: {e}")

        return queue_status

    async def get_stuck_repairs(self) -> List[Dict[str, Any]]:
        """Identify repairs that have been stuck in the same status for too long."""
        threshold_hours = self.thresholds.queue_stuck_hours
        is_sqlite = 'sqlite' in settings.database.database_url.lower()

        if is_sqlite:
            # SQLite-compatible query
            query = f"""
            SELECT
                r.id,
                r.unit_number,
                r.queue_status,
                r.repair_date,
                r.repair_date as updated_at,
                c.name as customer_name,
                t.id as technician_id,
                u.username as technician_name,
                (julianday('now') - julianday(r.repair_date)) * 24 as stuck_hours
            FROM technician_portal_repair r
            LEFT JOIN core_customer c ON r.customer_id = c.id
            LEFT JOIN technician_portal_technician t ON r.technician_id = t.id
            LEFT JOIN auth_user u ON t.user_id = u.id
            WHERE r.queue_status NOT IN ('COMPLETED', 'DENIED')
                AND r.repair_date < datetime('now', '-{threshold_hours} hours')
            ORDER BY r.repair_date ASC
            LIMIT 50
            """
            # For SQLite, we don't need parameters
            query_params = None
        else:
            # PostgreSQL query
            query = """
            SELECT
                r.id,
                r.unit_number,
                r.queue_status,
                r.repair_date,
                r.repair_date as updated_at,
                c.name as customer_name,
                t.id as technician_id,
                u.username as technician_name,
                EXTRACT(EPOCH FROM (now() - r.repair_date)) / 3600 as stuck_hours
            FROM technician_portal_repair r
            LEFT JOIN core_customer c ON r.customer_id = c.id
            LEFT JOIN technician_portal_technician t ON r.technician_id = t.id
            LEFT JOIN auth_user u ON t.user_id = u.id
            WHERE r.queue_status NOT IN ('COMPLETED', 'DENIED')
                AND r.repair_date < now() - interval '%s hours'
            ORDER BY r.repair_date ASC
            LIMIT 50
            """
            query_params = (threshold_hours,)

        stuck_repairs = []
        try:
            with self.db_monitor.get_connection() as conn:
                with conn.cursor() as cursor:
                    if query_params:
                        cursor.execute(query, query_params)
                    else:
                        cursor.execute(query)
                    rows = cursor.fetchall()

                    for row in rows:
                        # Handle dates - SQLite returns strings, PostgreSQL returns datetime
                        created_at = row[3]
                        updated_at = row[4]
                        if created_at and hasattr(created_at, 'isoformat'):
                            created_at = created_at.isoformat()
                        if updated_at and hasattr(updated_at, 'isoformat'):
                            updated_at = updated_at.isoformat()

                        stuck_repairs.append({
                            "repair_id": row[0],
                            "unit_number": row[1],
                            "status": row[2],
                            "created_at": created_at,
                            "updated_at": updated_at,
                            "customer_name": row[5],
                            "technician_id": row[6],
                            "technician_name": row[7],
                            "stuck_hours": round(row[8] or 0, 2)
                        })
        except Exception as e:
            logger.error(f"Failed to get stuck repairs: {e}")

        return stuck_repairs

    async def get_processing_times(self) -> Dict[str, Any]:
        """Calculate average processing times between status transitions.

        Note: Without created_at/updated_at columns, we return counts of completed
        repairs and age since repair_date instead of true processing times.
        """
        is_sqlite = 'sqlite' in settings.database.database_url.lower()

        if is_sqlite:
            # SQLite-compatible query - use repair_date since no created_at/updated_at
            query = """
            SELECT
                COUNT(*) as completed_count,
                AVG((julianday('now') - julianday(repair_date)) * 24) as avg_age_hours,
                MIN((julianday('now') - julianday(repair_date)) * 24) as min_age_hours,
                MAX((julianday('now') - julianday(repair_date)) * 24) as max_age_hours
            FROM technician_portal_repair
            WHERE queue_status = 'COMPLETED'
                AND repair_date > datetime('now', '-30 days')
            """
        else:
            # PostgreSQL query - use repair_date since no created_at/updated_at
            query = """
            SELECT
                COUNT(*) as completed_count,
                AVG(EXTRACT(EPOCH FROM (now() - repair_date)) / 3600) as avg_age_hours,
                MIN(EXTRACT(EPOCH FROM (now() - repair_date)) / 3600) as min_age_hours,
                MAX(EXTRACT(EPOCH FROM (now() - repair_date)) / 3600) as max_age_hours
            FROM technician_portal_repair
            WHERE queue_status = 'COMPLETED'
                AND repair_date > now() - interval '30 days'
            """

        processing_times = {}
        try:
            with self.db_monitor.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    row = cursor.fetchone()

                    if row:
                        processing_times = {
                            "completed_count_30d": row[0] or 0,
                            "average_age_hours": round(row[1] or 0, 2),
                            "min_age_hours": round(row[2] or 0, 2),
                            "max_age_hours": round(row[3] or 0, 2),
                            "note": "Processing times unavailable - schema lacks created_at/updated_at"
                        }
        except Exception as e:
            logger.error(f"Failed to get processing times: {e}")

        return processing_times

    async def get_technician_queue_load(self) -> List[Dict[str, Any]]:
        """Get queue load per technician."""
        is_sqlite = 'sqlite' in settings.database.database_url.lower()

        if is_sqlite:
            # SQLite-compatible query - use repair_date since no updated_at column
            query = """
            SELECT
                t.id as technician_id,
                u.username as technician_name,
                COUNT(r.id) as total_repairs,
                SUM(CASE WHEN r.queue_status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN r.queue_status = 'PENDING' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN r.queue_status = 'APPROVED' THEN 1 ELSE 0 END) as approved,
                AVG(CASE
                    WHEN r.queue_status = 'IN_PROGRESS' THEN
                        (julianday('now') - julianday(r.repair_date)) * 24
                    ELSE NULL
                END) as avg_in_progress_hours
            FROM technician_portal_technician t
            JOIN auth_user u ON t.user_id = u.id
            LEFT JOIN technician_portal_repair r ON t.id = r.technician_id
                AND r.queue_status NOT IN ('COMPLETED', 'DENIED')
            GROUP BY t.id, u.username
            HAVING COUNT(r.id) > 0
            ORDER BY total_repairs DESC
            """
        else:
            # PostgreSQL query - use repair_date since no updated_at column
            query = """
            SELECT
                t.id as technician_id,
                u.username as technician_name,
                COUNT(r.id) as total_repairs,
                SUM(CASE WHEN r.queue_status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN r.queue_status = 'PENDING' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN r.queue_status = 'APPROVED' THEN 1 ELSE 0 END) as approved,
                AVG(CASE
                    WHEN r.queue_status = 'IN_PROGRESS' THEN
                        EXTRACT(EPOCH FROM (now() - r.repair_date)) / 3600
                    ELSE NULL
                END) as avg_in_progress_hours
            FROM technician_portal_technician t
            JOIN auth_user u ON t.user_id = u.id
            LEFT JOIN technician_portal_repair r ON t.id = r.technician_id
                AND r.queue_status NOT IN ('COMPLETED', 'DENIED')
            GROUP BY t.id, u.username
            HAVING COUNT(r.id) > 0
            ORDER BY total_repairs DESC
            """

        technician_load = []
        try:
            with self.db_monitor.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    for row in rows:
                        technician_load.append({
                            "technician_id": row[0],
                            "technician_name": row[1],
                            "total_active_repairs": row[2],
                            "in_progress": row[3],
                            "pending": row[4],
                            "approved": row[5],
                            "avg_in_progress_hours": round(row[6] or 0, 2) if row[6] else None
                        })
        except Exception as e:
            logger.error(f"Failed to get technician queue load: {e}")

        return technician_load

    async def get_queue_throughput(self) -> Dict[str, Any]:
        """Calculate queue throughput metrics."""
        is_sqlite = 'sqlite' in settings.database.database_url.lower()

        if is_sqlite:
            # SQLite-compatible query (no FILTER clause, use CASE WHEN instead)
            query = """
            WITH daily_stats AS (
                SELECT
                    DATE(repair_date) as date,
                    SUM(CASE WHEN queue_status = 'REQUESTED' THEN 1 ELSE 0 END) as new_requests,
                    SUM(CASE WHEN queue_status = 'COMPLETED' THEN 1 ELSE 0 END) as completed_repairs
                FROM technician_portal_repair
                WHERE repair_date > datetime('now', '-7 days')
                GROUP BY DATE(repair_date)
            )
            SELECT
                AVG(new_requests) as avg_daily_requests,
                AVG(completed_repairs) as avg_daily_completions,
                SUM(new_requests) as total_requests_7d,
                SUM(completed_repairs) as total_completions_7d
            FROM daily_stats
            """
        else:
            # PostgreSQL query - use repair_date since no created_at column
            query = """
            WITH daily_stats AS (
                SELECT
                    DATE(repair_date) as date,
                    COUNT(*) FILTER (WHERE queue_status = 'REQUESTED') as new_requests,
                    COUNT(*) FILTER (WHERE queue_status = 'COMPLETED') as completed_repairs
                FROM technician_portal_repair
                WHERE repair_date > now() - interval '7 days'
                GROUP BY DATE(repair_date)
            )
            SELECT
                AVG(new_requests) as avg_daily_requests,
                AVG(completed_repairs) as avg_daily_completions,
                SUM(new_requests) as total_requests_7d,
                SUM(completed_repairs) as total_completions_7d
            FROM daily_stats
            """

        throughput = {}
        try:
            with self.db_monitor.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    row = cursor.fetchone()

                    if row:
                        throughput = {
                            "avg_daily_requests": round(row[0] or 0, 2),
                            "avg_daily_completions": round(row[1] or 0, 2),
                            "total_requests_7d": row[2] or 0,
                            "total_completions_7d": row[3] or 0,
                            "completion_rate_pct": round(
                                (row[3] / row[2] * 100) if row[2] and row[2] > 0 else 0, 2
                            )
                        }
        except Exception as e:
            logger.error(f"Failed to get queue throughput: {e}")

        return throughput

    async def check_health(self) -> HealthCheckResult:
        """Check overall queue health."""
        try:
            queue_status = await self.get_queue_status()
            stuck_repairs = await self.get_stuck_repairs()

            total_pending = sum(
                status_data["count"]
                for status, status_data in queue_status.items()
                if status in ["PENDING", "REQUESTED", "APPROVED"]
            )

            if stuck_repairs:
                status = "degraded"
                message = f"Found {len(stuck_repairs)} stuck repairs"
            elif total_pending > self.thresholds.pending_repairs:
                status = "degraded"
                message = f"High pending repair count: {total_pending}"
            else:
                status = "healthy"
                message = "Queue is processing normally"

            return HealthCheckResult(
                component="queue",
                status=status,
                message=message,
                details={
                    "total_pending": total_pending,
                    "stuck_repairs_count": len(stuck_repairs)
                }
            )
        except Exception as e:
            logger.error(f"Queue health check failed: {e}")
            return HealthCheckResult(
                component="queue",
                status="unhealthy",
                message=f"Queue health check failed: {str(e)}"
            )

    def check_thresholds(
        self,
        queue_status: Dict[str, Any],
        stuck_repairs: List[Dict[str, Any]],
        throughput: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Check if queue metrics exceed thresholds."""
        issues = []

        # Check for stuck repairs
        if stuck_repairs:
            issues.append({
                "type": "stuck_repairs",
                "severity": "warning",
                "message": f"Found {len(stuck_repairs)} repairs stuck for over {self.thresholds.queue_stuck_hours} hours",
                "count": len(stuck_repairs),
                "repairs": [r["repair_id"] for r in stuck_repairs[:5]]  # First 5 IDs
            })

        # Check queue depth
        total_queue = sum(status_data["count"] for status_data in queue_status.values())
        if total_queue > self.thresholds.queue_depth:
            issues.append({
                "type": "high_queue_depth",
                "severity": "warning",
                "message": f"Queue depth ({total_queue}) exceeds threshold ({self.thresholds.queue_depth})",
                "value": total_queue,
                "threshold": self.thresholds.queue_depth
            })

        # Check pending repairs
        pending_count = queue_status.get("PENDING", {}).get("count", 0)
        if pending_count > self.thresholds.pending_repairs:
            issues.append({
                "type": "high_pending_count",
                "severity": "warning",
                "message": f"Pending repairs ({pending_count}) exceeds threshold ({self.thresholds.pending_repairs})",
                "value": pending_count,
                "threshold": self.thresholds.pending_repairs
            })

        # Check completion rate
        if throughput.get("completion_rate_pct", 100) < 50:
            issues.append({
                "type": "low_completion_rate",
                "severity": "critical",
                "message": f"Low completion rate: {throughput.get('completion_rate_pct', 0)}%",
                "value": throughput.get("completion_rate_pct", 0)
            })

        return issues

    async def monitor(self) -> Dict[str, Any]:
        """Perform comprehensive queue monitoring."""
        try:
            # Run all monitoring tasks concurrently
            tasks = [
                self.get_queue_status(),
                self.get_stuck_repairs(),
                self.get_processing_times(),
                self.get_technician_queue_load(),
                self.get_queue_throughput(),
                self.check_health()
            ]

            (
                queue_status,
                stuck_repairs,
                processing_times,
                technician_load,
                throughput,
                health
            ) = await asyncio.gather(*tasks)

            # Check thresholds
            issues = self.check_thresholds(queue_status, stuck_repairs, throughput)

            return {
                "health": health.dict(),
                "queue_status": queue_status,
                "stuck_repairs": stuck_repairs,
                "processing_times": processing_times,
                "technician_load": technician_load,
                "throughput": throughput,
                "issues": issues,
                "has_issues": len(issues) > 0,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Queue monitoring failed: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }