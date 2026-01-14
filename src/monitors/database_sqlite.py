"""SQLite database monitoring for RS Systems."""

import sqlite3
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from contextlib import contextmanager
import os

from ..config import settings
from ..models.django_models import HealthCheckResult, SystemMetrics

logger = logging.getLogger(__name__)


class SQLiteMonitor:
    """Monitor SQLite database performance and health."""

    def __init__(self):
        self.config = settings.database
        self.thresholds = settings.thresholds
        self.db_path = self._extract_db_path()
        self.connection = None
        self._initialize_connection()

    def _extract_db_path(self) -> str:
        """Extract database path from SQLite URL."""
        url = self.config.database_url
        if url.startswith('sqlite:///'):
            return url.replace('sqlite:///', '')
        elif url.startswith('sqlite://'):
            return url.replace('sqlite://', '')
        return url

    def _initialize_connection(self):
        """Initialize SQLite connection."""
        try:
            if not os.path.exists(self.db_path):
                logger.error(f"SQLite database not found at: {self.db_path}")
                return

            # Test connection
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()

            logger.info(f"SQLite connection initialized for: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize SQLite connection: {e}")

    @contextmanager
    def get_connection(self):
        """Get a database connection compatible with PostgreSQL-style usage."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            # Create a wrapper that makes cursor compatible with 'with' statement
            class CursorContextManager:
                def __init__(self, connection):
                    self.connection = connection
                    # Initialize cursor immediately so methods work without 'with' block
                    self.cursor = self.connection.cursor()

                def cursor(self):
                    return self

                def __enter__(self):
                    return self.cursor

                def __exit__(self, exc_type, exc_val, exc_tb):
                    if self.cursor:
                        self.cursor.close()
                        self.cursor = None

                def execute(self, *args, **kwargs):
                    return self.cursor.execute(*args, **kwargs)

                def fetchall(self):
                    return self.cursor.fetchall()

                def fetchone(self):
                    return self.cursor.fetchone()

            # Wrap the connection
            class ConnectionWrapper:
                def __init__(self, conn):
                    self._conn = conn

                def cursor(self):
                    return CursorContextManager(self._conn)

                def close(self):
                    self._conn.close()

                def commit(self):
                    self._conn.commit()

                def rollback(self):
                    self._conn.rollback()

            yield ConnectionWrapper(conn)
        except Exception as e:
            logger.error(f"SQLite connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    async def check_health(self) -> HealthCheckResult:
        """Perform SQLite database health check."""
        start_time = datetime.now()
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Simple health check query
                cursor.execute("SELECT 1")
                cursor.fetchone()

                # Get database size
                db_size = os.path.getsize(self.db_path) / (1024 * 1024)  # Size in MB

                # Check if database is locked
                cursor.execute("PRAGMA database_list")
                db_info = cursor.fetchall()

            response_time = (datetime.now() - start_time).total_seconds() * 1000

            return HealthCheckResult(
                component="database",
                status="healthy",
                message="SQLite database is responding normally",
                response_time_ms=response_time,
                details={
                    "database_path": self.db_path,
                    "database_size_mb": round(db_size, 2),
                    "response_time_ms": response_time,
                    "database_info": [dict(row) for row in db_info] if db_info else []
                }
            )
        except Exception as e:
            logger.error(f"SQLite health check failed: {e}")
            return HealthCheckResult(
                component="database",
                status="unhealthy",
                message=f"SQLite health check failed: {str(e)}",
                response_time_ms=None
            )

    async def get_slow_queries(self, threshold_ms: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get slow queries - not directly available in SQLite."""
        # SQLite doesn't have built-in slow query logging like PostgreSQL
        # Return empty list or implement custom query timing if needed
        logger.info("Slow query monitoring not available for SQLite")
        return []

    async def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Get basic database stats
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]

                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]

                cursor.execute("PRAGMA cache_size")
                cache_size = cursor.fetchone()[0]

                # Get table information
                cursor.execute("""
                    SELECT name, type
                    FROM sqlite_master
                    WHERE type IN ('table', 'index')
                """)
                schema_objects = cursor.fetchall()

                return {
                    "database_path": self.db_path,
                    "database_size_bytes": page_count * page_size,
                    "page_count": page_count,
                    "page_size": page_size,
                    "cache_size": cache_size,
                    "tables": [row[0] for row in schema_objects if row[1] == 'table'],
                    "indexes": [row[0] for row in schema_objects if row[1] == 'index']
                }
        except Exception as e:
            logger.error(f"Failed to get connection stats: {e}")
            return {"error": str(e)}

    async def get_table_sizes(self) -> List[Dict[str, Any]]:
        """Get sizes of all tables in the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Get all tables
                cursor.execute("""
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                    AND name NOT LIKE 'sqlite_%'
                """)
                tables = cursor.fetchall()

                table_stats = []
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]

                    table_stats.append({
                        "table_name": table_name,
                        "row_count": row_count
                    })

                return sorted(table_stats, key=lambda x: x["row_count"], reverse=True)
        except Exception as e:
            logger.error(f"Failed to get table sizes: {e}")
            return []

    async def check_locks(self) -> List[Dict[str, Any]]:
        """Check for database locks."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Check if database is in WAL mode
                cursor.execute("PRAGMA journal_mode")
                journal_mode = cursor.fetchone()[0]

                # Check lock status
                cursor.execute("PRAGMA locking_mode")
                locking_mode = cursor.fetchone()[0]

                return [{
                    "journal_mode": journal_mode,
                    "locking_mode": locking_mode,
                    "message": "SQLite lock monitoring is limited compared to PostgreSQL"
                }]
        except Exception as e:
            logger.error(f"Failed to check locks: {e}")
            return []

    async def get_performance_metrics(self) -> SystemMetrics:
        """Get database performance metrics."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Get basic metrics
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]

                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]

                # Count tables
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                table_count = cursor.fetchone()[0]

                return SystemMetrics(
                    component="database",
                    metrics={
                        "database_size_mb": round((page_count * page_size) / (1024 * 1024), 2),
                        "table_count": table_count,
                        "page_count": page_count,
                        "page_size": page_size
                    },
                    timestamp=datetime.now()
                )
        except Exception as e:
            logger.error(f"Failed to get performance metrics: {e}")
            return SystemMetrics(
                component="database",
                metrics={"error": str(e)},
                timestamp=datetime.now()
            )