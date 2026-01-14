"""Django model definitions for RS Systems monitoring."""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict


class RepairStatus(str, Enum):
    """Repair status choices matching Django model."""

    REQUESTED = "REQUESTED"
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    DENIED = "DENIED"


class DamageType(str, Enum):
    """Damage type choices for repairs."""

    CHIP = "Chip"
    CRACK = "Crack"
    STAR_BREAK = "Star Break"
    BULLS_EYE = "Bull's Eye"
    COMBINATION_BREAK = "Combination Break"
    HALF_MOON = "Half-Moon"
    OTHER = "Other"


class CustomerModel(BaseModel):
    """Customer model representation."""

    id: int
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    email_verified: bool = False
    email_verified_at: Optional[datetime] = None
    phone_verified: bool = False
    phone_verified_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class TechnicianModel(BaseModel):
    """Technician model representation."""

    id: int
    user_id: int
    phone_number: Optional[str] = None
    expertise: Optional[str] = None
    is_active: bool = True
    # Verification fields
    email_verified: bool = False
    email_verified_at: Optional[datetime] = None
    phone_verified: bool = False
    phone_verified_at: Optional[datetime] = None
    # Manager fields
    is_manager: bool = False
    approval_limit: Optional[float] = None
    can_assign_work: bool = False
    can_override_pricing: bool = False
    # Performance fields
    repairs_completed: int = 0
    average_repair_time: Optional[str] = None
    customer_rating: Optional[float] = None
    working_hours: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class RepairModel(BaseModel):
    """Repair model representation."""

    id: int
    technician_id: int
    customer_id: Optional[int] = None
    unit_number: str
    repair_date: datetime
    description: Optional[str] = None
    queue_status: RepairStatus
    damage_type: DamageType
    cost: Optional[float] = None
    cost_override: Optional[float] = None
    override_reason: Optional[str] = None
    drilled_before_repair: bool = False
    windshield_temperature: Optional[float] = None
    resin_viscosity: Optional[str] = None
    # Photo fields
    customer_submitted_photo: Optional[str] = None
    damage_photo_before: Optional[str] = None
    damage_photo_after: Optional[str] = None
    additional_photos: List[str] = Field(default_factory=list)
    # Notes
    customer_notes: Optional[str] = None
    technician_notes: Optional[str] = None
    # Batch repair tracking
    repair_batch_id: Optional[str] = None
    break_number: Optional[int] = None
    total_breaks_in_batch: Optional[int] = None
    is_multi_break_estimate: bool = False
    # Timestamps
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class UserModel(BaseModel):
    """Django User model representation."""

    id: int
    username: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_active: bool = True
    is_staff: bool = False
    is_superuser: bool = False
    last_login: Optional[datetime] = None
    date_joined: datetime

    model_config = ConfigDict(from_attributes=True)


class RewardModel(BaseModel):
    """Reward model representation - tracks customer points balance."""

    id: int
    customer_user_id: int
    points: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class SystemMetrics(BaseModel):
    """System-wide metrics for monitoring."""

    # Repair metrics
    total_repairs: int = 0
    pending_repairs: int = 0
    in_progress_repairs: int = 0
    completed_repairs_today: int = 0
    average_repair_time_hours: float = 0.0
    stuck_repairs: List[int] = Field(default_factory=list)

    # Customer metrics
    total_customers: int = 0
    active_customers_30d: int = 0
    new_customers_today: int = 0

    # Technician metrics
    total_technicians: int = 0
    active_technicians_today: int = 0
    inactive_technicians: List[int] = Field(default_factory=list)

    # Database metrics
    db_connection_count: int = 0
    db_connection_pool_usage_pct: float = 0.0
    slow_query_count: int = 0
    slow_queries: List[Dict[str, Any]] = Field(default_factory=list)

    # API metrics
    api_request_count: int = 0
    api_error_count: int = 0
    api_error_rate_pct: float = 0.0
    api_avg_response_time_ms: float = 0.0
    api_endpoints_health: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    # Storage metrics
    s3_total_size_gb: float = 0.0
    s3_object_count: int = 0
    s3_estimated_cost_usd: float = 0.0
    s3_large_files: List[Dict[str, Any]] = Field(default_factory=list)

    # System health
    overall_health_score: float = 100.0
    health_status: str = "HEALTHY"
    active_alerts: List[Dict[str, Any]] = Field(default_factory=list)
    last_check_timestamp: datetime = Field(default_factory=datetime.now)


class HealthCheckResult(BaseModel):
    """Health check result for a specific component."""

    component: str
    status: str  # "healthy", "degraded", "unhealthy"
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.now)
    response_time_ms: Optional[float] = None


class Alert(BaseModel):
    """Alert model for system notifications."""

    id: str
    severity: str  # "info", "warning", "critical"
    component: str
    title: str
    message: str
    threshold_value: Optional[float] = None
    actual_value: Optional[float] = None
    created_at: datetime = Field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    is_resolved: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MonitoringQuery(BaseModel):
    """Query parameters for monitoring requests."""

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    component: Optional[str] = None
    metric_type: Optional[str] = None
    limit: int = 100
    offset: int = 0
    include_details: bool = False