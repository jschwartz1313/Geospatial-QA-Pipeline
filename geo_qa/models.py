"""Pydantic models for configuration and QA results."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class QAStatus(str, Enum):
    """Status values for QA checks."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    NA = "NA"


class LayerConfig(BaseModel):
    """Configuration for a single ArcGIS REST layer to validate."""

    layer_name: str
    service_url: str  # Using str instead of HttpUrl for flexibility with malformed URLs
    expected_geometry: str = "Unknown"  # Point, Line, Polygon, Unknown
    owner: str | None = None
    notes: str | None = None


class RuleResult(BaseModel):
    """Result from a single QA rule check."""

    rule_name: str
    status: QAStatus
    message: str
    evidence: dict[str, Any] = {}


class LayerQAResult(BaseModel):
    """Complete QA results for a single layer."""

    # Layer identification
    layer_name: str
    service_url: str

    # Overall status
    overall_status: QAStatus = QAStatus.FAIL
    reachable: bool = False

    # Metadata fields
    count_estimate: int | None = None
    geometry_type_reported: str | None = None
    expected_geometry: str = "Unknown"
    max_record_count: int | None = None
    metadata_score: int = 0

    # Detailed check results
    pagination_ok: str = "NA"  # PASS/WARN/FAIL/NA
    null_fields_over_80pct: int = 0
    pct_invalid_geometry: float = 0.0
    pct_empty_geometry: float = 0.0
    last_edit_date: str | None = None
    format_supported: str = "unknown"  # geojson, pjson, both, unknown
    spatial_reference_wkid: int | None = None

    # Rule results and errors
    rule_results: list[RuleResult] = []
    errors: list[str] = []

    # Top issues summary (for CSV export)
    top_issues: str = ""

    # Raw metadata (for JSON export)
    raw_metadata: dict[str, Any] | None = None

    def compute_top_issues(self) -> str:
        """Generate semicolon-separated string of top issues."""
        issues = []
        for result in self.rule_results:
            if result.status in [QAStatus.FAIL, QAStatus.WARN]:
                issues.append(f"{result.rule_name}: {result.message}")
        return "; ".join(issues[:3])  # Limit to top 3 issues

    def aggregate_status(self) -> QAStatus:
        """Compute overall status from individual rule results."""
        if not self.reachable:
            return QAStatus.FAIL

        statuses = [r.status for r in self.rule_results]

        if QAStatus.FAIL in statuses:
            return QAStatus.FAIL
        elif QAStatus.WARN in statuses:
            return QAStatus.WARN
        else:
            return QAStatus.PASS


class PipelineRun(BaseModel):
    """Metadata about a QA pipeline run."""

    timestamp: datetime
    total_layers: int
    pass_count: int = 0
    warn_count: int = 0
    fail_count: int = 0
    config_file: str
    output_dir: str
