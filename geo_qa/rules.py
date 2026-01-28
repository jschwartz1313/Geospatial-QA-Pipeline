"""QA rules for validating ArcGIS REST layers."""

import logging
from datetime import datetime

import pandas as pd
from shapely.geometry import shape

from geo_qa.arcgis import ArcGISClient
from geo_qa.models import LayerConfig, QAStatus, RuleResult

logger = logging.getLogger(__name__)


def check_reachability(metadata: dict | None, **kwargs) -> RuleResult:
    """
    Check if layer metadata can be fetched.

    Args:
        metadata: Layer metadata dict (None if fetch failed)

    Returns:
        RuleResult with PASS if metadata exists, FAIL otherwise
    """
    try:
        if metadata is not None:
            return RuleResult(
                rule_name="reachability",
                status=QAStatus.PASS,
                message="Service is reachable and returns metadata",
                evidence={"metadata_exists": True},
            )
        else:
            return RuleResult(
                rule_name="reachability",
                status=QAStatus.FAIL,
                message="Cannot fetch metadata from service",
                evidence={"metadata_exists": False},
            )
    except Exception as e:
        logger.error(f"Error in check_reachability: {e}")
        return RuleResult(
            rule_name="reachability",
            status=QAStatus.FAIL,
            message=f"Exception during reachability check: {str(e)}",
            evidence={"error": str(e)},
        )


def check_queryability(count: int | None, metadata: dict | None, **kwargs) -> RuleResult:
    """
    Check if layer can be queried for feature count.

    Args:
        count: Feature count (None if query failed)
        metadata: Layer metadata

    Returns:
        RuleResult with PASS if queryable, WARN if count failed but metadata exists
    """
    try:
        if count is not None:
            return RuleResult(
                rule_name="queryability",
                status=QAStatus.PASS,
                message=f"Layer is queryable ({count} features)",
                evidence={"count": count, "queryable": True},
            )
        elif metadata is not None:
            return RuleResult(
                rule_name="queryability",
                status=QAStatus.WARN,
                message="Metadata exists but query endpoint failed",
                evidence={"count": None, "queryable": False},
            )
        else:
            return RuleResult(
                rule_name="queryability",
                status=QAStatus.FAIL,
                message="Layer is not queryable",
                evidence={"count": None, "queryable": False},
            )
    except Exception as e:
        logger.error(f"Error in check_queryability: {e}")
        return RuleResult(
            rule_name="queryability",
            status=QAStatus.FAIL,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e)},
        )


def check_metadata_completeness(metadata: dict | None, **kwargs) -> RuleResult:
    """
    Score metadata completeness (0-100).

    Scoring:
    - Has description (10 pts)
    - Has geometryType (20 pts)
    - Has extent (20 pts)
    - Has fields with >=3 items (20 pts)
    - Has capabilities (10 pts)
    - Has maxRecordCount (10 pts)
    - Has pagination support (10 pts)

    Args:
        metadata: Layer metadata

    Returns:
        RuleResult with score and status (PASS >= 70, WARN >= 40, FAIL < 40)
    """
    try:
        if metadata is None:
            return RuleResult(
                rule_name="metadata_completeness",
                status=QAStatus.FAIL,
                message="No metadata available",
                evidence={"score": 0, "components": {}},
            )

        score = 0
        components = {}

        # Description (10 pts)
        if metadata.get("description"):
            score += 10
            components["description"] = True
        else:
            components["description"] = False

        # Geometry type (20 pts)
        if metadata.get("geometryType"):
            score += 20
            components["geometryType"] = True
        else:
            components["geometryType"] = False

        # Extent (20 pts)
        if metadata.get("extent"):
            score += 20
            components["extent"] = True
        else:
            components["extent"] = False

        # Fields (20 pts)
        fields = metadata.get("fields", [])
        if len(fields) >= 3:
            score += 20
            components["fields"] = len(fields)
        else:
            components["fields"] = len(fields)

        # Capabilities (10 pts)
        if metadata.get("capabilities"):
            score += 10
            components["capabilities"] = True
        else:
            components["capabilities"] = False

        # Max record count (10 pts)
        if metadata.get("maxRecordCount"):
            score += 10
            components["maxRecordCount"] = True
        else:
            components["maxRecordCount"] = False

        # Pagination support (10 pts)
        adv_query = metadata.get("advancedQueryCapabilities", {})
        if adv_query.get("supportsPagination") or adv_query.get("supportsQueryWithPagination"):
            score += 10
            components["pagination"] = True
        else:
            components["pagination"] = False

        # Determine status
        if score >= 70:
            status = QAStatus.PASS
            message = f"Metadata is complete (score: {score}/100)"
        elif score >= 40:
            status = QAStatus.WARN
            message = f"Metadata is partially complete (score: {score}/100)"
        else:
            status = QAStatus.FAIL
            message = f"Metadata is incomplete (score: {score}/100)"

        return RuleResult(
            rule_name="metadata_completeness",
            status=status,
            message=message,
            evidence={"score": score, "components": components},
        )

    except Exception as e:
        logger.error(f"Error in check_metadata_completeness: {e}")
        return RuleResult(
            rule_name="metadata_completeness",
            status=QAStatus.FAIL,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e), "score": 0},
        )


def check_record_availability(count: int | None, **kwargs) -> RuleResult:
    """
    Check if layer has any records.

    Args:
        count: Feature count

    Returns:
        RuleResult with WARN if count is 0, PASS otherwise
    """
    try:
        if count is None:
            return RuleResult(
                rule_name="record_availability",
                status=QAStatus.WARN,
                message="Cannot determine record count",
                evidence={"count": None},
            )
        elif count == 0:
            return RuleResult(
                rule_name="record_availability",
                status=QAStatus.WARN,
                message="Layer contains no features",
                evidence={"count": 0},
            )
        else:
            return RuleResult(
                rule_name="record_availability",
                status=QAStatus.PASS,
                message=f"Layer contains {count} features",
                evidence={"count": count},
            )
    except Exception as e:
        logger.error(f"Error in check_record_availability: {e}")
        return RuleResult(
            rule_name="record_availability",
            status=QAStatus.WARN,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e)},
        )


def check_pagination_support(
    metadata: dict | None,
    count: int | None,
    client: ArcGISClient,
    service_url: str,
    **kwargs,
) -> RuleResult:
    """
    Check if pagination works when needed.

    Args:
        metadata: Layer metadata
        count: Feature count
        client: ArcGIS client instance
        service_url: Service URL

    Returns:
        RuleResult with pagination test status
    """
    try:
        if metadata is None or count is None:
            return RuleResult(
                rule_name="pagination_support",
                status=QAStatus.NA,
                message="Cannot test pagination without metadata/count",
                evidence={"pagination_tested": False},
            )

        max_record_count = metadata.get("maxRecordCount", 1000)

        # Only test if count exceeds max record count
        if count <= max_record_count:
            return RuleResult(
                rule_name="pagination_support",
                status=QAStatus.NA,
                message=f"Pagination not needed (count: {count}, max: {max_record_count})",
                evidence={"pagination_tested": False, "count": count, "max": max_record_count},
            )

        # Try to fetch second page
        try:
            query_url = f"{service_url}/query"
            params = {
                "where": "1=1",
                "returnCountOnly": "false",
                "resultOffset": str(max_record_count),
                "resultRecordCount": "10",
                "f": "pjson",
            }
            response = client._make_request(query_url, params=params)
            second_page = response.get("features", [])

            if second_page:
                return RuleResult(
                    rule_name="pagination_support",
                    status=QAStatus.PASS,
                    message="Pagination works correctly",
                    evidence={
                        "pagination_tested": True,
                        "second_page_features": len(second_page),
                    },
                )
            else:
                return RuleResult(
                    rule_name="pagination_support",
                    status=QAStatus.WARN,
                    message="Pagination may not work (no features on second page)",
                    evidence={"pagination_tested": True, "second_page_features": 0},
                )
        except Exception as page_error:
            return RuleResult(
                rule_name="pagination_support",
                status=QAStatus.FAIL,
                message=f"Pagination failed: {str(page_error)}",
                evidence={"pagination_tested": True, "error": str(page_error)},
            )

    except Exception as e:
        logger.error(f"Error in check_pagination_support: {e}")
        return RuleResult(
            rule_name="pagination_support",
            status=QAStatus.FAIL,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e)},
        )


def check_schema_sanity(features: list[dict] | None, **kwargs) -> RuleResult:
    """
    Check for schema issues (null percentages, duplicates, OBJECTID).

    Args:
        features: List of feature dictionaries

    Returns:
        RuleResult with schema sanity check
    """
    try:
        if not features:
            return RuleResult(
                rule_name="schema_sanity",
                status=QAStatus.NA,
                message="No features available for schema check",
                evidence={},
            )

        # Extract attributes from features
        attributes_list = [f.get("attributes", {}) for f in features]
        if not attributes_list:
            return RuleResult(
                rule_name="schema_sanity",
                status=QAStatus.WARN,
                message="Features have no attributes",
                evidence={},
            )

        # Build DataFrame
        df = pd.DataFrame(attributes_list)

        if df.empty:
            return RuleResult(
                rule_name="schema_sanity",
                status=QAStatus.WARN,
                message="Empty attribute table",
                evidence={},
            )

        issues = []
        evidence = {}

        # Check for duplicate field names (should be caught by pandas, but check anyway)
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        if duplicate_cols:
            issues.append(f"Duplicate fields: {duplicate_cols}")
            evidence["duplicate_fields"] = duplicate_cols

        # Check for OBJECTID-like field
        objectid_fields = [
            col
            for col in df.columns
            if "OBJECTID" in col.upper() or "FID" in col.upper() or "OID" in col.upper()
        ]
        if not objectid_fields:
            issues.append("No OBJECTID-like field found")
            evidence["has_objectid"] = False
        else:
            evidence["has_objectid"] = True
            evidence["objectid_fields"] = objectid_fields

        # Compute null percentages
        null_pcts = df.isnull().sum() / len(df)
        high_null_fields = null_pcts[null_pcts > 0.8].index.tolist()

        if len(high_null_fields) >= 5:
            issues.append(f"{len(high_null_fields)} fields have >80% nulls")
            evidence["high_null_fields"] = high_null_fields
            evidence["high_null_count"] = len(high_null_fields)

        # Determine status
        if duplicate_cols or len(high_null_fields) >= 5:
            status = QAStatus.WARN
            message = "; ".join(issues) if issues else "Schema has warnings"
        else:
            status = QAStatus.PASS
            message = "Schema appears healthy"

        evidence["total_fields"] = len(df.columns)
        evidence["sample_size"] = len(df)

        return RuleResult(
            rule_name="schema_sanity",
            status=status,
            message=message,
            evidence=evidence,
        )

    except Exception as e:
        logger.error(f"Error in check_schema_sanity: {e}")
        return RuleResult(
            rule_name="schema_sanity",
            status=QAStatus.FAIL,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e)},
        )


def check_geometry_sanity(
    features: list[dict] | None,
    config: LayerConfig,
    **kwargs,
) -> RuleResult:
    """
    Check geometry validity and type matching.

    Args:
        features: List of feature dictionaries
        config: Layer configuration

    Returns:
        RuleResult with geometry sanity check
    """
    try:
        if not features:
            return RuleResult(
                rule_name="geometry_sanity",
                status=QAStatus.NA,
                message="No features available for geometry check",
                evidence={},
            )

        total = len(features)
        empty_count = 0
        invalid_count = 0
        type_mismatch_count = 0

        for feature in features:
            geom_data = feature.get("geometry")

            # Check for empty geometry
            if not geom_data:
                empty_count += 1
                continue

            # Try to parse geometry
            try:
                # Handle esriJSON geometry format
                if isinstance(geom_data, dict):
                    # Check if it's truly empty (e.g., {"rings": []})
                    if not any(geom_data.values()):
                        empty_count += 1
                        continue

                    # Convert to shapely geometry
                    geom = shape(geom_data)

                    # Check validity
                    if not geom.is_valid:
                        invalid_count += 1

                    # Check type matching
                    expected = config.expected_geometry.lower()
                    geom_type = geom.geom_type.lower()

                    # Normalize types for comparison
                    if expected in ["point", "multipoint"] and geom_type not in [
                        "point",
                        "multipoint",
                    ]:
                        type_mismatch_count += 1
                    elif expected in [
                        "line",
                        "linestring",
                        "multilinestring",
                    ] and geom_type not in [
                        "linestring",
                        "multilinestring",
                    ]:
                        type_mismatch_count += 1
                    elif expected in ["polygon", "multipolygon"] and geom_type not in [
                        "polygon",
                        "multipolygon",
                    ]:
                        type_mismatch_count += 1

            except Exception as geom_error:
                logger.debug(f"Geometry parse error: {geom_error}")
                invalid_count += 1

        # Calculate percentages
        pct_empty = (empty_count / total) * 100 if total > 0 else 0
        pct_invalid = (invalid_count / total) * 100 if total > 0 else 0
        pct_mismatch = (type_mismatch_count / total) * 100 if total > 0 else 0

        evidence = {
            "total_features": total,
            "empty_count": empty_count,
            "invalid_count": invalid_count,
            "type_mismatch_count": type_mismatch_count,
            "pct_empty": round(pct_empty, 2),
            "pct_invalid": round(pct_invalid, 2),
            "pct_mismatch": round(pct_mismatch, 2),
        }

        # Determine status
        if pct_empty > 25 or pct_invalid > 25:
            status = QAStatus.FAIL
            message = f"Geometry issues: {pct_empty:.1f}% empty, {pct_invalid:.1f}% invalid"
        elif pct_empty > 5 or pct_invalid > 5:
            status = QAStatus.WARN
            message = f"Some geometry issues: {pct_empty:.1f}% empty, {pct_invalid:.1f}% invalid"
        elif pct_mismatch > 10:
            status = QAStatus.WARN
            message = f"Geometry type mismatch: {pct_mismatch:.1f}%"
        else:
            status = QAStatus.PASS
            message = "Geometry appears healthy"

        return RuleResult(
            rule_name="geometry_sanity",
            status=status,
            message=message,
            evidence=evidence,
        )

    except Exception as e:
        logger.error(f"Error in check_geometry_sanity: {e}")
        return RuleResult(
            rule_name="geometry_sanity",
            status=QAStatus.FAIL,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e)},
        )


def check_update_recency(
    metadata: dict | None,
    threshold_months: int = 24,
    **kwargs,
) -> RuleResult:
    """
    Check when layer was last updated.

    Args:
        metadata: Layer metadata
        threshold_months: Warn if older than this many months (default: 24)

    Returns:
        RuleResult with update recency check
    """
    try:
        if metadata is None:
            return RuleResult(
                rule_name="update_recency",
                status=QAStatus.NA,
                message="No metadata available",
                evidence={},
            )

        # Try to find last edit date
        edit_info = metadata.get("editFieldsInfo", {})
        edit_date_field = edit_info.get("editDateField")

        # Also check for lastEditDate directly
        last_edit_ms = None
        if metadata.get("editingInfo"):
            last_edit_ms = metadata["editingInfo"].get("lastEditDate")

        if not last_edit_ms and not edit_date_field:
            return RuleResult(
                rule_name="update_recency",
                status=QAStatus.NA,
                message="No last edit date available in metadata",
                evidence={},
            )

        # If we don't have the timestamp but have the field name, we can't check without features
        if not last_edit_ms:
            return RuleResult(
                rule_name="update_recency",
                status=QAStatus.NA,
                message=f"Last edit date field '{edit_date_field}' found but no timestamp in metadata",
                evidence={"edit_field": edit_date_field},
            )

        # Convert epoch milliseconds to datetime
        last_edit_date = datetime.fromtimestamp(last_edit_ms / 1000)
        now = datetime.now()
        age_months = (now - last_edit_date).days / 30.44

        evidence = {
            "last_edit_date": last_edit_date.isoformat(),
            "months_old": round(age_months, 1),
            "threshold_months": threshold_months,
        }

        if age_months > threshold_months:
            status = QAStatus.WARN
            message = (
                f"Layer not updated in {age_months:.0f} months (threshold: {threshold_months})"
            )
        else:
            status = QAStatus.PASS
            message = f"Layer recently updated ({age_months:.0f} months ago)"

        return RuleResult(
            rule_name="update_recency",
            status=status,
            message=message,
            evidence=evidence,
        )

    except Exception as e:
        logger.error(f"Error in check_update_recency: {e}")
        return RuleResult(
            rule_name="update_recency",
            status=QAStatus.FAIL,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e)},
        )


def check_spatial_reference(metadata: dict | None, **kwargs) -> RuleResult:
    """
    Check if spatial reference is present and valid.

    Args:
        metadata: Layer metadata

    Returns:
        RuleResult with spatial reference check
    """
    try:
        if metadata is None:
            return RuleResult(
                rule_name="spatial_reference",
                status=QAStatus.NA,
                message="No metadata available",
                evidence={},
            )

        extent = metadata.get("extent", {})
        spatial_ref = extent.get("spatialReference", {})

        wkid = spatial_ref.get("wkid") or spatial_ref.get("latestWkid")
        wkt_str = spatial_ref.get("wkt")

        evidence = {"wkid": wkid, "wkt": wkt_str[:200] if wkt_str else None}

        if not wkid and not wkt_str:
            return RuleResult(
                rule_name="spatial_reference",
                status=QAStatus.WARN,
                message="No spatial reference information found",
                evidence=evidence,
            )

        # Check for common WKIDs
        common_wkids = [
            4326,
            3857,
            2263,
            2264,
            26918,
            26919,
        ]  # WGS84, Web Mercator, State Plane, UTM
        if wkid and wkid not in common_wkids:
            return RuleResult(
                rule_name="spatial_reference",
                status=QAStatus.WARN,
                message=f"Unusual spatial reference (WKID: {wkid})",
                evidence=evidence,
            )

        return RuleResult(
            rule_name="spatial_reference",
            status=QAStatus.PASS,
            message=f"Spatial reference present (WKID: {wkid})",
            evidence=evidence,
        )

    except Exception as e:
        logger.error(f"Error in check_spatial_reference: {e}")
        return RuleResult(
            rule_name="spatial_reference",
            status=QAStatus.FAIL,
            message=f"Exception: {str(e)}",
            evidence={"error": str(e)},
        )
