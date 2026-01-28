"""Report generation for QA results (CSV, Markdown, JSON)."""

import json
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

from geo_qa.models import LayerQAResult, PipelineRun, QAStatus

logger = logging.getLogger(__name__)


def generate_csv_dataframe(results: list[LayerQAResult]) -> pd.DataFrame:
    """
    Generate DataFrame from QA results.

    Args:
        results: List of LayerQAResult objects

    Returns:
        DataFrame with all results
    """
    rows = []
    for result in results:
        row = {
            "layer_name": result.layer_name,
            "service_url": result.service_url,
            "overall_status": result.overall_status.value,
            "reachable": result.reachable,
            "count_estimate": result.count_estimate,
            "geometry_type_reported": result.geometry_type_reported,
            "expected_geometry": result.expected_geometry,
            "max_record_count": result.max_record_count,
            "pagination_ok": result.pagination_ok,
            "metadata_score": result.metadata_score,
            "null_fields_over_80pct": result.null_fields_over_80pct,
            "pct_invalid_geometry": result.pct_invalid_geometry,
            "pct_empty_geometry": result.pct_empty_geometry,
            "last_edit_date": result.last_edit_date,
            "format_supported": result.format_supported,
            "spatial_reference_wkid": result.spatial_reference_wkid,
            "top_issues": result.top_issues,
        }
        rows.append(row)

    return pd.DataFrame(rows)


def generate_csv_report(results: list[LayerQAResult], output_path: Path) -> None:
    """
    Generate CSV report from QA results.

    Args:
        results: List of LayerQAResult objects
        output_path: Path to output CSV file
    """
    logger.info(f"Generating CSV report: {output_path}")

    df = generate_csv_dataframe(results)

    # Write to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"CSV report written: {output_path} ({len(results)} layers)")


def generate_markdown_report_string(
    results: list[LayerQAResult],
    run_metadata: PipelineRun,
) -> str:
    """
    Generate Markdown report as a string.

    Args:
        results: List of LayerQAResult objects
        run_metadata: Pipeline run metadata

    Returns:
        Markdown formatted string
    """
    lines = []

    # Header
    lines.append("# Geospatial QA Report")
    lines.append("")

    # Run metadata
    lines.append("## Run Metadata")
    lines.append("")
    lines.append(f"- **Timestamp:** {run_metadata.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- **Config File:** {run_metadata.config_file}")
    lines.append(f"- **Total Layers:** {run_metadata.total_layers}")
    lines.append("")

    # Summary counts
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **PASS:** {run_metadata.pass_count}")
    lines.append(f"- **WARN:** {run_metadata.warn_count}")
    lines.append(f"- **FAIL:** {run_metadata.fail_count}")
    lines.append("")

    # Most common issues
    all_issues = []
    for result in results:
        for rule_result in result.rule_results:
            if rule_result.status in [QAStatus.FAIL, QAStatus.WARN]:
                all_issues.append(f"{rule_result.rule_name}: {rule_result.message}")

    if all_issues:
        issue_counts = Counter(all_issues)
        lines.append("## Most Common Issues")
        lines.append("")
        for issue, count in issue_counts.most_common(10):
            lines.append(f"- **{count}x** {issue}")
        lines.append("")

    # Separate results by status
    fail_results = [r for r in results if r.overall_status == QAStatus.FAIL]
    warn_results = [r for r in results if r.overall_status == QAStatus.WARN]
    pass_results = [r for r in results if r.overall_status == QAStatus.PASS]

    # FAIL layers
    if fail_results:
        lines.append("## Failed Layers")
        lines.append("")
        lines.append("| Layer Name | Service URL | Top Issues |")
        lines.append("|------------|-------------|------------|")
        for result in fail_results:
            issues = (
                result.top_issues[:100] + "..."
                if len(result.top_issues) > 100
                else result.top_issues
            )
            lines.append(f"| {result.layer_name} | {result.service_url[:50]}... | {issues} |")
        lines.append("")

    # WARN layers
    if warn_results:
        lines.append("## Warning Layers")
        lines.append("")
        lines.append("| Layer Name | Service URL | Top Issues |")
        lines.append("|------------|-------------|------------|")
        for result in warn_results:
            issues = (
                result.top_issues[:100] + "..."
                if len(result.top_issues) > 100
                else result.top_issues
            )
            lines.append(f"| {result.layer_name} | {result.service_url[:50]}... | {issues} |")
        lines.append("")

    # PASS layers
    if pass_results:
        lines.append("## Passed Layers")
        lines.append("")
        for result in pass_results:
            lines.append(f"- **{result.layer_name}** ({result.count_estimate} features)")
        lines.append("")

    # Detailed results table
    lines.append("## Detailed Results")
    lines.append("")
    lines.append("| Layer | Status | Reachable | Count | Geometry | Metadata Score | Issues |")
    lines.append("|-------|--------|-----------|-------|----------|----------------|--------|")

    for result in fail_results + warn_results + pass_results:
        status_emoji = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(
            result.overall_status.value, "❓"
        )
        reachable_emoji = "✅" if result.reachable else "❌"
        count_str = str(result.count_estimate) if result.count_estimate is not None else "N/A"
        geom_str = result.geometry_type_reported or "N/A"
        issue_count = len(
            [r for r in result.rule_results if r.status in [QAStatus.FAIL, QAStatus.WARN]]
        )

        lines.append(
            f"| {result.layer_name} | {status_emoji} {result.overall_status.value} | "
            f"{reachable_emoji} | {count_str} | {geom_str} | {result.metadata_score}/100 | {issue_count} |"
        )

    lines.append("")

    return "\n".join(lines)


def generate_markdown_report(
    results: list[LayerQAResult],
    output_path: Path,
    run_metadata: PipelineRun,
) -> None:
    """
    Generate human-readable Markdown report.

    Args:
        results: List of LayerQAResult objects
        output_path: Path to output MD file
        run_metadata: Pipeline run metadata
    """
    logger.info(f"Generating Markdown report: {output_path}")

    content = generate_markdown_report_string(results, run_metadata)

    # Write to file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    logger.info(f"Markdown report written: {output_path}")


def write_issue_json(result: LayerQAResult, output_dir: Path) -> None:
    """
    Write detailed issue JSON for a single layer.

    Args:
        result: LayerQAResult object
        output_dir: Directory to write JSON files
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize layer name for filename
    safe_name = "".join(c if c.isalnum() or c in "_- " else "_" for c in result.layer_name)
    output_path = output_dir / f"{safe_name}.json"

    data = {
        "layer_name": result.layer_name,
        "service_url": result.service_url,
        "overall_status": result.overall_status.value,
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "reachable": result.reachable,
            "count_estimate": result.count_estimate,
            "geometry_type_reported": result.geometry_type_reported,
            "metadata_score": result.metadata_score,
        },
        "rule_results": [
            {
                "rule_name": r.rule_name,
                "status": r.status.value,
                "message": r.message,
                "evidence": r.evidence,
            }
            for r in result.rule_results
        ],
        "errors": result.errors,
        "metadata_excerpt": {
            "name": result.raw_metadata.get("name") if result.raw_metadata else None,
            "geometryType": (
                result.raw_metadata.get("geometryType") if result.raw_metadata else None
            ),
            "maxRecordCount": (
                result.raw_metadata.get("maxRecordCount") if result.raw_metadata else None
            ),
            "capabilities": (
                result.raw_metadata.get("capabilities") if result.raw_metadata else None
            ),
        },
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    logger.debug(f"Issue JSON written: {output_path}")
