"""Streamlit web interface for the Geospatial QA Pipeline."""

import io
import json
from datetime import datetime

import pandas as pd
import streamlit as st

from geo_qa.arcgis import ArcGISClient, run_qa_for_layer
from geo_qa.models import LayerConfig, LayerQAResult, PipelineRun, QAStatus
from geo_qa.report import generate_csv_dataframe, generate_markdown_report_string


def get_status_emoji(status: QAStatus | str) -> str:
    """Get emoji for status value."""
    status_str = status.value if isinstance(status, QAStatus) else str(status)
    return {
        "PASS": "✅",
        "WARN": "⚠️",
        "FAIL": "❌",
        "NA": "➖",
    }.get(status_str, "❓")


def get_status_color(status: QAStatus | str) -> str:
    """Get color for status value."""
    status_str = status.value if isinstance(status, QAStatus) else str(status)
    return {
        "PASS": "green",
        "WARN": "orange",
        "FAIL": "red",
        "NA": "gray",
    }.get(status_str, "gray")


def create_layer_config_from_url(url: str, name: str = "") -> LayerConfig:
    """Create a LayerConfig from a single URL."""
    layer_name = name if name else f"layer_{hash(url) % 10000}"
    return LayerConfig(
        layer_name=layer_name,
        service_url=url.strip(),
        expected_geometry="Unknown",
    )


def parse_csv_config(csv_content: str) -> list[LayerConfig]:
    """Parse CSV content into LayerConfig objects."""
    df = pd.read_csv(io.StringIO(csv_content))
    configs = []

    required_cols = {"layer_name", "service_url"}
    if not required_cols.issubset(set(df.columns)):
        raise ValueError(f"CSV must contain columns: {required_cols}")

    for _, row in df.iterrows():
        config = LayerConfig(
            layer_name=row["layer_name"],
            service_url=row["service_url"],
            expected_geometry=row.get("expected_geometry", "Unknown"),
            owner=row.get("owner"),
            notes=row.get("notes"),
        )
        configs.append(config)

    return configs


def run_pipeline(configs: list[LayerConfig], sample_size: int = 200) -> list[LayerQAResult]:
    """Run QA pipeline for all configured layers."""
    client = ArcGISClient(timeout=30, retries=2)
    results = []

    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, config in enumerate(configs):
        status_text.text(f"Analyzing: {config.layer_name} ({i+1}/{len(configs)})")
        result = run_qa_for_layer(config, client)
        results.append(result)
        progress_bar.progress((i + 1) / len(configs))

    status_text.text("Analysis complete!")
    return results


def display_summary(results: list[LayerQAResult]) -> None:
    """Display summary statistics."""
    pass_count = sum(1 for r in results if r.overall_status == QAStatus.PASS)
    warn_count = sum(1 for r in results if r.overall_status == QAStatus.WARN)
    fail_count = sum(1 for r in results if r.overall_status == QAStatus.FAIL)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Layers", len(results))
    with col2:
        st.metric("Passed", pass_count, delta=None)
    with col3:
        st.metric("Warnings", warn_count, delta=None)
    with col4:
        st.metric("Failed", fail_count, delta=None)


def display_results_table(results: list[LayerQAResult]) -> None:
    """Display results in a table."""
    data = []
    for r in results:
        data.append({
            "Status": f"{get_status_emoji(r.overall_status)} {r.overall_status.value}",
            "Layer": r.layer_name,
            "Reachable": "✅" if r.reachable else "❌",
            "Features": r.count_estimate or "N/A",
            "Geometry": r.geometry_type_reported or "N/A",
            "Metadata Score": f"{r.metadata_score}/100",
            "Issues": len([x for x in r.rule_results if x.status in [QAStatus.FAIL, QAStatus.WARN]]),
        })

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)


def display_layer_details(result: LayerQAResult) -> None:
    """Display detailed results for a single layer."""
    st.subheader(f"{get_status_emoji(result.overall_status)} {result.layer_name}")

    # Basic info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write(f"**Status:** {result.overall_status.value}")
        st.write(f"**Reachable:** {'Yes' if result.reachable else 'No'}")
    with col2:
        st.write(f"**Features:** {result.count_estimate or 'N/A'}")
        st.write(f"**Geometry:** {result.geometry_type_reported or 'N/A'}")
    with col3:
        st.write(f"**Metadata Score:** {result.metadata_score}/100")
        st.write(f"**Format:** {result.format_supported}")

    st.write(f"**URL:** `{result.service_url}`")

    # Rule results
    st.write("**QA Rule Results:**")
    for rule in result.rule_results:
        emoji = get_status_emoji(rule.status)
        with st.expander(f"{emoji} {rule.rule_name}: {rule.status.value}"):
            st.write(rule.message)
            if rule.evidence:
                st.json(rule.evidence)

    # Errors
    if result.errors:
        st.error("**Errors:**")
        for error in result.errors:
            st.write(f"- {error}")


def generate_download_buttons(results: list[LayerQAResult], run_info: PipelineRun) -> None:
    """Generate download buttons for reports."""
    st.subheader("Download Reports")

    col1, col2, col3 = st.columns(3)

    # CSV Report
    with col1:
        csv_df = generate_csv_dataframe(results)
        csv_buffer = io.StringIO()
        csv_df.to_csv(csv_buffer, index=False)
        st.download_button(
            label="Download CSV",
            data=csv_buffer.getvalue(),
            file_name="qa_report.csv",
            mime="text/csv",
        )

    # Markdown Report
    with col2:
        md_content = generate_markdown_report_string(results, run_info)
        st.download_button(
            label="Download Markdown",
            data=md_content,
            file_name="qa_report.md",
            mime="text/markdown",
        )

    # JSON Report
    with col3:
        json_data = [r.model_dump(mode="json") for r in results]
        json_str = json.dumps(json_data, indent=2, default=str)
        st.download_button(
            label="Download JSON",
            data=json_str,
            file_name="qa_report.json",
            mime="application/json",
        )


def main() -> None:
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Geospatial QA Pipeline",
        page_icon="🗺️",
        layout="wide",
    )

    st.title("🗺️ Geospatial QA Pipeline")
    st.markdown(
        "Validate ArcGIS REST layer datasets with comprehensive quality checks."
    )

    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")

        input_method = st.radio(
            "Input Method",
            ["Upload CSV", "Enter URL"],
            help="Choose how to specify layers to validate",
        )

        st.divider()

        # Advanced settings
        with st.expander("Advanced Settings"):
            sample_size = st.slider(
                "Sample Size",
                min_value=50,
                max_value=500,
                value=200,
                step=50,
                help="Number of features to sample for validation",
            )

    # Main content area
    configs: list[LayerConfig] = []

    if input_method == "Upload CSV":
        st.subheader("Upload Layer Configuration")
        st.markdown(
            """
            Upload a CSV file with the following columns:
            - `layer_name` (required): Unique name for the layer
            - `service_url` (required): ArcGIS REST endpoint URL
            - `expected_geometry` (optional): Point, Line, Polygon, or Unknown
            - `owner` (optional): Data owner
            - `notes` (optional): Additional notes
            """
        )

        uploaded_file = st.file_uploader("Choose CSV file", type=["csv"])

        if uploaded_file is not None:
            try:
                csv_content = uploaded_file.getvalue().decode("utf-8")
                configs = parse_csv_config(csv_content)
                st.success(f"Loaded {len(configs)} layer(s) from CSV")

                # Show preview
                with st.expander("Preview Layers"):
                    preview_data = [
                        {
                            "Layer Name": c.layer_name,
                            "URL": c.service_url[:60] + "..." if len(c.service_url) > 60 else c.service_url,
                            "Expected Geometry": c.expected_geometry,
                        }
                        for c in configs
                    ]
                    st.dataframe(pd.DataFrame(preview_data), hide_index=True)

            except Exception as e:
                st.error(f"Error parsing CSV: {e}")

    else:  # Enter URL
        st.subheader("Enter Layer URL")
        st.markdown(
            """
            Enter an ArcGIS REST FeatureServer or MapServer layer URL.
            Example: `https://services.arcgis.com/.../FeatureServer/0`
            """
        )

        url_input = st.text_area(
            "Layer URLs (one per line)",
            placeholder="https://services.arcgis.com/example/ArcGIS/rest/services/Layer/FeatureServer/0",
            height=100,
        )

        layer_name = st.text_input(
            "Layer Name (optional)",
            placeholder="my_layer",
            help="Provide a custom name for the layer (only used for single URL)",
        )

        if url_input.strip():
            urls = [u.strip() for u in url_input.strip().split("\n") if u.strip()]
            for i, url in enumerate(urls):
                name = layer_name if len(urls) == 1 and layer_name else f"layer_{i+1}"
                configs.append(create_layer_config_from_url(url, name))

            st.info(f"Ready to analyze {len(configs)} layer(s)")

    # Run analysis button
    st.divider()

    if configs:
        if st.button("🔍 Run QA Analysis", type="primary", use_container_width=True):
            with st.spinner("Running quality analysis..."):
                results = run_pipeline(configs, sample_size)

                # Store results in session state
                st.session_state["results"] = results
                st.session_state["run_info"] = PipelineRun(
                    timestamp=datetime.now(),
                    total_layers=len(configs),
                    pass_count=sum(1 for r in results if r.overall_status == QAStatus.PASS),
                    warn_count=sum(1 for r in results if r.overall_status == QAStatus.WARN),
                    fail_count=sum(1 for r in results if r.overall_status == QAStatus.FAIL),
                    config_file="web_upload",
                    output_dir="web_session",
                )

    # Display results if available
    if "results" in st.session_state:
        results = st.session_state["results"]
        run_info = st.session_state["run_info"]

        st.divider()
        st.header("Results")

        # Summary
        display_summary(results)

        # Results table
        st.subheader("Layer Overview")
        display_results_table(results)

        # Detailed results per layer
        st.subheader("Detailed Results")
        for result in results:
            with st.expander(
                f"{get_status_emoji(result.overall_status)} {result.layer_name} - {result.overall_status.value}"
            ):
                display_layer_details(result)

        # Download buttons
        st.divider()
        generate_download_buttons(results, run_info)

    else:
        st.info("Configure layers above and click 'Run QA Analysis' to start.")

    # Footer
    st.divider()
    st.markdown(
        """
        <div style="text-align: center; color: gray; font-size: 0.8em;">
        Geospatial QA Pipeline v0.1.0 |
        <a href="https://github.com/yourusername/Geospatial-QA-Pipeline">GitHub</a>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
