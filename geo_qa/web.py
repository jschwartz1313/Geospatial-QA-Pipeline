"""Streamlit web interface for the Geospatial QA Pipeline."""

import io
import json
from datetime import datetime

import pandas as pd
import pydeck as pdk
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
        "PASS": "#28a745",
        "WARN": "#ffc107",
        "FAIL": "#dc3545",
        "NA": "#6c757d",
    }.get(status_str, "#6c757d")


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


def display_summary_metrics(results: list[LayerQAResult]) -> None:
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


def display_charts(results: list[LayerQAResult]) -> None:
    """Display charts and visualizations."""
    st.subheader("Visualizations")

    col1, col2 = st.columns(2)

    # Status Distribution Pie Chart
    with col1:
        st.write("**Status Distribution**")
        pass_count = sum(1 for r in results if r.overall_status == QAStatus.PASS)
        warn_count = sum(1 for r in results if r.overall_status == QAStatus.WARN)
        fail_count = sum(1 for r in results if r.overall_status == QAStatus.FAIL)

        status_df = pd.DataFrame({
            "Status": ["PASS", "WARN", "FAIL"],
            "Count": [pass_count, warn_count, fail_count],
            "Color": ["#28a745", "#ffc107", "#dc3545"]
        })
        status_df = status_df[status_df["Count"] > 0]

        if not status_df.empty:
            # Use a horizontal bar chart for status distribution
            st.bar_chart(status_df.set_index("Status")["Count"], color="#4CAF50")

    # Metadata Scores Bar Chart
    with col2:
        st.write("**Metadata Scores by Layer**")
        scores_df = pd.DataFrame({
            "Layer": [r.layer_name for r in results],
            "Score": [r.metadata_score for r in results]
        })
        scores_df = scores_df.sort_values("Score", ascending=True)
        st.bar_chart(scores_df.set_index("Layer"), color="#2196F3")

    # Feature Counts
    st.write("**Feature Counts by Layer**")
    counts_data = []
    for r in results:
        if r.count_estimate is not None and r.count_estimate > 0:
            counts_data.append({"Layer": r.layer_name, "Features": r.count_estimate})

    if counts_data:
        counts_df = pd.DataFrame(counts_data)
        counts_df = counts_df.sort_values("Features", ascending=True)
        st.bar_chart(counts_df.set_index("Layer"), color="#9C27B0")
    else:
        st.info("No feature count data available")

    # Issues Summary
    st.write("**Issues by Rule**")
    issue_counts: dict[str, int] = {}
    for r in results:
        for rule in r.rule_results:
            if rule.status in [QAStatus.FAIL, QAStatus.WARN]:
                rule_name = rule.rule_name
                issue_counts[rule_name] = issue_counts.get(rule_name, 0) + 1

    if issue_counts:
        issues_df = pd.DataFrame({
            "Rule": list(issue_counts.keys()),
            "Issues": list(issue_counts.values())
        })
        issues_df = issues_df.sort_values("Issues", ascending=True)
        st.bar_chart(issues_df.set_index("Rule"), color="#FF5722")
    else:
        st.success("No issues found across all layers!")


def display_map_preview(result: LayerQAResult, client: ArcGISClient) -> None:
    """Display map preview for a layer."""
    st.write("**Map Preview**")

    # Try to get sample features with geometry
    try:
        features = client.sample_features(
            result.service_url,
            sample_size=100,
            return_geometry=True,
        )

        if not features:
            st.info("No features available for map preview")
            return

        # Extract coordinates based on geometry type
        points = []
        for feature in features:
            geom = feature.get("geometry", {})
            if not geom:
                continue

            # Handle different geometry types
            if "x" in geom and "y" in geom:
                # Point geometry
                points.append({"lat": geom["y"], "lon": geom["x"]})
            elif "rings" in geom:
                # Polygon - use centroid approximation
                rings = geom["rings"]
                if rings and rings[0]:
                    coords = rings[0]
                    avg_x = sum(c[0] for c in coords) / len(coords)
                    avg_y = sum(c[1] for c in coords) / len(coords)
                    # Check if coordinates are in Web Mercator
                    if abs(avg_x) > 180 or abs(avg_y) > 90:
                        # Skip Web Mercator coordinates for now
                        continue
                    points.append({"lat": avg_y, "lon": avg_x})
            elif "paths" in geom:
                # Polyline - use midpoint
                paths = geom["paths"]
                if paths and paths[0]:
                    coords = paths[0]
                    mid_idx = len(coords) // 2
                    if abs(coords[mid_idx][0]) <= 180 and abs(coords[mid_idx][1]) <= 90:
                        points.append({"lat": coords[mid_idx][1], "lon": coords[mid_idx][0]})

        if not points:
            st.info("Could not extract valid coordinates for map preview")
            return

        # Create DataFrame for pydeck
        df = pd.DataFrame(points)

        # Calculate center
        center_lat = df["lat"].mean()
        center_lon = df["lon"].mean()

        # Create pydeck layer
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["lon", "lat"],
            get_color=[255, 87, 34, 160],
            get_radius=5000,
            radius_min_pixels=3,
            radius_max_pixels=15,
        )

        # Create view
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=4,
            pitch=0,
        )

        # Render map
        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_style="mapbox://styles/mapbox/light-v10",
        ))

        st.caption(f"Showing {len(points)} sample features")

    except Exception as e:
        st.warning(f"Could not generate map preview: {e}")


def display_results_table(
    results: list[LayerQAResult],
    status_filter: list[str],
    search_query: str,
) -> list[LayerQAResult]:
    """Display results in a filterable table."""
    # Apply filters
    filtered_results = results

    if status_filter:
        filtered_results = [
            r for r in filtered_results
            if r.overall_status.value in status_filter
        ]

    if search_query:
        search_lower = search_query.lower()
        filtered_results = [
            r for r in filtered_results
            if search_lower in r.layer_name.lower()
            or search_lower in r.service_url.lower()
        ]

    if not filtered_results:
        st.warning("No layers match the current filters")
        return []

    # Build table data
    data = []
    for r in filtered_results:
        data.append({
            "Status": f"{get_status_emoji(r.overall_status)} {r.overall_status.value}",
            "Layer": r.layer_name,
            "Reachable": "✅" if r.reachable else "❌",
            "Features": r.count_estimate if r.count_estimate else "N/A",
            "Geometry": r.geometry_type_reported or "N/A",
            "Metadata Score": f"{r.metadata_score}/100",
            "Issues": len([x for x in r.rule_results if x.status in [QAStatus.FAIL, QAStatus.WARN]]),
        })

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)

    return filtered_results


def display_layer_details(result: LayerQAResult) -> None:
    """Display detailed results for a single layer."""
    # Basic info in columns
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write(f"**Status:** {get_status_emoji(result.overall_status)} {result.overall_status.value}")
        st.write(f"**Reachable:** {'Yes' if result.reachable else 'No'}")
    with col2:
        st.write(f"**Features:** {result.count_estimate or 'N/A'}")
        st.write(f"**Geometry:** {result.geometry_type_reported or 'N/A'}")
    with col3:
        st.write(f"**Metadata Score:** {result.metadata_score}/100")
        st.write(f"**Format:** {result.format_supported}")

    st.write(f"**URL:** `{result.service_url}`")

    # Rule results in a table format
    st.write("**QA Rule Results:**")

    rules_data = []
    for rule in result.rule_results:
        rules_data.append({
            "Status": f"{get_status_emoji(rule.status)} {rule.status.value}",
            "Rule": rule.rule_name,
            "Message": rule.message,
        })

    rules_df = pd.DataFrame(rules_data)
    st.dataframe(rules_df, use_container_width=True, hide_index=True)

    # Expandable evidence sections
    for rule in result.rule_results:
        if rule.evidence:
            with st.expander(f"Evidence: {rule.rule_name}"):
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
            label="📊 Download CSV",
            data=csv_buffer.getvalue(),
            file_name="qa_report.csv",
            mime="text/csv",
        )

    # Markdown Report
    with col2:
        md_content = generate_markdown_report_string(results, run_info)
        st.download_button(
            label="📝 Download Markdown",
            data=md_content,
            file_name="qa_report.md",
            mime="text/markdown",
        )

    # JSON Report
    with col3:
        json_data = [r.model_dump(mode="json") for r in results]
        json_str = json.dumps(json_data, indent=2, default=str)
        st.download_button(
            label="📋 Download JSON",
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

        # Filters (shown after results)
        if "results" in st.session_state:
            st.divider()
            st.header("Filters")

            status_filter = st.multiselect(
                "Filter by Status",
                options=["PASS", "WARN", "FAIL"],
                default=[],
                help="Show only layers with selected status",
            )

            search_query = st.text_input(
                "Search Layers",
                placeholder="Search by name or URL...",
                help="Filter layers by name or URL",
            )

            st.session_state["status_filter"] = status_filter
            st.session_state["search_query"] = search_query

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
                st.session_state["status_filter"] = []
                st.session_state["search_query"] = ""
                st.rerun()

    # Display results if available
    if "results" in st.session_state:
        results = st.session_state["results"]
        run_info = st.session_state["run_info"]
        status_filter = st.session_state.get("status_filter", [])
        search_query = st.session_state.get("search_query", "")

        st.divider()
        st.header("Results")

        # Summary metrics
        display_summary_metrics(results)

        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 Overview",
            "📊 Charts",
            "🗺️ Map Preview",
            "📥 Download"
        ])

        with tab1:
            st.subheader("Layer Overview")
            filtered_results = display_results_table(results, status_filter, search_query)

            # Detailed results per layer
            st.subheader("Detailed Results")
            for result in filtered_results:
                with st.expander(
                    f"{get_status_emoji(result.overall_status)} {result.layer_name} - {result.overall_status.value}"
                ):
                    display_layer_details(result)

        with tab2:
            display_charts(results)

        with tab3:
            st.subheader("Map Preview")
            st.info("Select a layer below to preview its features on the map.")

            # Create a client for fetching map data
            client = ArcGISClient(timeout=30, retries=2)

            # Let user select a layer
            layer_names = [r.layer_name for r in results if r.reachable]
            if layer_names:
                selected_layer = st.selectbox("Select Layer", layer_names)
                selected_result = next(r for r in results if r.layer_name == selected_layer)
                display_map_preview(selected_result, client)
            else:
                st.warning("No reachable layers available for map preview")

        with tab4:
            generate_download_buttons(results, run_info)

    else:
        st.info("Configure layers above and click 'Run QA Analysis' to start.")

    # Footer
    st.divider()
    st.markdown(
        """
        <div style="text-align: center; color: gray; font-size: 0.8em;">
        Geospatial QA Pipeline v0.1.0 |
        <a href="https://github.com/jschwartz1313/Geospatial-QA-Pipeline">GitHub</a>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
