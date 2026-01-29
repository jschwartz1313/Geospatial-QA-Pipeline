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


def calculate_health_score(result: LayerQAResult) -> int:
    """
    Calculate overall health score (0-100) for a layer.

    Scoring:
    - Each PASS rule: +11 points (9 rules = 99, rounded to 100)
    - Each WARN rule: +5 points
    - Each FAIL rule: +0 points
    - Each NA rule: +8 points (neutral)
    """
    if not result.rule_results:
        return 0

    total_points = 0
    for rule in result.rule_results:
        if rule.status == QAStatus.PASS:
            total_points += 11
        elif rule.status == QAStatus.WARN:
            total_points += 5
        elif rule.status == QAStatus.NA:
            total_points += 8
        # FAIL = 0 points

    # Normalize to 100
    max_points = len(result.rule_results) * 11
    score = int((total_points / max_points) * 100) if max_points > 0 else 0
    return min(score, 100)


def get_health_color(score: int) -> str:
    """Get color based on health score."""
    if score >= 80:
        return "#28a745"  # Green
    elif score >= 60:
        return "#ffc107"  # Yellow
    elif score >= 40:
        return "#fd7e14"  # Orange
    else:
        return "#dc3545"  # Red


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
    """Display summary statistics with health scores."""
    pass_count = sum(1 for r in results if r.overall_status == QAStatus.PASS)
    warn_count = sum(1 for r in results if r.overall_status == QAStatus.WARN)
    fail_count = sum(1 for r in results if r.overall_status == QAStatus.FAIL)

    # Calculate average health score
    health_scores = [calculate_health_score(r) for r in results]
    avg_health = sum(health_scores) // len(health_scores) if health_scores else 0

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Layers", len(results))
    with col2:
        st.metric("Passed", pass_count, delta=None)
    with col3:
        st.metric("Warnings", warn_count, delta=None)
    with col4:
        st.metric("Failed", fail_count, delta=None)
    with col5:
        st.metric("Avg Health", f"{avg_health}%", delta=None)


def display_charts(results: list[LayerQAResult]) -> None:
    """Display charts and visualizations."""
    st.subheader("Visualizations")

    col1, col2 = st.columns(2)

    # Status Distribution
    with col1:
        st.write("**Status Distribution**")
        pass_count = sum(1 for r in results if r.overall_status == QAStatus.PASS)
        warn_count = sum(1 for r in results if r.overall_status == QAStatus.WARN)
        fail_count = sum(1 for r in results if r.overall_status == QAStatus.FAIL)

        status_df = pd.DataFrame({
            "Status": ["PASS", "WARN", "FAIL"],
            "Count": [pass_count, warn_count, fail_count],
        })
        status_df = status_df[status_df["Count"] > 0]

        if not status_df.empty:
            st.bar_chart(status_df.set_index("Status")["Count"], color="#4CAF50")

    # Health Scores
    with col2:
        st.write("**Health Scores by Layer**")
        health_df = pd.DataFrame({
            "Layer": [r.layer_name for r in results],
            "Health Score": [calculate_health_score(r) for r in results]
        })
        health_df = health_df.sort_values("Health Score", ascending=True)
        st.bar_chart(health_df.set_index("Layer"), color="#2196F3")

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

    try:
        features = client.sample_features(
            result.service_url,
            sample_size=100,
            return_geometry=True,
        )

        if not features:
            st.info("No features available for map preview")
            return

        points = []
        for feature in features:
            geom = feature.get("geometry", {})
            if not geom:
                continue

            if "x" in geom and "y" in geom:
                points.append({"lat": geom["y"], "lon": geom["x"]})
            elif "rings" in geom:
                rings = geom["rings"]
                if rings and rings[0]:
                    coords = rings[0]
                    avg_x = sum(c[0] for c in coords) / len(coords)
                    avg_y = sum(c[1] for c in coords) / len(coords)
                    if abs(avg_x) > 180 or abs(avg_y) > 90:
                        continue
                    points.append({"lat": avg_y, "lon": avg_x})
            elif "paths" in geom:
                paths = geom["paths"]
                if paths and paths[0]:
                    coords = paths[0]
                    mid_idx = len(coords) // 2
                    if abs(coords[mid_idx][0]) <= 180 and abs(coords[mid_idx][1]) <= 90:
                        points.append({"lat": coords[mid_idx][1], "lon": coords[mid_idx][0]})

        if not points:
            st.info("Could not extract valid coordinates for map preview")
            return

        df = pd.DataFrame(points)
        center_lat = df["lat"].mean()
        center_lon = df["lon"].mean()

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["lon", "lat"],
            get_color=[255, 87, 34, 160],
            get_radius=5000,
            radius_min_pixels=3,
            radius_max_pixels=15,
        )

        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=4,
            pitch=0,
        )

        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_style="mapbox://styles/mapbox/light-v10",
        ))

        st.caption(f"Showing {len(points)} sample features")

    except Exception as e:
        st.warning(f"Could not generate map preview: {e}")


def display_data_preview(result: LayerQAResult, client: ArcGISClient) -> None:
    """Display sample attribute data for a layer."""
    st.write("**Data Preview**")

    try:
        features = client.sample_features(
            result.service_url,
            sample_size=50,
            return_geometry=False,
        )

        if not features:
            st.info("No features available for data preview")
            return

        # Extract attributes
        rows = []
        for feature in features:
            attrs = feature.get("attributes", {})
            if attrs:
                rows.append(attrs)

        if not rows:
            st.info("No attribute data available")
            return

        df = pd.DataFrame(rows)

        # Show column info
        st.write(f"**{len(df)} rows × {len(df.columns)} columns**")

        # Display the dataframe
        st.dataframe(df, use_container_width=True, hide_index=True, height=400)

        # Column statistics
        with st.expander("Column Statistics"):
            stats_data = []
            for col in df.columns:
                null_count = df[col].isnull().sum()
                null_pct = (null_count / len(df)) * 100
                unique_count = df[col].nunique()
                stats_data.append({
                    "Column": col,
                    "Type": str(df[col].dtype),
                    "Non-Null": len(df) - null_count,
                    "Null %": f"{null_pct:.1f}%",
                    "Unique": unique_count,
                })
            stats_df = pd.DataFrame(stats_data)
            st.dataframe(stats_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.warning(f"Could not load data preview: {e}")


def convert_to_geojson(features: list[dict], layer_name: str) -> dict:
    """Convert esriJSON features to GeoJSON format."""
    geojson = {
        "type": "FeatureCollection",
        "name": layer_name,
        "features": []
    }

    for feature in features:
        geom_data = feature.get("geometry", {})
        attrs = feature.get("attributes", {})

        geojson_geom = None

        # Convert esriJSON geometry to GeoJSON
        if "x" in geom_data and "y" in geom_data:
            geojson_geom = {
                "type": "Point",
                "coordinates": [geom_data["x"], geom_data["y"]]
            }
        elif "rings" in geom_data and geom_data["rings"]:
            geojson_geom = {
                "type": "Polygon",
                "coordinates": geom_data["rings"]
            }
        elif "paths" in geom_data and geom_data["paths"]:
            if len(geom_data["paths"]) == 1:
                geojson_geom = {
                    "type": "LineString",
                    "coordinates": geom_data["paths"][0]
                }
            else:
                geojson_geom = {
                    "type": "MultiLineString",
                    "coordinates": geom_data["paths"]
                }
        elif "points" in geom_data and geom_data["points"]:
            geojson_geom = {
                "type": "MultiPoint",
                "coordinates": geom_data["points"]
            }

        if geojson_geom:
            geojson["features"].append({
                "type": "Feature",
                "geometry": geojson_geom,
                "properties": attrs
            })

    return geojson


def display_results_table(
    results: list[LayerQAResult],
    status_filter: list[str],
    search_query: str,
) -> list[LayerQAResult]:
    """Display results in a filterable table with health scores."""
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

    data = []
    for r in filtered_results:
        health = calculate_health_score(r)
        data.append({
            "Status": f"{get_status_emoji(r.overall_status)} {r.overall_status.value}",
            "Layer": r.layer_name,
            "Health": f"{health}%",
            "Reachable": "✅" if r.reachable else "❌",
            "Features": r.count_estimate if r.count_estimate else "N/A",
            "Geometry": r.geometry_type_reported or "N/A",
            "Metadata": f"{r.metadata_score}/100",
            "Issues": len([x for x in r.rule_results if x.status in [QAStatus.FAIL, QAStatus.WARN]]),
        })

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)

    return filtered_results


def display_layer_details(result: LayerQAResult) -> None:
    """Display detailed results for a single layer."""
    health_score = calculate_health_score(result)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.write(f"**Status:** {get_status_emoji(result.overall_status)} {result.overall_status.value}")
        st.write(f"**Reachable:** {'Yes' if result.reachable else 'No'}")
    with col2:
        st.write(f"**Features:** {result.count_estimate or 'N/A'}")
        st.write(f"**Geometry:** {result.geometry_type_reported or 'N/A'}")
    with col3:
        st.write(f"**Metadata Score:** {result.metadata_score}/100")
        st.write(f"**Format:** {result.format_supported}")
    with col4:
        st.write(f"**Health Score:** {health_score}%")
        # Progress bar for health
        st.progress(health_score / 100)

    st.write(f"**URL:** `{result.service_url}`")

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

    for rule in result.rule_results:
        if rule.evidence:
            with st.expander(f"Evidence: {rule.rule_name}"):
                st.json(rule.evidence)

    if result.errors:
        st.error("**Errors:**")
        for error in result.errors:
            st.write(f"- {error}")


def generate_download_buttons(
    results: list[LayerQAResult],
    run_info: PipelineRun,
    client: ArcGISClient,
) -> None:
    """Generate download buttons for reports including GeoJSON."""
    st.subheader("Download Reports")

    col1, col2, col3 = st.columns(3)

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

    with col2:
        md_content = generate_markdown_report_string(results, run_info)
        st.download_button(
            label="📝 Download Markdown",
            data=md_content,
            file_name="qa_report.md",
            mime="text/markdown",
        )

    with col3:
        json_data = [r.model_dump(mode="json") for r in results]
        json_str = json.dumps(json_data, indent=2, default=str)
        st.download_button(
            label="📋 Download JSON",
            data=json_str,
            file_name="qa_report.json",
            mime="application/json",
        )

    # GeoJSON Export Section
    st.subheader("Export GeoJSON")
    st.write("Download sampled features as GeoJSON for use in GIS tools.")

    reachable_layers = [r for r in results if r.reachable]
    if reachable_layers:
        selected_for_export = st.selectbox(
            "Select layer to export",
            options=[r.layer_name for r in reachable_layers],
            key="geojson_export_select"
        )

        if st.button("🌍 Generate GeoJSON", key="generate_geojson"):
            with st.spinner("Fetching features..."):
                try:
                    selected_result = next(r for r in results if r.layer_name == selected_for_export)
                    features = client.sample_features(
                        selected_result.service_url,
                        sample_size=200,
                        return_geometry=True,
                    )

                    if features:
                        geojson = convert_to_geojson(features, selected_for_export)
                        geojson_str = json.dumps(geojson, indent=2)

                        st.download_button(
                            label=f"⬇️ Download {selected_for_export}.geojson",
                            data=geojson_str,
                            file_name=f"{selected_for_export}.geojson",
                            mime="application/geo+json",
                        )
                        st.success(f"Generated GeoJSON with {len(geojson['features'])} features")
                    else:
                        st.warning("No features available for export")
                except Exception as e:
                    st.error(f"Error generating GeoJSON: {e}")
    else:
        st.warning("No reachable layers available for GeoJSON export")


def display_custom_thresholds() -> dict:
    """Display custom threshold settings and return the values."""
    st.subheader("Custom Thresholds")
    st.write("Adjust the thresholds used for QA rule evaluation.")

    col1, col2 = st.columns(2)

    with col1:
        empty_geom_warn = st.slider(
            "Empty Geometry Warning %",
            min_value=1,
            max_value=50,
            value=5,
            help="Warn if empty geometry percentage exceeds this value",
        )
        empty_geom_fail = st.slider(
            "Empty Geometry Fail %",
            min_value=5,
            max_value=100,
            value=25,
            help="Fail if empty geometry percentage exceeds this value",
        )
        invalid_geom_warn = st.slider(
            "Invalid Geometry Warning %",
            min_value=1,
            max_value=50,
            value=5,
            help="Warn if invalid geometry percentage exceeds this value",
        )
        invalid_geom_fail = st.slider(
            "Invalid Geometry Fail %",
            min_value=5,
            max_value=100,
            value=25,
            help="Fail if invalid geometry percentage exceeds this value",
        )

    with col2:
        null_field_threshold = st.slider(
            "Null Field Threshold %",
            min_value=50,
            max_value=100,
            value=80,
            help="Flag fields with null percentage above this value",
        )
        metadata_pass_score = st.slider(
            "Metadata Pass Score",
            min_value=50,
            max_value=100,
            value=70,
            help="Minimum metadata score to pass",
        )
        update_recency_months = st.slider(
            "Update Recency (months)",
            min_value=6,
            max_value=60,
            value=24,
            help="Warn if layer not updated within this many months",
        )

    return {
        "empty_geom_warn": empty_geom_warn,
        "empty_geom_fail": empty_geom_fail,
        "invalid_geom_warn": invalid_geom_warn,
        "invalid_geom_fail": invalid_geom_fail,
        "null_field_threshold": null_field_threshold,
        "metadata_pass_score": metadata_pass_score,
        "update_recency_months": update_recency_months,
    }


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

    # Sidebar
    with st.sidebar:
        st.header("Configuration")

        input_method = st.radio(
            "Input Method",
            ["Upload CSV", "Enter URL"],
            help="Choose how to specify layers to validate",
        )

        st.divider()

        with st.expander("Advanced Settings"):
            sample_size = st.slider(
                "Sample Size",
                min_value=50,
                max_value=500,
                value=200,
                step=50,
                help="Number of features to sample for validation",
            )

        with st.expander("Custom Thresholds"):
            thresholds = display_custom_thresholds()
            st.session_state["thresholds"] = thresholds

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

    # Main content
    configs: list[LayerConfig] = []

    if input_method == "Upload CSV":
        st.subheader("Upload Layer Configuration")
        st.markdown(
            """
            Upload a CSV file with the following columns:
            - `layer_name` (required): Unique name for the layer
            - `service_url` (required): ArcGIS REST endpoint URL
            - `expected_geometry` (optional): Point, Line, Polygon, or Unknown
            """
        )

        uploaded_file = st.file_uploader("Choose CSV file", type=["csv"])

        if uploaded_file is not None:
            try:
                csv_content = uploaded_file.getvalue().decode("utf-8")
                configs = parse_csv_config(csv_content)
                st.success(f"Loaded {len(configs)} layer(s) from CSV")

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

    else:
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

    st.divider()

    if configs:
        if st.button("🔍 Run QA Analysis", type="primary", use_container_width=True):
            with st.spinner("Running quality analysis..."):
                results = run_pipeline(configs, sample_size)

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

    if "results" in st.session_state:
        results = st.session_state["results"]
        run_info = st.session_state["run_info"]
        status_filter = st.session_state.get("status_filter", [])
        search_query = st.session_state.get("search_query", "")

        st.divider()
        st.header("Results")

        display_summary_metrics(results)

        # Create client for data operations
        client = ArcGISClient(timeout=30, retries=2)

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📋 Overview",
            "📊 Charts",
            "🗺️ Map Preview",
            "📄 Data Preview",
            "📥 Download",
            "⚙️ Thresholds"
        ])

        with tab1:
            st.subheader("Layer Overview")
            filtered_results = display_results_table(results, status_filter, search_query)

            st.subheader("Detailed Results")
            for result in filtered_results:
                health = calculate_health_score(result)
                with st.expander(
                    f"{get_status_emoji(result.overall_status)} {result.layer_name} - "
                    f"{result.overall_status.value} ({health}% health)"
                ):
                    display_layer_details(result)

        with tab2:
            display_charts(results)

        with tab3:
            st.subheader("Map Preview")
            st.info("Select a layer below to preview its features on the map.")

            layer_names = [r.layer_name for r in results if r.reachable]
            if layer_names:
                selected_layer = st.selectbox("Select Layer", layer_names, key="map_select")
                selected_result = next(r for r in results if r.layer_name == selected_layer)
                display_map_preview(selected_result, client)
            else:
                st.warning("No reachable layers available for map preview")

        with tab4:
            st.subheader("Data Preview")
            st.info("Select a layer to view sample attribute data.")

            layer_names = [r.layer_name for r in results if r.reachable]
            if layer_names:
                selected_layer = st.selectbox("Select Layer", layer_names, key="data_select")
                selected_result = next(r for r in results if r.layer_name == selected_layer)
                display_data_preview(selected_result, client)
            else:
                st.warning("No reachable layers available for data preview")

        with tab5:
            generate_download_buttons(results, run_info, client)

        with tab6:
            st.subheader("Current Thresholds")
            st.info("These thresholds can be adjusted in the sidebar under 'Custom Thresholds'.")

            thresholds = st.session_state.get("thresholds", {})
            if thresholds:
                thresh_df = pd.DataFrame([
                    {"Setting": k.replace("_", " ").title(), "Value": v}
                    for k, v in thresholds.items()
                ])
                st.dataframe(thresh_df, use_container_width=True, hide_index=True)
            else:
                st.write("Using default thresholds. Expand 'Custom Thresholds' in sidebar to customize.")

    else:
        st.info("Configure layers above and click 'Run QA Analysis' to start.")

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
