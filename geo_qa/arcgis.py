"""ArcGIS REST API client with retry logic and pagination support."""

import logging
import time
from typing import TYPE_CHECKING

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from geo_qa.models import LayerQAResult

logger = logging.getLogger(__name__)


class ArcGISClient:
    """
    HTTP client for ArcGIS REST API with retry logic and pagination.

    Features:
    - Automatic retries with exponential backoff on 429/5xx/timeouts
    - Polite sleep between requests
    - Pagination support for large feature sets
    - Format detection (GeoJSON vs JSON)
    """

    def __init__(
        self,
        timeout: int = 20,
        retries: int = 2,
        sleep_between_requests: float = 0.2,
    ):
        """
        Initialize ArcGIS REST client.

        Args:
            timeout: Request timeout in seconds (default: 20)
            retries: Number of retry attempts (default: 2)
            sleep_between_requests: Sleep duration between requests in seconds (default: 0.2)
        """
        self.timeout = timeout
        self.retries = retries
        self.sleep_between_requests = sleep_between_requests
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "geo-qa/0.1.0 (Geospatial QA Pipeline)",
            }
        )

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if request should be retried based on exception type."""
        if isinstance(exception, requests.exceptions.Timeout):
            return True
        if isinstance(exception, requests.exceptions.RequestException):
            if hasattr(exception, "response") and exception.response is not None:
                # Retry on 429 (rate limit) and 5xx (server errors)
                return exception.response.status_code in [429] or (
                    500 <= exception.response.status_code < 600
                )
        return False

    @retry(
        retry=retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.HTTPError)),
        stop=stop_after_attempt(3),  # Will be overridden by instance setting
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _make_request(self, url: str, params: dict | None = None) -> dict:
        """
        Make HTTP GET request with retry logic.

        Args:
            url: URL to request
            params: Query parameters

        Returns:
            Parsed JSON response

        Raises:
            requests.RequestException: On network errors
        """
        time.sleep(self.sleep_between_requests)

        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()

        return response.json()

    def fetch_metadata(self, service_url: str) -> dict | None:
        """
        Fetch layer metadata from ArcGIS REST endpoint.

        Args:
            service_url: Base URL of the ArcGIS REST service

        Returns:
            Metadata dictionary or None on failure
        """
        try:
            logger.debug(f"Fetching metadata from: {service_url}")
            metadata = self._make_request(service_url, params={"f": "pjson"})
            logger.debug(f"Successfully fetched metadata for: {metadata.get('name', 'unknown')}")
            return metadata
        except Exception as e:
            logger.error(f"Failed to fetch metadata from {service_url}: {e}")
            return None

    def count_features(self, service_url: str, where: str = "1=1") -> int | None:
        """
        Get feature count from layer.

        Args:
            service_url: Base URL of the ArcGIS REST service
            where: SQL where clause (default: "1=1" for all features)

        Returns:
            Feature count or None on failure
        """
        try:
            query_url = f"{service_url}/query"
            params = {"where": where, "returnCountOnly": "true", "f": "pjson"}

            logger.debug(f"Counting features at: {query_url}")
            response = self._make_request(query_url, params=params)

            count = response.get("count")
            if count is not None:
                logger.debug(f"Feature count: {count}")
                return count
            else:
                logger.warning(f"No count field in response from {query_url}")
                return None

        except Exception as e:
            logger.warning(f"Failed to count features from {service_url}: {e}")
            return None

    def sample_features(
        self,
        service_url: str,
        sample_size: int = 200,
        where: str = "1=1",
        out_fields: str = "*",
        return_geometry: bool = True,
        metadata: dict | None = None,
    ) -> list[dict] | None:
        """
        Sample features from layer with pagination support.

        Args:
            service_url: Base URL of the ArcGIS REST service
            sample_size: Maximum number of features to retrieve
            where: SQL where clause
            out_fields: Fields to return (default: "*" for all)
            return_geometry: Whether to include geometry (default: True)
            metadata: Layer metadata (to get maxRecordCount)

        Returns:
            List of feature dictionaries or None on failure
        """
        try:
            query_url = f"{service_url}/query"

            # Determine max records per request
            max_record_count = 1000  # Default
            if metadata:
                max_record_count = metadata.get("maxRecordCount", 1000)

            logger.debug(
                f"Sampling up to {sample_size} features (maxRecordCount: {max_record_count})"
            )

            features = []
            offset = 0

            while len(features) < sample_size:
                # Calculate how many to request this iteration
                limit = min(sample_size - len(features), max_record_count)

                params = {
                    "where": where,
                    "outFields": out_fields,
                    "returnGeometry": str(return_geometry).lower(),
                    "f": "pjson",
                    "resultOffset": str(offset),
                    "resultRecordCount": str(limit),
                }

                logger.debug(f"Fetching features: offset={offset}, limit={limit}")
                response = self._make_request(query_url, params=params)

                batch = response.get("features", [])
                if not batch:
                    logger.debug("No more features available")
                    break

                features.extend(batch)
                offset += len(batch)

                # If we got fewer features than requested, we've hit the end
                if len(batch) < limit:
                    break

                # Safety check to prevent infinite loops
                if offset > sample_size * 2:
                    logger.warning("Pagination safety limit reached")
                    break

            logger.debug(f"Sampled {len(features)} features")
            return features

        except Exception as e:
            logger.error(f"Failed to sample features from {service_url}: {e}")
            return None

    def determine_format_support(self, service_url: str) -> str:
        """
        Determine which output formats are supported (GeoJSON vs JSON).

        Args:
            service_url: Base URL of the ArcGIS REST service

        Returns:
            "geojson", "pjson", "both", or "unknown"
        """
        try:
            query_url = f"{service_url}/query"
            supports_geojson = False
            supports_pjson = False

            # Try GeoJSON
            try:
                self._make_request(
                    query_url,
                    params={
                        "where": "1=1",
                        "returnCountOnly": "true",
                        "f": "geojson",
                    },
                )
                supports_geojson = True
            except Exception:
                pass

            # Try pjson (default JSON)
            try:
                self._make_request(
                    query_url,
                    params={
                        "where": "1=1",
                        "returnCountOnly": "true",
                        "f": "pjson",
                    },
                )
                supports_pjson = True
            except Exception:
                pass

            if supports_geojson and supports_pjson:
                return "both"
            elif supports_geojson:
                return "geojson"
            elif supports_pjson:
                return "pjson"
            else:
                return "unknown"

        except Exception as e:
            logger.warning(f"Could not determine format support for {service_url}: {e}")
            return "unknown"


def run_qa_for_layer(config, client: ArcGISClient) -> "LayerQAResult":
    """
    Run all QA rules for a single layer.

    Args:
        config: LayerConfig for the layer
        client: ArcGIS client instance

    Returns:
        LayerQAResult with all checks performed
    """
    from geo_qa import rules
    from geo_qa.models import LayerQAResult, QAStatus

    logger.info(f"Starting QA for layer: {config.layer_name}")

    # Initialize result
    result = LayerQAResult(
        layer_name=config.layer_name,
        service_url=config.service_url,
        expected_geometry=config.expected_geometry,
    )

    try:
        # Step 1: Fetch metadata
        logger.debug(f"Fetching metadata for {config.layer_name}")
        metadata = client.fetch_metadata(config.service_url)

        if metadata is None:
            # Cannot proceed without metadata
            result.reachable = False
            result.overall_status = QAStatus.FAIL
            result.errors.append("Failed to fetch metadata")
            result.rule_results.append(rules.check_reachability(metadata=None))
            logger.error(f"Layer {config.layer_name} is not reachable")
            return result

        result.reachable = True
        result.raw_metadata = metadata

        # Extract key metadata fields
        result.geometry_type_reported = metadata.get("geometryType", "").replace("esriGeometry", "")
        result.max_record_count = metadata.get("maxRecordCount")

        extent = metadata.get("extent", {})
        spatial_ref = extent.get("spatialReference", {})
        result.spatial_reference_wkid = spatial_ref.get("wkid") or spatial_ref.get("latestWkid")

        # Step 2: Count features
        logger.debug(f"Counting features for {config.layer_name}")
        count = client.count_features(config.service_url)
        result.count_estimate = count

        # Step 3: Sample features (if count > 0)
        features = None
        if count and count > 0:
            logger.debug(f"Sampling features for {config.layer_name}")
            features = client.sample_features(
                config.service_url,
                sample_size=200,
                metadata=metadata,
            )

        # Step 4: Determine format support
        logger.debug(f"Checking format support for {config.layer_name}")
        result.format_supported = client.determine_format_support(config.service_url)

        # Step 5: Run all QA rules
        logger.debug(f"Running QA rules for {config.layer_name}")

        # Rule A: Reachability
        result.rule_results.append(rules.check_reachability(metadata=metadata))

        # Rule B: Queryability
        result.rule_results.append(rules.check_queryability(count=count, metadata=metadata))

        # Rule C: Metadata completeness
        completeness_result = rules.check_metadata_completeness(metadata=metadata)
        result.rule_results.append(completeness_result)
        result.metadata_score = completeness_result.evidence.get("score", 0)

        # Rule D: Record availability
        result.rule_results.append(rules.check_record_availability(count=count))

        # Rule E: Pagination support
        pagination_result = rules.check_pagination_support(
            metadata=metadata,
            count=count,
            client=client,
            service_url=config.service_url,
        )
        result.rule_results.append(pagination_result)
        result.pagination_ok = pagination_result.status.value

        # Rule F: Schema sanity
        schema_result = rules.check_schema_sanity(features=features)
        result.rule_results.append(schema_result)
        result.null_fields_over_80pct = schema_result.evidence.get("high_null_count", 0)

        # Rule G: Geometry sanity
        geometry_result = rules.check_geometry_sanity(features=features, config=config)
        result.rule_results.append(geometry_result)
        result.pct_invalid_geometry = geometry_result.evidence.get("pct_invalid", 0.0)
        result.pct_empty_geometry = geometry_result.evidence.get("pct_empty", 0.0)

        # Rule H: Update recency
        recency_result = rules.check_update_recency(metadata=metadata)
        result.rule_results.append(recency_result)
        result.last_edit_date = recency_result.evidence.get("last_edit_date")

        # Rule I: Spatial reference
        result.rule_results.append(rules.check_spatial_reference(metadata=metadata))

        # Step 6: Aggregate overall status
        result.overall_status = result.aggregate_status()

        # Step 7: Compute top issues
        result.top_issues = result.compute_top_issues()

        logger.info(
            f"QA complete for {config.layer_name}: {result.overall_status.value} "
            f"({len([r for r in result.rule_results if r.status == QAStatus.FAIL])} FAIL, "
            f"{len([r for r in result.rule_results if r.status == QAStatus.WARN])} WARN)"
        )

    except Exception as e:
        logger.error(f"Unexpected error during QA for {config.layer_name}: {e}", exc_info=True)
        result.errors.append(f"Unexpected error: {str(e)}")
        result.overall_status = QAStatus.FAIL

    return result
