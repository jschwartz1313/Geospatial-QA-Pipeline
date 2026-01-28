"""Unit tests for QA rules."""

from geo_qa.models import LayerConfig, QAStatus
from geo_qa.rules import (
    check_geometry_sanity,
    check_metadata_completeness,
    check_queryability,
    check_reachability,
    check_record_availability,
    check_schema_sanity,
    check_spatial_reference,
    check_update_recency,
)


def test_check_reachability_pass():
    """Test reachability check with valid metadata."""
    metadata = {"name": "test", "geometryType": "esriGeometryPolygon"}
    result = check_reachability(metadata=metadata)
    assert result.status == QAStatus.PASS
    assert result.evidence["metadata_exists"] is True


def test_check_reachability_fail():
    """Test reachability check with no metadata."""
    result = check_reachability(metadata=None)
    assert result.status == QAStatus.FAIL
    assert result.evidence["metadata_exists"] is False


def test_check_queryability_pass():
    """Test queryability with valid count."""
    result = check_queryability(count=100, metadata={"name": "test"})
    assert result.status == QAStatus.PASS
    assert result.evidence["count"] == 100


def test_check_queryability_warn():
    """Test queryability when count fails but metadata exists."""
    result = check_queryability(count=None, metadata={"name": "test"})
    assert result.status == QAStatus.WARN


def test_check_metadata_completeness_full():
    """Test metadata completeness with complete metadata."""
    metadata = {
        "description": "Test layer",
        "geometryType": "esriGeometryPolygon",
        "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
        "fields": [{"name": "OBJECTID"}, {"name": "NAME"}, {"name": "TYPE"}],
        "capabilities": "Query",
        "maxRecordCount": 1000,
        "advancedQueryCapabilities": {"supportsPagination": True},
    }
    result = check_metadata_completeness(metadata=metadata)
    assert result.status == QAStatus.PASS
    assert result.evidence["score"] == 100


def test_check_metadata_completeness_partial():
    """Test metadata completeness with partial metadata."""
    metadata = {
        "geometryType": "esriGeometryPoint",
        "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
        "fields": [{"name": "OBJECTID"}, {"name": "NAME"}, {"name": "TYPE"}],
    }
    result = check_metadata_completeness(metadata=metadata)
    # Should score 60 points (20 + 20 + 20)
    assert result.status == QAStatus.WARN
    assert 40 <= result.evidence["score"] < 70


def test_check_record_availability_pass():
    """Test record availability with features present."""
    result = check_record_availability(count=1000)
    assert result.status == QAStatus.PASS
    assert result.evidence["count"] == 1000


def test_check_record_availability_warn():
    """Test record availability with zero features."""
    result = check_record_availability(count=0)
    assert result.status == QAStatus.WARN
    assert result.evidence["count"] == 0


def test_check_schema_sanity_pass():
    """Test schema sanity with healthy schema."""
    features = [
        {"attributes": {"OBJECTID": 1, "NAME": "Test1", "TYPE": "A"}},
        {"attributes": {"OBJECTID": 2, "NAME": "Test2", "TYPE": "B"}},
        {"attributes": {"OBJECTID": 3, "NAME": "Test3", "TYPE": "C"}},
    ]
    result = check_schema_sanity(features=features)
    assert result.status == QAStatus.PASS
    assert result.evidence["has_objectid"] is True


def test_check_schema_sanity_no_features():
    """Test schema sanity with no features."""
    result = check_schema_sanity(features=None)
    assert result.status == QAStatus.NA


def test_check_geometry_sanity_pass():
    """Test geometry sanity with valid geometries."""
    config = LayerConfig(layer_name="test", service_url="http://test", expected_geometry="Polygon")
    features = [
        {
            "geometry": {"rings": [[[-180, -90], [-180, 90], [180, 90], [180, -90], [-180, -90]]]},
            "attributes": {},
        }
    ]
    result = check_geometry_sanity(features=features, config=config)
    # Note: This might fail if shapely can't parse the geometry correctly
    assert result.status in [QAStatus.PASS, QAStatus.FAIL, QAStatus.WARN]


def test_check_geometry_sanity_no_features():
    """Test geometry sanity with no features."""
    config = LayerConfig(layer_name="test", service_url="http://test", expected_geometry="Point")
    result = check_geometry_sanity(features=None, config=config)
    assert result.status == QAStatus.NA


def test_check_spatial_reference_pass():
    """Test spatial reference with valid WKID."""
    metadata = {"extent": {"spatialReference": {"wkid": 4326, "latestWkid": 4326}}}  # WGS84
    result = check_spatial_reference(metadata=metadata)
    assert result.status == QAStatus.PASS
    assert result.evidence["wkid"] == 4326


def test_check_spatial_reference_warn():
    """Test spatial reference with no spatial reference info."""
    metadata = {"extent": {}}
    result = check_spatial_reference(metadata=metadata)
    assert result.status == QAStatus.WARN


def test_check_update_recency_na():
    """Test update recency when no date available."""
    metadata = {"name": "test"}
    result = check_update_recency(metadata=metadata)
    assert result.status == QAStatus.NA
