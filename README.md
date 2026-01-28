# Geospatial QA Pipeline

A robust, production-ready QA and validation pipeline for ArcGIS REST layer datasets. Designed for data operations and analytics engineering teams working with geospatial data in energy siting and similar workflows.

## Problem Statement

Operationalizing ArcGIS REST layers for production workflows is fragile and error-prone. Datasets may:
- Be unreachable or have broken query endpoints
- Contain incomplete metadata
- Have invalid or empty geometries
- Lack proper pagination support for large datasets
- Be stale (not updated in years)
- Have schema issues (excessive nulls, missing OBJECTID fields)

This tool provides **standardized QA reporting** to identify these issues before they impact your workflows.

## Features

### QA Checks Performed

The pipeline runs **9 comprehensive validation rules** on each layer:

1. **Reachability** - Can metadata be fetched?
2. **Queryability** - Does the query endpoint work?
3. **Metadata Completeness** - Score (0-100) based on presence of description, geometry type, extent, fields, capabilities, pagination support
4. **Record Availability** - Does the layer contain any features?
5. **Pagination Support** - Can large datasets be paginated correctly?
6. **Schema Sanity** - Check for duplicate fields, missing OBJECTID, excessive nulls (>80% in 5+ fields)
7. **Geometry Validation** - Check for empty/invalid geometries, type mismatches
8. **Update Recency** - When was the layer last edited? (warns if >24 months)
9. **Spatial Reference** - Is WKID present and valid?

Each check returns:
- **PASS** - No issues detected
- **WARN** - Minor issues that may not block usage
- **FAIL** - Critical issues that should be addressed
- **NA** - Check not applicable (e.g., pagination not needed for small datasets)

### Output Reports

1. **`outputs/qa_report.csv`** - Structured results (1 row per layer) suitable for Excel/Pandas analysis
2. **`outputs/qa_report.md`** - Human-readable summary with:
   - Run metadata (timestamp, counts)
   - Most common issues across all layers
   - Failed/Warning/Passing layers organized by status
   - Detailed results table with emojis
3. **`outputs/issues/<layer_name>.json`** - Detailed findings per layer including:
   - All rule results with evidence
   - Metadata excerpt
   - Errors encountered
4. **`outputs/logs/run.log`** - Full execution log (DEBUG level)

## Installation

### Requirements

- Python 3.11+
- Virtual environment recommended

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/Geospatial-QA-Pipeline.git
cd Geospatial-QA-Pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install package with dependencies
pip install -e .

# Install dev dependencies (for testing/linting)
pip install -e ".[dev]"
```

## Usage

### Basic Usage

```bash
# Run QA pipeline on configured layers
python -m geo_qa run --config config/layers.csv --out outputs
```

### Configuration File Format

Create a CSV file with the following columns:

```csv
layer_name,service_url,expected_geometry,owner,notes
wetlands,https://services.arcgis.com/example/wetlands/FeatureServer/0,Polygon,Environmental Team,For constraints screening
transmission,https://services.arcgis.com/example/transmission/FeatureServer/0,Line,Grid Team,Transmission lines dataset
substations,https://services.arcgis.com/example/substations/FeatureServer/0,Point,Grid Team,Substation locations
```

**Required columns:**
- `layer_name` - Unique identifier for the layer
- `service_url` - Full URL to ArcGIS REST FeatureServer layer endpoint

**Optional columns:**
- `expected_geometry` - Expected geometry type (Point/Line/Polygon/Unknown)
- `owner` - Team or person responsible
- `notes` - Additional context

### Advanced Options

```bash
# Customize sample size, timeout, and retries
python -m geo_qa run \
  --config config/layers.csv \
  --out outputs \
  --sample-size 500 \
  --timeout 30 \
  --retries 3 \
  --log-level DEBUG
```

**Options:**
- `--sample-size` - Number of features to sample per layer (default: 200)
- `--timeout` - Request timeout in seconds (default: 20)
- `--retries` - Number of retry attempts for failed requests (default: 2)
- `--log-level` - Console log level: DEBUG, INFO, WARNING, ERROR (default: INFO)

## Example Output

### Console Output
```
================================================================================
Geospatial QA Pipeline Starting
================================================================================
Config file: config/layers.csv
Output directory: outputs

[1/10] Processing: wetlands
INFO: QA complete for wetlands: PASS (0 FAIL, 0 WARN)

[2/10] Processing: transmission
WARNING: Pagination may not work (no features on second page)
INFO: QA complete for transmission: WARN (0 FAIL, 1 WARN)

================================================================================
QA Pipeline Complete
================================================================================
Total layers: 10
  PASS: 7
  WARN: 2
  FAIL: 1

Reports generated:
  - CSV: outputs/qa_report.csv
  - Markdown: outputs/qa_report.md
  - Issues: outputs/issues/ (10 files)
  - Log: outputs/logs/run.log
================================================================================
```

### Sample CSV Output

| layer_name | overall_status | reachable | count_estimate | geometry_type_reported | metadata_score | top_issues |
|------------|----------------|-----------|----------------|------------------------|----------------|------------|
| wetlands | PASS | True | 45892 | Polygon | 100 | |
| transmission | WARN | True | 2341 | Polyline | 90 | pagination_support: Pagination may not work |
| broken_layer | FAIL | False | None | None | 0 | reachability: Cannot fetch metadata from service |

## Project Structure

```
geo-qa/
├── README.md                    # This file
├── pyproject.toml               # Package configuration & dependencies
├── config/
│   └── layers.csv               # Layer configuration (edit this)
├── geo_qa/
│   ├── __init__.py
│   ├── cli.py                   # Command-line interface
│   ├── arcgis.py                # ArcGIS REST client + QA orchestration
│   ├── rules.py                 # 9 QA rule implementations
│   ├── models.py                # Pydantic data models
│   ├── report.py                # Report generation (CSV/MD/JSON)
│   ├── logging_config.py        # Logging setup
│   └── utils.py                 # Config loader, utilities
├── tests/
│   ├── test_rules_unit.py
│   ├── test_arcgis_client.py
│   └── fixtures/
│       └── metadata_example.json
└── outputs/                     # Generated reports (gitignored)
    ├── qa_report.csv
    ├── qa_report.md
    ├── issues/
    │   └── layer_name.json
    └── logs/
        └── run.log
```

## Development

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run tests with coverage
pytest tests/ -v --cov=geo_qa --cov-report=term-missing
```

### Code Quality

```bash
# Lint code
ruff check .

# Format code
black .

# Type checking (optional)
mypy geo_qa/
```

### VS Code Integration

The project includes VS Code configuration:
- `.vscode/settings.json` - Python interpreter, formatters, linters
- `.vscode/tasks.json` - Tasks for test, lint, format, run-qa
- `.vscode/launch.json` - Debug configurations

Press `Ctrl+Shift+B` (or `Cmd+Shift+B` on Mac) to see available tasks.

## How to Add New Layers

1. Edit [config/layers.csv](config/layers.csv)
2. Add a new row with layer details
3. Run the pipeline: `python -m geo_qa run --config config/layers.csv --out outputs`
4. Review the results in `outputs/qa_report.md`

## Limitations & Known Issues

- **Public endpoints variability** - Some ArcGIS REST endpoints have quirks (missing fields, non-standard responses)
- **Network timeouts** - Very slow services may timeout despite retries (increase `--timeout` if needed)
- **Geometry parsing** - Complex esriJSON geometries may not validate correctly in all cases
- **Large datasets** - Pagination is tested but limited to sample size (default 200 features)
- **Authentication** - Currently only supports public (unauthenticated) ArcGIS REST endpoints

## Reliability Features

This tool is designed for production use with:
- **Retry logic** - Automatic retries with exponential backoff on 429/5xx/timeouts (tenacity)
- **Polite requests** - 0.2s sleep between requests to avoid rate limiting
- **Fail-safe design** - One bad layer doesn't crash the entire run
- **Structured logging** - Dual output (file DEBUG, console INFO) for observability
- **Type safety** - Pydantic models for configuration and results validation

## Contributing

Contributions welcome! Areas for improvement:
- [ ] YAML config support (richer layer configuration)
- [ ] HTML report generation (interactive web view)
- [ ] Authentication support (OAuth, API keys)
- [ ] Caching metadata to speed up repeat runs
- [ ] GitHub Actions CI
- [ ] Additional QA rules (custom field validation, spatial overlap checks)

## License

MIT License - see LICENSE file for details

## Credits

Built with:
- [Requests](https://requests.readthedocs.io/) - HTTP client
- [Pandas](https://pandas.pydata.org/) - Data manipulation
- [Shapely](https://shapely.readthedocs.io/) - Geometry validation
- [GeoPandas](https://geopandas.org/) - Geospatial data handling
- [Tenacity](https://tenacity.readthedocs.io/) - Retry logic
- [Pydantic](https://docs.pydantic.dev/) - Data validation

---

For questions or issues, please open a GitHub issue or contact the maintainers.