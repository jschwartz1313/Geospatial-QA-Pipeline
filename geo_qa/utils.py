"""Utility functions for configuration loading and file handling."""

import csv
import logging
from pathlib import Path

from geo_qa.models import LayerConfig

logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> list[LayerConfig]:
    """
    Load layer configuration from CSV file.

    Args:
        config_path: Path to CSV configuration file

    Returns:
        List of LayerConfig objects

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If CSV format is invalid
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    layers = []

    with open(config_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Validate required columns
        required_cols = {"layer_name", "service_url"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"CSV must contain columns: {required_cols}. " f"Found: {reader.fieldnames}"
            )

        for idx, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            try:
                # Handle optional fields with defaults
                layer = LayerConfig(
                    layer_name=row["layer_name"].strip(),
                    service_url=row["service_url"].strip(),
                    expected_geometry=row.get("expected_geometry", "Unknown").strip(),
                    owner=row.get("owner", "").strip() or None,
                    notes=row.get("notes", "").strip() or None,
                )
                layers.append(layer)
                logger.debug(f"Loaded config for layer: {layer.layer_name}")
            except Exception as e:
                logger.warning(f"Skipping invalid row {idx} in config: {e}")
                continue

    if not layers:
        raise ValueError(f"No valid layers found in {config_path}")

    logger.info(f"Loaded {len(layers)} layer configurations from {config_path}")
    return layers


def ensure_output_dirs(base_path: Path) -> None:
    """
    Create output directory structure if it doesn't exist.

    Creates:
        - base_path/
        - base_path/issues/
        - base_path/logs/

    Args:
        base_path: Base output directory path
    """
    base_path = Path(base_path)
    base_path.mkdir(parents=True, exist_ok=True)

    issues_dir = base_path / "issues"
    issues_dir.mkdir(exist_ok=True)

    logs_dir = base_path / "logs"
    logs_dir.mkdir(exist_ok=True)

    logger.debug(f"Output directories ensured at: {base_path}")
