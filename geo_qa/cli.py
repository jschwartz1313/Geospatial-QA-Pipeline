"""Command-line interface for geo-qa pipeline."""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from geo_qa.arcgis import ArcGISClient, run_qa_for_layer
from geo_qa.logging_config import setup_logging
from geo_qa.models import PipelineRun, QAStatus
from geo_qa.report import generate_csv_report, generate_markdown_report, write_issue_json
from geo_qa.utils import ensure_output_dirs, load_config

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Geospatial Dataset QA & Validation Pipeline for ArcGIS REST layers",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "command",
        choices=["run"],
        help="Command to execute (currently only 'run' is supported)",
    )

    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to layer configuration CSV file",
    )

    parser.add_argument(
        "--out",
        type=Path,
        default=Path("outputs"),
        help="Output directory for reports and logs",
    )

    parser.add_argument(
        "--sample-size",
        type=int,
        default=200,
        help="Number of features to sample per layer",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Request timeout in seconds",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Number of retry attempts for failed requests",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Console logging level",
    )

    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> int:
    """
    Execute the QA pipeline.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Setup output directories
    ensure_output_dirs(args.out)

    # Setup logging
    setup_logging(args.out / "logs", log_level=args.log_level)

    logger.info("=" * 80)
    logger.info("Geospatial QA Pipeline Starting")
    logger.info("=" * 80)
    logger.info(f"Config file: {args.config}")
    logger.info(f"Output directory: {args.out}")
    logger.info(f"Sample size: {args.sample_size}")
    logger.info(f"Timeout: {args.timeout}s, Retries: {args.retries}")
    logger.info("")

    try:
        # Load configuration
        logger.info("Loading configuration...")
        configs = load_config(args.config)
        logger.info(f"Loaded {len(configs)} layer configurations")
        logger.info("")

        # Initialize ArcGIS client
        client = ArcGISClient(
            timeout=args.timeout,
            retries=args.retries,
        )

        # Run QA for each layer
        results = []
        for idx, config in enumerate(configs, start=1):
            logger.info(f"[{idx}/{len(configs)}] Processing: {config.layer_name}")
            try:
                result = run_qa_for_layer(config, client)
                results.append(result)

                # Write individual issue JSON
                write_issue_json(result, args.out / "issues")

            except Exception as e:
                logger.error(f"Failed to process {config.layer_name}: {e}", exc_info=True)
                # Continue with next layer
                continue

            logger.info("")

        if not results:
            logger.error("No layers were successfully processed!")
            return 1

        # Count statuses
        pass_count = sum(1 for r in results if r.overall_status == QAStatus.PASS)
        warn_count = sum(1 for r in results if r.overall_status == QAStatus.WARN)
        fail_count = sum(1 for r in results if r.overall_status == QAStatus.FAIL)

        # Create run metadata
        run_metadata = PipelineRun(
            timestamp=datetime.now(),
            total_layers=len(results),
            pass_count=pass_count,
            warn_count=warn_count,
            fail_count=fail_count,
            config_file=str(args.config),
            output_dir=str(args.out),
        )

        # Generate reports
        logger.info("Generating reports...")

        csv_path = args.out / "qa_report.csv"
        generate_csv_report(results, csv_path)

        md_path = args.out / "qa_report.md"
        generate_markdown_report(results, md_path, run_metadata)

        logger.info("")
        logger.info("=" * 80)
        logger.info("QA Pipeline Complete")
        logger.info("=" * 80)
        logger.info(f"Total layers: {len(results)}")
        logger.info(f"  PASS: {pass_count}")
        logger.info(f"  WARN: {warn_count}")
        logger.info(f"  FAIL: {fail_count}")
        logger.info("")
        logger.info("Reports generated:")
        logger.info(f"  - CSV: {csv_path}")
        logger.info(f"  - Markdown: {md_path}")
        logger.info(f"  - Issues: {args.out / 'issues'}/ ({len(results)} files)")
        logger.info(f"  - Log: {args.out / 'logs' / 'run.log'}")
        logger.info("=" * 80)

        # Exit code: 0 if all passed, 1 if all failed
        if fail_count == len(results):
            return 1
        else:
            return 0

    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


def main() -> None:
    """Main CLI entry point."""
    args = parse_args()

    if args.command == "run":
        exit_code = run_pipeline(args)
        sys.exit(exit_code)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
