#!/usr/bin/env python3
"""
Pipeline orchestrator for batch processing Internet Archive items.

Coordinates the complete workflow in batches of 1,000 items:
1. Download PDFs from Internet Archive
2. Process with olmOCR
3. Ingest OCR results into database
4. Clean up PDFs to save space

Calls existing, working scripts - does not modify them.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import yaml

# Add paths for importing from component repos
SCRIPT_DIR = Path(__file__).parent
REPO_DIR = SCRIPT_DIR.parent


class PipelineOrchestrator:
    """Coordinate batch processing through all pipeline phases."""

    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.run_id = str(uuid.uuid4())[:8]

        # Track batch state
        self.current_batch = 0
        self.total_batches = 0

    def _load_config(self, config_path: str = None) -> Dict:
        """Load pipeline configuration."""
        if not config_path:
            config_path = REPO_DIR / "config" / "pipeline_config.yaml"

        if not Path(config_path).exists():
            # Use example config
            config_path = REPO_DIR / "config" / "pipeline_config.yaml.example"

        with open(config_path) as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        """Setup logging."""
        log_dir = Path(self.config.get("logging", {}).get("log_dir", "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        level = self.config.get("logging", {}).get("level", "INFO")

        logging.basicConfig(
            level=getattr(logging, level),
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _record_pipeline_run(
        self,
        phase: str,
        status: str,
        batch_number: int = None,
        items_processed: int = 0,
        items_total: int = None,
        error_message: str = None
    ):
        """Record pipeline run in database."""
        import sqlite3

        db_path = self.config["directories"]["database"]
        conn = sqlite3.connect(db_path)

        try:
            conn.execute("""
                INSERT INTO pipeline_runs
                (run_id, batch_number, phase, status, items_processed, items_total,
                 started_date, completed_date, error_message, config_snapshot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                batch_number,
                phase,
                status,
                items_processed,
                items_total,
                datetime.now(),
                datetime.now() if status in ('completed', 'failed') else None,
                error_message,
                json.dumps(self.config)
            ))
            conn.commit()
        except Exception as e:
            self.logger.warning(f"Could not record pipeline run: {e}")
        finally:
            conn.close()

    def run_download_phase(self, batch_size: int, batch_number: int = None, start_from: int = 0) -> bool:
        """
        Run download phase using existing archive_cluster_downloader.py.

        Args:
            batch_size: Number of items to download
            batch_number: Batch number for tracking
            start_from: Starting position in search results

        Returns:
            True if successful
        """
        self.logger.info("=" * 70)
        self.logger.info(f"PHASE 1: DOWNLOAD (Batch {batch_number or 'N/A'})")
        self.logger.info("=" * 70)

        # Use identifier-based downloader to avoid API pagination bugs
        pipeline_dir = Path(__file__).parent.parent
        downloader_script = pipeline_dir / "orchestration" / "download_from_identifiers.py"

        if not downloader_script.exists():
            self.logger.error(f"Downloader script not found: {downloader_script}")
            return False

        # Build command using identifier-based downloader
        pdf_dir = Path(self.config["directories"]["pdf_dir"])
        pdf_dir.mkdir(parents=True, exist_ok=True)

        db_path = self.config["directories"]["database"]
        download_cfg = self.config.get("download", {})

        # Check for identifiers file
        identifiers_file = download_cfg.get("identifiers_file")
        if not identifiers_file:
            self.logger.error("No identifiers_file specified in config download section")
            self.logger.error("Run fetch_identifiers.py first and add path to config")
            return False

        identifiers_path = Path(identifiers_file)
        if not identifiers_path.exists():
            self.logger.error(f"Identifiers file not found: {identifiers_path}")
            return False

        cmd = [
            "python3",
            str(downloader_script),
            "--identifiers-file", str(identifiers_path),
            "--start-from", str(start_from),
            "--max-items", str(batch_size),
            "--download-dir", str(pdf_dir),
            "--db-path", str(db_path),
            "--delay", str(download_cfg.get("delay", 0.05)),
        ]

        if download_cfg.get("download_all_pdfs"):
            cmd.append("--download-all-pdfs")

        if download_cfg.get("subcollection"):
            cmd.extend(["--subcollection", download_cfg["subcollection"]])

        self.logger.info(f"Running: {' '.join(cmd)}")

        try:
            # Don't capture output - let it stream to console to avoid buffer blocking
            result = subprocess.run(cmd, check=True)
            self.logger.info("Download phase completed successfully")
            self._record_pipeline_run("download", "completed", batch_number, batch_size)
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Download phase failed: {e}")
            self._record_pipeline_run("download", "failed", batch_number, 0,
                                     error_message=str(e))
            return False

    def run_ocr_phase(self, batch_number: int = None) -> bool:
        """
        Run OCR phase using existing olmocr scripts.

        Note: This submits SLURM jobs and waits for completion.

        Returns:
            True if successful
        """
        self.logger.info("=" * 70)
        self.logger.info(f"PHASE 2: OCR PROCESSING (Batch {batch_number or 'N/A'})")
        self.logger.info("=" * 70)

        olmocr_repo = Path(self.config["components"]["olmocr_repo"])
        submit_script = olmocr_repo / "smart_submit_pdf_jobs.sh"

        if not submit_script.exists():
            self.logger.error(f"olmOCR submit script not found: {submit_script}")
            return False

        pdf_dir = Path(self.config["directories"]["pdf_dir"])
        ocr_cfg = self.config.get("ocr", {})

        # Build environment variables for olmocr script
        env = os.environ.copy()
        if ocr_cfg.get("workers") and ocr_cfg["workers"] != "auto":
            env["WORKERS"] = str(ocr_cfg["workers"])
        if ocr_cfg.get("pages_per_group") and ocr_cfg["pages_per_group"] != "auto":
            env["PAGES_PER_GROUP"] = str(ocr_cfg["pages_per_group"])

        # Submit olmOCR jobs
        cmd = [
            str(submit_script),
            "--pdf-dir", str(pdf_dir),
        ]

        self.logger.info(f"Submitting olmOCR jobs: {' '.join(cmd)}")

        try:
            # Submit jobs
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                env=env,
                cwd=str(olmocr_repo)
            )

            self.logger.info("olmOCR jobs submitted")
            self.logger.info(result.stdout)

            # Wait for jobs to complete
            self.logger.info("Waiting for olmOCR jobs to complete...")
            max_wait = ocr_cfg.get("max_wait_hours", 24) * 3600
            self._wait_for_slurm_jobs(max_wait)

            self._record_pipeline_run("ocr", "completed", batch_number)
            return True

        except subprocess.CalledProcessError as e:
            self.logger.error(f"OCR phase failed: {e}")
            self.logger.error(f"STDOUT: {e.stdout}")
            self.logger.error(f"STDERR: {e.stderr}")
            self._record_pipeline_run("ocr", "failed", batch_number,
                                     error_message=str(e))
            return False

    def _wait_for_slurm_jobs(self, max_wait_seconds: int):
        """Wait for SLURM jobs to complete."""
        start_time = time.time()
        check_interval = 60  # Check every minute

        while time.time() - start_time < max_wait_seconds:
            # Check if any olmocr jobs are running
            try:
                result = subprocess.run(
                    ["squeue", "-u", os.environ.get("USER"), "-n", "olmocr_pdf", "-h"],
                    capture_output=True,
                    text=True
                )

                if not result.stdout.strip():
                    self.logger.info("All olmOCR jobs completed")
                    return

                # Count running jobs
                job_count = len(result.stdout.strip().split("\n"))
                self.logger.info(f"  {job_count} olmOCR jobs still running...")

            except Exception as e:
                self.logger.warning(f"Could not check SLURM queue: {e}")

            time.sleep(check_interval)

        self.logger.error(f"Timeout waiting for olmOCR jobs ({max_wait_seconds}s)")
        raise TimeoutError("olmOCR jobs did not complete in time")

    def run_ingest_phase(self, batch_number: int = None) -> bool:
        """
        Run ingestion phase using existing ingest_ocr_results.py.

        Returns:
            True if successful
        """
        self.logger.info("=" * 70)
        self.logger.info(f"PHASE 3: INGEST OCR RESULTS (Batch {batch_number or 'N/A'})")
        self.logger.info("=" * 70)

        downloader_repo = Path(self.config["components"]["downloader_repo"])
        ingest_script = downloader_repo / "ingest_ocr_results.py"

        if not ingest_script.exists():
            self.logger.error(f"Ingestion script not found: {ingest_script}")
            return False

        pdf_dir = Path(self.config["directories"]["pdf_dir"])
        db_path = self.config["directories"]["database"]

        # Build command
        cmd = [
            "python3",
            str(ingest_script),
            str(pdf_dir),
            "--db-path", str(db_path),
            "--parse-jsonl",
        ]

        self.logger.info(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info("Ingestion phase completed successfully")
            self.logger.info(result.stdout)
            self._record_pipeline_run("ingest", "completed", batch_number)
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Ingestion phase failed: {e}")
            self.logger.error(f"STDOUT: {e.stdout}")
            self.logger.error(f"STDERR: {e.stderr}")
            self._record_pipeline_run("ingest", "failed", batch_number,
                                     error_message=str(e))
            return False

    def run_cleanup_phase(self, batch_number: int = None, dry_run: bool = False) -> bool:
        """
        Run cleanup phase using cleanup_pdfs.py.

        Returns:
            True if successful
        """
        self.logger.info("=" * 70)
        self.logger.info(f"PHASE 4: CLEANUP PDFs (Batch {batch_number or 'N/A'})")
        self.logger.info("=" * 70)

        cleanup_script = SCRIPT_DIR / "cleanup_pdfs.py"
        db_path = self.config["directories"]["database"]

        cleanup_cfg = self.config.get("cleanup", {})

        # Build command
        cmd = [
            "python3",
            str(cleanup_script),
            "--db-path", str(db_path),
            "--grace-period", str(cleanup_cfg.get("grace_period_days", 7)),
            "--max-deletions", str(self.config.get("safety", {}).get("max_deletions_per_run", 2000)),
        ]

        if dry_run or not cleanup_cfg.get("auto_delete", False):
            cmd.append("--dry-run")

        if not cleanup_cfg.get("require_confirmation", True):
            cmd.append("--no-confirm")

        self.logger.info(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info("Cleanup phase completed successfully")
            self.logger.info(result.stdout)
            self._record_pipeline_run("cleanup", "completed", batch_number)
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Cleanup phase failed: {e}")
            self.logger.error(f"STDOUT: {e.stdout}")
            self.logger.error(f"STDERR: {e.stderr}")
            self._record_pipeline_run("cleanup", "failed", batch_number,
                                     error_message=str(e))
            return False

    def run_batch(self, batch_size: int, batch_number: int, cleanup: bool = True, start_from: int = 0) -> bool:
        """
        Run complete pipeline for one batch.

        Args:
            batch_size: Number of items in batch
            batch_number: Batch number for tracking
            cleanup: Whether to run cleanup phase
            start_from: Starting position in search results

        Returns:
            True if all phases successful
        """
        self.logger.info("")
        self.logger.info("#" * 70)
        self.logger.info(f"# BATCH {batch_number}: Processing {batch_size} items (starting from {start_from})")
        self.logger.info("#" * 70)
        self.logger.info("")

        # Phase 1: Download
        if not self.run_download_phase(batch_size, batch_number, start_from):
            self.logger.error(f"Batch {batch_number} failed at download phase")
            return False

        # Phase 2: OCR
        if not self.run_ocr_phase(batch_number):
            self.logger.error(f"Batch {batch_number} failed at OCR phase")
            return False

        # Phase 3: Ingest
        if not self.run_ingest_phase(batch_number):
            self.logger.error(f"Batch {batch_number} failed at ingestion phase")
            return False

        # Phase 4: Cleanup (optional)
        if cleanup:
            if not self.run_cleanup_phase(batch_number):
                self.logger.warning(f"Batch {batch_number} cleanup had issues (non-fatal)")

        self.logger.info("")
        self.logger.info(f"✓ Batch {batch_number} completed successfully")
        self.logger.info("")

        return True

    def run_batches(
        self,
        total_items: int,
        batch_size: int = 1000,
        start_batch: int = 1,
        cleanup: bool = True
    ):
        """
        Run pipeline in batches.

        Args:
            total_items: Total number of items to process
            batch_size: Items per batch
            start_batch: Batch number to start from (for resuming)
            cleanup: Whether to run cleanup after each batch
        """
        self.total_batches = (total_items + batch_size - 1) // batch_size

        self.logger.info("=" * 70)
        self.logger.info("ARCHIVE-OLM PIPELINE - BATCH PROCESSING")
        self.logger.info("=" * 70)
        self.logger.info(f"Run ID: {self.run_id}")
        self.logger.info(f"Total items: {total_items}")
        self.logger.info(f"Batch size: {batch_size}")
        self.logger.info(f"Total batches: {self.total_batches}")
        self.logger.info(f"Starting from batch: {start_batch}")
        self.logger.info(f"Auto cleanup: {cleanup}")
        self.logger.info("=" * 70)

        for batch_num in range(start_batch, self.total_batches + 1):
            self.current_batch = batch_num

            # Calculate items for this batch
            items_remaining = total_items - ((batch_num - 1) * batch_size)
            items_this_batch = min(batch_size, items_remaining)

            # Calculate starting position in search results
            start_from = (batch_num - 1) * batch_size

            # Run the batch
            success = self.run_batch(items_this_batch, batch_num, cleanup, start_from)

            if not success:
                self.logger.error(f"Pipeline failed at batch {batch_num}")
                self.logger.error("Fix issues and resume with: --start-batch {batch_num}")
                return False

            # Delay between batches
            if batch_num < self.total_batches:
                delay = self.config.get("batching", {}).get("batch_delay", 300)
                self.logger.info(f"Waiting {delay}s before next batch...")
                time.sleep(delay)

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("✓ ALL BATCHES COMPLETED SUCCESSFULLY")
        self.logger.info("=" * 70)
        self.logger.info(f"Processed {total_items} items in {self.total_batches} batches")

        return True


def main():
    parser = argparse.ArgumentParser(
        description="Archive-OLM Pipeline Orchestrator"
    )

    parser.add_argument(
        "--config",
        help="Path to configuration file (default: config/pipeline_config.yaml)"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # run-batches command
    batches_parser = subparsers.add_parser(
        "run-batches",
        help="Process items in batches (download → OCR → ingest → cleanup)"
    )
    batches_parser.add_argument(
        "--total-items",
        type=int,
        required=True,
        help="Total number of items to process"
    )
    batches_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Items per batch (default: 1000)"
    )
    batches_parser.add_argument(
        "--start-batch",
        type=int,
        default=1,
        help="Batch number to start from (for resuming)"
    )
    batches_parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip cleanup phase"
    )

    # Individual phase commands
    for phase in ["download", "ocr", "ingest", "cleanup"]:
        phase_parser = subparsers.add_parser(phase, help=f"Run {phase} phase only")
        if phase == "download":
            phase_parser.add_argument(
                "--batch-size",
                type=int,
                default=1000,
                help="Number of items to download"
            )
        if phase == "cleanup":
            phase_parser.add_argument(
                "--dry-run",
                action="store_true",
                help="Preview without deleting"
            )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Create orchestrator
    orchestrator = PipelineOrchestrator(args.config)

    # Run command
    if args.command == "run-batches":
        success = orchestrator.run_batches(
            total_items=args.total_items,
            batch_size=args.batch_size,
            start_batch=args.start_batch,
            cleanup=not args.no_cleanup
        )
        sys.exit(0 if success else 1)

    elif args.command == "download":
        success = orchestrator.run_download_phase(args.batch_size)
        sys.exit(0 if success else 1)

    elif args.command == "ocr":
        success = orchestrator.run_ocr_phase()
        sys.exit(0 if success else 1)

    elif args.command == "ingest":
        success = orchestrator.run_ingest_phase()
        sys.exit(0 if success else 1)

    elif args.command == "cleanup":
        success = orchestrator.run_cleanup_phase(dry_run=args.dry_run)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
