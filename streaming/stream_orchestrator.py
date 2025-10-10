#!/usr/bin/env python3
"""
Streaming Pipeline Orchestrator

Coordinates three concurrent processes:
1. Continuous downloader
2. OCR dispatcher
3. Cleanup worker
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List

import yaml


class StreamOrchestrator:
    """Orchestrate streaming pipeline processes."""

    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.processes: List[subprocess.Popen] = []

        # Get repository directories
        self.repo_dir = Path(__file__).parent.parent
        self.streaming_dir = self.repo_dir / "streaming"

    def _load_config(self, config_path: str) -> dict:
        """Load pipeline configuration."""
        with open(config_path) as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        """Setup logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def _get_db_path(self) -> str:
        """Get database path (prefer environment variable for local copy)."""
        env_db_path = os.environ.get('PIPELINE_DB_PATH')
        if env_db_path:
            self.logger.info(f"Using database from environment: {env_db_path}")
            return env_db_path

        config_db_path = self.config["directories"]["database"]
        self.logger.info(f"Using database from config: {config_db_path}")
        return config_db_path

    def _setup_directories(self, pdf_dir: Path):
        """Setup working directories for streaming pipeline."""
        # Create main directories
        download_queue = pdf_dir / "download_queue"
        ocr_processing = pdf_dir / "ocr_processing"
        ocr_completed = pdf_dir / "ocr_completed"

        for d in [download_queue, ocr_processing, ocr_completed]:
            d.mkdir(parents=True, exist_ok=True)

        return download_queue, ocr_processing, ocr_completed

    def _launch_downloader(
        self,
        identifiers_file: Path,
        start_from: int,
        max_items: int,
        download_queue: Path,
        db_path: str
    ) -> subprocess.Popen:
        """Launch continuous downloader process."""
        download_cfg = self.config.get("download", {})

        cmd = [
            "python3",
            str(self.streaming_dir / "continuous_downloader.py"),
            "--identifiers-file", str(identifiers_file),
            "--start-from", str(start_from),
            "--max-items", str(max_items),
            "--download-queue", str(download_queue),
            "--db-path", db_path,
            "--delay", str(download_cfg.get("delay", 0.05)),
            "--disk-threshold", "0.90"
        ]

        if download_cfg.get("subcollection"):
            cmd.extend(["--subcollection", download_cfg["subcollection"]])

        self.logger.info(f"Launching downloader: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            env=os.environ.copy(),
            stdout=sys.stdout,
            stderr=sys.stderr,
            bufsize=0  # Unbuffered
        )
        return process

    def _launch_dispatcher(
        self,
        download_queue: Path,
        ocr_processing: Path
    ) -> subprocess.Popen:
        """Launch OCR dispatcher process."""
        olmocr_repo = Path(self.config["components"]["olmocr_repo"])
        olmocr_script = olmocr_repo / "smart_submit_pdf_jobs.sh"

        ocr_cfg = self.config.get("ocr", {})
        pages_per_chunk = ocr_cfg.get("max_pages_per_chunk", 1000)

        cmd = [
            "python3",
            str(self.streaming_dir / "ocr_dispatcher.py"),
            "--download-queue", str(download_queue),
            "--ocr-processing", str(ocr_processing),
            "--olmocr-script", str(olmocr_script),
            "--pages-per-chunk", str(pages_per_chunk),
            "--check-interval", "60"
        ]

        self.logger.info(f"Launching dispatcher: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            env=os.environ.copy(),
            stdout=sys.stdout,
            stderr=sys.stderr,
            bufsize=0  # Unbuffered
        )
        return process

    def _launch_cleanup_worker(
        self,
        ocr_processing: Path,
        ocr_completed: Path,
        db_path: str
    ) -> subprocess.Popen:
        """Launch cleanup worker process."""
        downloader_repo = Path(self.config["components"]["downloader_repo"])
        ingest_script = downloader_repo / "ingest_ocr_results.py"
        split_script = self.repo_dir / "orchestration" / "split_jsonl_to_json.py"

        cmd = [
            "python3",
            str(self.streaming_dir / "cleanup_worker.py"),
            "--ocr-processing", str(ocr_processing),
            "--ocr-completed", str(ocr_completed),
            "--split-script", str(split_script),
            "--ingest-script", str(ingest_script),
            "--db-path", db_path,
            "--check-interval", "60",
            "--dispatcher-state", str(ocr_processing / ".dispatcher_state")
        ]

        self.logger.info(f"Launching cleanup worker: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            env=os.environ.copy(),
            stdout=sys.stdout,
            stderr=sys.stderr,
            bufsize=0  # Unbuffered
        )
        return process

    def _cleanup(self):
        """Cleanup on exit."""
        self.logger.info("Stopping all processes...")
        for process in self.processes:
            if process.poll() is None:  # Still running
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()

    def run(self, start_from: int, max_items: int):
        """Run streaming pipeline."""
        # Register signal handlers
        signal.signal(signal.SIGTERM, lambda s, f: self._cleanup())
        signal.signal(signal.SIGINT, lambda s, f: self._cleanup())

        self.logger.info("=" * 70)
        self.logger.info("STREAMING PIPELINE")
        self.logger.info("=" * 70)

        try:
            # Get configuration
            pdf_dir = Path(self.config["directories"]["pdf_dir"])
            identifiers_file = Path(self.config["download"]["identifiers_file"])
            db_path = self._get_db_path()

            # Setup directories
            download_queue, ocr_processing, ocr_completed = self._setup_directories(pdf_dir)

            self.logger.info(f"PDF directory: {pdf_dir}")
            self.logger.info(f"Download queue: {download_queue}")
            self.logger.info(f"OCR processing: {ocr_processing}")
            self.logger.info(f"OCR completed: {ocr_completed}")
            self.logger.info(f"Database: {db_path}")
            self.logger.info("=" * 70)

            # Launch processes
            self.processes.append(
                self._launch_downloader(
                    identifiers_file,
                    start_from,
                    max_items,
                    download_queue,
                    db_path
                )
            )

            # Give downloader time to start
            time.sleep(5)

            self.processes.append(
                self._launch_dispatcher(
                    download_queue,
                    ocr_processing
                )
            )

            self.processes.append(
                self._launch_cleanup_worker(
                    ocr_processing,
                    ocr_completed,
                    db_path
                )
            )

            self.logger.info("All processes launched")
            self.logger.info("=" * 70)

            # Monitor processes
            while True:
                # Check if any process died
                for i, process in enumerate(self.processes):
                    if process.poll() is not None:
                        self.logger.error(f"Process {i} exited with code {process.returncode}")
                        return process.returncode

                time.sleep(10)

        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
            self._cleanup()
            return 0

        except Exception as e:
            self.logger.error(f"Error: {e}")
            self._cleanup()
            return 1


def main():
    parser = argparse.ArgumentParser(
        description="Streaming pipeline orchestrator"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to pipeline configuration YAML"
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Starting index in identifiers list"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        required=True,
        help="Maximum items to process"
    )

    args = parser.parse_args()

    orchestrator = StreamOrchestrator(args.config)
    exit_code = orchestrator.run(args.start_from, args.max_items)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
