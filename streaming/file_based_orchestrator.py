#!/usr/bin/env python3
"""
File-Based Streaming Pipeline Orchestrator

Coordinates three concurrent processes:
1. Continuous downloader
2. OCR dispatcher
3. Cleanup worker

All state tracked via JSON files - no database required.
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


class FileBasedOrchestrator:
    """Orchestrate file-based streaming pipeline processes."""

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

    def _launch_downloader(
        self,
        identifiers_file: Path,
        start_from: int,
        max_items: int,
        base_dir: Path,
        collection: str
    ) -> subprocess.Popen:
        """Launch continuous downloader process."""
        download_cfg = self.config.get("download", {})

        cmd = [
            "python3",
            "-u",  # Unbuffered output
            str(self.streaming_dir / "file_based_downloader.py"),
            "--identifiers-file", str(identifiers_file),
            "--start-from", str(start_from),
            "--max-items", str(max_items),
            "--base-dir", str(base_dir),
            "--delay", str(download_cfg.get("delay", 0.05)),
            "--collection", collection,
            "--disk-threshold", "0.90"
        ]

        self.logger.info(f"Launching downloader: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            env=os.environ.copy(),
            stdout=sys.stdout,
            stderr=sys.stderr,
            bufsize=0
        )
        return process

    def _launch_dispatcher(
        self,
        base_dir: Path
    ) -> subprocess.Popen:
        """Launch OCR dispatcher process."""
        olmocr_repo = Path(self.config["components"]["olmocr_repo"])
        olmocr_script = olmocr_repo / "smart_submit_pdf_jobs.sh"

        ocr_cfg = self.config.get("ocr", {})
        pdfs_per_batch = ocr_cfg.get("pdfs_per_batch", 200)

        cmd = [
            "python3",
            "-u",  # Unbuffered output
            str(self.streaming_dir / "file_based_dispatcher.py"),
            "--base-dir", str(base_dir),
            "--olmocr-script", str(olmocr_script),
            "--pdfs-per-chunk", str(pdfs_per_batch),
            "--check-interval", "60"
        ]

        self.logger.info(f"Launching dispatcher: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            env=os.environ.copy(),
            stdout=sys.stdout,
            stderr=sys.stderr,
            bufsize=0
        )
        return process

    def _launch_cleanup_worker(
        self,
        base_dir: Path
    ) -> subprocess.Popen:
        """Launch cleanup worker process."""
        split_script = self.repo_dir / "orchestration" / "split_jsonl_to_json.py"

        cmd = [
            "python3",
            "-u",  # Unbuffered output
            str(self.streaming_dir / "file_based_cleanup.py"),
            "--base-dir", str(base_dir),
            "--split-script", str(split_script),
            "--check-interval", "60"
        ]

        self.logger.info(f"Launching cleanup worker: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            env=os.environ.copy(),
            stdout=sys.stdout,
            stderr=sys.stderr,
            bufsize=0
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
        self.logger.info("FILE-BASED STREAMING PIPELINE")
        self.logger.info("=" * 70)

        try:
            # Get configuration
            base_dir = Path(self.config["directories"]["base_dir"])
            identifiers_file = Path(self.config["download"]["identifiers_file"])
            collection = self.config["download"].get("collection", "unknown")

            self.logger.info(f"Base directory: {base_dir}")
            self.logger.info(f"Collection: {collection}")
            self.logger.info("=" * 70)

            # Launch processes
            self.processes.append(
                self._launch_downloader(
                    identifiers_file,
                    start_from,
                    max_items,
                    base_dir,
                    collection
                )
            )

            # Give downloader time to start
            time.sleep(5)

            self.processes.append(
                self._launch_dispatcher(base_dir)
            )

            self.processes.append(
                self._launch_cleanup_worker(base_dir)
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
        description="File-based streaming pipeline orchestrator"
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

    orchestrator = FileBasedOrchestrator(args.config)
    exit_code = orchestrator.run(args.start_from, args.max_items)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
