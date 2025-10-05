#!/usr/bin/env python3
"""
Safe PDF cleanup after OCR ingestion.

Deletes PDF files only after verifying OCR data is safely stored in database.
Multiple safety checks prevent accidental data loss.
"""

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

# Add parent directory to path to import from IA_downloader_cluster
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "InternetArchive"))

try:
    from archive_db import ArchiveDatabase
except ImportError:
    print("ERROR: Could not import archive_db. Make sure IA_downloader_cluster is accessible.")
    sys.exit(1)


class PDFCleanup:
    """Safe PDF deletion with multiple verification levels."""

    def __init__(
        self,
        db_path: str,
        grace_period_days: int = 7,
        dry_run: bool = False,
        require_confirmation: bool = True,
        max_deletions: int = 2000,
    ):
        self.db_path = db_path
        self.grace_period_days = grace_period_days
        self.dry_run = dry_run
        self.require_confirmation = require_confirmation
        self.max_deletions = max_deletions

        self.db = ArchiveDatabase(db_path)
        self._setup_logging()

        self.stats = {
            "checked": 0,
            "safe_to_delete": 0,
            "deleted": 0,
            "failed": 0,
            "skipped": 0,
        }

    def _setup_logging(self):
        """Setup logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(__name__)

    def is_safe_to_delete(self, pdf_record: Dict) -> Tuple[bool, str]:
        """
        Multi-level safety check before deletion.

        Returns:
            (safe, reason) tuple
        """
        pdf_id = pdf_record["id"]
        filepath = pdf_record["filepath"]
        filename = pdf_record["filename"]

        # Check 1: PDF must exist and be marked as downloaded
        if pdf_record["download_status"] != "downloaded":
            return False, f"Status is '{pdf_record['download_status']}', not 'downloaded'"

        if not filepath or filepath == "NULL":
            return False, "No filepath in database"

        pdf_path = Path(filepath)
        if not pdf_path.exists():
            return False, f"PDF file not found at {filepath}"

        # Check 2: OCR must be completed
        ocr_record = self.db.conn.execute(
            """
            SELECT status, ocr_data, completed_date, json_output_path
            FROM ocr_processing
            WHERE pdf_file_id = ?
            """,
            (pdf_id,),
        ).fetchone()

        if not ocr_record:
            return False, "No OCR record found"

        if ocr_record["status"] != "completed":
            return False, f"OCR status is '{ocr_record['status']}', not 'completed'"

        # Check 3: OCR data must be stored in database
        if not ocr_record["ocr_data"]:
            return False, "No OCR data in database (ocr_data column is NULL)"

        # Check 4: OCR data must be valid JSON
        try:
            ocr_data = json.loads(ocr_record["ocr_data"])
            if not ocr_data or len(ocr_data) == 0:
                return False, "OCR data is empty list"
        except json.JSONDecodeError as e:
            return False, f"OCR data is invalid JSON: {e}"

        # Check 5: Grace period must have passed
        if ocr_record["completed_date"]:
            completed = datetime.fromisoformat(ocr_record["completed_date"])
            grace_cutoff = datetime.now() - timedelta(days=self.grace_period_days)

            if completed > grace_cutoff:
                days_left = self.grace_period_days - (datetime.now() - completed).days
                return False, f"Grace period not elapsed (wait {days_left} more days)"

        # Check 6: Verify OCR output file exists (if specified)
        if ocr_record["json_output_path"]:
            ocr_file = Path(ocr_record["json_output_path"])
            if not ocr_file.exists():
                self.logger.warning(
                    f"OCR output file missing for {filename}: {ocr_file}. "
                    "Data is in database so proceeding."
                )

        # All checks passed
        return True, "All safety checks passed"

    def find_candidates(self, older_than_days: int = None) -> List[Dict]:
        """
        Find PDFs that are candidates for deletion.

        Args:
            older_than_days: Additional age filter beyond grace period

        Returns:
            List of PDF records that may be safe to delete
        """
        query = """
            SELECT p.id, p.identifier, p.filename, p.filepath,
                   p.download_status, p.subcollection, p.download_date
            FROM pdf_files p
            WHERE p.download_status = 'downloaded'
              AND p.filepath IS NOT NULL
              AND p.filepath != 'NULL'
            ORDER BY p.download_date
        """

        candidates = []
        for row in self.db.conn.execute(query):
            pdf_record = dict(row)

            # Apply age filter if specified
            if older_than_days:
                download_date = datetime.fromisoformat(pdf_record["download_date"])
                age_cutoff = datetime.now() - timedelta(days=older_than_days)
                if download_date > age_cutoff:
                    continue

            candidates.append(pdf_record)

        return candidates

    def delete_pdf(self, pdf_record: Dict) -> bool:
        """
        Delete PDF file and update database.

        Args:
            pdf_record: PDF database record

        Returns:
            True if successful
        """
        pdf_id = pdf_record["id"]
        filepath = pdf_record["filepath"]
        filename = pdf_record["filename"]

        try:
            # Delete physical file
            pdf_path = Path(filepath)
            if pdf_path.exists():
                pdf_path.unlink()
                self.logger.info(f"Deleted: {filename}")
            else:
                self.logger.warning(f"File already missing: {filepath}")

            # Update database
            self.db.conn.execute(
                """
                UPDATE pdf_files
                SET download_status = 'deleted',
                    deleted_date = ?
                WHERE id = ?
                """,
                (datetime.now(), pdf_id),
            )

            # Log in audit table
            self.db._log_audit(
                "cleanup",
                "pdf_files",
                pdf_id,
                {"filename": filename, "original_path": filepath},
            )

            self.db.conn.commit()
            return True

        except Exception as e:
            self.logger.error(f"Error deleting {filename}: {e}")
            return False

    def run_cleanup(
        self, older_than_days: int = None, subcollection: str = None
    ) -> Dict:
        """
        Run cleanup operation.

        Args:
            older_than_days: Only delete PDFs older than this
            subcollection: Filter by subcollection

        Returns:
            Statistics dictionary
        """
        self.logger.info("=" * 70)
        self.logger.info("PDF Cleanup Operation")
        self.logger.info("=" * 70)
        self.logger.info(f"Database: {self.db_path}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        self.logger.info(f"Grace period: {self.grace_period_days} days")
        if older_than_days:
            self.logger.info(f"Age filter: Older than {older_than_days} days")
        if subcollection:
            self.logger.info(f"Subcollection filter: {subcollection}")
        self.logger.info("-" * 70)

        # Find candidates
        candidates = self.find_candidates(older_than_days)

        if subcollection:
            candidates = [c for c in candidates if c["subcollection"] == subcollection]

        self.logger.info(f"Found {len(candidates)} PDF files to check")
        self.logger.info("")

        # Check each candidate
        safe_to_delete = []
        for pdf_record in candidates:
            self.stats["checked"] += 1
            filename = pdf_record["filename"]

            is_safe, reason = self.is_safe_to_delete(pdf_record)

            if is_safe:
                safe_to_delete.append(pdf_record)
                self.stats["safe_to_delete"] += 1
                self.logger.info(f"✓ Safe to delete: {filename}")
            else:
                self.stats["skipped"] += 1
                if self.logger.level == logging.DEBUG:
                    self.logger.debug(f"✗ Cannot delete {filename}: {reason}")

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info(f"Safety check complete: {self.stats['safe_to_delete']} safe to delete")
        self.logger.info("=" * 70)

        if not safe_to_delete:
            self.logger.info("No PDFs ready for deletion.")
            return self.stats

        # Apply max deletions limit
        if len(safe_to_delete) > self.max_deletions:
            self.logger.warning(
                f"Found {len(safe_to_delete)} PDFs but limiting to {self.max_deletions}"
            )
            safe_to_delete = safe_to_delete[: self.max_deletions]

        # Show what will be deleted
        self.logger.info("")
        self.logger.info("PDFs to be deleted:")
        for i, pdf in enumerate(safe_to_delete[:10], 1):
            self.logger.info(f"  {i}. {pdf['filename']}")
        if len(safe_to_delete) > 10:
            self.logger.info(f"  ... and {len(safe_to_delete) - 10} more")

        # Confirm if required
        if self.require_confirmation and not self.dry_run:
            self.logger.info("")
            response = input(
                f"Delete {len(safe_to_delete)} PDF files? [yes/no]: "
            ).lower()
            if response != "yes":
                self.logger.info("Deletion cancelled by user.")
                return self.stats

        # Delete files
        if not self.dry_run:
            self.logger.info("")
            self.logger.info("Deleting PDFs...")
            for pdf_record in safe_to_delete:
                if self.delete_pdf(pdf_record):
                    self.stats["deleted"] += 1
                else:
                    self.stats["failed"] += 1

        # Print summary
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("Cleanup Summary")
        self.logger.info("=" * 70)
        self.logger.info(f"Checked: {self.stats['checked']}")
        self.logger.info(f"Safe to delete: {self.stats['safe_to_delete']}")
        if self.dry_run:
            self.logger.info("(Dry run - no files deleted)")
        else:
            self.logger.info(f"Deleted: {self.stats['deleted']}")
            self.logger.info(f"Failed: {self.stats['failed']}")
        self.logger.info(f"Skipped: {self.stats['skipped']}")

        return self.stats


def main():
    parser = argparse.ArgumentParser(
        description="Safely delete PDFs after OCR data is stored in database"
    )
    parser.add_argument(
        "--db-path",
        default="archive_tracking.db",
        help="Path to database (default: archive_tracking.db)",
    )
    parser.add_argument(
        "--grace-period",
        type=int,
        default=7,
        help="Days to wait after OCR completion before deleting (default: 7)",
    )
    parser.add_argument(
        "--older-than",
        type=int,
        help="Only delete PDFs older than N days",
    )
    parser.add_argument(
        "--subcollection",
        help="Only process this subcollection",
    )
    parser.add_argument(
        "--max-deletions",
        type=int,
        default=2000,
        help="Maximum PDFs to delete in one run (default: 2000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting",
    )
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Skip confirmation prompt (use with caution)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show debug output including skipped files",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run cleanup
    cleanup = PDFCleanup(
        db_path=args.db_path,
        grace_period_days=args.grace_period,
        dry_run=args.dry_run,
        require_confirmation=not args.no_confirm,
        max_deletions=args.max_deletions,
    )

    stats = cleanup.run_cleanup(
        older_than_days=args.older_than,
        subcollection=args.subcollection,
    )

    # Exit code based on results
    if stats["failed"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
