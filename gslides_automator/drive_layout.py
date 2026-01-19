from __future__ import annotations

from dataclasses import dataclass
import csv
import io
import re
import time
from typing import Dict, Iterable, List, Optional, Sequence, Set

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


@dataclass
class DriveLayout:
    root_id: str
    l0_raw_id: str
    l1_merged_id: str
    l2_slide_id: str
    l3_pdf_id: str
    templates_id: str
    data_template_id: str
    report_template_id: str
    entities_csv_id: str


def _extract_id_from_url(shared_drive_url: str) -> str:
    """
    Extract a Drive folder/file ID from a shared Drive URL or raw ID.

    Google Drive IDs are typically 20-50 characters and contain alphanumerics,
    underscores, and hyphens, but don't start or end with hyphens.
    """
    # First try to extract from URL patterns
    patterns = [
        r"/folders/([A-Za-z0-9_\-]+)",
        r"[?&]id=([A-Za-z0-9_\-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, shared_drive_url)
        if match:
            return match.group(1)

    # If no URL pattern matches, check if it's a raw ID
    # Google Drive IDs are typically 19+ characters, contain alphanumerics/underscores/hyphens,
    # but don't start or end with hyphens, and don't contain spaces or other special chars
    # Also check that it doesn't look like a phrase (multiple consecutive lowercase words)
    if (
        re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_\-]*[A-Za-z0-9_]", shared_drive_url)
        and len(shared_drive_url) >= 19
        and " " not in shared_drive_url
        and not re.search(
            r"[a-z]+-[a-z]+-[a-z]+", shared_drive_url
        )  # Reject phrase-like patterns
    ):
        return shared_drive_url

    raise ValueError(
        "Could not extract Drive folder ID from URL. Pass a folder link or ID."
    )


def retry_with_exponential_backoff(
    func, max_retries=5, initial_delay=1, max_delay=60, backoff_factor=2
):
    """
    Retry a function with exponential backoff on 429 (Too Many Requests) and 5xx (Server) errors.

    Args:
        func: Function to retry (should be a callable that takes no arguments)
        max_retries: Maximum number of retry attempts (default: 5)
        initial_delay: Initial delay in seconds before first retry (default: 1)
        max_delay: Maximum delay in seconds between retries (default: 60)
        backoff_factor: Factor to multiply delay by after each retry (default: 2)

    Returns:
        The return value of func() if successful

    Raises:
        HttpError: If the error is not retryable or if max_retries is exceeded
        Exception: Any other exception raised by func()
    """
    delay = initial_delay

    for attempt in range(max_retries + 1):
        try:
            return func()
        except HttpError as error:
            status = error.resp.status
            # Check if it's a retryable error (429 Too Many Requests or 5xx Server Errors)
            is_retryable = (status == 429) or (500 <= status < 600)

            if is_retryable:
                if attempt < max_retries:
                    # Calculate wait time with exponential backoff
                    wait_time = min(delay, max_delay)
                    if status == 429:
                        error_msg = "Rate limit exceeded (429)"
                    else:
                        error_msg = f"Server error ({status})"
                    print(
                        f"    ⚠️  {error_msg}. Retrying in {wait_time:.1f} seconds... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    delay *= backoff_factor
                else:
                    if status == 429:
                        error_msg = "Rate limit exceeded (429)"
                    else:
                        error_msg = f"Server error ({status})"
                    print(f"    ✗ {error_msg}. Max retries ({max_retries}) reached.")
                    raise
            else:
                # For non-retryable errors, re-raise immediately
                raise
        except Exception as e:
            # For non-HttpError exceptions, check if it's a rate limit error
            error_str = str(e).lower()
            if "429" in error_str or "rate limit" in error_str or "quota" in error_str:
                if attempt < max_retries:
                    wait_time = min(delay, max_delay)
                    print(
                        f"    ⚠️  Rate limit error. Retrying in {wait_time:.1f} seconds... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    delay *= backoff_factor
                else:
                    print(
                        f"    ✗ Rate limit error. Max retries ({max_retries}) reached."
                    )
                    raise
            else:
                # For non-retryable errors, re-raise immediately
                raise


def execute_with_retry(request):
    """
    Execute a Google API request with rate limit retry.

    Args:
        request: A Google API request object (e.g., from drive_service.files().get())

    Returns:
        The result of request.execute()
    """
    return retry_with_exponential_backoff(lambda: request.execute())


def _find_child_by_name(
    drive_service,
    parent_id: str,
    names: str | Sequence[str],
    mime_type: str | None = None,
) -> str:
    """
    Locate a child by exact name (supports multiple candidate names).
    """
    candidates: Iterable[str] = [names] if isinstance(names, str) else names
    mime_clause = f" and mimeType='{mime_type}'" if mime_type else ""

    for name in candidates:
        query = (
            f"'{parent_id}' in parents and name='{name}' and trashed=false{mime_clause}"
        )
        result = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id,name,mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=5,
            )
        )
        files = result.get("files", [])
        if files:
            return files[0]["id"]

    raise FileNotFoundError(
        f"Could not find any of {list(candidates)} inside parent id {parent_id}"
    )


def _find_or_create_folder(
    drive_service,
    parent_id: str,
    folder_name: str,
) -> str:
    """
    Find a folder by name in the parent, or create it if it doesn't exist.
    """
    try:
        return _find_child_by_name(
            drive_service,
            parent_id,
            folder_name,
            mime_type="application/vnd.google-apps.folder",
        )
    except FileNotFoundError:
        # Create the folder if it doesn't exist
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = execute_with_retry(
            drive_service.files()
            .create(
                body=file_metadata,
                fields="id",
                supportsAllDrives=True,
            )
        )
        return folder.get("id")


def resolve_layout(shared_drive_url: str, creds) -> DriveLayout:
    """
    Discover the standard folder/file layout starting from the shared drive URL.

    Required files (will raise FileNotFoundError if missing):
    - entities.csv
    - data-template (or data-template.gsheet)
    - report-template (or report-template.gslide)

    Optional folders (will be created if missing):
    - L0-Raw
    - L1-Merged
    - L2-Slide
    - L3-PDF
    - Templates
    """
    drive_service = build("drive", "v3", credentials=creds)
    root_id = _extract_id_from_url(shared_drive_url)

    # Optional folders - create if missing
    l0_id = _find_or_create_folder(drive_service, root_id, "L0-Raw")
    l1_id = _find_or_create_folder(drive_service, root_id, "L1-Merged")
    l2_id = _find_or_create_folder(drive_service, root_id, "L2-Slide")
    l3_id = _find_or_create_folder(drive_service, root_id, "L3-PDF")
    templates_id = _find_or_create_folder(drive_service, root_id, "Templates")

    # Required files - raise error if missing
    data_template_id = _find_child_by_name(
        drive_service,
        templates_id,
        names=("data-template.gsheet", "data-template"),
        mime_type="application/vnd.google-apps.spreadsheet",
    )
    report_template_id = _find_child_by_name(
        drive_service,
        templates_id,
        names=("report-template.gslide", "report-template"),
        mime_type="application/vnd.google-apps.presentation",
    )
    entities_csv_id = _find_child_by_name(
        drive_service,
        root_id,
        names=("entities.csv", "entities"),
        mime_type="text/csv",
    )

    return DriveLayout(
        root_id=root_id,
        l0_raw_id=l0_id,
        l1_merged_id=l1_id,
        l2_slide_id=l2_id,
        l3_pdf_id=l3_id,
        templates_id=templates_id,
        data_template_id=data_template_id,
        report_template_id=report_template_id,
        entities_csv_id=entities_csv_id,
    )


def load_entities(entities_csv_id: str, creds) -> List[str]:
    """
    Download entities.csv and return entity names (first column) where the L1 column
    (second column) is exactly `Y`. Works with both old format (Entity, Generate, Slides)
    and new format (Entity, L1, L2, L3).
    """
    drive_service = build("drive", "v3", credentials=creds)
    request = drive_service.files().get_media(
        fileId=entities_csv_id, supportsAllDrives=True
    )
    
    def _download():
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer.read().decode("utf-8")
    
    content = retry_with_exponential_backoff(_download)

    reader = csv.reader(io.StringIO(content))
    entities: List[str] = []
    header_skipped = False
    for row in reader:
        if not row:
            continue

        name = row[0].strip()
        flag = row[1].strip() if len(row) > 1 else ""

        if not name:
            continue

        # Skip header row
        if not header_skipped and name.lower().startswith("entity"):
            header_skipped = True
            continue

        # Support both old format (Generate=Y) and new format (L1=Y)
        if flag.upper() == "Y":
            entities.append(name)
    return entities


def _parse_slides_value(slides_value: str) -> Optional[Set[int]]:
    """
    Parse a slides column value into a set of slide numbers.

    Accepts:
    - Empty string or blank: returns None (all slides)
    - "All" (case-insensitive): returns None (all slides)
    - Comma-separated numbers and ranges (e.g., "1,2-4,6"): returns Set[int]

    Returns None when the value is blank, "All", or no valid numbers are found
    to indicate that all slides should be processed.
    """
    if not slides_value:
        return None

    slides_value = slides_value.strip()

    # Handle "All" as a literal string (case-insensitive)
    if slides_value.lower() == "all":
        return None

    slides: Set[int] = set()
    for part in slides_value.split(","):
        part = part.strip()
        if not part:
            continue

        if "-" in part:
            bounds = part.split("-", 1)
            if len(bounds) != 2:
                continue
            try:
                start = int(bounds[0].strip())
                end = int(bounds[1].strip())
            except ValueError:
                continue

            if start > end:
                start, end = end, start

            for num in range(start, end + 1):
                if num > 0:
                    slides.add(num)
        else:
            try:
                value = int(part)
            except ValueError:
                continue
            if value > 0:
                slides.add(value)

    return slides or None


def load_entities_with_slides(
    entities_csv_id: str, creds
) -> Dict[str, Optional[Set[int]]]:
    """
    Download entities.csv and return a mapping of entity name to requested slide
    numbers for rows marked with L1=Y (or Generate=Y for old format).
    A value of None means all slides.
    Works with both old format (Entity, Generate, Slides) and new format (Entity, L1, L2, L3).
    """
    drive_service = build("drive", "v3", credentials=creds)
    request = drive_service.files().get_media(
        fileId=entities_csv_id, supportsAllDrives=True
    )
    
    def _download():
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer.read().decode("utf-8")
    
    content = retry_with_exponential_backoff(_download)

    reader = csv.reader(io.StringIO(content))
    entities: Dict[str, Optional[Set[int]]] = {}
    header_skipped = False
    for row in reader:
        if not row:
            continue

        name = row[0].strip()
        flag = row[1].strip() if len(row) > 1 else ""
        # For old format: column 2 is slides, for new format: column 2 is L2
        slides_value = row[2].strip() if len(row) > 2 else ""

        if not name:
            continue

        # Skip header row
        if not header_skipped and name.lower().startswith("entity"):
            header_skipped = True
            continue

        # Support both old format (Generate=Y) and new format (L1=Y)
        if flag.upper() == "Y":
            slides = _parse_slides_value(slides_value)
            entities[name] = slides

    return entities


@dataclass
class EntityFlags:
    """Flags for entity generation from entities.csv."""

    entity_name: str
    l1: bool  # True if L1 should be generated
    l2: Optional[
        Set[int]
    ]  # None if L2 should not be processed, Set[int] for specific slides, special value for all slides
    l3: bool  # True if L3 PDF should be generated


def load_entities_with_flags(entities_csv_id: str, creds) -> List[EntityFlags]:
    """
    Download entities.csv and return a list of EntityFlags for all entities.

    CSV format (4 columns):
    - Column 0: Entity name
    - Column 1: L1 flag (Y/N or empty)
    - Column 2: L2 flag (empty, "All", or "1,2-4,6" format)
    - Column 3: L3 flag (Y/N or empty)

    Args:
        entities_csv_id: Google Drive file ID of entities.csv
        creds: Google OAuth credentials

    Returns:
        List of EntityFlags objects
    """
    drive_service = build("drive", "v3", credentials=creds)
    request = drive_service.files().get_media(
        fileId=entities_csv_id, supportsAllDrives=True
    )
    
    def _download():
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer.read().decode("utf-8")
    
    content = retry_with_exponential_backoff(_download)

    reader = csv.reader(io.StringIO(content))
    entities: List[EntityFlags] = []
    header_skipped = False

    for row in reader:
        if not row:
            continue

        name = row[0].strip() if len(row) > 0 else ""
        l1_value = row[1].strip() if len(row) > 1 else ""
        l2_value = row[2].strip() if len(row) > 2 else ""
        l3_value = row[3].strip() if len(row) > 3 else ""

        if not name:
            continue

        # Skip header row
        if not header_skipped and name.lower().startswith("entity"):
            header_skipped = True
            continue

        # Parse flags
        l1 = l1_value.upper() == "Y"
        # For L2: empty means don't process, "All" or specific slides means process
        # Use a special sentinel set() to represent "all slides" vs None for "don't process"
        if not l2_value or not l2_value.strip():
            l2_slides = None  # Don't process L2
        else:
            l2_slides = _parse_slides_value(
                l2_value
            )  # None for "All", Set[int] for specific slides
            # If _parse_slides_value returns None (meaning "all slides"), use empty set as sentinel
            if l2_slides is None:
                l2_slides = set()  # Empty set means "all slides"
        l3 = l3_value.upper() == "Y"

        entities.append(EntityFlags(entity_name=name, l1=l1, l2=l2_slides, l3=l3))

    return entities
