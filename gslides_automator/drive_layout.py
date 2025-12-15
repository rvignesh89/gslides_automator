from __future__ import annotations

from dataclasses import dataclass
import csv
import io
import re
from typing import Iterable, List, Sequence

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


@dataclass
class DriveLayout:
    root_id: str
    l0_data_id: str
    l1_data_id: str
    l2_report_id: str
    templates_id: str
    data_template_id: str
    report_template_id: str
    entities_csv_id: str


def _extract_id_from_url(shared_drive_url: str) -> str:
    """
    Extract a Drive folder/file ID from a shared Drive URL or raw ID.
    """
    if re.fullmatch(r"[A-Za-z0-9_\-]+", shared_drive_url):
        return shared_drive_url

    patterns = [
        r"/folders/([A-Za-z0-9_\-]+)",
        r"[?&]id=([A-Za-z0-9_\-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, shared_drive_url)
        if match:
            return match.group(1)
    raise ValueError("Could not extract Drive folder ID from URL. Pass a folder link or ID.")


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
        query = f"'{parent_id}' in parents and name='{name}' and trashed=false{mime_clause}"
        result = drive_service.files().list(
            q=query,
            fields="files(id,name,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=5,
        ).execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]

    raise FileNotFoundError(
        f"Could not find any of {list(candidates)} inside parent id {parent_id}"
    )


def resolve_layout(shared_drive_url: str, creds) -> DriveLayout:
    """
    Discover the standard folder/file layout starting from the shared drive URL.
    """
    drive_service = build("drive", "v3", credentials=creds)
    root_id = _extract_id_from_url(shared_drive_url)

    l0_id = _find_child_by_name(drive_service, root_id, "L0-Data", mime_type="application/vnd.google-apps.folder")
    l1_id = _find_child_by_name(drive_service, root_id, "L1-Data", mime_type="application/vnd.google-apps.folder")
    l2_id = _find_child_by_name(drive_service, root_id, "L2-Reports", mime_type="application/vnd.google-apps.folder")
    templates_id = _find_child_by_name(drive_service, root_id, "Templates", mime_type="application/vnd.google-apps.folder")

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
        l0_data_id=l0_id,
        l1_data_id=l1_id,
        l2_report_id=l2_id,
        templates_id=templates_id,
        data_template_id=data_template_id,
        report_template_id=report_template_id,
        entities_csv_id=entities_csv_id,
    )


def load_entities(entities_csv_id: str, creds) -> List[str]:
    """
    Download entities.csv and return entity names (first column) where the adjacent
    `generate` column (second column) is exactly `Y`.
    """
    drive_service = build("drive", "v3", credentials=creds)
    request = drive_service.files().get_media(fileId=entities_csv_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buffer.seek(0)
    content = buffer.read().decode("utf-8")

    reader = csv.reader(io.StringIO(content))
    entities: List[str] = []
    for row in reader:
        if not row:
            continue

        name = row[0].strip()
        generate_flag = row[1].strip() if len(row) > 1 else ""

        if not name:
            continue

        # Skip header row
        if not entities and name.lower().startswith("entity"):
            continue

        # Only include rows explicitly marked for generation
        if generate_flag == "Y":
            entities.append(name)
    return entities

