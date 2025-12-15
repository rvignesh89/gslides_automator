"""
Package exports for gslides_automator.
"""

from __future__ import annotations

from typing import Iterable, List, Optional

from gslides_automator.auth import load_credentials
from gslides_automator.generate_data import generate_data as _gd
from gslides_automator.generate_report import generate_report as _gr
from gslides_automator.drive_layout import DriveLayout, load_entities, resolve_layout


def _ensure_list(value) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return list(value)


def generate_data(
    shared_drive_url: str,
    service_account_file: str,
    entities: Iterable[str] | str | None = None,
) -> dict:
    """Populate L1-Data from L0-Data using the shared-drive layout."""
    creds = load_credentials(service_account_file)
    layout: DriveLayout = resolve_layout(shared_drive_url, creds)

    target_entities = _ensure_list(entities)
    if target_entities is None:
        target_entities = load_entities(layout.entities_csv_id, creds)

    return _gd.generate_data(
        entities=target_entities,
        creds=creds,
    )


def generate_report(
    shared_drive_url: str,
    service_account_file: str,
    entities: Iterable[str] | str | None = None,
) -> dict:
    """Build Google Slides reports from L1-Data for selected entities/slides."""
    creds = load_credentials(service_account_file)
    layout: DriveLayout = resolve_layout(shared_drive_url, creds)

    target_entities = _ensure_list(entities)
    if target_entities is None:
        target_entities = load_entities(layout.entities_csv_id, creds)

    return _gr.generate_report(
        entities=target_entities,
        creds=creds,
        input_folder_id=layout.l1_data_id,
        template_id=layout.report_template_id,
        output_folder_id=layout.l2_report_id,
    )