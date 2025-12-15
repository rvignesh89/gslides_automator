"""
Package exports for gslides_automator.
"""

from __future__ import annotations

from typing import List

from gslides_automator.auth import load_credentials
from gslides_automator.generate_data import generate_data as _gd
from gslides_automator.generate_report import generate_report as _gr
from gslides_automator.drive_layout import DriveLayout, resolve_layout


def generate_data(
    shared_drive_url: str,
    service_account_credentials: str,
) -> dict:
    """Populate L1-Data from L0-Data using the shared-drive layout."""
    creds = load_credentials(service_account_credentials)
    layout: DriveLayout = resolve_layout(shared_drive_url, creds)

    return _gd(
        creds=creds,
        layout=layout,
    )


def generate_report(
    shared_drive_url: str,
    service_account_credentials: str,
) -> dict:
    """Build Google Slides reports from L1-Data for entities marked with generate=Y."""
    creds = load_credentials(service_account_credentials)
    layout: DriveLayout = resolve_layout(shared_drive_url, creds)

    return _gr(
        creds=creds,
        input_folder_id=layout.l1_data_id,
        template_id=layout.report_template_id,
        output_folder_id=layout.l2_report_id,
        layout=layout,
    )