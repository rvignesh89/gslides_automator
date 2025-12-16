"""
Package exports for gslides_automator.
"""

from __future__ import annotations

from typing import List

from gslides_automator.auth import load_credentials
from gslides_automator.l1_generate import l1_generate as _l1g
from gslides_automator.l2_generate import l2_generate as _l2g
from gslides_automator.drive_layout import DriveLayout, resolve_layout

def l1_generate(shared_drive_url: str, service_account_credentials: str) -> dict:
    """Generate L1-Data from L0-Data for entities marked with generate=Y."""
    creds = load_credentials(service_account_credentials)
    layout: DriveLayout = resolve_layout(shared_drive_url, creds)
    return _l1g(creds=creds, layout=layout)


def l2_generate(shared_drive_url: str, service_account_credentials: str) -> dict:
    """Generate L2-Reports from L1-Data for entities marked with generate=Y."""
    creds = load_credentials(service_account_credentials)
    layout: DriveLayout = resolve_layout(shared_drive_url, creds)
    return _l2g(creds=creds, layout=layout)