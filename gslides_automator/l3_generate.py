#!/usr/bin/env python3
from __future__ import annotations
"""
Script to export Google Slides presentations to PDF format.
Exports presentations from L2-Slides folder and saves them to L3-PDF folder.
"""

import os
import sys
import io
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# Add project root to path to import modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

from gslides_automator.l1_generate import (
    find_existing_file,
    delete_file,
    retry_with_exponential_backoff,
)


def export_slide_to_pdf(slide_id: str, entity_name: str, l3_folder_id: str, creds) -> bool:
    """
    Export a Google Slides presentation to PDF and save it to L3-PDF folder.

    Args:
        slide_id: ID of the Google Slides presentation
        entity_name: Name of the entity (for file naming)
        l3_folder_id: ID of the L3-PDF folder
        creds: Service account credentials

    Returns:
        bool: True if successful, False otherwise
    """
    drive_service = build('drive', 'v3', credentials=creds)

    print(f"  Exporting slide to PDF for {entity_name}...")

    def _export_pdf():
        # Export the presentation as PDF
        request = drive_service.files().export(
            fileId=slide_id,
            mimeType='application/pdf'
        )

        pdf_content = io.BytesIO()
        downloader = MediaIoBaseDownload(pdf_content, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

        pdf_content.seek(0)
        pdf_bytes = pdf_content.read()

        # Check if PDF file already exists and delete it
        pdf_filename = f"{entity_name}.pdf"
        existing_pdf_id = find_existing_file(drive_service, pdf_filename, l3_folder_id)
        if existing_pdf_id:
            print(f"    Found existing PDF, deleting...")
            if not delete_file(drive_service, existing_pdf_id):
                print(f"    ✗ Failed to delete existing PDF")
                return False

        # Upload the PDF to L3-PDF folder
        media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype='application/pdf', resumable=True)

        file_metadata = {
            'name': pdf_filename,
            'parents': [l3_folder_id]
        }

        uploaded_file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name',
            supportsAllDrives=True
        ).execute()

        return uploaded_file.get('id') is not None

    try:
        pdf_id = retry_with_exponential_backoff(_export_pdf)
        if pdf_id:
            print(f"    ✓ PDF exported successfully: {entity_name}.pdf")
            return True
        else:
            print(f"    ✗ Failed to export PDF")
            return False
    except HttpError as error:
        print(f"    ✗ Error exporting PDF: {error}")
        return False
    except Exception as e:
        print(f"    ✗ Error exporting PDF: {e}")
        return False
