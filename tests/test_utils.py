"""
Test utility functions for creating test data and Drive structures.
"""

from __future__ import annotations

import csv
import io
import time
import uuid
from typing import Dict, List, Optional

import gspread
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from gslides_automator.drive_layout import DriveLayout
from gslides_automator.gslides_api import GSlidesAPI


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


def create_test_drive_structure(root_id: str, creds) -> DriveLayout:
    """
    Create a complete test Drive structure with all required folders.

    Args:
        root_id: ID of the root folder
        creds: Service account credentials

    Returns:
        DriveLayout object with all folder IDs populated
    """
    drive_service = build("drive", "v3", credentials=creds)

    # Create required folders
    folders = {}
    folder_names = {
        "l0_raw": "L0-Raw",
        "l1_merged": "L1-Merged",
        "l2_slide": "L2-Slide",
        "l3_pdf": "L3-PDF",
        "templates": "Templates",
    }

    for key, name in folder_names.items():
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [root_id],
        }
        folder = execute_with_retry(
            drive_service.files().create(
                body=file_metadata,
                fields="id",
                supportsAllDrives=True,
            )
        )
        folders[key] = folder.get("id")

    return DriveLayout(
        root_id=root_id,
        l0_raw_id=folders["l0_raw"],
        l1_merged_id=folders["l1_merged"],
        l2_slide_id=folders["l2_slide"],
        l3_pdf_id=folders["l3_pdf"],
        templates_id=folders["templates"],
        data_template_id="",  # Will be set when template is created
        report_template_id="",  # Will be set when template is created
        entities_csv_id="",  # Will be set when CSV is created
    )


def create_test_entities_csv(
    root_id: str,
    entities: Dict[str, Dict[str, str]],
    creds,
) -> str:
    """
    Create a test entities.csv file.

    Args:
        root_id: ID of the root folder where entities.csv should be created
        entities: Dict mapping entity names to their flags:
                  {"entity-1": {"l1": "Y", "l2": "1,2", "l3": "Y"}, ...}
        creds: Service account credentials

    Returns:
        File ID of the created CSV file
    """
    drive_service = build("drive", "v3", credentials=creds)

    # Create CSV content
    csv_content = io.StringIO()
    writer = csv.writer(csv_content)
    writer.writerow(["Entity", "L1", "L2", "L3"])  # Header

    for entity_name, flags in entities.items():
        l1 = flags.get("l1", "N")
        l2 = flags.get("l2", "")
        l3 = flags.get("l3", "N")
        writer.writerow([entity_name, l1, l2, l3])

    csv_content.seek(0)
    csv_bytes = csv_content.read().encode("utf-8")

    # Upload CSV file
    file_metadata = {
        "name": "entities.csv",
        "parents": [root_id],
    }

    media = MediaIoBaseUpload(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        resumable=False,
    )

    file = execute_with_retry(
        drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )
    )

    return file.get("id")


def create_test_data_template(templates_folder_id: str, creds) -> str:
    """
    Create a test data template spreadsheet with sample sheets.

    Args:
        templates_folder_id: ID of the templates folder
        creds: Service account credentials

    Returns:
        Spreadsheet ID of the created template
    """
    drive_service = build("drive", "v3", credentials=creds)
    gspread_client = gspread.authorize(creds)

    # Create spreadsheet
    file_metadata = {
        "name": "data-template.gsheet",
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [templates_folder_id],
    }

    spreadsheet_file = execute_with_retry(
        drive_service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True,
        )
    )

    spreadsheet_id = spreadsheet_file.get("id")
    spreadsheet = gspread_client.open_by_key(spreadsheet_id)

    # Create common_data sheet
    try:
        common_data_sheet = spreadsheet.add_worksheet(
            title="common_data",
            rows=10,
            cols=5,
        )
    except Exception:
        common_data_sheet = spreadsheet.worksheet("common_data")

    # Add headers and sample data to common_data
    common_data_sheet.update(
        range_name="A1:E1",
        values=[["entity_name", "brand_name", "year", "region", "status"]],
    )
    common_data_sheet.update(
        range_name="A2:E2",
        values=[["entity-1", "TestBrand", "2024", "North", "Active"]],
    )

    # Create data sheet for text placeholders
    try:
        data_sheet = spreadsheet.add_worksheet(title="data", rows=10, cols=2)
    except Exception:
        data_sheet = spreadsheet.worksheet("data")

    # Add sample placeholder data
    data_sheet.update(range_name="A1:B1", values=[["placeholder", "value"]])
    data_sheet.update(
        range_name="A2:B5",
        values=[
            ["brand_name_", "TestBrand"],
            ["year_", "2024"],
            ["region_", "North"],
            ["status_", "Active"],
        ],
    )

    # Create chart sheet
    try:
        chart_sheet = spreadsheet.add_worksheet(title="chart-sales", rows=10, cols=3)
    except Exception:
        chart_sheet = spreadsheet.worksheet("chart-sales")

    chart_sheet.update(range_name="A1:C1", values=[["Month", "Sales", "Target"]])
    chart_sheet.update(
        range_name="A2:C5",
        values=[
            ["Jan", "1000", "1200"],
            ["Feb", "1200", "1200"],
            ["Mar", "1500", "1300"],
            ["Apr", "1400", "1400"],
        ],
    )

    # Create table sheet
    try:
        table_sheet = spreadsheet.add_worksheet(
            title="table-performance", rows=10, cols=4
        )
    except Exception:
        table_sheet = spreadsheet.worksheet("table-performance")

    table_sheet.update(range_name="A1:D1", values=[["Metric", "Q1", "Q2", "Q3"]])
    table_sheet.update(
        range_name="A2:D4",
        values=[
            ["Revenue", "10000", "12000", "15000"],
            ["Profit", "2000", "2500", "3000"],
            ["Growth", "10%", "15%", "20%"],
        ],
    )

    # Delete default Sheet1 if it exists
    try:
        default_sheet = spreadsheet.worksheet("Sheet1")
        spreadsheet.del_worksheet(default_sheet)
    except Exception:
        pass

    return spreadsheet_id


def create_test_slide_template(templates_folder_id: str, creds) -> str:
    """
    Create a test slide template with placeholders.

    Args:
        templates_folder_id: ID of the templates folder
        creds: Service account credentials

    Returns:
        Presentation ID of the created template
    """
    slides_service = GSlidesAPI.get_instance(creds)
    drive_service = build("drive", "v3", credentials=creds)

    # Create presentation file directly in the templates folder using Drive API
    # Service accounts cannot create presentations at the root level
    file_metadata = {
        "name": "report-template.gslide",
        "mimeType": "application/vnd.google-apps.presentation",
        "parents": [templates_folder_id],
    }

    # Create empty presentation file in the folder
    presentation_file = execute_with_retry(
        drive_service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True,
        )
    )

    presentation_id = presentation_file.get("id")

    # Now use Slides API to add content to the presentation
    # Get the presentation to check for slides
    presentation = slides_service.get_presentation(presentation_id)

    # Get the first slide (presentations always have at least one slide)
    slides = presentation.get("slides", [])
    if not slides:
        # Create a slide if none exists (shouldn't happen, but handle it)
        create_result = slides_service.batch_update(
            presentation_id,
            {
                "requests": [
                    {
                        "createSlide": {
                            "insertionIndex": 0,
                            "slideLayoutReference": {"predefinedLayout": "BLANK"},
                        }
                    }
                ]
            }
        )
        slide_id = create_result["replies"][0]["createSlide"]["objectId"]
    else:
        slide_id = slides[0].get("objectId")

    # Add text placeholders to the slide
    requests = []

    # Add a title with placeholder
    requests.append(
        {
            "createShape": {
                "objectId": f"title_{uuid.uuid4().hex[:8]}",
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": {
                        "height": {"magnitude": 400000, "unit": "EMU"},
                        "width": {"magnitude": 6000000, "unit": "EMU"},
                    },
                    "transform": {
                        "scaleX": 1.0,
                        "scaleY": 1.0,
                        "translateX": 100000,
                        "translateY": 100000,
                        "unit": "EMU",
                    },
                },
            },
        }
    )

    # Add text content with placeholder
    requests.append(
        {
            "insertText": {
                "objectId": requests[-1]["createShape"]["objectId"],
                "text": "{{brand_name_}} Report - {{year_}}",
            },
        }
    )

    # Add another text box with placeholder
    text_box_id = f"text_{uuid.uuid4().hex[:8]}"
    requests.append(
        {
            "createShape": {
                "objectId": text_box_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": {
                        "height": {"magnitude": 200000, "unit": "EMU"},
                        "width": {"magnitude": 6000000, "unit": "EMU"},
                    },
                    "transform": {
                        "scaleX": 1.0,
                        "scaleY": 1.0,
                        "translateX": 100000,
                        "translateY": 600000,
                        "unit": "EMU",
                    },
                },
            },
        }
    )

    requests.append(
        {
            "insertText": {
                "objectId": text_box_id,
                "text": "Region: {{region_}}\nStatus: {{status_}}",
            },
        }
    )

    # Execute batch update
    if requests:
        slides_service.batch_update(
            presentation_id,
            {"requests": requests}
        )

    return presentation_id


def create_test_l0_data(
    l0_root_id: str,
    entity_name: str,
    creds,
) -> str:
    """
    Create test L0-Raw data for an entity (CSV files and images).

    Args:
        l0_root_id: ID of the L0-Raw root folder
        entity_name: Name of the entity
        creds: Service account credentials

    Returns:
        Folder ID of the created entity folder
    """
    drive_service = build("drive", "v3", credentials=creds)

    # Create entity folder
    file_metadata = {
        "name": entity_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [l0_root_id],
    }

    folder = execute_with_retry(
        drive_service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True,
        )
    )

    entity_folder_id = folder.get("id")

    # Create CSV files
    csv_files = {
        "data.csv": [
            ["placeholder", "value"],
            ["brand_name_", f"{entity_name}Brand"],
            ["year_", "2024"],
            ["region_", "North"],
            ["status_", "Active"],
        ],
        "chart-sales.csv": [
            ["Month", "Sales", "Target"],
            ["Jan", "1000", "1200"],
            ["Feb", "1200", "1200"],
            ["Mar", "1500", "1300"],
        ],
        "table-performance.csv": [
            ["Metric", "Q1", "Q2", "Q3"],
            ["Revenue", "10000", "12000", "15000"],
            ["Profit", "2000", "2500", "3000"],
        ],
    }

    for filename, rows in csv_files.items():
        csv_content = io.StringIO()
        writer = csv.writer(csv_content)
        writer.writerows(rows)
        csv_content.seek(0)
        csv_bytes = csv_content.read().encode("utf-8")

        file_metadata = {
            "name": filename,
            "parents": [entity_folder_id],
        }

        media = MediaIoBaseUpload(
            io.BytesIO(csv_bytes),
            mimetype="text/csv",
            resumable=False,
        )

        execute_with_retry(
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
        )

    # Create a simple test image (1x1 pixel PNG)
    # Note: In real tests, you might want to use actual image files
    # For now, we'll create a minimal PNG file
    png_data = (
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00"
        b"\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01"
        b"\r\n-\xdb\x00\x00\x00\x00IEND\xaeB`\x82"
    )

    file_metadata = {
        "name": "picture-distribution.png",
        "parents": [entity_folder_id],
    }

    media = MediaIoBaseUpload(
        io.BytesIO(png_data),
        mimetype="image/png",
        resumable=False,
    )

    execute_with_retry(
        drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )
    )

    return entity_folder_id


def cleanup_test_drive(root_id: str, creds) -> None:
    """
    Clean up all files and folders in a test drive.

    Args:
        root_id: ID of the root folder to clean up
        creds: Service account credentials
    """
    drive_service = build("drive", "v3", credentials=creds)

    try:
        # List all files in the root folder
        results = execute_with_retry(
            drive_service.files().list(
                q=f"'{root_id}' in parents and trashed=false",
                fields="files(id, name, mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        files = results.get("files", [])

        # Delete all files and folders recursively
        for file in files:
            try:
                if file.get("mimeType") == "application/vnd.google-apps.folder":
                    # Recursively delete folder contents first
                    cleanup_test_drive(file["id"], creds)

                execute_with_retry(
                    drive_service.files().delete(
                        fileId=file["id"],
                        supportsAllDrives=True,
                    )
                )
            except Exception as e:
                print(f"Warning: Failed to delete {file.get('name', 'unknown')}: {e}")
    except Exception as e:
        print(f"Warning: Failed to cleanup test drive {root_id}: {e}")


def verify_drive_structure(layout: DriveLayout, creds) -> bool:
    """
    Verify that a Drive structure matches the expected layout.

    Args:
        layout: DriveLayout object to verify
        creds: Service account credentials

    Returns:
        True if structure is valid, False otherwise
    """
    drive_service = build("drive", "v3", credentials=creds)

    required_folders = {
        "L0-Raw": layout.l0_raw_id,
        "L1-Merged": layout.l1_merged_id,
        "L2-Slide": layout.l2_slide_id,
        "L3-PDF": layout.l3_pdf_id,
        "Templates": layout.templates_id,
    }

    for folder_name, folder_id in required_folders.items():
        try:
            folder = execute_with_retry(
                drive_service.files().get(
                    fileId=folder_id,
                    fields="id, name, mimeType",
                    supportsAllDrives=True,
                )
            )

            if folder.get("mimeType") != "application/vnd.google-apps.folder":
                return False
        except Exception:
            return False

    return True


def get_spreadsheet_data(
    spreadsheet_id: str, sheet_name: str, creds
) -> Optional[List[List[str]]]:
    """
    Get all data from a spreadsheet sheet.

    Args:
        spreadsheet_id: ID of the spreadsheet
        sheet_name: Name of the sheet
        creds: Service account credentials

    Returns:
        List of rows (each row is a list of strings), or None if error
    """
    gspread_client = gspread.authorize(creds)

    try:
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        return worksheet.get_all_values()
    except Exception:
        return None


def get_slide_text_content(presentation_id: str, slide_index: int, creds) -> str:
    """
    Get all text content from a slide.

    Args:
        presentation_id: ID of the presentation
        slide_index: Index of the slide (0-based)
        creds: Service account credentials

    Returns:
        Combined text content from all text elements on the slide
    """
    slides_service = build("slides", "v1", credentials=creds)

    try:
        presentation = (
            slides_service.presentations()
            .get(
                presentationId=presentation_id,
            )
            .execute()
        )

        slides = presentation.get("slides", [])
        if slide_index >= len(slides):
            return ""

        slide = slides[slide_index]
        text_content = []

        for page_element in slide.get("pageElements", []):
            if "shape" in page_element:
                shape = page_element["shape"]
                if "text" in shape:
                    text_elements = shape["text"].get("textElements", [])
                    for text_element in text_elements:
                        if "textRun" in text_element:
                            text_content.append(
                                text_element["textRun"].get("content", "")
                            )

        return "".join(text_content)
    except Exception:
        return ""
