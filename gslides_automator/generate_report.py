#!/usr/bin/env python3
from __future__ import annotations
"""
Script to dynamically generate Google Slides presentations from Google Sheets.
Processes multiple entity spreadsheets from a Google Drive folder.
For each spreadsheet, reads sheets named <type>-<placeholder> (chart/table/picture),
copies a template presentation, and replaces placeholders with linked assets from the sheets.
"""

import gspread
import os
import sys
import re
import time
import json
import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)
try:
    from gslides_automator.auth import get_oauth_credentials
    from gslides_automator.drive_layout import load_entities, resolve_layout, DriveLayout
except ImportError:  # Fallback for package-relative execution
    from .auth import get_oauth_credentials
    from .drive_layout import load_entities, resolve_layout, DriveLayout

def retry_with_exponential_backoff(func, max_retries=5, initial_delay=1, max_delay=60, backoff_factor=2):
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
                    print(f"    ⚠️  {error_msg}. Retrying in {wait_time:.1f} seconds... (attempt {attempt + 1}/{max_retries})")
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
            # For non-HttpError exceptions, re-raise immediately
            raise

def list_entity_folders(parent_folder_id, creds):
    """
    List all entity subfolders in the parent folder.

    Args:
        parent_folder_id: ID of the parent folder containing entity folders
        creds: Service account credentials

    Returns:
        list: List of tuples (folder_id, folder_name)
    """
    drive_service = build('drive', 'v3', credentials=creds)
    folders = []

    try:
        # Query for folders in the parent folder
        query = f"mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"

        results = drive_service.files().list(
            q=query,
            fields='files(id, name)',
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        items = results.get('files', [])

        for item in items:
            folders.append((item['id'], item['name']))

        return folders

    except HttpError as error:
        print(f"Error listing entity folders: {error}")
        return []

def list_spreadsheets_in_folder(folder_id, creds):
    """
    List all Google Sheets files in a Google Drive folder.

    Args:
        folder_id: ID of the folder to search
        creds: Service account credentials

    Returns:
        list: List of tuples (spreadsheet_id, spreadsheet_name)
    """
    drive_service = build('drive', 'v3', credentials=creds)
    spreadsheets = []

    try:
        # Query for Google Sheets files in the folder
        query = f"mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"

        results = drive_service.files().list(
            q=query,
            fields='files(id, name)',
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        items = results.get('files', [])

        for item in items:
            spreadsheets.append((item['id'], item['name']))

        return spreadsheets

    except HttpError as error:
        print(f"Error listing spreadsheets in folder: {error}")
        return []

def parse_sheet_name(sheet_name):
    """
    Parse sheet name to extract placeholder type and name using hyphen prefixes.

    Args:
        sheet_name: Name of the sheet (e.g., "chart-pass-percentage")

    Returns:
        tuple: (placeholder_type, placeholder_name) or None if pattern doesn't match
    """
    pattern = r'^(chart|table|picture)-(.+)$'
    match = re.match(pattern, sheet_name)
    if match:
        placeholder_type = match.group(1)
        placeholder_name = match.group(2)
        return placeholder_type, placeholder_name
    return None

def get_entity_name_from_common_data(spreadsheet_id, creds):
    """
    Read entity_name from the 'common_data' sheet in the spreadsheet.

    Args:
        spreadsheet_id: ID of the spreadsheet
        creds: Service account credentials

    Returns:
        str: Entity name from the first data row (row 2) in the 'entity_name' column,
             or None if the sheet doesn't exist
    """
    gspread_client = gspread.authorize(creds)

    try:
        # Open the spreadsheet
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)

        # Find the 'common_data' sheet
        try:
            common_data_sheet = spreadsheet.worksheet('common_data')
        except gspread.exceptions.WorksheetNotFound:
            print("Error: 'common_data' sheet not found in spreadsheet")
            return None

        # Read the first data row (row 2, assuming row 1 is header)
        # Get all values from the sheet
        all_values = common_data_sheet.get_all_values()

        if not all_values or len(all_values) < 2:
            print("Error: 'common_data' sheet has no data rows")
            return None

        # Find the 'entity_name' column index from header row (row 1, index 0)
        header_row = all_values[0]
        try:
            entity_name_col_index = header_row.index('entity_name')
        except ValueError:
            print("Error: 'entity_name' column not found in 'common_data' sheet")
            return None

        # Get the entity name from the first data row (row 2, index 1)
        data_row = all_values[1]
        if len(data_row) <= entity_name_col_index:
            print("Error: 'entity_name' column is empty in 'common_data' sheet")
            return None

        entity_name = data_row[entity_name_col_index].strip()
        if not entity_name:
            print("Error: 'entity_name' is empty in 'common_data' sheet")
            return None

        return entity_name

    except Exception as e:
        print(f"Error reading entity_name from 'common_data' sheet: {e}")
        return None

def read_data_from_sheet(spreadsheet_id, sheet_name, creds):
    """
    Read data from a data sheet where column 1 contains keys and column 2 contains values.

    Args:
        spreadsheet_id: ID of the spreadsheet
        sheet_name: Name of the data sheet (e.g., "data")
        creds: Service account credentials

    Returns:
        dict: Dictionary mapping keys (column 1) to values (column 2) from each row,
             or None if the sheet doesn't exist or has errors
    """
    gspread_client = gspread.authorize(creds)

    try:
        # Open the spreadsheet
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)

        # Find the data sheet
        try:
            data_sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Error: Data sheet '{sheet_name}' not found in spreadsheet")
            return None

        # Read all values from the sheet
        all_values = data_sheet.get_all_values()

        if not all_values:
            print(f"Error: Data sheet '{sheet_name}' has no data rows")
            return None

        # Create dictionary mapping column 1 (key) to column 2 (value) from each row
        data_dict = {}
        for row in all_values:
            if len(row) >= 2:  # Ensure row has at least 2 columns
                key = row[0].strip()
                value = row[1].strip() if len(row) > 1 else ""
                if key:  # Only process rows with non-empty keys
                    data_dict[key] = value

        if not data_dict:
            print(f"Error: No valid key-value pairs found in data sheet '{sheet_name}'")
            return None

        return data_dict

    except Exception as e:
        print(f"Error reading data from sheet '{sheet_name}': {e}")
        return None

def read_table_from_sheet(spreadsheet_id, sheet_name, creds):
    """
    Read 2D table data from a sheet. Returns a list of rows (list of strings).
    Keeps the raw values; formatting is preserved in Slides by reusing existing cell styles.
    """
    gspread_client = gspread.authorize(creds)

    try:
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"    ⚠️  Table sheet '{sheet_name}' not found in spreadsheet")
            return None

        values = worksheet.get_all_values()
        return values or []
    except Exception as e:
        print(f"    ⚠️  Error reading table data from sheet '{sheet_name}': {e}")
        return None

def delete_existing_presentation(entity_name, output_folder_id, creds):
    """
    Delete an existing presentation for a entity if it exists in the output folder.

    Args:
        entity_name: Name of the entity (e.g., "Madurai")
        output_folder_id: ID of the folder to search for the presentation
        creds: Service account credentials

    Returns:
        bool: True if a presentation was found and deleted, False otherwise
    """
    drive_service = build('drive', 'v3', credentials=creds)

    try:
        # Search for existing presentation with the expected name
        expected_filename = f"{entity_name}.gslides"
        query = f"'{output_folder_id}' in parents and name='{expected_filename}' and mimeType='application/vnd.google-apps.presentation' and trashed=false"

        def list_files():
            return drive_service.files().list(
                q=query,
                fields='files(id, name)',
                pageSize=10,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

        results = retry_with_exponential_backoff(list_files)

        files = results.get('files', [])
        if files:
            # Delete all matching files (should typically be just one)
            for file in files:
                # First check if file is accessible
                try:
                    file_check = drive_service.files().get(
                        fileId=file['id'],
                        fields='id, name',
                        supportsAllDrives=True
                    ).execute()
                except HttpError as check_error:
                    if check_error.resp.status == 404:
                        try:
                            from .auth import get_service_account_email
                            service_account_email = get_service_account_email()
                            print(f"  ⚠️  Presentation '{file['name']}' not accessible to service account.")
                            print(f"      Service account email: {service_account_email}")
                            print(f"      Please ensure the file is shared with this service account with 'Editor' permissions.")
                        except Exception:
                            print(f"  ⚠️  Presentation '{file['name']}' not accessible to service account.")
                            print(f"      Please ensure the file is shared with your service account with 'Editor' permissions.")
                        continue
                    else:
                        print(f"  ⚠️  Error checking presentation access: {check_error}")
                        continue

                def delete_file():
                    return drive_service.files().delete(
                        fileId=file['id'],
                        supportsAllDrives=True
                    ).execute()

                try:
                    retry_with_exponential_backoff(delete_file)
                    print(f"  ✓ Deleted existing presentation: {file['name']} (ID: {file['id']})")
                except HttpError as error:
                    if error.resp.status == 404:
                        try:
                            from .auth import get_service_account_email
                            service_account_email = get_service_account_email()
                            print(f"  ⚠️  Error deleting presentation '{file['name']}': File not found or not accessible.")
                            print(f"      Service account email: {service_account_email}")
                            print(f"      Please ensure the file is shared with this service account with 'Editor' permissions.")
                        except Exception:
                            print(f"  ⚠️  Error deleting presentation '{file['name']}': File not found or not accessible.")
                            print(f"      Please ensure the file is shared with your service account with 'Editor' permissions.")
                    elif error.resp.status == 403:
                        try:
                            from .auth import get_service_account_email
                            service_account_email = get_service_account_email()
                            print(f"  ⚠️  Error deleting presentation '{file['name']}': Permission denied.")
                            print(f"      Service account email: {service_account_email}")
                            print(f"      Please ensure the file is shared with this service account with 'Editor' permissions.")
                        except Exception:
                            print(f"  ⚠️  Error deleting presentation '{file['name']}': Permission denied.")
                            print(f"      Please ensure the file is shared with your service account with 'Editor' permissions.")
                    else:
                        print(f"  ⚠️  Error deleting existing presentation {file['name']}: {error}")
                    return False
            return True
        else:
            print(f"  ℹ️  No existing presentation found for '{entity_name}'")
            return False

    except HttpError as error:
        print(f"  ⚠️  Error searching for existing presentation: {error}")
        return False

def find_existing_presentation(entity_name, output_folder_id, creds):
    """
    Find an existing presentation for a entity in the output folder without deleting it.

    Args:
        entity_name: Name of the entity (e.g., "Madurai")
        output_folder_id: ID of the folder to search for the presentation
        creds: Service account credentials

    Returns:
        str: Presentation ID if found, None otherwise
    """
    drive_service = build('drive', 'v3', credentials=creds)

    try:
        # Search for existing presentation with the expected name
        expected_filename = f"{entity_name}.gslides"
        query = f"'{output_folder_id}' in parents and name='{expected_filename}' and mimeType='application/vnd.google-apps.presentation' and trashed=false"

        def list_files():
            return drive_service.files().list(
                q=query,
                fields='files(id, name)',
                pageSize=10,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

        results = retry_with_exponential_backoff(list_files)

        files = results.get('files', [])
        if files:
            # Return the first matching file ID
            file_id = files[0]['id']
            # Verify file is accessible
            try:
                file_check = drive_service.files().get(
                    fileId=file_id,
                    fields='id, name',
                    supportsAllDrives=True
                ).execute()
                return file_id
            except HttpError as check_error:
                if check_error.resp.status == 404:
                    try:
                        from .auth import get_service_account_email
                        from .auth import get_service_account_email
                        service_account_email = get_service_account_email()
                        print(f"  ⚠️  Presentation '{files[0]['name']}' not accessible to service account.")
                        print(f"      Service account email: {service_account_email}")
                        print(f"      Please ensure the file is shared with this service account with 'Editor' permissions.")
                    except Exception:
                        print(f"  ⚠️  Presentation '{files[0]['name']}' not accessible to service account.")
                        print(f"      Please ensure the file is shared with your service account with 'Editor' permissions.")
                else:
                    print(f"  ⚠️  Error checking presentation access: {check_error}")
                return None
        else:
            return None

    except HttpError as error:
        print(f"  ⚠️  Error searching for existing presentation: {error}")
        return None

def replace_slides_from_template(presentation_id, template_id, slide_numbers, creds):
    """
    Replace specified slides in the presentation with slides from the template.
    This deletes the target slides and recreates them by copying elements from template.

    Args:
        presentation_id: ID of the target presentation
        template_id: ID of the template presentation
        slide_numbers: Set of slide numbers (1-based) to replace
        creds: Service account credentials

    Returns:
        bool: True if successful, False otherwise
    """
    import copy
    import uuid

    slides_service = build('slides', 'v1', credentials=creds)

    try:
        # Get template and target presentations
        template_presentation = slides_service.presentations().get(presentationId=template_id).execute()
        template_slides = template_presentation.get('slides', [])

        target_presentation = slides_service.presentations().get(presentationId=presentation_id).execute()
        target_slides = target_presentation.get('slides', [])

        if not template_slides or not target_slides:
            print(f"  ⚠️  Template or target presentation has no slides.")
            return False

        max_slide = max(slide_numbers)
        if len(template_slides) < max_slide or len(target_slides) < max_slide:
            print(f"  ⚠️  Template or target has fewer slides than requested.")
            return False

        # Tables cannot be safely recreated slide-by-slide because formatting would be lost
        table_slides = []
        for slide_number in slide_numbers:
            slide_index = slide_number - 1
            target_slide = target_slides[slide_index]
            if any('table' in element for element in target_slide.get('pageElements', [])):
                table_slides.append(slide_number)

        if table_slides:
            slide_list = ', '.join(str(s) for s in sorted(table_slides))
            raise ValueError(
                f"Slide(s) {slide_list} contain table elements. Per-slide regeneration is not supported because tables cannot be recreated with proper formatting via the API. "
                f"Please regenerate the entire report without --slides."
            )

        # Delete target slides first (in reverse order to maintain indices)
        delete_requests = []
        for slide_number in sorted(slide_numbers, reverse=True):
            slide_index = slide_number - 1
            target_slide_id = target_slides[slide_index].get('objectId')
            if target_slide_id:
                delete_requests.append({
                    'deleteObject': {
                        'objectId': target_slide_id
                    }
                })

        if delete_requests:
            slides_service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={'requests': delete_requests}
            ).execute()

        # Now create new slides and copy elements from template
        for slide_number in sorted(slide_numbers):
            slide_index = slide_number - 1
            template_slide = template_slides[slide_index]

            # Create new blank slide
            create_result = slides_service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={'requests': [{
                    'createSlide': {
                        'insertionIndex': slide_index,
                        'slideLayoutReference': {
                            'predefinedLayout': 'BLANK'
                        }
                    }
                }]}
            ).execute()

            new_slide_id = create_result['replies'][0]['createSlide']['objectId']

            # Copy page elements from template slide
            template_elements = template_slide.get('pageElements', [])
            if template_elements:
                copy_requests = []
                for element in template_elements:
                    if 'shape' in element:
                        shape = element.get('shape', {})
                        shape_type = shape.get('shapeType', 'TEXT_BOX')
                        transform = element.get('transform', {})
                        size = element.get('size', {})

                        new_element_id = str(uuid.uuid4()).replace('-', '')[:26]

                        # Get shape properties to preserve formatting - copy all writable properties
                        shape_properties = {}
                        if 'shapeProperties' in shape:
                            # Copy all shapeProperties from template, but filter to only writable top-level fields
                            all_props = copy.deepcopy(shape.get('shapeProperties', {}))
                            # List of writable top-level fields in shapeProperties (excluding read-only and nested fields)
                            # Note: solidFill, gradientFill, etc. are nested under shapeBackgroundFill, not top-level
                            writable_top_level_fields = [
                                'outline', 'shadow', 'link', 'contentAlignment',
                                'shapeBackgroundFill'  # This contains nested fill properties
                            ]
                            # Only include writable top-level fields
                            for field in writable_top_level_fields:
                                if field in all_props:
                                    shape_properties[field] = all_props[field]

                        # contentAlignment can be directly on the shape object (not in shapeProperties)
                        # When reading from template, it might be on the shape itself
                        # We need to include it in shapeProperties for the update request
                        if 'contentAlignment' in shape and 'contentAlignment' not in shape_properties:
                            shape_properties['contentAlignment'] = shape.get('contentAlignment')

                        # Also check for contentAlignment at element level (if not already found)
                        if 'contentAlignment' in element and 'contentAlignment' not in shape_properties:
                            shape_properties['contentAlignment'] = element.get('contentAlignment')

                        create_shape_request = {
                            'createShape': {
                                'objectId': new_element_id,
                                'shapeType': shape_type,
                                'elementProperties': {
                                    'pageObjectId': new_slide_id,
                                    'size': size,
                                    'transform': transform
                                }
                            }
                        }

                        copy_requests.append(create_shape_request)

                        # Update shape properties to preserve formatting
                        if shape_properties:
                            copy_requests.append({
                                'updateShapeProperties': {
                                    'objectId': new_element_id,
                                    'shapeProperties': shape_properties,
                                    'fields': ','.join(shape_properties.keys())
                                }
                            })

                        # Copy text content with formatting if present
                        if 'text' in shape:
                            text_obj = shape.get('text', {})
                            text_elements = text_obj.get('textElements', [])

                            # Get default paragraph style if present
                            default_paragraph_style = None
                            if 'paragraphStyle' in text_obj:
                                default_paragraph_style = text_obj['paragraphStyle']

                            # Whitelist of writable text style fields
                            writable_text_style_fields = [
                                'bold', 'italic', 'underline', 'strikethrough',
                                'fontFamily', 'fontSize', 'foregroundColor', 'backgroundColor',
                                'weightedFontFamily'  # Font weight (object with fontFamily and weight)
                            ]

                            # Helper function to filter text style to only writable fields
                            def filter_text_style(text_style):
                                """Filter text style to only include writable fields."""
                                if not text_style:
                                    return {}
                                filtered = {}
                                for field in writable_text_style_fields:
                                    if field in text_style:
                                        filtered[field] = text_style[field]
                                return filtered

                            # Whitelist of writable paragraph style fields
                            writable_paragraph_fields = [
                                'alignment', 'direction', 'spacingMode', 'spaceAbove', 'spaceBelow',
                                'lineSpacing', 'indentFirstLine', 'indentStart', 'indentEnd'
                            ]

                            # Helper function to filter paragraph style to only writable fields
                            def filter_paragraph_style(para_style):
                                """Filter paragraph style to only include writable fields."""
                                if not para_style:
                                    return {}
                                filtered = {}
                                for field in writable_paragraph_fields:
                                    if field in para_style:
                                        filtered[field] = para_style[field]
                                return filtered

                            # Phase 1: Collect textRuns and paragraphStyles into in-memory structures
                            collected_text_runs = []  # Array of {startIndex, endIndex, content, style}
                            collected_paragraph_markers = []  # Array of {endIndex, style} - will be converted to paragraphStyles later

                            for te in text_elements:
                                if 'textRun' in te:
                                    text_run = te['textRun']
                                    content = text_run.get('content', '')
                                    style = text_run.get('style', {})

                                    # Get startIndex and endIndex from the element
                                    start_index = te.get('startIndex', 0)
                                    end_index = te.get('endIndex', start_index + len(content) if content else start_index)

                                    if content:
                                        collected_text_runs.append({
                                            'startIndex': start_index,
                                            'endIndex': end_index,
                                            'content': content,
                                            'style': style
                                        })

                                elif 'paragraphMarker' in te:
                                    para_marker = te['paragraphMarker']
                                    para_style = para_marker.get('style', {}) if 'style' in para_marker else None

                                    # Use paragraph-specific style if available, otherwise use default
                                    paragraph_style = para_style if para_style else default_paragraph_style

                                    if paragraph_style:
                                        # Get endIndex from the element (marks end of paragraph)
                                        end_index = te.get('endIndex', 0)

                                        collected_paragraph_markers.append({
                                            'endIndex': end_index,
                                            'style': paragraph_style
                                        })

                            # Convert paragraphMarkers to paragraphStyles with startIndex/endIndex
                            # Sort by endIndex to process in order
                            collected_paragraph_markers.sort(key=lambda x: x['endIndex'])
                            collected_paragraph_styles = []

                            for i, para_marker in enumerate(collected_paragraph_markers):
                                # Determine startIndex: previous paragraph's endIndex, or 0 for first paragraph
                                start_index = 0
                                if i > 0:
                                    start_index = collected_paragraph_styles[i - 1]['endIndex']

                                collected_paragraph_styles.append({
                                    'startIndex': start_index,
                                    'endIndex': para_marker['endIndex'],
                                    'style': para_marker['style']
                                })

                            # Handle last paragraph if it doesn't have a paragraph marker
                            # Find the maximum endIndex from textRuns to determine if there's unhandled text
                            max_text_index = 0
                            if collected_text_runs:
                                max_text_index = max(tr['endIndex'] for tr in collected_text_runs)

                            # Check if there's text after the last paragraph marker
                            last_para_end = collected_paragraph_styles[-1]['endIndex'] if collected_paragraph_styles else 0
                            if max_text_index > last_para_end and default_paragraph_style:
                                collected_paragraph_styles.append({
                                    'startIndex': last_para_end,
                                    'endIndex': max_text_index,
                                    'style': default_paragraph_style
                                })

                            # Phase 2: Generate API requests based on collected data

                            # Sort textRuns by startIndex to build text in correct order
                            collected_text_runs.sort(key=lambda x: x['startIndex'])

                            # Build full text content by concatenating textRuns
                            full_text_content = ''.join(tr['content'] for tr in collected_text_runs)

                            # Insert text if there's any content
                            if full_text_content:
                                copy_requests.append({
                                    'insertText': {
                                        'objectId': new_element_id,
                                        'insertionIndex': 0,
                                        'text': full_text_content
                                    }
                                })

                            # Apply text styles for each textRun using original indices
                            for text_run in collected_text_runs:
                                style = text_run.get('style', {})
                                if style:
                                    style_update = filter_text_style(style)
                                    if style_update:
                                        copy_requests.append({
                                            'updateTextStyle': {
                                                'objectId': new_element_id,
                                                'textRange': {
                                                    'type': 'FIXED_RANGE',
                                                    'startIndex': text_run['startIndex'],
                                                    'endIndex': text_run['endIndex']
                                                },
                                                'style': style_update,
                                                'fields': ','.join(style_update.keys())
                                            }
                                        })

                            # Apply paragraph styles for each paragraph
                            for para_info in collected_paragraph_styles:
                                para_style = para_info['style']
                                para_start = para_info['startIndex']
                                para_end = para_info['endIndex']

                                if para_end > para_start:  # Only apply if paragraph has content
                                    para_style_update = filter_paragraph_style(para_style)
                                    if para_style_update:
                                        copy_requests.append({
                                            'updateParagraphStyle': {
                                                'objectId': new_element_id,
                                                'textRange': {
                                                    'type': 'FIXED_RANGE',
                                                    'startIndex': para_start,
                                                    'endIndex': para_end
                                                },
                                                'style': para_style_update,
                                                'fields': ','.join(para_style_update.keys())
                                            }
                                        })

                            # If no paragraph markers found, apply default paragraph style to entire text
                            if not collected_paragraph_styles and default_paragraph_style and full_text_content:
                                para_style_update = filter_paragraph_style(default_paragraph_style)
                                if para_style_update:
                                    copy_requests.append({
                                        'updateParagraphStyle': {
                                            'objectId': new_element_id,
                                            'textRange': {
                                                'type': 'ALL'
                                            },
                                            'style': para_style_update,
                                            'fields': ','.join(para_style_update.keys())
                                        }
                                    })

                            # Remove trailing newline if present (extra newline issue)
                            if full_text_content and full_text_content.endswith('\n'):
                                # Calculate the index of the trailing newline
                                text_length = len(full_text_content)
                                copy_requests.append({
                                    'deleteText': {
                                        'objectId': new_element_id,
                                        'textRange': {
                                            'type': 'FIXED_RANGE',
                                            'startIndex': text_length - 1,
                                            'endIndex': text_length
                                        }
                                    }
                                })

                    elif 'image' in element:
                        # Handle image elements - copy them from template
                        image = element.get('image', {})
                        transform = element.get('transform', {})
                        size = element.get('size', {})

                        # Get image URL from the template element
                        # Image can have sourceUrl or contentUrl
                        image_url = None
                        if 'sourceUrl' in image:
                            image_url = image['sourceUrl']
                        elif 'contentUrl' in image:
                            image_url = image['contentUrl']

                        if image_url:
                            create_image_request = {
                                'createImage': {
                                    'url': image_url,
                                    'elementProperties': {
                                        'pageObjectId': new_slide_id,
                                        'size': size,
                                        'transform': transform
                                    }
                                }
                            }

                            copy_requests.append(create_image_request)
                        else:
                            print(f"    ⚠️  Warning: Image element found but no URL available, skipping")

                # Execute copy requests in batches
                if copy_requests:
                    batch_size = 50
                    for i in range(0, len(copy_requests), batch_size):
                        batch = copy_requests[i:i+batch_size]
                        slides_service.presentations().batchUpdate(
                            presentationId=presentation_id,
                            body={'requests': batch}
                        ).execute()

        print(f"  ✓ Replaced {len(slide_numbers)} slide(s) from template")
        return True

    except Exception as error:
        print(f"  ✗ Error replacing slides from template: {error}")
        import traceback
        traceback.print_exc()
        return False

def copy_template_presentation(spreadsheet_name, template_id, output_folder_id, creds):
    """
    Copy the template presentation, rename it, and move it to the output folder.

    Args:
        spreadsheet_name: Name to use for the new presentation (e.g., "Madurai")
        template_id: ID of the template presentation
        output_folder_id: ID of the folder to save the presentation
        creds: Service account credentials

    Returns:
        str: ID of the copied presentation
    """
    drive_service = build('drive', 'v3', credentials=creds)

    # Copy the template
    print(f"Copying template presentation...")
    copied_file = drive_service.files().copy(
        fileId=template_id,
        body={'name': f"{spreadsheet_name}.gslides"},
        supportsAllDrives=True
    ).execute()

    new_presentation_id = copied_file.get('id')
    print(f"Created presentation: {spreadsheet_name}.gslides (ID: {new_presentation_id})")

    # Move to output folder
    print(f"Moving presentation to output folder...")
    file_metadata = drive_service.files().get(fileId=new_presentation_id, fields='parents', supportsAllDrives=True).execute()
    previous_parents = ",".join(file_metadata.get('parents', []))

    if previous_parents:
        drive_service.files().update(
            fileId=new_presentation_id,
            addParents=output_folder_id,
            removeParents=previous_parents,
            fields='id, parents',
            supportsAllDrives=True
        ).execute()
    else:
        # If no previous parents, just add to the folder
        drive_service.files().update(
            fileId=new_presentation_id,
            addParents=output_folder_id,
            fields='id, parents',
            supportsAllDrives=True
        ).execute()

    return new_presentation_id

def get_chart_id_from_sheet(spreadsheet_id, sheet_name, creds):
    """
    Get the first chart ID from a sheet.

    Args:
        spreadsheet_id: ID of the spreadsheet
        sheet_name: Name of the sheet containing the chart
        creds: Service account credentials

    Returns:
        int: Chart ID, or None if not found
    """
    sheets_service = build('sheets', 'v4', credentials=creds)

    try:
        # Get the spreadsheet to find charts
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # Find the sheet and get its charts
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                # Charts are stored in the 'charts' property of the sheet
                charts = sheet.get('charts', [])
                if charts:
                    # Return the first chart's ID
                    return charts[0].get('chartId')
                break

        return None

    except HttpError as error:
        print(f"Error getting chart from sheet '{sheet_name}': {error}")
        return None

def get_image_file_from_folder(entity_folder_id, picture_name, creds):
    """
    Get the image file ID from the entity folder by matching the expected filename pattern.
    Images are named like: picture-<picture_name>.<extension>

    Args:
        entity_folder_id: ID of the entity folder containing the image files
        picture_name: Name of the picture placeholder (e.g., "block_wise_school_performance_count")
        creds: Service account credentials

    Returns:
        str: Image file ID that can be used to get public URL, or None if not found
    """
    drive_service = build('drive', 'v3', credentials=creds)

    try:
        # Construct expected filename: picture-<picture_name>
        expected_filename_base = f"picture-{picture_name}"

        # Try different image extensions
        image_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.svg']

        image_mime_types = [
            'image/png',
            'image/jpeg',
            'image/jpg',
            'image/gif',
            'image/bmp',
            'image/webp',
            'image/svg+xml'
        ]

        # Build query to search for image files matching the expected filename pattern
        mime_query = " or ".join([f"mimeType='{mime}'" for mime in image_mime_types])

        # Search for files in the entity folder that match the expected filename
        # The file name should be: expected_filename_base + extension
        for ext in image_extensions:
            image_filename = expected_filename_base + ext
            query = f"'{entity_folder_id}' in parents and name='{image_filename}' and trashed=false and ({mime_query})"

            try:
                results = drive_service.files().list(
                    q=query,
                    fields='files(id, name, mimeType)',
                    pageSize=10,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()

                files = results.get('files', [])
                if files:
                    # Return the file ID - we'll grant public access in replace_textbox_with_image
                    return files[0]['id']
            except HttpError as e:
                # Continue to next extension if this one fails
                continue

        # If exact match not found, try a more flexible search
        # Look for files that start with the expected filename base
        query = f"'{entity_folder_id}' in parents and name contains '{expected_filename_base}' and trashed=false and ({mime_query})"

        try:
            results = drive_service.files().list(
                q=query,
                fields='files(id, name, mimeType)',
                pageSize=10,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

            files = results.get('files', [])
            if files:
                # Return the file ID - we'll grant public access in replace_textbox_with_image
                return files[0]['id']
        except HttpError:
            pass

        # If no image found, return None
        print(f"    ⚠️  No image file found matching 'picture-{picture_name}' in entity folder")
        return None

    except HttpError as error:
        print(f"Error getting image file for 'picture-{picture_name}': {error}")
        return None
    except Exception as error:
        print(f"Error getting image file for 'picture-{picture_name}': {error}")
        return None

def replace_textbox_with_chart(presentation_id, slide_id, slide_number, textbox_element, spreadsheet_id, sheet_name, creds):
    """
    Replace a textbox element with a linked chart from a sheet.
    Maintains the z-order position of the original textbox.

    Args:
        presentation_id: ID of the presentation
        slide_id: ID of the slide
        slide_number: Slide number (1-based)
        textbox_element: The textbox page element to replace
        spreadsheet_id: ID of the spreadsheet containing the chart
        sheet_name: Name of the sheet containing the chart
        creds: Service account credentials

    Returns:
        bool: True if successful, False otherwise
    """
    slides_service = build('slides', 'v1', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    # Get the slide to find the z-order index of the textbox
    presentation = slides_service.presentations().get(presentationId=presentation_id).execute()
    presentation_slides = presentation.get('slides', [])

    # Find the slide and get its pageElements
    slide = None
    for s in presentation_slides:
        if s.get('objectId') == slide_id:
            slide = s
            break

    if not slide:
        print(f"Error: Slide {slide_id} not found")
        return False

    # Find the z-order index of the textbox element
    textbox_object_id = textbox_element.get('objectId')
    page_elements = slide.get('pageElements', [])
    z_order_index = None

    for idx, element in enumerate(page_elements):
        if element.get('objectId') == textbox_object_id:
            z_order_index = idx
            break

    if z_order_index is None:
        print(f"Warning: Could not find textbox element in slide {slide_number}, z-order may not be preserved")
        # Continue anyway, but z-order won't be preserved

    # Get position and size from textbox
    transform = textbox_element.get('transform', {})

    # Extract translate values (position)
    translate_x = transform.get('translateX', 0)
    translate_y = transform.get('translateY', 0)

    # Handle both numeric and object formats
    if isinstance(translate_x, dict):
        translate_x = translate_x.get('magnitude', 0)
    if isinstance(translate_y, dict):
        translate_y = translate_y.get('magnitude', 0)

    # Extract scale factors
    scale_x = transform.get('scaleX', 1)
    scale_y = transform.get('scaleY', 1)

    # Handle both numeric and object formats for scale
    if isinstance(scale_x, dict):
        scale_x = scale_x.get('magnitude', 1)
    if isinstance(scale_y, dict):
        scale_y = scale_y.get('magnitude', 1)

    # Get base size from size field
    size = textbox_element.get('size', {})
    width_obj = size.get('width', {})
    height_obj = size.get('height', {})

    base_width = width_obj.get('magnitude', 4000000) if isinstance(width_obj, dict) else (width_obj if width_obj else 4000000)
    base_height = height_obj.get('magnitude', 3000000) if isinstance(height_obj, dict) else (height_obj if height_obj else 3000000)

    # Calculate actual rendered size (base size * scale)
    actual_width = base_width * scale_x
    actual_height = base_height * scale_y

    # Get the sheet ID for the chart
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None

    for sheet in spreadsheet.get('sheets', []):
        if sheet['properties']['title'] == sheet_name:
            sheet_id = sheet['properties']['sheetId']
            break

    if sheet_id is None:
        print(f"Warning: Sheet '{sheet_name}' not found in spreadsheet")
        return False

    # Find existing chart in the sheet
    chart_id = get_chart_id_from_sheet(spreadsheet_id, sheet_name, creds)

    if chart_id is None:
        print(f"Error: No chart found in sheet '{sheet_name}'. Chart must exist in the sheet.")
        return False

    # Prepare requests to delete textbox and insert chart
    requests = [
        {
            'deleteObject': {
                'objectId': textbox_element.get('objectId')
            }
        },
        {
            'createSheetsChart': {
                'spreadsheetId': spreadsheet_id,
                'chartId': chart_id,
                'linkingMode': 'LINKED',
                'elementProperties': {
                    'pageObjectId': slide_id,
                    'size': {
                        'height': {'magnitude': actual_height, 'unit': 'EMU'},
                        'width': {'magnitude': actual_width, 'unit': 'EMU'}
                    },
                    'transform': {
                        'scaleX': 1,
                        'scaleY': 1,
                        'translateX': translate_x,
                        'translateY': translate_y,
                        'unit': 'EMU'
                    }
                }
            }
        }
    ]

    # Execute the batch update with retry logic
    body = {'requests': requests}

    def execute_batch_update():
        return slides_service.presentations().batchUpdate(
            presentationId=presentation_id,
            body=body
        ).execute()

    try:
        response = retry_with_exponential_backoff(execute_batch_update)

        # Get the objectId of the newly created chart and restore z-order
        if z_order_index is not None:
            # Extract the objectId from the response
            replies = response.get('replies', [])
            new_chart_object_id = None

            for reply in replies:
                if 'createSheetsChart' in reply:
                    new_chart_object_id = reply['createSheetsChart'].get('objectId')
                    break

            if new_chart_object_id:
                # After deletion, the z-order indices shift, so we need to adjust
                # Since we deleted the element at z_order_index, the new element is at the end
                # We need to move it back to z_order_index
                # Get current slide state to find the correct new index
                updated_presentation = slides_service.presentations().get(presentationId=presentation_id).execute()
                updated_slides = updated_presentation.get('slides', [])

                for s in updated_slides:
                    if s.get('objectId') == slide_id:
                        updated_page_elements = s.get('pageElements', [])
                        # Find the current index of the new chart
                        current_index = None
                        for idx, element in enumerate(updated_page_elements):
                            if element.get('objectId') == new_chart_object_id:
                                current_index = idx
                                break

                        # If found and it's not already at the correct position, move it
                        if current_index is not None and current_index != z_order_index:
                            # Calculate how many positions to move
                            # After deletion, elements after the deleted one shift down by 1
                            # The new element is at the end, we need to move it to z_order_index
                            positions_to_move = current_index - z_order_index

                            if positions_to_move > 0:
                                # Need to move backward (toward front of array = lower z-order)
                                # Use SEND_BACKWARD the required number of times
                                order_requests = []
                                for _ in range(positions_to_move):
                                    order_requests.append({
                                        'updatePageElementsZOrder': {
                                            'pageElementObjectIds': [new_chart_object_id],
                                            'operation': 'SEND_BACKWARD'
                                        }
                                    })

                                def execute_order_update():
                                    return slides_service.presentations().batchUpdate(
                                        presentationId=presentation_id,
                                        body={'requests': order_requests}
                                    ).execute()

                                try:
                                    retry_with_exponential_backoff(execute_order_update)
                                except HttpError as order_error:
                                    print(f"    ⚠️  Warning: Could not restore z-order position: {order_error}")
                        break

        print(f"    ✓ Replaced textbox with chart from sheet '{sheet_name}' in slide {slide_number}")
        return True
    except HttpError as error:
        print(f"Error replacing textbox with chart in slide {slide_number}: {error}")
        return False

def replace_textbox_with_image(presentation_id, slide_id, slide_number, textbox_element, image_url_or_file_id, creds):
    """
    Replace a textbox element with an image, resizing it to match the textbox dimensions.
    Maintains the z-order position of the original textbox.

    Args:
        presentation_id: ID of the presentation
        slide_id: ID of the slide
        slide_number: Slide number (1-based)
        textbox_element: The textbox page element to replace
        image_url_or_file_id: Full image URL (with access token if from Drive) or Drive file ID
        creds: Service account credentials

    Returns:
        bool: True if successful, False otherwise
    """
    slides_service = build('slides', 'v1', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    # Get the slide to find the z-order index of the textbox
    presentation = slides_service.presentations().get(presentationId=presentation_id).execute()
    presentation_slides = presentation.get('slides', [])

    # Find the slide and get its pageElements
    slide = None
    for s in presentation_slides:
        if s.get('objectId') == slide_id:
            slide = s
            break

    if not slide:
        print(f"Error: Slide {slide_id} not found")
        return False

    # Find the z-order index of the textbox element
    textbox_object_id = textbox_element.get('objectId')
    page_elements = slide.get('pageElements', [])
    z_order_index = None

    for idx, element in enumerate(page_elements):
        if element.get('objectId') == textbox_object_id:
            z_order_index = idx
            break

    if z_order_index is None:
        print(f"Warning: Could not find textbox element in slide {slide_number}, z-order may not be preserved")
        # Continue anyway, but z-order won't be preserved

    # Get position and size from textbox
    transform = textbox_element.get('transform', {})

    # Extract translate values (position)
    translate_x = transform.get('translateX', 0)
    translate_y = transform.get('translateY', 0)

    # Handle both numeric and object formats
    if isinstance(translate_x, dict):
        translate_x = translate_x.get('magnitude', 0)
    if isinstance(translate_y, dict):
        translate_y = translate_y.get('magnitude', 0)

    # Extract scale factors
    scale_x = transform.get('scaleX', 1)
    scale_y = transform.get('scaleY', 1)

    # Handle both numeric and object formats for scale
    if isinstance(scale_x, dict):
        scale_x = scale_x.get('magnitude', 1)
    if isinstance(scale_y, dict):
        scale_y = scale_y.get('magnitude', 1)

    # Get base size from size field
    size = textbox_element.get('size', {})
    width_obj = size.get('width', {})
    height_obj = size.get('height', {})

    base_width = width_obj.get('magnitude', 4000000) if isinstance(width_obj, dict) else (width_obj if width_obj else 4000000)
    base_height = height_obj.get('magnitude', 3000000) if isinstance(height_obj, dict) else (height_obj if height_obj else 3000000)

    # Calculate actual rendered size (base size * scale)
    actual_width = base_width * scale_x
    actual_height = base_height * scale_y

    # Determine if image_url_or_file_id is a URL or a Drive file ID
    is_url = image_url_or_file_id.startswith('http://') or image_url_or_file_id.startswith('https://')

    image_url = image_url_or_file_id
    had_public_permission = False
    permission_id = None

    if not is_url:
        # It's a Drive file ID - temporarily grant public access
        file_id = image_url_or_file_id

        try:
            # First, check if file already has public access
            try:
                permissions = drive_service.permissions().list(
                    fileId=file_id,
                    fields='permissions(id,type,role)',
                    supportsAllDrives=True
                ).execute()

                # Check if 'anyone' permission already exists
                has_public_access = False
                for perm in permissions.get('permissions', []):
                    if perm.get('type') == 'anyone' and perm.get('role') in ['reader', 'viewer']:
                        has_public_access = True
                        permission_id = perm.get('id')
                        break
            except HttpError:
                # If we can't check permissions, assume it's not public
                has_public_access = False

            # If not publicly accessible, try to grant temporary public access
            if not has_public_access:
                try:
                    permission = {
                        'type': 'anyone',
                        'role': 'reader'
                    }
                    result = drive_service.permissions().create(
                        fileId=file_id,
                        body=permission,
                        supportsAllDrives=True
                    ).execute()
                    permission_id = result.get('id')
                    had_public_permission = True
                    print(f"    ℹ️  Temporarily granted public access to image file for insertion")
                except HttpError as perm_error:
                    # If we can't modify permissions, check if file has a shareable link
                    print(f"    ⚠️  Cannot modify file permissions (app lacks write access). Checking for existing shareable link...")
                    # Try to get webContentLink - this might work if file is already shared
                    try:
                        file_metadata = drive_service.files().get(
                            fileId=file_id,
                            fields='webContentLink,webViewLink',
                            supportsAllDrives=True
                        ).execute()

                        web_content_link = file_metadata.get('webContentLink')
                        if web_content_link:
                            # Extract file ID from webContentLink and construct direct download URL
                            image_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                            print(f"    ℹ️  Using existing shareable link (file may need to be manually shared)")
                        else:
                            # No shareable link available
                            raise ValueError("File is not publicly accessible and app cannot modify permissions. Please manually share the file with 'Anyone with the link' access.")
                    except Exception as link_error:
                        raise ValueError(f"File is not publicly accessible. Please manually share the image file (ID: {file_id}) with 'Anyone with the link' access, or grant the app write access to modify permissions. Error: {perm_error}")

            # Get the public URL for the image
            file_metadata = drive_service.files().get(
                fileId=file_id,
                fields='webContentLink',
                supportsAllDrives=True
            ).execute()

            web_content_link = file_metadata.get('webContentLink')
            if web_content_link:
                # Convert webContentLink to direct download URL
                # webContentLink format: https://drive.google.com/uc?id=FILE_ID&export=download
                # We need: https://drive.google.com/uc?export=download&id=FILE_ID
                image_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            else:
                # Fallback to constructing URL manually
                image_url = f"https://drive.google.com/uc?export=download&id={file_id}"

        except (HttpError, ValueError) as e:
            error_msg = str(e)
            if "manually share" in error_msg.lower() or "cannot modify" in error_msg.lower():
                print(f"    ⚠️  {error_msg}")
            else:
                print(f"    ⚠️  Error setting up image file permissions: {e}")
            # Fallback to basic Drive URL (may not work if file is not public)
            image_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            print(f"    ⚠️  Attempting to use file URL anyway (may fail if file is not publicly accessible)")

    # Prepare requests to delete textbox and insert image
    create_image_request = {
        'createImage': {
            'url': image_url,
            'elementProperties': {
                'pageObjectId': slide_id,
                'size': {
                    'height': {'magnitude': actual_height, 'unit': 'EMU'},
                    'width': {'magnitude': actual_width, 'unit': 'EMU'}
                },
                'transform': {
                    'scaleX': 1,
                    'scaleY': 1,
                    'translateX': translate_x,
                    'translateY': translate_y,
                    'unit': 'EMU'
                }
            }
        }
    }

    requests = [
        {
            'deleteObject': {
                'objectId': textbox_element.get('objectId')
            }
        },
        create_image_request
    ]

    # Execute the batch update with retry logic
    body = {'requests': requests}

    def execute_batch_update():
        return slides_service.presentations().batchUpdate(
            presentationId=presentation_id,
            body=body
        ).execute()

    try:
        response = retry_with_exponential_backoff(execute_batch_update)

        # Get the objectId of the newly created image and restore z-order
        if z_order_index is not None:
            # Extract the objectId from the response
            replies = response.get('replies', [])
            new_image_object_id = None

            for reply in replies:
                if 'createImage' in reply:
                    new_image_object_id = reply['createImage'].get('objectId')
                    break

            if new_image_object_id:
                # After deletion, the z-order indices shift, so we need to adjust
                # Since we deleted the element at z_order_index, the new element is at the end
                # We need to move it back to z_order_index
                # Get current slide state to find the correct new index
                updated_presentation = slides_service.presentations().get(presentationId=presentation_id).execute()
                updated_slides = updated_presentation.get('slides', [])

                for s in updated_slides:
                    if s.get('objectId') == slide_id:
                        updated_page_elements = s.get('pageElements', [])
                        # Find the current index of the new image
                        current_index = None
                        for idx, element in enumerate(updated_page_elements):
                            if element.get('objectId') == new_image_object_id:
                                current_index = idx
                                break

                        # If found and it's not already at the correct position, move it
                        if current_index is not None and current_index != z_order_index:
                            # Calculate how many positions to move
                            # After deletion, elements after the deleted one shift down by 1
                            # The new element is at the end, we need to move it to z_order_index
                            positions_to_move = current_index - z_order_index

                            if positions_to_move > 0:
                                # Need to move backward (toward front of array = lower z-order)
                                # Use SEND_BACKWARD the required number of times
                                order_requests = []
                                for _ in range(positions_to_move):
                                    order_requests.append({
                                        'updatePageElementsZOrder': {
                                            'pageElementObjectIds': [new_image_object_id],
                                            'operation': 'SEND_BACKWARD'
                                        }
                                    })

                                def execute_order_update():
                                    return slides_service.presentations().batchUpdate(
                                        presentationId=presentation_id,
                                        body={'requests': order_requests}
                                    ).execute()

                                try:
                                    retry_with_exponential_backoff(execute_order_update)
                                except HttpError as order_error:
                                    print(f"    ⚠️  Warning: Could not restore z-order position: {order_error}")
                        break

        print(f"    ✓ Replaced textbox with image in slide {slide_number}")
        return True
    except HttpError as error:
        print(f"Error replacing textbox with image in slide {slide_number}: {error}")
        return False
    finally:
        # Always revoke the temporary public permission, whether insertion succeeded or failed
        if had_public_permission and permission_id and not is_url:
            try:
                drive_service.permissions().delete(
                    fileId=image_url_or_file_id,
                    permissionId=permission_id,
                    supportsAllDrives=True
                ).execute()
                print(f"    ℹ️  Revoked temporary public access from image file")
            except HttpError as revoke_error:
                print(f"    ⚠️  Warning: Could not revoke temporary public access: {revoke_error}")

def replace_multiple_placeholders_in_textbox(presentation_id, slide_number, textbox_element, placeholder_map, creds):
    """
    Replace multiple placeholders in a single textbox efficiently.
    Preserves the text style from each deleted placeholder text.

    Args:
        presentation_id: ID of the presentation
        slide_number: Slide number (1-based)
        textbox_element: The textbox page element
        placeholder_map: Dictionary mapping placeholder text to replacement text
                       (e.g., {'{{percentage}}': '97.5', '{{entity_rank}}': '31'})
        creds: Service account credentials

    Returns:
        bool: True if successful, False otherwise
    """
    slides_service = build('slides', 'v1', credentials=creds)

    shape_id = textbox_element.get('objectId')

    # Find the exact range of all placeholders in the text
    text_content = textbox_element['shape']['text']
    text_elements = text_content.get('textElements', [])

    # Build full text to find placeholder positions
    full_text = ''
    for element in text_elements:
        if 'textRun' in element:
            full_text += element['textRun'].get('content', '')

    # Whitelist of writable text style fields
    writable_text_style_fields = [
        'bold', 'italic', 'underline', 'strikethrough',
        'fontFamily', 'fontSize', 'foregroundColor', 'backgroundColor',
        'weightedFontFamily'  # Font weight (object with fontFamily and weight)
    ]

    # Helper function to filter text style to only writable fields
    def filter_text_style(text_style):
        """Filter text style to only include writable fields."""
        if not text_style:
            return {}
        filtered = {}
        for field in writable_text_style_fields:
            if field in text_style:
                filtered[field] = text_style[field]
        return filtered

    # Helper function to extract text style from a position
    def get_style_at_position(position):
        """Extract text style from the textRun at the given position."""
        for element in text_elements:
            if 'textRun' in element:
                element_start = element.get('startIndex', 0)
                element_end = element.get('endIndex', element_start + len(element['textRun'].get('content', '')))

                # Check if this textRun overlaps with the position
                if element_start <= position < element_end:
                    text_run = element['textRun']
                    return text_run.get('style', {})
        return {}

    # Find all placeholders and their positions
    placeholder_positions = []
    for placeholder_text, replacement_text in placeholder_map.items():
        start_pos = 0
        while True:
            pos = full_text.find(placeholder_text, start_pos)
            if pos == -1:
                break
            # Extract style from the first character of the placeholder
            placeholder_style = get_style_at_position(pos)
            filtered_style = filter_text_style(placeholder_style)

            placeholder_positions.append({
                'placeholder': placeholder_text,
                'replacement': replacement_text,
                'start': pos,
                'end': pos + len(placeholder_text),
                'style': filtered_style
            })
            start_pos = pos + 1

    if not placeholder_positions:
        return False

    # Sort by position in reverse order (end to start) to maintain indices during replacement
    placeholder_positions.sort(key=lambda x: x['start'], reverse=True)

    # Build batch requests for all replacements
    requests = []
    for placeholder_info in placeholder_positions:
        placeholder_text = placeholder_info['placeholder']
        replacement_text = placeholder_info['replacement']
        start_index = placeholder_info['start']
        end_index = placeholder_info['end']
        filtered_style = placeholder_info['style']

        # Delete the placeholder text and insert replacement text
        requests.append({
            'deleteText': {
                'objectId': shape_id,
                'textRange': {
                    'type': 'FIXED_RANGE',
                    'startIndex': start_index,
                    'endIndex': end_index
                }
            }
        })
        requests.append({
            'insertText': {
                'objectId': shape_id,
                'insertionIndex': start_index,
                'text': replacement_text
            }
        })

        # If we have a style to apply, add an updateTextStyle request
        if filtered_style:
            replacement_end = start_index + len(replacement_text)
            requests.append({
                'updateTextStyle': {
                    'objectId': shape_id,
                    'textRange': {
                        'type': 'FIXED_RANGE',
                        'startIndex': start_index,
                        'endIndex': replacement_end
                    },
                    'style': filtered_style,
                    'fields': ','.join(filtered_style.keys())
                }
            })

    # Execute the batch update with retry logic
    body = {'requests': requests}

    def execute_batch_update():
        return slides_service.presentations().batchUpdate(
            presentationId=presentation_id,
            body=body
        ).execute()

    try:
        response = retry_with_exponential_backoff(execute_batch_update)
        replaced_count = len(placeholder_positions)
        print(f"    ✓ Replaced {replaced_count} placeholder(s) in slide {slide_number}")
        return True
    except HttpError as error:
        print(f"Error replacing multiple placeholders in slide {slide_number}: {error}")
        return False

def populate_table_with_data(slides_service, presentation_id, slide_number, table_element, table_data):
    """
    Populate a Slides table element with data while preserving existing text formatting.
    """
    table = table_element.get('table', {})
    table_id = table_element.get('objectId')
    table_rows = table.get('tableRows', [])

    if not table_rows or not table_id:
        print(f"    ⚠️  Table on slide {slide_number} has no rows or objectId, skipping")
        return False

    num_rows = len(table_rows)
    num_cols = 0
    for row in table_rows:
        num_cols = max(num_cols, len(row.get('tableCells', [])))

    if num_cols == 0:
        print(f"    ⚠️  Table on slide {slide_number} has no columns, skipping")
        return False

    # Determine data dimensions
    data_rows = len(table_data)
    data_cols = max((len(r) for r in table_data), default=0)

    # If we need more rows, add them before populating
    if data_rows > num_rows:
        rows_to_add = data_rows - num_rows
        print(f"    ℹ️  Adding {rows_to_add} row(s) to table on slide {slide_number} to accommodate data")

        # Google Slides API limits: max 20 rows per insertTableRows request
        max_rows_per_request = 20
        current_row_index = num_rows - 1  # Start inserting after the last existing row
        remaining_rows = rows_to_add

        try:
            # Break insertion into batches of max 20 rows
            while remaining_rows > 0:
                rows_in_batch = min(remaining_rows, max_rows_per_request)

                insert_request = {
                    'insertTableRows': {
                        'tableObjectId': table_id,
                        'cellLocation': {
                            'rowIndex': current_row_index,
                            'columnIndex': 0
                        },
                        'insertBelow': True,
                        'number': rows_in_batch
                    }
                }

                def execute_insert_rows():
                    return slides_service.presentations().batchUpdate(
                        presentationId=presentation_id,
                        body={'requests': [insert_request]}
                    ).execute()

                retry_with_exponential_backoff(execute_insert_rows)

                # Update for next batch: move insertion point and reduce remaining count
                current_row_index += rows_in_batch
                remaining_rows -= rows_in_batch

            # Update num_rows to reflect the new table size
            num_rows = data_rows
        except HttpError as error:
            print(f"    ⚠️  Error adding rows to table on slide {slide_number}: {error}")
            return False

    # Warn if data has more columns than table (we can't add columns easily)
    if data_cols > num_cols:
        print(f"    ⚠️  Table data for slide {slide_number} has more columns ({data_cols}) than the table ({num_cols}). Extra columns will be ignored.")

    # Reuse the first available text style in a cell so formatting stays consistent
    writable_text_style_fields = [
        'bold', 'italic', 'underline', 'strikethrough',
        'fontFamily', 'fontSize', 'foregroundColor', 'backgroundColor',
        'weightedFontFamily'
    ]

    def filter_text_style(text_style):
        if not text_style:
            return {}
        filtered = {}
        for field in writable_text_style_fields:
            if field in text_style:
                filtered[field] = text_style[field]
        return filtered

    def get_first_text_style(cell):
        text_elements = cell.get('text', {}).get('textElements', [])
        for element in text_elements:
            if 'textRun' in element:
                style = element['textRun'].get('style', {})
                filtered = filter_text_style(style)
                if filtered:
                    return filtered
        return {}

    def cell_has_text(cell):
        """Check if a cell has any text content."""
        if not cell:
            return False
        text_elements = cell.get('text', {}).get('textElements', [])
        for element in text_elements:
            if 'textRun' in element:
                content = element['textRun'].get('content', '')
                if content and content.strip():
                    return True
        return False

    # Get reference text style from last existing row for new rows
    reference_text_style = {}
    if table_rows:
        last_row = table_rows[-1]
        last_row_cells = last_row.get('tableCells', [])
        if last_row_cells:
            reference_text_style = get_first_text_style(last_row_cells[0])

    requests = []
    for r in range(num_rows):
        # Get row cells if this is an existing row
        row_cells = []
        if r < len(table_rows):
            row_cells = table_rows[r].get('tableCells', [])

        for c in range(num_cols):
            # Skip if column doesn't exist in this row
            if r < len(table_rows) and c >= len(row_cells):
                continue

            # Get cell for style reference (only for existing rows)
            cell = None
            if r < len(table_rows) and c < len(row_cells):
                cell = row_cells[c]

            # Get value from data
            value = ''
            if r < len(table_data) and c < len(table_data[r]):
                value = str(table_data[r][c]) if table_data[r][c] is not None else ''

            # Use cell style if available (existing rows), otherwise use reference style (new rows)
            if cell:
                text_style = get_first_text_style(cell)
            else:
                text_style = reference_text_style

            # Clear existing text only if cell has text content
            # Skip deleteText for empty cells to avoid API errors
            if cell and cell_has_text(cell):
                requests.append({
                    'deleteText': {
                        'objectId': table_id,
                        'cellLocation': {
                            'rowIndex': r,
                            'columnIndex': c
                        },
                        'textRange': {'type': 'ALL'}
                    }
                })

            # Insert new value (skip insert for empty strings)
            if value:
                requests.append({
                    'insertText': {
                        'objectId': table_id,
                        'cellLocation': {
                            'rowIndex': r,
                            'columnIndex': c
                        },
                        'insertionIndex': 0,
                        'text': value
                    }
                })

                if text_style:
                    requests.append({
                        'updateTextStyle': {
                            'objectId': table_id,
                            'cellLocation': {
                                'rowIndex': r,
                                'columnIndex': c
                            },
                            'textRange': {
                                'type': 'FIXED_RANGE',
                                'startIndex': 0,
                                'endIndex': len(value)
                            },
                            'style': text_style,
                            'fields': ','.join(text_style.keys())
                        }
                    })

    if not requests:
        return True

    # Execute in batches to avoid request limits
    batch_size = 50
    try:
        for i in range(0, len(requests), batch_size):
            slides_service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={'requests': requests[i:i + batch_size]}
            ).execute()
        print(f"    ✓ Populated table on slide {slide_number}")
        return True
    except HttpError as error:
        print(f"    ⚠️  Error populating table on slide {slide_number}: {error}")
        return False

def process_all_slides(presentation_id, sheet_mappings, spreadsheet_id, entity_name, data_sheet, entity_folder_id, creds):
    """
    Process all slides in the presentation, replacing placeholders based on sheet mappings.

    Args:
        presentation_id: ID of the presentation
        sheet_mappings: List of sheet mapping dictionaries with 'placeholder_type', 'placeholder_name', 'sheet_name'
        spreadsheet_id: ID of the spreadsheet
        entity_name: Name of the entity for text placeholder replacement
        data_sheet: Dictionary with text replacement values from the 'data' tab
        entity_folder_id: ID of the entity folder containing image files
        creds: Service account credentials

    Returns:
        bool: True if successful, False otherwise
    """
    slides_service = build('slides', 'v1', credentials=creds)

    try:
        # Get the presentation
        presentation = slides_service.presentations().get(presentationId=presentation_id).execute()
        presentation_slides = presentation.get('slides', [])

        # Build lookup dictionaries keyed by placeholder name
        chart_mapping_by_name = {}
        table_mapping_by_name = {}
        for mapping in sheet_mappings:
            placeholder_type = mapping['placeholder_type']
            placeholder_name = mapping['placeholder_name']
            if placeholder_type == 'chart':
                chart_mapping_by_name[f"chart-{placeholder_name}"] = mapping
            elif placeholder_type == 'table':
                table_mapping_by_name[placeholder_name] = mapping

        # Text placeholder to look for
        entity_placeholder = "{{entity_name}}"
        table_placeholder_pattern = r'^\{\{table-([^}]+)\}\}$'
        table_data_cache = {}
        table_decisions = {}

        # Loop through all slides
        for slide_index, slide in enumerate(presentation_slides):
            slide_number = slide_index + 1  # 1-based slide number (used only for messaging)

            slide_id = slide.get('objectId')

            print(f"\nProcessing slide {slide_number}...")

            # Loop through all elements in the slide
            for page_element in slide.get('pageElements', []):
                # Process tables first
                if 'table' in page_element:
                    table_obj = page_element.get('table', {})
                    table_rows = table_obj.get('tableRows', [])
                    if not table_rows or not table_rows[0].get('tableCells'):
                        continue

                    first_cell = table_rows[0].get('tableCells')[0]
                    text_elements = first_cell.get('text', {}).get('textElements', [])
                    top_left_text = ''
                    for element in text_elements:
                        if 'textRun' in element:
                            top_left_text += element['textRun'].get('content', '')
                    top_left_text = top_left_text.strip()

                    table_match = re.match(table_placeholder_pattern, top_left_text)
                    if not table_match:
                        continue

                    table_name = table_match.group(1).strip()
                    mapping = table_mapping_by_name.get(table_name)
                    sheet_name = mapping['sheet_name'] if mapping else f"table-{table_name}"

                    if table_name not in table_data_cache:
                        table_values = read_table_from_sheet(spreadsheet_id, sheet_name, creds)
                        table_data_cache[table_name] = table_values
                    else:
                        # Duplicate reference detected
                        if table_name not in table_decisions:
                            response = input(f"Multiple references to table '{table_name}' detected. Continue replacing everywhere? [y/N]: ").strip().lower()
                            table_decisions[table_name] = response in ('y', 'yes')
                        if not table_decisions.get(table_name, False):
                            print(f"    ✗ Stopping at duplicate table '{table_name}' per user choice.")
                            return False
                        table_values = table_data_cache[table_name]

                    if table_values is None:
                        print(f"    ⚠️  Skipping table '{table_name}' on slide {slide_number} due to missing data")
                        continue

                    success = populate_table_with_data(
                        slides_service=slides_service,
                        presentation_id=presentation_id,
                        slide_number=slide_number,
                        table_element=page_element,
                        table_data=table_values
                    )
                    if not success:
                        print(f"    ⚠️  Failed to populate table '{table_name}' on slide {slide_number}")
                    continue

                # Only process text elements (shapes with text)
                if 'shape' in page_element and 'text' in page_element['shape']:
                    text_content = page_element['shape']['text'].get('textElements', [])
                    full_text = ''

                    # Build full text from all text elements
                    for element in text_content:
                        if 'textRun' in element:
                            full_text += element['textRun'].get('content', '')

                    full_text_stripped = full_text.strip()

                    # Check for chart placeholders: {{chart-placeholder_name}} format
                    chart_pattern = r'\{\{(chart-[^}]+)\}\}'
                    chart_match = re.match(chart_pattern, full_text_stripped)
                    if chart_match:
                        placeholder_name = chart_match.group(1).strip()
                        if placeholder_name in chart_mapping_by_name:
                            mapping = chart_mapping_by_name[placeholder_name]
                            success = replace_textbox_with_chart(
                                presentation_id=presentation_id,
                                slide_id=slide_id,
                                slide_number=slide_number,
                                textbox_element=page_element,
                                spreadsheet_id=spreadsheet_id,
                                sheet_name=mapping['sheet_name'],
                                creds=creds
                            )
                            if not success:
                                print(f"    ⚠️  Failed to replace chart placeholder: {placeholder_name}")
                        else:
                            print(f"    ⚠️  No mapping found for chart placeholder: {placeholder_name} in slide {slide_number}")
                        continue  # Skip further processing for chart placeholders

                    # Check for {{picture-placeholder_name}} format and replace with image
                    picture_pattern = r'\{\{picture-([^}]+)\}\}'
                    picture_match = re.search(picture_pattern, full_text)
                    if picture_match:
                        placeholder_name = picture_match.group(1).strip()
                        # Search for image file in entity folder: picture-<placeholder_name>
                        image_url_or_file_id = get_image_file_from_folder(
                            entity_folder_id=entity_folder_id,
                            picture_name=placeholder_name,
                            creds=creds
                        )
                        if image_url_or_file_id:
                            # Replace textbox with image
                            success = replace_textbox_with_image(
                                presentation_id=presentation_id,
                                slide_id=slide_id,
                                slide_number=slide_number,
                                textbox_element=page_element,
                                image_url_or_file_id=image_url_or_file_id,
                                creds=creds
                            )
                            if not success:
                                print(f"    ⚠️  Failed to replace picture placeholder: {placeholder_name}")
                        else:
                            print(f"    ⚠️  No image found matching 'picture-{placeholder_name}' in entity folder")
                        continue  # Skip further processing for picture placeholders

                    # Collect all placeholders to replace (entity_name + data sheet placeholders)
                    placeholder_map = {}

                    # Add entity_name placeholder if present
                    if entity_placeholder in full_text:
                        placeholder_map[entity_placeholder] = entity_name

                    # Check if there's a data sheet for this slide and add data placeholders
                    if data_sheet:
                        placeholder_pattern = r'\{\{([^}]+)\}\}'
                        found_placeholders = re.findall(placeholder_pattern, full_text)

                        for placeholder_name in found_placeholders:
                            full_placeholder = f"{{{{{placeholder_name}}}}}"
                            if placeholder_name != 'entity_name' and placeholder_name in data_sheet:
                                replacement_value = data_sheet[placeholder_name]
                                placeholder_map[full_placeholder] = replacement_value

                    # Replace all placeholders in one batch operation
                    if placeholder_map:
                        success = replace_multiple_placeholders_in_textbox(
                            presentation_id=presentation_id,
                            slide_number=slide_number,
                            textbox_element=page_element,
                            placeholder_map=placeholder_map,
                            creds=creds
                        )
                        if not success:
                            print(f"    ⚠️  Failed to replace placeholders in slide {slide_number}")

        return True

    except HttpError as error:
        print(f"Error processing slides: {error}")
        return False

def process_spreadsheet(spreadsheet_id, spreadsheet_name, template_id, output_folder_id, entity_folder_id, creds):
    """
    Process a spreadsheet to generate a Google Slides presentation.

    Args:
        spreadsheet_id: ID of the source spreadsheet
        spreadsheet_name: Name to use for the output presentation
        template_id: ID of the template presentation
        output_folder_id: ID of the folder to save the presentation
        entity_folder_id: ID of the entity folder containing image files
        creds: Service account credentials
        slides: Optional set of slide numbers to process. If None, processes all slides.

    Returns:
        str: ID of the created presentation, or None if failed
    """
    print(f"\n{'='*80}")
    print(f"Processing spreadsheet: {spreadsheet_name}")
    print(f"{'='*80}\n")

    # Initialize services
    gspread_client = gspread.authorize(creds)

    # Use entity_name from file/folder name
    entity_name = spreadsheet_name

    try:
        # Get all worksheets from the spreadsheet
        print("Reading spreadsheet worksheets...")
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        worksheets = spreadsheet.worksheets()

        # Filter and parse sheets matching the pattern (no slide numbers)
        sheet_mappings = []
        for worksheet in worksheets:
            sheet_name = worksheet.title
            parsed = parse_sheet_name(sheet_name)
            if parsed:
                placeholder_type, placeholder_name = parsed
                sheet_mappings.append({
                    'sheet_name': sheet_name,
                    'placeholder_type': placeholder_type,
                    'placeholder_name': placeholder_name
                })
                print(f"  Found: {sheet_name} -> Type: {placeholder_type}, Placeholder: {placeholder_name}")

        if not sheet_mappings:
            print("⚠️  No sheets matching the pattern <type>:<placeholder> found!")
        else:
            print(f"\nFound {len(sheet_mappings)} sheet mappings\n")

        # Read the shared 'data' sheet for text placeholders
        print("Reading data sheet...")
        try:
            data_sheet = read_data_from_sheet(spreadsheet_id, "data", creds)
            if data_sheet:
                print(f"  Loaded {len(data_sheet)} column(s): {', '.join(data_sheet.keys())}")
            else:
                print("  ⚠️  No data found in 'data' sheet")
        except Exception as e:
            print(f"  ⚠️  Failed to read 'data' sheet: {e}")
            data_sheet = None

        # Delete existing presentation if it exists then recreate
        print("Checking for existing presentation...")
        delete_existing_presentation(entity_name, output_folder_id, creds)

        # Copy template presentation (use entity_name from file/folder name)
        presentation_id = copy_template_presentation(
            entity_name, template_id, output_folder_id, creds
        )

        # Process all slides and replace placeholders
        success = process_all_slides(
            presentation_id=presentation_id,
            sheet_mappings=sheet_mappings,
            spreadsheet_id=spreadsheet_id,
            entity_name=entity_name,
            data_sheet=data_sheet,
            entity_folder_id=entity_folder_id,
            creds=creds,
        )

        if not success:
            print("Warning: Some placeholders may not have been replaced successfully")

        print(f"\n{'='*80}")
        print(f"✓ Presentation created successfully!")
        print(f"  Presentation ID: {presentation_id}")
        print(f"  View at: https://docs.google.com/presentation/d/{presentation_id}/edit")
        print(f"{'='*80}\n")

        return presentation_id

    except Exception as e:
        print(f"\nError processing spreadsheet: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_report(creds=None, layout: DriveLayout = None, input_folder_id=None, template_id=None, output_folder_id=None):
    """
    Generate Google Slides presentations from Google Sheets for entities marked for generation in entities.csv.

    Args:
        creds: Google OAuth credentials. If None, will be obtained automatically.
        layout: DriveLayout object containing configuration. Required if not using individual folder IDs.
        input_folder_id: Input folder ID. If None, uses layout.l1_data_id.
        template_id: Template presentation ID. If None, uses layout.report_template_id.
        output_folder_id: Output folder ID. If None, uses layout.l2_report_id.

    Returns:
        dict: Dictionary with 'successful' (list of tuples (entity_name, presentation_id))
              and 'failed' (list of tuples (entity_name, error_message))

    Raises:
        FileNotFoundError: If service account credentials are not found
        ValueError: If neither layout nor all individual folder IDs are provided
        Exception: Other errors during processing
    """
    if creds is None:
        creds = get_oauth_credentials()

    # Use layout if provided, otherwise use individual parameters
    if layout is not None:
        if input_folder_id is None:
            input_folder_id = layout.l1_data_id
        if template_id is None:
            template_id = layout.report_template_id
        if output_folder_id is None:
            output_folder_id = layout.l2_report_id
    elif input_folder_id is None or template_id is None or output_folder_id is None:
        raise ValueError("Either layout (DriveLayout) must be provided, or all of input_folder_id, template_id, and output_folder_id must be provided.")

    print(f"Input folder: {input_folder_id}")
    print(f"Template: {template_id}")
    print(f"Output folder: {output_folder_id}\n")

    # Determine which entities to process
    if layout and layout.entities_csv_id:
        target_entities = load_entities(layout.entities_csv_id, creds)
        print(f"Loaded {len(target_entities)} entities with generate=Y from entities.csv\n")
        if not target_entities:
            print("✗ No entities marked with generate=Y in entities.csv.")
            return {'successful': [], 'failed': []}
    else:
        print("\n✗ No entities CSV ID found in layout.")
        return {'successful': [], 'failed': []}

    # List all entity folders in the input folder
    print("Scanning input folder for entity folders...")
    all_entity_folders = list_entity_folders(input_folder_id, creds)

    if not all_entity_folders:
        print("No entity folders found in the input folder!")
        return {'successful': [], 'failed': []}

    # Filter to only include target entities
    entity_folders = []
    for folder_id, folder_name in all_entity_folders:
        if folder_name in target_entities:
            entity_folders.append((folder_id, folder_name))

    if not entity_folders:
        print(f"✗ No matching entity folders found for entities marked generate=Y.")
        print(f"  Available folders: {', '.join([name for _, name in all_entity_folders])}")
        print(f"  Requested entities: {', '.join(target_entities)}")
        return {'successful': [], 'failed': []}

    print(f"Found {len(entity_folders)} matching entity folder(s) to process:\n")
    for folder_id, folder_name in entity_folders:
        print(f"  - {folder_name} (ID: {folder_id})")
    print()

    # Process each entity folder
    successful = []
    failed = []

    for entity_folder_id, entity_name in entity_folders:
        try:
            # Find spreadsheets in this entity folder
            print(f"\nProcessing entity: {entity_name}")
            spreadsheets = list_spreadsheets_in_folder(entity_folder_id, creds)

            if not spreadsheets:
                print(f"  ⚠️  No spreadsheets found in entity folder '{entity_name}'")
                failed.append((entity_name, "No spreadsheets found"))
                continue

            if len(spreadsheets) > 1:
                print(f"  ⚠️  Multiple spreadsheets found in entity folder '{entity_name}', processing the first one")

            # Process the first spreadsheet in the entity folder
            spreadsheet_id, spreadsheet_name = spreadsheets[0]

            # Process the spreadsheet
            presentation_id = process_spreadsheet(
                spreadsheet_id=spreadsheet_id,
                spreadsheet_name=entity_name,
                template_id=template_id,
                output_folder_id=output_folder_id,
                entity_folder_id=entity_folder_id,
                creds=creds
            )

            if presentation_id:
                successful.append((entity_name, presentation_id))
            else:
                failed.append((entity_name, "Processing returned None"))

        except Exception as e:
            print(f"\n{'='*80}")
            print(f"✗ Error processing entity '{entity_name}': {e}")
            print(f"{'='*80}\n")
            failed.append((entity_name, str(e)))
            import traceback
            traceback.print_exc()
            # Continue processing other entities
            continue

    # Print summary
    print("\n" + "=" * 80)
    print("PROCESSING SUMMARY")
    print("=" * 80)
    print(f"Total entities processed: {len(entity_folders)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print()

    if successful:
        print("Successfully processed entities:")
        for entity_name, presentation_id in successful:
            print(f"  ✓ {entity_name}")
            print(f"    View at: https://docs.google.com/presentation/d/{presentation_id}/edit")
        print()

    if failed:
        print("Failed entities:")
        for entity_name, error in failed:
            print(f"  ✗ {entity_name}: {error}")
        print()

    print("=" * 80)

    return {'successful': successful, 'failed': failed}


def main():
    """
    Main function to process all spreadsheets in the input folder and generate presentations (CLI entry point).
    """
    parser = argparse.ArgumentParser(
        description='Generate Google Slides presentations from Google Sheets for entities with generate=Y in entities.csv'
    )
    parser.add_argument(
        '--shared-drive-url',
        required=True,
        help='Shared Drive root URL or ID that contains L1/L2 data, templates and entities file.',
    )
    parser.add_argument(
        '--service-account-credentials',
        default=None,
        help='Path to the service account JSON key file.',
    )
    args = parser.parse_args()

    print("Google Slide Automator - Report Generation")
    print("=" * 80)

    try:
        # Get credentials
        print("Authenticating...")
        creds = get_oauth_credentials(service_account_credentials=args.service_account_credentials)

        layout = resolve_layout(args.shared_drive_url, creds)

        # Call the main function
        generate_report(
            creds=creds,
            layout=layout
        )

    except ValueError as e:
        print(f"\nError: {e}")
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        if "credentials file" in str(e):
            print("\nTo set up service account credentials:")
            print("1. Go to Google Cloud Console (https://console.cloud.google.com/)")
            print("2. Create a new project or select an existing one")
            print("3. Enable Google Sheets API, Google Slides API, and Google Drive API")
            print("4. Go to 'Credentials' → 'Create Credentials' → 'Service account'")
            print("5. Create a service account and download the JSON key file")
            from .auth import PROJECT_ROOT as AUTH_PROJECT_ROOT
            print(f"6. Save the JSON key file as 'service-account-credentials.json' in: {AUTH_PROJECT_ROOT}")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
