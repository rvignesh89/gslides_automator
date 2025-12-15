#!/usr/bin/env python3
from __future__ import annotations
"""
Script to generate L1-Data from L0-Data for entities in Tamil Nadu.
Reads CSV files and images from L0-Data folder, clones entity data templates,
populates Google Sheets tabs with CSV data, and copies images to L1-Data folder.
"""

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import gspread
import os
import sys
import time
import csv
import io
import argparse
import re

# Add project root to path to import auth module
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

from gslides_automator.drive_layout import load_entities, resolve_layout, DriveLayout
from gslides_automator.auth import get_oauth_credentials

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
            # For non-HttpError exceptions, check if it's a gspread rate limit error
            error_str = str(e).lower()
            if '429' in error_str or 'rate limit' in error_str or 'quota' in error_str:
                if attempt < max_retries:
                    wait_time = min(delay, max_delay)
                    print(f"    ⚠️  Rate limit error. Retrying in {wait_time:.1f} seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    delay *= backoff_factor
                else:
                    print(f"    ✗ Rate limit error. Max retries ({max_retries}) reached.")
                    raise
            else:
                # For non-retryable errors, re-raise immediately
                raise


def read_entities_from_csv(csv_path):
    """
    Read entity names from a CSV file.
    The CSV should have entity names in the first column.

    Args:
        csv_path: Path to the CSV file

    Returns:
        list: List of entity names (strings), or empty list if error
    """
    try:
        entities = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row:  # Check if row is not empty
                    entity_name = row[0].strip()
                    if entity_name:  # Only add non-empty names
                        entities.append(entity_name)

        # Remove header if it exists
        if entities and entities[0].lower() in ['entity', 'entities', 'name', 'names']:
            entities = entities[1:]

        return entities
    except Exception as e:
        print(f"Error reading entities from CSV file: {e}")
        import traceback
        traceback.print_exc()
        return []

def find_existing_file(drive_service, file_name, folder_id):
    """
    Check if a file with the given name exists in the specified folder.

    Args:
        drive_service: Google Drive API service instance
        file_name: Name of the file to search for
        folder_id: ID of the folder to search in

    Returns:
        str: File ID if found, None otherwise
    """
    def _find():
        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        if files:
            return files[0]['id']
        return None

    try:
        return retry_with_exponential_backoff(_find)
    except HttpError as error:
        print(f"Error searching for existing file '{file_name}': {error}")
        return None


def delete_file(drive_service, file_id):
    """
    Delete a file from Google Drive.

    Args:
        drive_service: Google Drive API service instance
        file_id: ID of the file to delete

    Returns:
        bool: True if successful, False otherwise
    """
    # First, check if the file exists and is accessible
    try:
        file_metadata = drive_service.files().get(
            fileId=file_id,
            fields='id, name',
            supportsAllDrives=True
        ).execute()
        file_name = file_metadata.get('name', 'Unknown')
    except HttpError as check_error:
        if check_error.resp.status == 404:
            # File not found - might not be accessible to service account
            try:
                from .auth import get_service_account_email
                service_account_email = get_service_account_email()
                print(f"  ⚠️  File not found or not accessible to service account.")
                print(f"      Service account email: {service_account_email}")
                print(f"      Please ensure the file is shared with this service account with 'Editor' permissions.")
            except Exception:
                print(f"  ⚠️  File not found or not accessible to service account.")
                print(f"      Please ensure the file is shared with your service account with 'Editor' permissions.")
            return False
        else:
            print(f"  ⚠️  Error checking file access: {check_error}")
            return False

    def _delete():
        drive_service.files().delete(
            fileId=file_id,
            supportsAllDrives=True
        ).execute()
        return True

    try:
        return retry_with_exponential_backoff(_delete)
    except HttpError as error:
        if error.resp.status == 404:
            try:
                from .auth import get_service_account_email
                service_account_email = get_service_account_email()
                print(f"  ⚠️  Error deleting file '{file_name}': File not found or not accessible.")
                print(f"      Service account email: {service_account_email}")
                print(f"      Please ensure the file is shared with this service account with 'Editor' permissions.")
            except Exception:
                print(f"  ⚠️  Error deleting file '{file_name}': File not found or not accessible.")
                print(f"      Please ensure the file is shared with your service account with 'Editor' permissions.")
        elif error.resp.status == 403:
            try:
                from .auth import get_service_account_email
                service_account_email = get_service_account_email()
                print(f"  ⚠️  Error deleting file '{file_name}': Permission denied.")
                print(f"      Service account email: {service_account_email}")
                print(f"      Please ensure the file is shared with this service account with 'Editor' permissions.")
            except Exception:
                print(f"  ⚠️  Error deleting file '{file_name}': Permission denied.")
                print(f"      Please ensure the file is shared with your service account with 'Editor' permissions.")
        else:
            print(f"  ⚠️  Error deleting file '{file_name}': {error}")
        return False


def find_or_create_entity_folder(drive_service, entity_name, parent_folder_id):
    """
    Find entity subfolder in parent folder, create if doesn't exist.

    Args:
        drive_service: Google Drive API service instance
        entity_name: Name of the entity (folder name)
        parent_folder_id: ID of the parent folder

    Returns:
        str: Folder ID, or None if failed
    """
    def _find_folder():
        query = f"mimeType='application/vnd.google-apps.folder' and name='{entity_name}' and '{parent_folder_id}' in parents and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        if files:
            return files[0]['id']
        return None

    try:
        # Try to find existing folder
        folder_id = retry_with_exponential_backoff(_find_folder)
        if folder_id:
            return folder_id

        # Create new folder if not found
        def _create_folder():
            file_metadata = {
                'name': entity_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = drive_service.files().create(
                body=file_metadata,
                fields='id',
                supportsAllDrives=True
            ).execute()
            return folder.get('id')

        folder_id = retry_with_exponential_backoff(_create_folder)
        return folder_id
    except HttpError as error:
        print(f"Error finding/creating entity folder '{entity_name}': {error}")
        return None


def clone_template_to_entity(drive_service, template_id, entity_name, folder_id):
    """
    Clone template spreadsheet to entity folder, deleting existing if present.

    Args:
        drive_service: Google Drive API service instance
        template_id: ID of the template spreadsheet
        entity_name: Name of the entity (file name)
        folder_id: ID of the folder to place the file in

    Returns:
        str: ID of the copied file, or None if failed
    """
    file_name = f"{entity_name}"

    # Check if file already exists
    existing_file_id = find_existing_file(drive_service, file_name, folder_id)
    if existing_file_id:
        print(f"  Found existing spreadsheet, deleting...")
        if delete_file(drive_service, existing_file_id):
            print(f"  ✓ Deleted existing spreadsheet")
        else:
            print(f"  ✗ Failed to delete existing spreadsheet")
            return None

    def _copy_template():
        # Copy the template
        copied_file = drive_service.files().copy(
            fileId=template_id,
            body={'name': file_name},
            supportsAllDrives=True
        ).execute()

        new_file_id = copied_file.get('id')

        # Move to target folder
        file_metadata = drive_service.files().get(
            fileId=new_file_id,
            fields='parents',
            supportsAllDrives=True
        ).execute()
        previous_parents = ",".join(file_metadata.get('parents', []))

        # Move the file to the target folder
        if previous_parents:
            drive_service.files().update(
                fileId=new_file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents',
                supportsAllDrives=True
            ).execute()
        else:
            drive_service.files().update(
                fileId=new_file_id,
                addParents=folder_id,
                fields='id, parents',
                supportsAllDrives=True
            ).execute()

        return new_file_id

    try:
        new_file_id = retry_with_exponential_backoff(_copy_template)
        return new_file_id
    except HttpError as error:
        if error.resp.status == 404:
            print(f"Error: Template file not found (404). The file may have been deleted or you don't have access.")
        elif error.resp.status == 403:
            print(f"Error: Permission denied (403). You may not have permission to copy this file.")
        else:
            print(f"Error copying template: {error}")
        return None


def list_csv_files_in_folder(drive_service, folder_id):
    """
    List all CSV files in a Google Drive folder.

    Args:
        drive_service: Google Drive API service instance
        folder_id: ID of the folder to search

    Returns:
        list: List of tuples (file_id, file_name)
    """
    def _list_files():
        query = f"mimeType='text/csv' and '{folder_id}' in parents and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields='files(id, name)',
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        return [(f['id'], f['name']) for f in files]

    try:
        return retry_with_exponential_backoff(_list_files)
    except HttpError as error:
        print(f"Error listing CSV files in folder: {error}")
        return []


def download_csv_from_drive(drive_service, file_id):
    """
    Download CSV file content from Google Drive.

    Args:
        drive_service: Google Drive API service instance
        file_id: ID of the CSV file

    Returns:
        list: List of rows (each row is a list of values), or None if failed
    """
    def _download():
        request = drive_service.files().get_media(fileId=file_id)
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        file_content.seek(0)
        # Decode and parse CSV
        content_str = file_content.read().decode('utf-8')
        # Use csv.reader with proper settings to preserve data integrity
        csv_reader = csv.reader(io.StringIO(content_str), quoting=csv.QUOTE_MINIMAL)
        rows = list(csv_reader)
        # Ensure all rows have consistent structure (pad with empty strings if needed)
        if rows:
            max_cols = max(len(row) for row in rows)
            # Pad rows to have the same number of columns
            normalized_rows = []
            for row in rows:
                padded_row = row + [''] * (max_cols - len(row))
                normalized_rows.append(padded_row)
            return normalized_rows
        return rows

    try:
        return retry_with_exponential_backoff(_download)
    except HttpError as error:
        print(f"Error downloading CSV file: {error}")
        return None


def parse_csv_filename(filename):
    """
    Parse CSV filename to extract tab name.
    Example: s25-chart:pass_percentage.csv -> s25-chart:pass_percentage

    Args:
        filename: CSV filename

    Returns:
        str: Tab name (without .csv extension)
    """
    # Remove .csv extension
    if filename.endswith('.csv'):
        return filename[:-4]
    return filename


def find_existing_spreadsheet(drive_service, entity_name, folder_id):
    """
    Find existing spreadsheet in L1 folder (don't create new one).

    Args:
        drive_service: Google Drive API service instance
        entity_name: Name of the entity (file name)
        folder_id: ID of the folder to search in

    Returns:
        str: Spreadsheet ID if found, None otherwise
    """
    file_name = f"{entity_name}"
    return find_existing_file(drive_service, file_name, folder_id)




def _column_number_to_letter(n):
    """
    Convert a column number (1-based) to Excel column letter (A, B, ..., Z, AA, AB, ...).

    Args:
        n: Column number (1-based)

    Returns:
        str: Column letter(s)
    """
    result = ""
    while n > 0:
        n -= 1
        result = chr(65 + (n % 26)) + result
        n //= 26
    return result


def _convert_value_to_proper_type(value):
    """
    Convert a CSV string value to its proper type (number, boolean, or string).
    This prevents Google Sheets from adding apostrophes.

    Args:
        value: String value from CSV

    Returns:
        Value converted to appropriate type (int, float, bool, or str)
    """
    if value is None or value == '':
        return ''

    value_str = str(value).strip()

    # Try to convert to number
    try:
        # Try integer first
        if value_str.isdigit() or (value_str.startswith('-') and value_str[1:].isdigit()):
            return int(value_str)
        # Try float
        return float(value_str)
    except ValueError:
        pass

    # Try boolean
    if value_str.lower() in ('true', 'false'):
        return value_str.lower() == 'true'

    # Return as string
    return value_str


def write_csv_to_sheet_tab(gspread_client, spreadsheet_id, tab_name, csv_data, creds):
    """
    Write CSV data to specified tab starting from A1.
    Does not clear existing data - new data will overwrite starting from A1.

    Args:
        gspread_client: Authorized gspread client
        spreadsheet_id: ID of the spreadsheet
        tab_name: Name of the tab/worksheet
        csv_data: List of rows (each row is a list of values)
        creds: Service account credentials

    Returns:
        bool: True if successful, False otherwise
    """
    def _write_data():
        # Use Sheets API directly for better control over data types
        sheets_service = build('sheets', 'v4', credentials=creds)

        # Get the worksheet ID
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(tab_name)
            sheet_id = worksheet.id
        except gspread.exceptions.WorksheetNotFound:
            print(f"    ⚠️  Tab '{tab_name}' not found in spreadsheet")
            return False

        if not csv_data:
            print(f"    ⚠️  No data to write for tab '{tab_name}'")
            return False

        # Convert CSV data to proper types and format for Sheets API
        values = []
        for row in csv_data:
            formatted_row = []
            for cell in row:
                converted_value = _convert_value_to_proper_type(cell)
                formatted_row.append(converted_value)
            values.append(formatted_row)

        # Use batchUpdate to write data with proper types
        range_name = f"{tab_name}!A1"
        body = {
            'values': values
        }

        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',  # RAW preserves exact values without interpretation
            body=body
        ).execute()

        return True

    try:
        return retry_with_exponential_backoff(_write_data)
    except Exception as e:
        print(f"    ✗ Error writing data to tab '{tab_name}': {e}")
        return False


def list_image_files_in_folder(drive_service, folder_id):
    """
    List all image files in a Google Drive folder.

    Args:
        drive_service: Google Drive API service instance
        folder_id: ID of the folder to search

    Returns:
        list: List of tuples (file_id, file_name)
    """
    image_mime_types = [
        'image/png',
        'image/jpeg',
        'image/jpg',
        'image/gif',
        'image/bmp',
        'image/webp',
        'image/svg+xml'
    ]

    mime_query = " or ".join([f"mimeType='{mime}'" for mime in image_mime_types])

    def _list_files():
        query = f"'{folder_id}' in parents and trashed=false and ({mime_query})"
        results = drive_service.files().list(
            q=query,
            fields='files(id, name)',
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        return [(f['id'], f['name']) for f in files]

    try:
        return retry_with_exponential_backoff(_list_files)
    except HttpError as error:
        print(f"Error listing image files in folder: {error}")
        return []


def copy_image_to_folder(drive_service, source_file_id, destination_folder_id, file_name):
    """
    Copy image file from source to destination folder, deleting existing if present.

    Args:
        drive_service: Google Drive API service instance
        source_file_id: ID of the source image file
        destination_folder_id: ID of the destination folder
        file_name: Name for the copied file

    Returns:
        str: ID of the copied file, or None if failed
    """
    # Check if file already exists
    existing_file_id = find_existing_file(drive_service, file_name, destination_folder_id)
    if existing_file_id:
        print(f"    Found existing image '{file_name}', deleting...")
        if delete_file(drive_service, existing_file_id):
            print(f"    ✓ Deleted existing image")
        else:
            print(f"    ✗ Failed to delete existing image")
            return None

    def _copy_file():
        # Copy the file
        copied_file = drive_service.files().copy(
            fileId=source_file_id,
            body={'name': file_name},
            supportsAllDrives=True
        ).execute()

        new_file_id = copied_file.get('id')

        # Move to target folder
        file_metadata = drive_service.files().get(
            fileId=new_file_id,
            fields='parents',
            supportsAllDrives=True
        ).execute()
        previous_parents = ",".join(file_metadata.get('parents', []))

        # Move the file to the target folder
        if previous_parents:
            drive_service.files().update(
                fileId=new_file_id,
                addParents=destination_folder_id,
                removeParents=previous_parents,
                fields='id, parents',
                supportsAllDrives=True
            ).execute()
        else:
            drive_service.files().update(
                fileId=new_file_id,
                addParents=destination_folder_id,
                fields='id, parents',
                supportsAllDrives=True
            ).execute()

        return new_file_id

    try:
        new_file_id = retry_with_exponential_backoff(_copy_file)
        return new_file_id
    except HttpError as error:
        print(f"    ✗ Error copying image '{file_name}': {error}")
        return None


def process_entity(entity_name, creds, layout: DriveLayout):
    """
    Main processing function for a single entity.

    Args:
        entity_name: Name of the entity
        creds: Service account credentials
        layout: DriveLayout object containing configuration

    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n{'='*80}")
    print(f"Processing entity: {entity_name}")
    print(f"{'='*80}\n")

    drive_service = build('drive', 'v3', credentials=creds)
    gspread_client = gspread.authorize(creds)

    l1_root_id = layout.l1_data_id
    l0_root_id = layout.l0_data_id
    template_id = layout.data_template_id

    try:
        # 1. Find/create L1-Data entity folder
        print(f"Finding/creating L1-Data folder for {entity_name}...")
        l1_folder_id = find_or_create_entity_folder(drive_service, entity_name, l1_root_id)
        if not l1_folder_id:
            print(f"✗ Failed to find/create L1-Data folder for {entity_name}")
            return False
        print(f"✓ L1-Data folder ID: {l1_folder_id}")

        # 2. Find L0-Data entity folder
        print(f"Finding L0-Data folder for {entity_name}...")
        l0_folder_id = find_or_create_entity_folder(drive_service, entity_name, l0_root_id)
        if not l0_folder_id:
            print(f"✗ Failed to find L0-Data folder for {entity_name}")
            return False
        print(f"✓ L0-Data folder ID: {l0_folder_id}")

        # 3. Handle spreadsheet creation/update: always clone template fresh
        print(f"Cloning template spreadsheet for {entity_name}...")
        spreadsheet_id = clone_template_to_entity(drive_service, template_id, entity_name, l1_folder_id)
        if not spreadsheet_id:
            print(f"✗ Failed to clone template spreadsheet for {entity_name}")
            return False
        print(f"✓ Cloned spreadsheet ID: {spreadsheet_id}")

        # 4. Process CSV files and write to matching tabs
        print(f"Processing CSV files from L0-Data...")
        csv_files = list_csv_files_in_folder(drive_service, l0_folder_id)
        if not csv_files:
            print(f"  ⚠️  No CSV files found in L0-Data folder for {entity_name}")
        else:
            print(f"  Found {len(csv_files)} CSV file(s)")

            if csv_files:
                csv_success = 0
                csv_failed = 0

                for file_id, file_name in csv_files:
                    print(f"  Processing: {file_name}")
                    tab_name = parse_csv_filename(file_name)

                    # Download CSV
                    csv_data = download_csv_from_drive(drive_service, file_id)
                    if not csv_data:
                        print(f"    ✗ Failed to download CSV file")
                        csv_failed += 1
                        continue

                    # Write to sheet tab
                    if write_csv_to_sheet_tab(gspread_client, spreadsheet_id, tab_name, csv_data, creds):
                        print(f"    ✓ Wrote data to tab '{tab_name}'")
                        csv_success += 1
                    else:
                        print(f"    ✗ Failed to write data to tab '{tab_name}'")
                        csv_failed += 1

                print(f"  CSV processing summary: {csv_success} succeeded, {csv_failed} failed")

        # 5. Copy image files (delete existing if present)
        print(f"Copying image files from L0-Data to L1-Data...")
        image_files = list_image_files_in_folder(drive_service, l0_folder_id)
        if not image_files:
            print(f"  ⚠️  No image files found in L0-Data folder for {entity_name}")
        else:
            print(f"  Found {len(image_files)} image file(s)")

            if image_files:
                image_success = 0
                image_failed = 0

                for file_id, file_name in image_files:
                    print(f"  Copying: {file_name}")
                    new_file_id = copy_image_to_folder(drive_service, file_id, l1_folder_id, file_name)
                    if new_file_id:
                        print(f"    ✓ Copied image '{file_name}'")
                        image_success += 1
                    else:
                        print(f"    ✗ Failed to copy image '{file_name}'")
                        image_failed += 1

                print(f"  Image copying summary: {image_success} succeeded, {image_failed} failed")

        print(f"\n✓ Successfully processed entity: {entity_name}")
        return True

    except Exception as e:
        print(f"\n✗ Error processing entity '{entity_name}': {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_data(entities=None, creds=None, layout: DriveLayout = None):
    """
    Generate L1-Data from L0-Data for specified entities.

    Args:
        entities: List of entity names to process, or None to process all entities from CSV
        creds: Google OAuth credentials. If None, will be obtained automatically.
        layout: DriveLayout object containing configuration. Required.

    Returns:
        dict: Dictionary with 'successful' and 'failed' lists of entity names

    Raises:
        FileNotFoundError: If service account credentials are not found
        ValueError: If layout is not provided
        Exception: Other errors during processing
    """
    if layout is None:
        raise ValueError("layout (DriveLayout) is required. Pass it as a parameter.")

    if creds is None:
        creds = get_oauth_credentials()

    # Parse entity names
    if entities is None:
        if layout.entities_csv_id:
            entities = load_entities(layout.entities_csv_id, creds)
            print(f"✓ Loaded {len(entities)} entities from entities.csv")
        else:
            print("\n✗ No entities CSV ID found in layout and no entities provided.")
            return {'successful': [], 'failed': []}
    else:
        if isinstance(entities, str):
            entities = [d.strip() for d in entities.split(',') if d.strip()]
        if not entities:
            print("\n✗ No valid entity names provided.")
            return {'successful': [], 'failed': []}
        print(f"✓ Using provided entity names: {', '.join(entities)}")

    print(f"\n✓ Processing {len(entities)} entities")
    print(f"  Entities: {', '.join(entities)}\n")

    # Process each entity
    successful = []
    failed = []

    for i, entity in enumerate(entities, 1):
        print(f"\n[{i}/{len(entities)}] Processing entity: {entity}")
        if process_entity(entity, creds, layout):
            successful.append(entity)
        else:
            failed.append(entity)

        # Small delay to avoid rate limits
        if i < len(entities):
            time.sleep(0.5)

    # Print summary
    print(f"\n{'='*80}")
    print("PROCESSING SUMMARY")
    print(f"{'='*80}")
    print(f"Total entities: {len(entities)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print()

    if successful:
        print("Successfully processed entities:")
        for entity in successful:
            print(f"  ✓ {entity}")
        print()

    if failed:
        print("Failed entities:")
        for entity in failed:
            print(f"  ✗ {entity}")
        print()

    print("=" * 80)

    return {'successful': successful, 'failed': failed}


def main():
    """
    Main function to process entities (CLI entry point).
    """
    parser = argparse.ArgumentParser(
        description='Generate L1-Data from L0-Data for specified entities'
    )
    parser.add_argument(
        '--shared-drive-url',
        required=True,
        help='Shared Drive root URL or ID that contains L0/L1 data and templates.',
    )
    parser.add_argument(
        '--service-account-file',
        default=None,
        help='Path to the service account JSON key file.',
    )
    parser.add_argument(
        '--entities',
        type=str,
        help='Comma-separated list of entity names to process (e.g., "Hyundai,Volvo"). If not provided, all entities from the Google Sheet will be processed.'
    )
    args = parser.parse_args()

    print("Google Slide Automator")
    print("=" * 80)

    try:
        # Get credentials
        print("Authenticating...")
        creds = get_oauth_credentials(service_account_file=args.service_account_file)

        layout = resolve_layout(args.shared_drive_url, creds)

        # Call the main function
        generate_data(
            entities=args.entities,
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
            print("3. Enable Google Sheets API and Google Drive API")
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

