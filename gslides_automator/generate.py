#!/usr/bin/env python3
"""
Unified script to generate L1-Merged, L2-Slides, and L3-PDF from entities.csv.
Processes each entity sequentially (L1 → L2 → L3) and stops on any error.
"""

from __future__ import annotations
import os
import sys
import argparse
from gslides_automator.drive_layout import (
    DriveLayout,
    EntityFlags,
    load_entities_with_flags,
    resolve_layout,
)
from gslides_automator.auth import get_oauth_credentials
from gslides_automator.l1_generate import (
    process_entity as l1_process_entity,
)
from gslides_automator.l2_generate import (
    process_spreadsheet as l2_process_spreadsheet,
    find_existing_presentation,
    list_spreadsheets_in_folder,
    list_entity_folders,
)
from gslides_automator.l3_generate import export_slide_to_pdf
# Add project root to path to import modules

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)


def generate_entity(entity_flags: EntityFlags, creds, layout: DriveLayout) -> None:
    """
    Generate L1, L2, and L3 for a single entity in sequence.
    Stops immediately on any error by raising an exception.

    Args:
        entity_flags: EntityFlags object containing entity name and L1/L2/L3 flags
        creds: Service account credentials
        layout: DriveLayout object containing configuration

    Raises:
        Exception: If any step fails
    """
    entity_name = entity_flags.entity_name

    # Check if there's any processing to do
    skip_processing = not (entity_flags.l1 or entity_flags.l2 is not None or entity_flags.l3)
    if skip_processing:
        print(f"No processing to do for entity: {entity_name}")
        return

    # Only print info if there's processing to do
    print(f"\nData levels to generate for {entity_name}:")
    print(f"  L1: {'Yes' if entity_flags.l1 else 'No'}")
    if entity_flags.l2 is None:
        l2_display = "No"
    elif entity_flags.l2 == set():  # Empty set means all slides
        l2_display = "All slides"
    else:
        l2_display = f"Slides {sorted(entity_flags.l2)}"
    print(f"  L2: {l2_display}")
    print(f"  L3: {'Yes' if entity_flags.l3 else 'No'}")

    presentation_id = None

    # Step 1: L1 Generation
    if entity_flags.l1:
        print(f"\n[L1] Generating L1-Merged for {entity_name}...\n")
        if not l1_process_entity(entity_name, creds, layout):
            raise Exception(f"L1 generation failed for entity '{entity_name}'")
        print(f"[L1] ✓ Successfully generated L1-Merged for {entity_name}\n")

    # Step 2: L2 Generation
    if entity_flags.l2 is not None:  # L2 is set (either all slides or specific slides)
        print(f"[L2] Generating L2-Slides for {entity_name}...")

        # Find the entity folder in L1-Merged
        entity_folders = list_entity_folders(layout.l1_merged_id, creds)
        entity_folder_id = None
        for folder_id, folder_name in entity_folders:
            if folder_name == entity_name:
                entity_folder_id = folder_id
                break

        if not entity_folder_id:
            raise Exception(f"Entity folder '{entity_name}' not found in L1-Merged")

        # Find spreadsheet in entity folder
        spreadsheets = list_spreadsheets_in_folder(entity_folder_id, creds)
        if not spreadsheets:
            raise Exception(f"No spreadsheet found in L1-Merged folder for entity '{entity_name}'")

        if len(spreadsheets) > 1:
            print("  ⚠️  Multiple spreadsheets found, using the first one")

        spreadsheet_id, spreadsheet_name = spreadsheets[0]

        # Convert empty set (all slides) to None for l2_process_spreadsheet
        slides_to_process = None if entity_flags.l2 == set() else entity_flags.l2

        # Process the spreadsheet to generate slides
        presentation_id = l2_process_spreadsheet(
            spreadsheet_id=spreadsheet_id,
            spreadsheet_name=entity_name,
            template_id=layout.report_template_id,
            output_folder_id=layout.l2_slide_id,
            entity_folder_id=entity_folder_id,
            creds=creds,
            slides=slides_to_process,
        )

        if not presentation_id:
            raise Exception(f"L2 generation failed for entity '{entity_name}'")

        print(f"[L2] ✓ Successfully generated L2-Slides for {entity_name}\n")

    # Step 3: L3 PDF Export
    if entity_flags.l3:
        print(f"[L3] Generating L3-PDF for {entity_name}...")

        # Find the presentation if not already known
        if not presentation_id:
            presentation_id = find_existing_presentation(entity_name, layout.l2_slide_id, creds)

        if not presentation_id:
            raise Exception(f"Presentation not found for entity '{entity_name}' in L2-Slides folder")

        # Export to PDF
        if not export_slide_to_pdf(presentation_id, entity_name, layout.l3_pdf_id, creds):
            raise Exception(f"L3 PDF export failed for entity '{entity_name}'")

        print(f"[L3] ✓ Successfully generated L3-PDF for {entity_name}\n")

    # Only print success message if there was processing
    print(f"✓ Successfully completed all steps for entity: {entity_name}\n")


def generate(creds=None, layout: DriveLayout = None):
    """
    Main generation function that processes all entities from entities.csv.
    Processes entities sequentially and stops on any error.

    Args:
        creds: Google OAuth credentials. If None, will be obtained automatically.
        layout: DriveLayout object containing configuration. Required.

    Returns:
        dict: Dictionary with 'successful' and 'failed' lists of entity names

    Raises:
        ValueError: If layout is not provided
        Exception: If any entity processing fails (stops immediately)
    """
    if layout is None:
        raise ValueError("layout (DriveLayout) is required. Pass it as a parameter.")

    if creds is None:
        creds = get_oauth_credentials()

    # Load entities from CSV
    if not layout.entities_csv_id:
        raise ValueError("No entities CSV ID found in layout.")

    print("Loading entities from entities.csv...", end=" ")
    entities = load_entities_with_flags(layout.entities_csv_id, creds)

    if not entities:
        print("⚠️  No entities found\n")
        return {'successful': [], 'failed': []}

    print(f"✓ Loaded {len(entities)} entities\n")

    # Process each entity sequentially
    successful = []
    failed = []

    for i, entity_flags in enumerate(entities, 1):
        entity_name = entity_flags.entity_name

        # Check if there's any processing to do
        has_processing = entity_flags.l1 or entity_flags.l2 is not None or entity_flags.l3

        # Only print and process if there's processing to do
        if has_processing:
            print(f"\n[{i}/{len(entities)}] Processing entity: {entity_name}")

        try:
            generate_entity(entity_flags, creds, layout)
            if has_processing:
                successful.append(entity_name)
        except Exception as e:
            error_msg = str(e)
            print(f"\n{'='*80}")
            print(f"✗ ERROR processing entity '{entity_name}': {error_msg}")
            print(f"{'='*80}\n")
            failed.append(entity_name)
            # Stop immediately on error as per requirements
            raise Exception(f"Stopped processing due to error in entity '{entity_name}': {error_msg}")

    # Print summary
    print(f"\n{'='*80}")
    print("PROCESSING SUMMARY ")
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
        description='Generate L1-Merged, L2-Slides, and L3-PDF for entities in entities.csv'
    )
    parser.add_argument(
        '--shared-drive-url',
        required=True,
        help='Shared Drive root URL or ID that contains L0/L1/L2/L3 data and templates.',
    )
    parser.add_argument(
        '--service-account-credentials',
        default=None,
        help='Path to the service account JSON key file.',
    )
    args = parser.parse_args()

    print("Google Slide Automator")
    print("=" * 80)

    try:
        # Get credentials
        print("Authenticating...")
        creds = get_oauth_credentials(service_account_credentials=args.service_account_credentials)

        layout = resolve_layout(args.shared_drive_url, creds)

        # Call the main function
        generate(
            creds=creds,
            layout=layout
        )

    except ValueError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        if "credentials file" in str(e):
            print("\nTo set up service account credentials:")
            print("1. Go to Google Cloud Console (https://console.cloud.google.com/)")
            print("2. Create a new project or select an existing one")
            print("3. Enable Google Sheets API, Google Slides API, and Google Drive API")
            print("4. Go to 'Credentials' → 'Create Credentials' → 'Service account'")
            print("5. Create a service account and download the JSON key file")
            from gslides_automator.auth import PROJECT_ROOT as AUTH_PROJECT_ROOT
            print(f"6. Save the JSON key file as 'service-account-credentials.json' in: {AUTH_PROJECT_ROOT}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
