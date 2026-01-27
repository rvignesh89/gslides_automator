"""
Tests for l1_generate module.
"""

from __future__ import annotations

import pytest
from googleapiclient.discovery import build

from gslides_automator.generate import generate
from tests.test_utils import (
    create_test_l0_data,
    get_spreadsheet_data,
    execute_with_retry,
)


class TestL1GenerateSingleEntity:
    """Tests for L1 generation with a single entity."""

    def test_l1_generate_single_entity(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test L1 generation for a single entity."""
        # Create entities CSV
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "", "l3": "N"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Create L0 data
        create_test_l0_data(
            test_drive_layout.l0_raw_id,
            "entity-1",
            test_credentials,
        )

        # Run generate (L1 only)
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Verify results
        assert result["successful"] == ["entity-1"]
        assert len(result["failed"]) == 0

        # Verify L1 folder structure
        drive_service = build("drive", "v3", credentials=test_credentials)

        # Check that entity folder was created in L1
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='entity-1' "
            f"and '{test_drive_layout.l1_merged_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        folders = results.get("files", [])
        assert len(folders) == 1

        entity_folder_id = folders[0]["id"]

        # Check that spreadsheet was created
        query = (
            f"mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='entity-1' "
            f"and '{entity_folder_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        spreadsheets = results.get("files", [])
        assert len(spreadsheets) == 1

        # Verify spreadsheet data
        spreadsheet_id = spreadsheets[0]["id"]
        data = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)
        assert data is not None
        assert len(data) > 1  # Header + data rows

        # Verify image was copied
        query = (
            f"name='picture-distribution.png' "
            f"and '{entity_folder_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        images = results.get("files", [])
        assert len(images) == 1


class TestL1GenerateMultipleEntities:
    """Tests for L1 generation with multiple entities."""

    def test_l1_generate_multiple_entities(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test L1 generation for multiple entities."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "", "l3": "N"},
            "entity-2": {"l1": "Y", "l2": "", "l3": "N"},
            "entity-3": {"l1": "N", "l2": "", "l3": "N"},  # Should be skipped
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Create L0 data for all entities
        for entity_name in ["entity-1", "entity-2", "entity-3"]:
            create_test_l0_data(
                test_drive_layout.l0_raw_id,
                entity_name,
                test_credentials,
            )

        # Run generate (L1 only)
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Verify results
        assert len(result["successful"]) == 2
        assert "entity-1" in result["successful"]
        assert "entity-2" in result["successful"]
        assert "entity-3" not in result["successful"]
        assert len(result["failed"]) == 0


class TestL1CSVProcessing:
    """Tests for CSV processing in L1 generation."""

    def test_l1_csv_processing(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Verify CSV data is correctly written to spreadsheet tabs."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "", "l3": "N"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        create_test_l0_data(
            test_drive_layout.l0_raw_id,
            "entity-1",
            test_credentials,
        )

        result = generate(creds=test_credentials, layout=test_drive_layout)
        assert result["successful"] == ["entity-1"]

        # Find the created spreadsheet
        drive_service = build("drive", "v3", credentials=test_credentials)

        # First find the entity folder in L1-Merged
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='entity-1' "
            f"and '{test_drive_layout.l1_merged_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        folders = results.get("files", [])
        assert len(folders) == 1
        entity_folder_id = folders[0]["id"]

        # Then find the spreadsheet inside the entity folder
        query = (
            f"mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='entity-1' "
            f"and '{entity_folder_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        spreadsheet_id = results["files"][0]["id"]

        # Verify data sheet
        data = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)
        assert data is not None
        assert len(data) >= 2  # At least header + one data row

        # Verify chart sheet
        chart_data = get_spreadsheet_data(
            spreadsheet_id, "chart-sales", test_credentials
        )
        assert chart_data is not None
        assert len(chart_data) >= 2

        # Verify table sheet
        table_data = get_spreadsheet_data(
            spreadsheet_id, "table-performance", test_credentials
        )
        assert table_data is not None
        assert len(table_data) >= 2


class TestL1ImageCopying:
    """Tests for image copying in L1 generation."""

    def test_l1_image_copying(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Verify images are copied from L0 to L1."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "", "l3": "N"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        create_test_l0_data(
            test_drive_layout.l0_raw_id,
            "entity-1",
            test_credentials,
        )

        result = generate(creds=test_credentials, layout=test_drive_layout)
        assert result["successful"] == ["entity-1"]

        # Verify image exists in L1
        drive_service = build("drive", "v3", credentials=test_credentials)

        # Find entity folder in L1
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='entity-1' "
            f"and '{test_drive_layout.l1_merged_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        entity_folder_id = results["files"][0]["id"]

        # Check for image
        query = (
            f"name='picture-distribution.png' "
            f"and '{entity_folder_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        images = results.get("files", [])
        assert len(images) == 1
        assert images[0]["name"] == "picture-distribution.png"


class TestL1EntityFiltering:
    """Tests for entity filtering in L1 generation."""

    def test_l1_entity_filtering(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test that only entities with generate=Y are processed."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "", "l3": "N"},
            "entity-2": {"l1": "N", "l2": "", "l3": "N"},
            "entity-3": {"l1": "Y", "l2": "", "l3": "N"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Create L0 data for all entities
        for entity_name in ["entity-1", "entity-2", "entity-3"]:
            create_test_l0_data(
                test_drive_layout.l0_raw_id,
                entity_name,
                test_credentials,
            )

        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Only entity-1 and entity-3 should be processed
        assert len(result["successful"]) == 2
        assert "entity-1" in result["successful"]
        assert "entity-3" in result["successful"]
        assert "entity-2" not in result["successful"]

        # Verify entity-2 folder doesn't exist in L1
        drive_service = build("drive", "v3", credentials=test_credentials)
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='entity-2' "
            f"and '{test_drive_layout.l1_merged_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files().list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 0


class TestL1ErrorCases:
    """Tests for error cases in L1 generation."""

    def test_l1_missing_csv(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test error handling when CSV files are missing."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "", "l3": "N"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Create entity folder but don't add CSV files
        drive_service = build("drive", "v3", credentials=test_credentials)
        file_metadata = {
            "name": "entity-1",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [test_drive_layout.l0_raw_id],
        }
        execute_with_retry(
            drive_service.files().create(
                body=file_metadata,
                fields="id",
                supportsAllDrives=True,
            )
        )

        # Run L1 generate - should succeed but with warning
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Entity should still be processed (spreadsheet created, just no CSV data)
        assert "entity-1" in result["successful"]

    def test_l1_missing_template(
        self,
        test_drive_layout,
        test_credentials,
    ):
        """Test error handling when template is missing."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "", "l3": "N"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Set invalid template ID
        test_drive_layout.data_template_id = "invalid-template-id-12345"

        create_test_l0_data(
            test_drive_layout.l0_raw_id,
            "entity-1",
            test_credentials,
        )

        # Should fail - generate raises an exception, so we catch it
        with pytest.raises(Exception):
           generate(creds=test_credentials, layout=test_drive_layout)

    def test_l1_special_characters(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test handling of entity names with special characters."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-with-special-chars-!@#": {"l1": "Y", "l2": "", "l3": "N"},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        create_test_l0_data(
            test_drive_layout.l0_raw_id,
            "entity-with-special-chars-!@#",
            test_credentials,
        )

        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Should handle special characters (may succeed or fail depending on Drive restrictions)
        # At minimum, should not crash
        assert isinstance(result, dict)
        assert "successful" in result
        assert "failed" in result
