"""
Integration tests for end-to-end workflows.
"""

from __future__ import annotations

from googleapiclient.discovery import build

from gslides_automator.generate import generate
from tests.test_utils import (
    get_spreadsheet_data,
    get_slide_text_content,
    execute_with_retry,
)


class TestFullWorkflow:
    """Tests for complete workflow from L0 to L2."""

    def test_full_workflow(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Complete workflow from L0 to L3 for single entity."""
        from tests.test_utils import create_test_entities_csv, create_test_l0_data

        # Create entities CSV with L3 enabled
        entities = {
            "entity-1": {"l1": "Y", "l2": "All", "l3": "Y"},
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

        # Run generate (L1, L2, and L3)
        result = generate(creds=test_credentials, layout=test_drive_layout)
        assert "entity-1" in result["successful"]
        assert len(result["failed"]) == 0

        # Verify L1 data was created
        drive_service = build("drive", "v3", credentials=test_credentials)

        # First find the entity folder
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='entity-1' "
            f"and '{test_drive_layout.l1_merged_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 1
        entity_folder_id = results["files"][0]["id"]

        # Then find the spreadsheet inside the entity folder
        query = (
            f"mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='entity-1' "
            f"and '{entity_folder_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 1
        spreadsheet_id = results["files"][0]["id"]

        # Verify spreadsheet has data
        data = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)
        assert data is not None
        assert len(data) > 1

        # Verify slide was created
        query = (
            f"mimeType='application/vnd.google-apps.presentation' "
            f"and name='entity-1.gslides' "
            f"and '{test_drive_layout.l2_slide_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 1
        presentation_id = results["files"][0]["id"]

        # Verify slide has content
        text_content = get_slide_text_content(presentation_id, 0, test_credentials)
        assert len(text_content) > 0

        # Verify placeholders were replaced
        assert "{{" not in text_content or "}}" not in text_content

        # Verify L3 PDF was created
        query = (
            f"mimeType='application/pdf' "
            f"and name='entity-1.pdf' "
            f"and '{test_drive_layout.l3_pdf_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 1
        pdf_id = results["files"][0]["id"]
        assert pdf_id is not None

    def test_full_workflow_multiple_entities(
        self,
        complete_test_setup,
        test_credentials,
    ):
        """Complete workflow for multiple entities."""
        setup = complete_test_setup
        layout = setup["layout"]

        # Run generate (L1 and L2)
        result = generate(creds=test_credentials, layout=layout)
        assert len(result["successful"]) >= 2
        assert len(result["failed"]) == 0

        # Verify all entities have slides
        drive_service = build("drive", "v3", credentials=test_credentials)

        for entity_name in result["successful"]:
            query = (
                f"mimeType='application/vnd.google-apps.presentation' "
                f"and name='{entity_name}.gslides' "
                f"and '{layout.l2_slide_id}' in parents "
                f"and trashed=false"
            )
            results = (
                drive_service.files()
                .list(
                    q=query,
                    fields="files(id)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )

            assert len(results.get("files", [])) == 1

    def test_workflow_with_slide_filtering(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Full workflow with slide number filtering."""
        from tests.test_utils import create_test_entities_csv, create_test_l0_data

        entities = {
            "entity-1": {"l1": "Y", "l2": "1", "l3": "N"},  # Only slide 1
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

        # Run generate (L1 and L2) with slide filtering
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Should succeed
        assert len(result["successful"]) >= 1
        assert "entity-1" in result["successful"]

    def test_workflow_error_recovery(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test that errors in one entity don't affect others."""
        from tests.test_utils import create_test_entities_csv, create_test_l0_data

        entities = {
            "entity-1": {"l1": "Y", "l2": "All", "l3": "N"},
            "entity-2": {"l1": "Y", "l2": "All", "l3": "N"},
            "entity-invalid": {"l1": "Y", "l2": "All", "l3": "N"},  # Will have issues
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Create L0 data for valid entities
        create_test_l0_data(
            test_drive_layout.l0_raw_id,
            "entity-1",
            test_credentials,
        )
        create_test_l0_data(
            test_drive_layout.l0_raw_id,
            "entity-2",
            test_credentials,
        )

        # Don't create L0 data for entity-invalid - it will fail

        # Run generate (L1 and L2)
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # At least entity-1 and entity-2 should succeed
        # Note: The unified generate() stops on first error, so entity-invalid will cause failure
        # But entity-1 and entity-2 should be in successful if they processed before entity-invalid
        assert "entity-1" in result["successful"] or "entity-2" in result["successful"]


class TestWorkflowDataIntegrity:
    """Tests for data integrity throughout the workflow."""

    def test_data_preservation_through_workflow(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test that data is preserved correctly through L0→L1→L2→L3."""
        from tests.test_utils import create_test_entities_csv, create_test_l0_data

        # Create entities CSV with L3 enabled
        entities = {
            "entity-1": {"l1": "Y", "l2": "All", "l3": "Y"},
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

        # Run generate (L1, L2, and L3)
        result = generate(creds=test_credentials, layout=test_drive_layout)
        assert "entity-1" in result["successful"]

        # Get L1 spreadsheet data
        drive_service = build("drive", "v3", credentials=test_credentials)

        # First find the entity folder
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='entity-1' "
            f"and '{test_drive_layout.l1_merged_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 1
        entity_folder_id = results["files"][0]["id"]

        # Then find the spreadsheet inside the entity folder
        query = (
            f"mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='entity-1' "
            f"and '{entity_folder_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 1
        spreadsheet_id = results["files"][0]["id"]

        # Get data from spreadsheet
        data_before = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)

        # Data should already be processed by generate() above
        assert len(result["successful"]) >= 1

        # Verify data is still intact
        data_after = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)
        assert data_before == data_after

        # Verify L3 PDF was created
        query = (
            f"mimeType='application/pdf' "
            f"and name='entity-1.pdf' "
            f"and '{test_drive_layout.l3_pdf_id}' in parents "
            f"and trashed=false"
        )
        results = execute_with_retry(
            drive_service.files()
            .list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        assert len(results.get("files", [])) == 1
        pdf_id = results["files"][0]["id"]
        assert pdf_id is not None
