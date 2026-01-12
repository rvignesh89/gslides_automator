"""
Integration tests for end-to-end workflows.
"""

from __future__ import annotations

import pytest
from googleapiclient.discovery import build

from gslides_automator.l1_generate import l1_generate
from gslides_automator.l2_generate import l2_generate
from tests.test_utils import (
    get_spreadsheet_data,
    get_slide_text_content,
    verify_drive_structure,
)


class TestFullWorkflowL0ToL2:
    """Tests for complete workflow from L0 to L2."""

    def test_full_workflow_l0_to_l2_single_entity(
        self,
        complete_test_setup,
        test_credentials,
    ):
        """Complete workflow from L0 to L2 for single entity."""
        setup = complete_test_setup
        layout = setup["layout"]

        # Step 1: Run L1 generation
        l1_result = l1_generate(creds=test_credentials, layout=layout)
        assert "entity-1" in l1_result["successful"]
        assert len(l1_result["failed"]) == 0

        # Verify L1 data was created
        drive_service = build("drive", "v3", credentials=test_credentials)
        query = (
            f"mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='entity-1' "
            f"and '{layout.l1_data_id}' in parents "
            f"and trashed=false"
        )
        results = drive_service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        assert len(results.get("files", [])) == 1
        spreadsheet_id = results["files"][0]["id"]

        # Verify spreadsheet has data
        data = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)
        assert data is not None
        assert len(data) > 1

        # Step 2: Run L2 generation
        l2_result = l2_generate(creds=test_credentials, layout=layout)
        assert len(l2_result["successful"]) >= 1

        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" in successful_entities

        # Verify slide was created
        query = (
            f"mimeType='application/vnd.google-apps.presentation' "
            f"and name='entity-1.gslides' "
            f"and '{layout.l2_report_id}' in parents "
            f"and trashed=false"
        )
        results = drive_service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        assert len(results.get("files", [])) == 1
        presentation_id = results["files"][0]["id"]

        # Verify slide has content
        text_content = get_slide_text_content(presentation_id, 0, test_credentials)
        assert len(text_content) > 0

        # Verify placeholders were replaced
        assert "{{" not in text_content or "}}" not in text_content

    def test_full_workflow_multiple_entities(
        self,
        complete_test_setup,
        test_credentials,
    ):
        """Complete workflow for multiple entities."""
        setup = complete_test_setup
        layout = setup["layout"]

        # Run L1
        l1_result = l1_generate(creds=test_credentials, layout=layout)
        assert len(l1_result["successful"]) >= 2
        assert len(l1_result["failed"]) == 0

        # Run L2
        l2_result = l2_generate(creds=test_credentials, layout=layout)
        assert len(l2_result["successful"]) >= 2

        # Verify all entities have slides
        drive_service = build("drive", "v3", credentials=test_credentials)
        successful_entities = [name for name, _ in l2_result["successful"]]

        for entity_name in successful_entities:
            query = (
                f"mimeType='application/vnd.google-apps.presentation' "
                f"and name='{entity_name}.gslides' "
                f"and '{layout.l2_report_id}' in parents "
                f"and trashed=false"
            )
            results = drive_service.files().list(
                q=query,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()

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
            "entity-1": {"generate": "Y", "slides": "1"},  # Only slide 1
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        create_test_l0_data(
            test_drive_layout.l0_data_id,
            "entity-1",
            test_credentials,
        )

        # Run L1
        l1_result = l1_generate(creds=test_credentials, layout=test_drive_layout)
        assert "entity-1" in l1_result["successful"]

        # Run L2 with slide filtering
        l2_result = l2_generate(creds=test_credentials, layout=test_drive_layout)

        # Should succeed
        assert len(l2_result["successful"]) >= 1
        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" in successful_entities

    def test_workflow_error_recovery(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test that errors in one entity don't affect others."""
        from tests.test_utils import create_test_entities_csv, create_test_l0_data

        entities = {
            "entity-1": {"generate": "Y", "slides": ""},
            "entity-2": {"generate": "Y", "slides": ""},
            "entity-invalid": {"generate": "Y", "slides": ""},  # Will have issues
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Create L0 data for valid entities
        create_test_l0_data(
            test_drive_layout.l0_data_id,
            "entity-1",
            test_credentials,
        )
        create_test_l0_data(
            test_drive_layout.l0_data_id,
            "entity-2",
            test_credentials,
        )

        # Don't create L0 data for entity-invalid - it will fail

        # Run L1
        l1_result = l1_generate(creds=test_credentials, layout=test_drive_layout)

        # entity-1 and entity-2 should succeed
        assert "entity-1" in l1_result["successful"]
        assert "entity-2" in l1_result["successful"]

        # entity-invalid might fail or succeed depending on implementation
        # The important thing is that other entities still process

        # Run L2
        l2_result = l2_generate(creds=test_credentials, layout=test_drive_layout)

        # At least entity-1 and entity-2 should succeed
        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" in successful_entities
        assert "entity-2" in successful_entities


class TestWorkflowDataIntegrity:
    """Tests for data integrity throughout the workflow."""

    def test_data_preservation_through_workflow(
        self,
        complete_test_setup,
        test_credentials,
    ):
        """Test that data is preserved correctly through L0→L1→L2."""
        setup = complete_test_setup
        layout = setup["layout"]

        # Run L1
        l1_result = l1_generate(creds=test_credentials, layout=layout)
        assert "entity-1" in l1_result["successful"]

        # Get L1 spreadsheet data
        drive_service = build("drive", "v3", credentials=test_credentials)
        query = (
            f"mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='entity-1' "
            f"and '{layout.l1_data_id}' in parents "
            f"and trashed=false"
        )
        results = drive_service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        spreadsheet_id = results["files"][0]["id"]

        # Get data from spreadsheet
        data_before = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)

        # Run L2
        l2_result = l2_generate(creds=test_credentials, layout=layout)
        assert len(l2_result["successful"]) >= 1

        # Verify data is still intact
        data_after = get_spreadsheet_data(spreadsheet_id, "data", test_credentials)
        assert data_before == data_after

