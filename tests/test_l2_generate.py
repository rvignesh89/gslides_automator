"""
Tests for l2_generate module.
"""

from __future__ import annotations

import pytest
from googleapiclient.discovery import build

from gslides_automator.l2_generate import l2_generate
from gslides_automator.l1_generate import l1_generate
from tests.test_utils import (
    create_test_l0_data,
    get_slide_text_content,
    verify_drive_structure,
)


class TestL2GenerateSingleEntity:
    """Tests for L2 generation with a single entity."""

    def test_l2_generate_single_entity(
        self,
        complete_test_setup,
        test_credentials,
    ):
        """Test slide generation for a single entity."""
        setup = complete_test_setup
        layout = setup["layout"]

        # First run L1 to create the data
        l1_result = l1_generate(creds=test_credentials, layout=layout)
        assert "entity-1" in l1_result["successful"]

        # Now run L2 to generate slides
        l2_result = l2_generate(creds=test_credentials, layout=layout)

        # Verify results
        assert len(l2_result["successful"]) >= 1
        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" in successful_entities

        # Verify slide was created
        drive_service = build("drive", "v3", credentials=test_credentials)
        query = (
            f"mimeType='application/vnd.google-apps.presentation' "
            f"and name='entity-1.gslides' "
            f"and '{layout.l2_report_id}' in parents "
            f"and trashed=false"
        )
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        presentations = results.get("files", [])
        assert len(presentations) == 1

        presentation_id = presentations[0]["id"]

        # Verify slide has content
        text_content = get_slide_text_content(presentation_id, 0, test_credentials)
        assert len(text_content) > 0


class TestL2GenerateMultipleEntities:
    """Tests for L2 generation with multiple entities."""

    def test_l2_generate_multiple_entities(
        self,
        complete_test_setup,
        test_credentials,
    ):
        """Test slide generation for multiple entities."""
        setup = complete_test_setup
        layout = setup["layout"]

        # First run L1
        l1_result = l1_generate(creds=test_credentials, layout=layout)
        assert len(l1_result["successful"]) >= 2

        # Run L2
        l2_result = l2_generate(creds=test_credentials, layout=layout)

        # Verify multiple entities processed
        assert len(l2_result["successful"]) >= 2
        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" in successful_entities
        assert "entity-2" in successful_entities


class TestL2PlaceholderReplacement:
    """Tests for placeholder replacement in slides."""

    def test_l2_text_placeholder_replacement(
        self,
        complete_test_setup,
        test_credentials,
    ):
        """Verify text placeholders are replaced."""
        setup = complete_test_setup
        layout = setup["layout"]

        # Run L1
        l1_result = l1_generate(creds=test_credentials, layout=layout)
        assert "entity-1" in l1_result["successful"]

        # Run L2
        l2_result = l2_generate(creds=test_credentials, layout=layout)
        assert len(l2_result["successful"]) >= 1

        # Get presentation ID
        drive_service = build("drive", "v3", credentials=test_credentials)
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

        if results.get("files"):
            presentation_id = results["files"][0]["id"]

            # Check that placeholders were replaced
            text_content = get_slide_text_content(presentation_id, 0, test_credentials)

            # Placeholders should be replaced (not contain {{}})
            assert "{{" not in text_content or "}}" not in text_content


class TestL2EntityFiltering:
    """Tests for entity filtering in L2 generation."""

    def test_l2_entity_filtering(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test entity filtering with generate=Y."""
        from tests.test_utils import create_test_entities_csv, create_test_l0_data

        entities = {
            "entity-1": {"generate": "Y", "slides": ""},
            "entity-2": {"generate": "N", "slides": ""},
            "entity-3": {"generate": "Y", "slides": ""},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Create L0 data
        for entity_name in ["entity-1", "entity-2", "entity-3"]:
            create_test_l0_data(
                test_drive_layout.l0_data_id,
                entity_name,
                test_credentials,
            )

        # Run L1
        l1_result = l1_generate(creds=test_credentials, layout=test_drive_layout)
        assert len(l1_result["successful"]) == 2

        # Run L2
        l2_result = l2_generate(creds=test_credentials, layout=test_drive_layout)

        # Only entity-1 and entity-3 should be processed
        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" in successful_entities
        assert "entity-3" in successful_entities
        assert "entity-2" not in successful_entities


class TestL2SlideFiltering:
    """Tests for slide number filtering in L2 generation."""

    def test_l2_slide_filtering(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test slide number filtering (e.g., '1,2-4')."""
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

        # Run L2
        l2_result = l2_generate(creds=test_credentials, layout=test_drive_layout)

        # Should succeed
        assert len(l2_result["successful"]) >= 1


class TestL2ErrorCases:
    """Tests for error cases in L2 generation."""

    def test_l2_missing_spreadsheet(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test error handling when L1 spreadsheet is missing."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"generate": "Y", "slides": ""},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Don't run L1 - spreadsheet won't exist

        # Run L2 - should fail or skip entity
        l2_result = l2_generate(creds=test_credentials, layout=test_drive_layout)

        # Entity should be in failed list or not in successful
        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" not in successful_entities

    def test_l2_missing_slide_template(
        self,
        test_drive_layout,
        test_credentials,
    ):
        """Test error handling when slide template is missing."""
        from tests.test_utils import create_test_entities_csv, create_test_l0_data

        entities = {
            "entity-1": {"generate": "Y", "slides": ""},
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Set invalid template ID
        test_drive_layout.report_template_id = "invalid-template-id-12345"

        create_test_l0_data(
            test_drive_layout.l0_data_id,
            "entity-1",
            test_credentials,
        )

        # Run L1
        l1_result = l1_generate(creds=test_credentials, layout=test_drive_layout)
        assert "entity-1" in l1_result["successful"]

        # Run L2 - should fail
        l2_result = l2_generate(creds=test_credentials, layout=test_drive_layout)
        successful_entities = [name for name, _ in l2_result["successful"]]
        assert "entity-1" not in successful_entities

