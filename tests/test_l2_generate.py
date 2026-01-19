"""
Tests for l2_generate module.
"""

from __future__ import annotations

from googleapiclient.discovery import build

from gslides_automator.generate import generate
from tests.test_utils import (
    create_test_l0_data,
    get_slide_text_content,
    execute_with_retry,
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

        # Run generate (L1 and L2)
        result = generate(creds=test_credentials, layout=layout)

        # Verify results
        assert len(result["successful"]) >= 1
        assert "entity-1" in result["successful"]

        # Verify slide was created
        drive_service = build("drive", "v3", credentials=test_credentials)
        query = (
            f"mimeType='application/vnd.google-apps.presentation' "
            f"and name='entity-1.gslides' "
            f"and '{layout.l2_slide_id}' in parents "
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

        # Run generate (L1 and L2)
        result = generate(creds=test_credentials, layout=layout)

        # Verify multiple entities processed
        assert len(result["successful"]) >= 2
        assert "entity-1" in result["successful"]
        assert "entity-2" in result["successful"]


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

        # Run generate (L1 and L2)
        result = generate(creds=test_credentials, layout=layout)
        assert len(result["successful"]) >= 1
        assert "entity-1" in result["successful"]

        # Get presentation ID
        drive_service = build("drive", "v3", credentials=test_credentials)
        query = (
            f"mimeType='application/vnd.google-apps.presentation' "
            f"and name='entity-1.gslides' "
            f"and '{layout.l2_slide_id}' in parents "
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
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "Y", "l2": "All", "l3": "N"},
            "entity-2": {"l1": "N", "l2": "", "l3": "N"},
            "entity-3": {"l1": "Y", "l2": "All", "l3": "N"},
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
                test_drive_layout.l0_raw_id,
                entity_name,
                test_credentials,
            )

        # Run generate (L1 and L2)
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Only entity-1 and entity-3 should be processed (they have l1=Y and l2 set)
        assert "entity-1" in result["successful"]
        assert "entity-3" in result["successful"]
        assert "entity-2" not in result["successful"]


class TestL2SlideFiltering:
    """Tests for slide number filtering in L2 generation."""

    def test_l2_slide_filtering(
        self,
        test_drive_layout,
        test_credentials,
        test_templates,
    ):
        """Test slide number filtering (e.g., '1,2-4')."""
        from tests.test_utils import create_test_entities_csv

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

        # Run generate (L1 and L2)
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Should succeed
        assert len(result["successful"]) >= 1
        assert "entity-1" in result["successful"]


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
            "entity-1": {"l1": "N", "l2": "All", "l3": "N"},  # L2 only, no L1
        }

        csv_file_id = create_test_entities_csv(
            test_drive_layout.root_id,
            entities,
            test_credentials,
        )
        test_drive_layout.entities_csv_id = csv_file_id

        # Don't run L1 - spreadsheet won't exist

        # Run generate - should fail or skip entity (no L1 data)
        result = generate(creds=test_credentials, layout=test_drive_layout)

        # Entity should be in failed list or not in successful
        assert "entity-1" not in result["successful"]

    def test_l2_missing_slide_template(
        self,
        test_drive_layout,
        test_credentials,
    ):
        """Test error handling when slide template is missing."""
        from tests.test_utils import create_test_entities_csv

        entities = {
            "entity-1": {"l1": "N", "l2": "All", "l3": "N"},  # L2 only, no L1
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
            test_drive_layout.l0_raw_id,
            "entity-1",
            test_credentials,
        )

        # Run generate - should fail due to invalid template
        result = generate(creds=test_credentials, layout=test_drive_layout)
        assert "entity-1" not in result["successful"]
