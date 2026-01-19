# Test Suite for Google Slides Automator

This directory contains the end-to-end test framework for the Google Slides Automator package.

## Overview

The test suite validates all functionality of the Google Slides automator on a separate test Google Drive. Each test creates isolated test data and cleans up afterward to ensure test independence and prevent interference between tests.

## Test Structure

- `conftest.py`: Pytest fixtures for Drive setup, credentials, and cleanup
- `test_utils.py`: Helper functions for creating test Drive structures, generating test data, and cleanup operations
- `test_drive_layout.py`: Tests for Drive layout resolution and entities.csv parsing
- `test_l1_generate.py`: Tests for L1 generation functionality (happy path, errors, edge cases)
- `test_l2_generate.py`: Tests for L2 slide generation (placeholder replacement, filtering, errors)
- `test_integration.py`: End-to-end workflow tests (L0→L1→L2)

## Prerequisites

1. **Python 3.11 or above**

2. **Test Service Account Credentials**
   - Create a service account in Google Cloud Console with the following scopes:
     - `https://www.googleapis.com/auth/spreadsheets`
     - `https://www.googleapis.com/auth/drive.readonly`
     - `https://www.googleapis.com/auth/drive.file`
     - `https://www.googleapis.com/auth/drive`
     - `https://www.googleapis.com/auth/presentations`
   - Download the JSON key file

3. **Test Google Drive**
   - The service account must have access to create folders and files in a test Google Drive
   - Tests will create isolated test folders for each test run

## Installation

Install the package with test dependencies:

```bash
pip install -e ".[dev]"
```

Or install test dependencies separately:

```bash
pip install pytest>=7.0.0 pytest-cov>=4.0.0 pytest-mock>=3.0.0
```

## Configuration

### Environment Variables

Set the following environment variables:

**Required:**
- `TEST_DRIVE_FOLDER_ID`: Folder ID where test subfolders will be created
  - Each test creates a unique subfolder inside this parent folder
  - Service accounts cannot create folders at the Drive root level, so a parent folder is required

**Optional:**
- `TEST_SERVICE_ACCOUNT_CREDENTIALS`: Path to test service account JSON file
  - If not set, tests will use `service-account-credentials.json` in the project root

Example:
```bash
export TEST_SERVICE_ACCOUNT_CREDENTIALS=/path/to/test-service-account-credentials.json
export TEST_DRIVE_FOLDER_ID=1ABC123def456GHI789  # Folder ID where test folders will be created
```

### Test Drive Setup

Tests require access to a Google Drive folder where test subfolders can be created:

- Set `TEST_DRIVE_FOLDER_ID` to a folder ID
- Tests will create unique subfolders with UUID-based names inside this folder
- Subfolders are automatically cleaned up after tests complete

**Important**: Ensure your test service account has permission to:
- Access the folder specified by `TEST_DRIVE_FOLDER_ID`
- Create folders and files inside this folder
- Delete folders and files (for cleanup)
- Access Google Sheets, Slides, and Drive APIs

**Note**: Service accounts cannot create folders at the Drive root level. You must provide a folder ID where the service account has Editor access.

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest tests/test_l1_generate.py
```

### Run Specific Test

```bash
pytest tests/test_l1_generate.py::TestL1GenerateSingleEntity::test_l1_generate_single_entity
```

### Run Tests with Coverage

```bash
pytest --cov=gslides_automator --cov-report=html
```

### Run Tests by Marker

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only e2e tests
pytest -m e2e
```

### Verbose Output

```bash
pytest -v
```

### Stop on First Failure

```bash
pytest -x
```

## Test Categories

Tests are organized into the following categories:

### Unit Tests (`test_drive_layout.py`)
- Drive layout resolution
- entities.csv parsing
- URL/ID extraction

### Integration Tests (`test_l1_generate.py`, `test_l2_generate.py`)
- L1 generation (CSV processing, image copying, template cloning)
- L2 generation (placeholder replacement, slide filtering)
- Error handling
- Edge cases

### End-to-End Tests (`test_integration.py`)
- Complete L0→L1→L2 workflows
- Multiple entity processing
- Data integrity verification
- Error recovery

## Test Data Strategy

Each test:
1. Creates a unique test folder in Google Drive (using UUIDs)
2. Sets up required folder structure (L0-Raw, L1-Merged, L2-Slide, templates)
3. Creates test data (CSV files, images, templates)
4. Runs the function under test
5. Verifies results (checks Drive contents, slide content, etc.)
6. Cleans up test folder (automatic via pytest fixtures)

## Test Fixtures

### `test_credentials`
Provides test service account credentials. Uses `TEST_SERVICE_ACCOUNT_CREDENTIALS` environment variable if set, otherwise falls back to `service-account-credentials.json` in project root.

### `test_drive_root`
Creates a test Google Drive root folder for each test. Automatically cleaned up after test completes.

### `test_drive_layout`
Creates a complete test Drive structure with all required folders and files. Returns a `DriveLayout` object.

### `test_entities_data`
Creates test `entities.csv` file with sample entities. Returns a tuple of `(entities_dict, csv_file_id)`.

### `test_templates`
Creates test data template and slide template. Returns a tuple of `(data_template_id, slide_template_id)`.

### `test_l0_data`
Creates test L0-Raw data for entities. Returns a dict mapping entity names to their L0 folder IDs.

### `complete_test_setup`
Complete test setup with all components. Returns a dict with all test components.

## Troubleshooting

### "Test credentials file not found"
- Set `TEST_SERVICE_ACCOUNT_CREDENTIALS` environment variable
- Or ensure `service-account-credentials.json` exists in project root

### "Permission denied" errors
- Ensure test service account has Editor permissions on the test Drive
- Check that all required APIs are enabled in Google Cloud Console

### "Rate limit exceeded" errors
- Tests include retry logic, but if you see frequent rate limit errors:
  - Reduce parallelism: `pytest -n 1`
  - Add delays between tests
  - Use a different test service account

### Tests failing due to cleanup issues
- Test folders are automatically cleaned up, but if cleanup fails:
  - Manually delete test folders from Google Drive
  - Check service account permissions
  - Review test logs for specific error messages

## Writing New Tests

When writing new tests:

1. Use existing fixtures from `conftest.py` when possible
2. Create isolated test data for each test
3. Clean up test data (handled automatically by fixtures)
4. Use descriptive test names that explain what is being tested
5. Add docstrings to test functions explaining the scenario
6. Include both positive and negative test cases

Example:

```python
def test_new_feature(
    self,
    complete_test_setup,
    test_credentials,
):
    """Test description of what this test verifies."""
    setup = complete_test_setup
    layout = setup["layout"]

    # Your test code here
    result = some_function(creds=test_credentials, layout=layout)

    # Assertions
    assert result["successful"] == ["entity-1"]
```

## Continuous Integration

Tests can be run in CI/CD pipelines. Ensure:

1. `TEST_SERVICE_ACCOUNT_CREDENTIALS` environment variable is set
2. Test service account credentials are securely stored (e.g., as secrets)
3. Test Drive is accessible from CI environment
4. All required APIs are enabled

## Contributing

When adding new functionality:

1. Write tests first (TDD approach recommended)
2. Ensure all tests pass before submitting PR
3. Maintain or improve test coverage
4. Update this README if adding new test utilities or fixtures

