# Contributing Guide

Thank you for your interest in contributing to `gslides_automator`! This guide will help you understand the codebase structure, where to make changes, and how to test your modifications.

## Table of Contents

- [Code Structure](#code-structure)
- [Where to Make Changes](#where-to-make-changes)
- [Development Setup](#development-setup)
- [Testing Changes](#testing-changes)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)

## Code Structure

The `gslides_automator` package follows a modular architecture with clear separation of concerns:

```
gslides_automator/
├── __init__.py          # Package exports and public API
├── __main__.py          # Module entrypoint for `python -m gslides_automator`
├── auth.py              # Authentication and credential management
├── cli.py               # Command-line interface
├── drive_layout.py      # Drive layout discovery and entity management
├── l1_generate.py       # L1 data generation (CSV → Google Sheets)
└── l2_generate.py       # L2 slide generation (Sheets → Google Slides)
```

### Module Responsibilities

#### `auth.py`
- **Purpose**: Manages Google API authentication using service account credentials
- **Key Functions**:
  - `get_oauth_credentials()`: Loads service account credentials from JSON file
  - `load_credentials()`: Public API for loading credentials
  - `get_service_account_email()`: Extracts service account email from credentials
- **Dependencies**: `google-auth`, `google-auth-oauthlib`

#### `drive_layout.py`
- **Purpose**: Discovers and manages Google Drive folder structure
- **Key Components**:
  - `DriveLayout`: Dataclass storing folder/file IDs for the standard layout
  - `resolve_layout()`: Discovers folder structure from a root Drive URL
  - `load_entities()`: Loads entity names from `entities.csv` where `Generate=Y`
  - `load_entities_with_slides()`: Loads entities with slide number filtering
- **Dependencies**: `google-api-python-client`

#### `l1_generate.py`
- **Purpose**: Generates L1-Merged data (Google Sheets) from L0-Raw data (CSV files)
- **Key Functions**:
  - `l1_generate()`: Main entry point for L1 generation
  - `process_entity()`: Processes a single entity
  - `clone_template_to_entity()`: Copies data template spreadsheet
  - `write_csv_to_sheet_tab()`: Writes CSV data to spreadsheet tabs
  - `copy_image_to_folder()`: Copies image files from L0 to L1
- **Workflow**:
  1. Reads CSV files from `L0-Raw/<entity>/`
  2. Clones `data-template.gsheet` to `L1-Merged/<entity>/`
  3. Populates spreadsheet tabs with CSV data
  4. Copies images to L1 folder
- **Dependencies**: `gspread`, `google-api-python-client`, `pandas`

#### `l2_generate.py`
- **Purpose**: Generates Google Slides presentations from L1-Merged spreadsheets
- **Key Functions**:
  - `l2_generate()`: Main entry point for L2 generation
  - `process_entity()`: Processes a single entity's slide generation
  - `replace_text_placeholders()`: Replaces text placeholders in slides
  - `replace_chart_placeholder()`: Replaces chart placeholders with embedded charts
  - `replace_table_placeholder()`: Replaces table placeholders with embedded tables
  - `replace_picture_placeholder()`: Replaces picture placeholders with images
- **Workflow**:
  1. Reads spreadsheets from `L1-Merged/<entity>/`
  2. Copies `report-template.gslide` to `L2-Slides/`
  3. Replaces placeholders with data from spreadsheets
  4. Embeds charts, tables, and images
- **Dependencies**: `gspread`, `google-api-python-client`

#### `cli.py`
- **Purpose**: Command-line interface for the package
- **Key Functions**:
  - `main()`: CLI entry point
  - `_build_parser()`: Builds argument parser with subcommands
  - `_run_l1_generate()`: Handler for `l1-generate` subcommand
  - `_run_l2_generate()`: Handler for `l2-generate` subcommand
- **Usage**: Provides `gslides_automator` CLI command

#### `__init__.py`
- **Purpose**: Defines the public API of the package
- **Exports**:
  - `l1_generate()`: Public function for L1 generation
  - `l2_generate()`: Public function for L2 generation
  - `DriveLayout`: Dataclass for drive layout
  - `resolve_layout()`: Function for resolving drive layout

## Where to Make Changes

### Modifying Authentication

**File**: `gslides_automator/auth.py`

- **To change credential loading**: Modify `get_oauth_credentials()` or `load_credentials()`
- **To add new scopes**: Update the `SCOPES` list at the top of the file
- **To change credential file location**: Modify `SERVICE_ACCOUNT_CREDENTIALS` constant

**Example**: If you need to add OAuth2 user credentials support:
```python
# In auth.py, add a new function:
def get_user_credentials(client_secrets_file: str, scopes: list):
    # Implementation for OAuth2 user flow
    pass
```

### Modifying Drive Layout Discovery

**File**: `gslides_automator/drive_layout.py`

- **To change folder structure**: Modify `resolve_layout()` to look for different folder names
- **To change entity CSV format**: Modify `load_entities()` or `load_entities_with_slides()`
- **To add new layout components**: Add fields to `DriveLayout` dataclass and update `resolve_layout()`

**Example**: If you want to support a different folder name:
```python
# In drive_layout.py, modify resolve_layout():
l0_id = _find_child_by_name(
    drive_service,
    root_id,
    ["L0-Data", "L0-Raw"],  # Support multiple names
    mime_type="application/vnd.google-apps.folder"
)
```

### Modifying L1 Generation (CSV → Sheets)

**File**: `gslides_automator/l1_generate.py`

- **To change CSV processing**: Modify `download_csv_from_drive()` or `write_csv_to_sheet_tab()`
- **To change spreadsheet creation**: Modify `clone_template_to_entity()`
- **To change image handling**: Modify `copy_image_to_folder()` or `list_image_files_in_folder()`
- **To change retry logic**: Modify `retry_with_exponential_backoff()`
- **To add new file types**: Add new functions similar to `list_csv_files_in_folder()` and processing logic

**Example**: If you want to support Excel files:
```python
# Add a new function in l1_generate.py:
def list_excel_files_in_folder(drive_service, folder_id):
    # Similar to list_csv_files_in_folder but for Excel MIME types
    pass

# Then modify process_entity() to handle Excel files
```

### Modifying L2 Generation (Sheets → Slides)

**File**: `gslides_automator/l2_generate.py`

- **To change placeholder replacement**: Modify `replace_text_placeholders()`, `replace_chart_placeholder()`, `replace_table_placeholder()`, or `replace_picture_placeholder()`
- **To change slide processing**: Modify `process_entity()` or slide iteration logic
- **To add new placeholder types**: Add new replacement functions and integrate them into the processing pipeline
- **To change chart/table embedding**: Modify the respective replacement functions

**Example**: If you want to add support for video placeholders:
```python
# In l2_generate.py, add:
def replace_video_placeholder(slides_service, presentation_id, slide_id, placeholder_name, video_url):
    # Implementation for video embedding
    pass

# Then integrate into process_entity() or the main replacement loop
```

### Modifying CLI

**File**: `gslides_automator/cli.py`

- **To add new subcommands**: Add a new parser in `_build_parser()` and a corresponding handler function
- **To change argument parsing**: Modify `_build_parser()` or individual subcommand parsers
- **To add global options**: Modify the main parser in `_build_parser()`

**Example**: If you want to add an `l3-generate` command:
```python
# In cli.py, add:
def _run_l3_generate(args: argparse.Namespace) -> int:
    from .l3_generate import l3_generate, get_oauth_credentials, resolve_layout
    # ... implementation
    return 0

# In _build_parser():
l3_parser = subparsers.add_parser("l3-generate", help="Generate PDFs from slides")
l3_parser.add_argument("--shared-drive-url", required=True)
l3_parser.set_defaults(func=_run_l3_generate)
```

### Modifying Public API

**File**: `gslides_automator/__init__.py`

- **To expose new functions**: Add imports and wrapper functions
- **To change function signatures**: Update both the wrapper and ensure backward compatibility

## Development Setup

### Prerequisites

1. **Python 3.11+** (as specified in `pyproject.toml`)
2. **Google Cloud Service Account** with appropriate API scopes enabled
3. **Service Account Credentials JSON file**

### Setup Steps

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd gslides_automator
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

   Or install in development mode:
   ```bash
   pip install -e .
   ```

4. **Set up service account credentials**:
   - Download your service account JSON key file from Google Cloud Console
   - Place it as `service-account-credentials.json` in the project root
   - Ensure the service account has access to your Google Drive shared drive

5. **Verify installation**:
   ```bash
   python -m gslides_automator --help
   ```

## Testing Changes

### Current Testing Status

**Note**: The project currently does not have automated tests. When adding new features or modifying existing code, manual testing is required.

### Manual Testing Workflow

#### 1. Test L1 Generation

1. **Prepare test data**:
   - Set up a Google Drive with the required folder structure
   - Add test entities to `entities.csv` with `Generate=Y`
   - Add CSV files and images to `L0-Raw/<entity>/` folders

2. **Run L1 generation**:
   ```bash
   python -m gslides_automator.l1_generate \
     --shared-drive-url <your-drive-url> \
     --service-account-credentials service-account-credentials.json
   ```

3. **Verify results**:
   - Check that spreadsheets are created in `L1-Merged/<entity>/`
   - Verify CSV data is correctly written to spreadsheet tabs
   - Confirm images are copied to L1 folders

#### 2. Test L2 Generation

1. **Ensure L1 data exists**:
   - Run L1 generation first or manually create L1 spreadsheets

2. **Run L2 generation**:
   ```bash
   python -m gslides_automator.l2_generate \
     --shared-drive-file <your-drive-url> \
     --credentials service-account-credentials.json
   ```

3. **Verify results**:
   - Check that slides are created in `L2-Slides/`
   - Open slides and verify placeholders are replaced correctly
   - Verify charts, tables, and images are embedded properly

#### 3. Test CLI Interface

```bash
# Test help command
gslides_automator --help

# Test l1-generate subcommand
gslides_automator l1-generate --help

# Test l2-generate subcommand
gslides_automator l2-generate --help
```

#### 4. Test Package API

Create a test script:

```python
# test_api.py
from gslides_automator import l1_generate, l2_generate

# Test L1 generation
result = l1_generate(
    shared_drive_url="<your-drive-url>",
    service_account_credentials="service-account-credentials.json"
)
print(f"L1 Generation: {result}")

# Test L2 generation
result = l2_generate(
    shared_drive_url="<your-drive-url>",
    service_account_credentials="service-account-credentials.json"
)
print(f"L2 Generation: {result}")
```

Run it:
```bash
python test_api.py
```

### Recommended Testing Approach

1. **Use a test Google Drive**: Create a separate Google Drive for testing to avoid affecting production data
2. **Test with minimal data**: Start with 1-2 entities and simple data
3. **Test edge cases**:
   - Empty CSV files
   - Missing files/folders
   - Invalid placeholder names
   - Large files
   - Rate limiting scenarios
4. **Test error handling**: Verify error messages are clear and helpful
5. **Test retry logic**: Simulate rate limit errors if possible

### Adding Automated Tests (Future)

Consider adding automated tests using `pytest`:

1. **Install pytest**:
   ```bash
   pip install pytest pytest-mock
   ```

2. **Create test structure**:
   ```
   tests/
   ├── __init__.py
   ├── test_auth.py
   ├── test_drive_layout.py
   ├── test_l1_generate.py
   └── test_l2_generate.py
   ```

3. **Example test**:
   ```python
   # tests/test_auth.py
   import pytest
   from gslides_automator.auth import load_credentials

   def test_load_credentials_file_not_found():
       with pytest.raises(FileNotFoundError):
           load_credentials("nonexistent.json")
   ```

4. **Run tests**:
   ```bash
   pytest tests/
   ```

## Code Style

### Python Style Guide

- Follow **PEP 8** style guidelines
- Use **type hints** for function parameters and return values (as seen in existing code)
- Use **docstrings** for all public functions and classes
- Keep functions focused on a single responsibility

### Naming Conventions

- **Functions**: `snake_case` (e.g., `process_entity`)
- **Classes**: `PascalCase` (e.g., `DriveLayout`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `SCOPES`)
- **Private functions**: Prefix with `_` if not part of public API (e.g., `_find_child_by_name`)

### Code Organization

- Group related functions together
- Keep imports at the top of files
- Use helper functions to avoid code duplication
- Add comments for complex logic

### Example Code Style

```python
def process_entity(entity_name: str, creds, layout: DriveLayout) -> bool:
    """
    Process a single entity's data.

    Args:
        entity_name: Name of the entity to process
        creds: Google API credentials
        layout: Drive layout configuration

    Returns:
        True if successful, False otherwise
    """
    # Implementation
    pass
```

## Submitting Changes

### Before Submitting

1. **Test your changes**: Follow the manual testing workflow above
2. **Check code style**: Ensure your code follows PEP 8
3. **Update documentation**: Update README.md if you change functionality
4. **Check for breaking changes**: Document any breaking changes

### Pull Request Process

1. **Create a branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**: Follow the guidelines above

3. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Description of your changes"
   ```

4. **Push and create PR**: Push your branch and create a pull request

### Pull Request Checklist

- [ ] Code follows PEP 8 style guidelines
- [ ] Functions have docstrings
- [ ] Type hints are used where appropriate
- [ ] Changes have been tested manually
- [ ] No breaking changes (or breaking changes are documented)
- [ ] README.md updated if needed

## Common Issues and Solutions

### Rate Limiting

If you encounter rate limiting errors:
- The code includes retry logic with exponential backoff
- You may need to increase delays in `retry_with_exponential_backoff()`
- Consider processing entities in smaller batches

### Permission Errors

If you see permission errors:
- Verify service account has access to the Google Drive
- Check that the service account email has "Editor" permissions
- Use `get_service_account_email()` to get the email address

### File Not Found Errors

If files/folders are not found:
- Verify the Drive folder structure matches the expected layout
- Check folder names match exactly (case-sensitive)
- Ensure files are in Shared Drive, not My Drive

## Getting Help

- Check the [README.md](README.md) for usage examples
- Review existing code for patterns and examples
- Open an issue for bugs or feature requests

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (see LICENSE.txt).



