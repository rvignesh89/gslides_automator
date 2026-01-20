# Contributing Guide

Thank you for your interest in contributing to `gslides_automator`! This guide will help you understand the codebase structure, where to make changes, and how to test your modifications.

## Table of Contents

- [Code Structure](#code-structure)
- [Development Setup](#development-setup)
- [Testing Changes](#testing-changes)
- [Code Style](#code-style)
- [CI Workflows](#ci-workflows)
- [Deployment Structure](#deployment-structure)
- [Submitting Changes](#submitting-changes)

## Code Structure

The `gslides_automator` package follows a modular architecture:

```
gslides_automator/
├── __init__.py          # Package exports and public API
├── __main__.py          # Module entrypoint for `python -m gslides_automator`
├── auth.py              # Authentication and credential management
├── cli.py               # Command-line interface
├── drive_layout.py      # Drive layout discovery and entity management
├── generate.py          # Unified generation workflow
├── l1_generate.py       # L1 data generation (CSV → Google Sheets)
├── l2_generate.py       # L2 slide generation (Sheets → Google Slides)
└── l3_generate.py       # L3 PDF generation (Slides → PDF)

tests/
├── conftest.py          # Pytest fixtures and test configuration
├── test_utils.py        # Test helper functions
├── test_drive_layout.py # Tests for drive layout resolution
├── test_l1_generate.py # Tests for L1 generation
├── test_l2_generate.py # Tests for L2 generation
└── test_integration.py # End-to-end workflow tests
```

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
   pip install -e ".[dev]"
   ```

   This installs the package in editable mode with development dependencies (pytest, pytest-cov, pytest-mock).

4. **Set up service account credentials**:
   - Download your service account JSON key file from Google Cloud Console
   - Place it as `service-account-credentials.json` in the project root
   - Ensure the service account has access to your Google Drive shared drive

5. **Verify installation**:
   ```bash
   python -m gslides_automator --help
   ```

## Testing Changes

### Running Tests

The project uses `pytest` for automated testing. Tests are located in the `tests/` directory.

**Run all tests**:
```bash
pytest
```

**Run specific test file**:
```bash
pytest tests/test_l1_generate.py
```

**Run tests with coverage**:
```bash
pytest --cov=gslides_automator --cov-report=html
```

### Test Configuration

Tests are configured in `pyproject.toml`.

### Test Environment Variables

For integration tests that require Google Drive access, set:
- `TEST_DRIVE_FOLDER_ID`: Folder ID where test subfolders will be created
- `TEST_SERVICE_ACCOUNT_CREDENTIALS`: Path to test service account JSON file (optional, defaults to `service-account-credentials.json`)

See `tests/README.md` for detailed test setup instructions.

## Code Style

### Python Style Guide

- Follow **PEP 8** style guidelines
- Use **type hints** for function parameters and return values
- Use **docstrings** for all public functions and classes
- Keep functions focused on a single responsibility

### Formatting and Linting

The project uses `ruff` for code formatting and linting. Code is automatically checked in CI:

```bash
ruff check .          # Check for linting issues
ruff format --check . # Check formatting
ruff format .         # Auto-format code
```

### Naming Conventions

- **Functions**: `snake_case` (e.g., `process_entity`)
- **Classes**: `PascalCase` (e.g., `DriveLayout`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `SCOPES`)
- **Private functions**: Prefix with `_` if not part of public API

## CI Workflows

The project uses GitHub Actions for continuous integration. The CI workflow (`.github/workflows/ci.yml`) runs automatically on push and pull requests to the `main` branch.

### CI Jobs

1. **Lint**: Runs `ruff` for code linting and format checking
2. **Test**: Runs `pytest` to execute all tests

Both jobs run on Ubuntu with Python 3.12. The test job requires GitHub secrets for integration tests:
- `TEST_DRIVE_FOLDER_ID`
- `TEST_SERVICE_ACCOUNT_CREDENTIALS`

## Deployment Structure

The package is distributed via PyPI using setuptools. Deployment configuration is in `pyproject.toml`:

- **Build system**: setuptools with wheel
- **Package metadata**: Name, version, description, dependencies
- **Entry points**: CLI command `gslides_automator` points to `gslides_automator.cli:main`
- **Optional dependencies**: Development dependencies under `[dev]` extras

To build and publish:
```bash
python -m build
twine upload dist/*
```

## Submitting Changes

### Before Submitting

1. **Run tests**: Ensure all tests pass locally (`pytest`)
2. **Check code style**: Run `ruff check .` and `ruff format --check .`
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

- [ ] Code passes linting (`ruff check .`)
- [ ] Code is properly formatted (`ruff format --check .`)
- [ ] All tests pass (`pytest`)
- [ ] Type hints are used where appropriate
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

