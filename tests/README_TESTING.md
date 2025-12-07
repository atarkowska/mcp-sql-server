# Testing Guide

## Setup

Install test dependencies:

```bash
pip install -r requirements-test.txt
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=server --cov-report=html --cov-report=term
```

### Run specific test file
```bash
pytest test_server.py
```

### Run specific test class
```bash
pytest test_server.py::TestListTables
```

### Run specific test
```bash
pytest test_server.py::TestListTables::test_list_tables_success
```

### Run with verbose output
```bash
pytest -v
```

### Run and stop at first failure
```bash
pytest -x
```

## Test Structure

The test suite includes:

- **TestGetDbConnection**: Tests for database connection functionality
- **TestListTables**: Tests for listing database tables
- **TestGetTableSchema**: Tests for retrieving table schema information
- **TestExecuteQuery**: Tests for executing SQL queries (SELECT, INSERT, UPDATE, DELETE)
- **TestExecuteSafeQuery**: Tests for safe query execution (SELECT only)

## Coverage Report

After running with coverage, open `htmlcov/index.html` in a browser to view detailed coverage report.

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    pip install -r requirements-test.txt
    pytest --cov=server --cov-report=xml
```
