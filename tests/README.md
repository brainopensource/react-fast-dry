# Well Production API Tests

This directory contains comprehensive tests for the Well Production API using pytest framework.

## Test Structure

```
tests/
├── __init__.py                    # Test package initialization
├── conftest.py                    # Pytest configuration and shared fixtures
├── pytest.ini                    # Pytest settings
├── requirements.txt               # Test dependencies
├── README.md                      # This file
├── api/                          # API endpoint tests
│   ├── __init__.py
│   ├── test_health.py            # Health and root endpoint tests
│   └── test_wells.py             # Well production endpoint tests
├── integration/                  # Integration tests
│   ├── __init__.py
│   └── test_api_workflow.py      # End-to-end workflow tests
├── unit/                         # Unit tests
│   ├── __init__.py
│   └── test_external_api_service.py  # Service layer tests
└── utils/                        # Test utilities
    ├── __init__.py
    └── test_helpers.py           # Helper functions and utilities
```

## Test Types

### API Tests (`tests/api/`)
- Test individual API endpoints
- Verify response formats and status codes
- Test error handling and edge cases
- Use FastAPI TestClient for isolated testing

### Integration Tests (`tests/integration/`)
- Test complete workflows and user scenarios
- Verify data consistency across endpoints
- Test API resilience and error recovery
- Simulate real-world usage patterns

### Unit Tests (`tests/unit/`)
- Test individual components in isolation
- Mock external dependencies
- Test business logic and validation
- Fast-running focused tests

## Running Tests

### Prerequisites
1. Install test dependencies:
```bash
# From the project root
pip install -r tests/requirements.txt

# Or using uv (if available)
uv pip install -r tests/requirements.txt
```

2. Ensure the main application dependencies are installed:
```bash
pip install -r requirements.txt
```

### Running All Tests
```bash
# From the project root
pytest tests/

# With verbose output
pytest tests/ -v

# With coverage report
pytest tests/ --cov=src --cov-report=term-missing
```

### Running Specific Test Types
```bash
# API tests only
pytest tests/api/

# Integration tests only
pytest tests/integration/

# Unit tests only
pytest tests/unit/

# Specific test file
pytest tests/api/test_health.py

# Specific test method
pytest tests/api/test_health.py::TestHealthEndpoints::test_health_endpoint_success
```

### Running Tests with Markers
```bash
# Run only unit tests (if marked)
pytest -m unit

# Run only integration tests (if marked)  
pytest -m integration

# Run only API tests (if marked)
pytest -m api

# Skip slow tests
pytest -m "not slow"
```

## Test Configuration

### Environment Variables
Tests use the same configuration as the main application but with test-specific defaults:
- Mock mode is enabled by default for external API calls
- Test database is isolated from production data
- Logging is configured for test output

### Fixtures
Common fixtures are defined in `conftest.py`:
- `test_client`: FastAPI test client
- `api_endpoints`: Common endpoint URLs
- `sample_well_code`: Test well code (59806)
- `sample_field_code`: Test field code (8908)
- `expected_well_reference`: Expected well reference ("1-C-1-BA")
- `expected_field_name`: Expected field name ("Candeias")
- `test_assertions`: Helper class for common assertions

## Test Data

Tests are designed to work with or without imported data:
- Tests that require data will skip if data is not available
- Integration tests can import test data as needed
- Mock data is used for external API responses
- No dependency on external services in test mode

## Coverage

Run tests with coverage to ensure adequate test coverage:
```bash
# Generate coverage report
pytest tests/ --cov=src --cov-report=html

# View coverage report
open htmlcov/index.html
```

Aim for:
- Overall coverage: >90%
- API endpoints: >95%
- Critical business logic: >95%

## Best Practices

### Writing Tests
1. **Use descriptive test names** that explain what is being tested
2. **Follow AAA pattern**: Arrange, Act, Assert
3. **Test one thing per test** method
4. **Use fixtures** for common setup and test data
5. **Mock external dependencies** in unit tests
6. **Test both success and failure paths**

### Test Organization
1. **Group related tests** in test classes
2. **Use meaningful class and method names**
3. **Keep tests independent** - no test should depend on another
4. **Use appropriate test types** (unit vs integration vs API)

### Performance
1. **Keep unit tests fast** (<1s each)
2. **Use session-scoped fixtures** for expensive setup
3. **Mock external calls** to avoid network delays
4. **Parallelize tests** when possible with pytest-xdist

## Continuous Integration

These tests are designed to run in CI/CD pipelines:
- No external dependencies required
- Deterministic test data
- Clear pass/fail criteria
- Appropriate test timeouts

Example CI configuration:
```yaml
- name: Run tests
  run: |
    pip install -r requirements.txt
    pip install -r tests/requirements.txt
    pytest tests/ --cov=src --cov-report=xml
```

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure `src` directory is in Python path (handled by conftest.py)
2. **Missing dependencies**: Install test requirements: `pip install -r tests/requirements.txt`
3. **Test data issues**: Tests should work without pre-imported data
4. **Port conflicts**: Tests use TestClient, no actual server needed

### Debugging Tests
```bash
# Run with pdb debugger
pytest tests/ --pdb

# Run with extra output
pytest tests/ -v -s

# Run specific failing test
pytest tests/api/test_health.py::TestHealthEndpoints::test_health_endpoint_success -v
```

## Contributing

When adding new features:
1. Add corresponding tests
2. Maintain test coverage above 90%
3. Follow existing test patterns
4. Update this README if needed
5. Ensure all tests pass before submitting

## Migration from Frontend Tests

The old manual test scripts in the `frontend/` directory have been converted to proper pytest tests:
- `frontend/test_health.py` → `tests/api/test_health.py`
- `frontend/test_import.py` → `tests/api/test_wells.py`
- `frontend/test_stats.py` → `tests/api/test_wells.py`
- `frontend/test_well_59806.py` → `tests/api/test_wells.py`
- `frontend/test_field_8908.py` → `tests/api/test_wells.py`
- `frontend/client.py` functionality → `tests/integration/test_api_workflow.py`

The new tests provide:
- Automated execution
- Better error reporting
- Test coverage metrics
- CI/CD integration
- Faster feedback loops 