# Developer Guide for FastAPI Hexagonal DDD Project

## Overview

This guide provides detailed instructions for developers to set up, develop, and contribute to our FastAPI project. The application follows Hexagonal Architecture (Ports and Adapters), Domain-Driven Design (DDD), and SOLID principles, implemented with a clean architecture approach.

## Project Structure

```
.
├── src/                    # Source code directory
│   ├── 1_application/       # Application services and use cases
│   ├── 2_domain/           # Domain models and business logic
│   ├── 5_shared/           # Shared functions and settings
│   ├── 3_infrastructure/   # External services and implementations
│   └── 4_interfaces/       # API endpoints and controllers
├── data/                 # Data storage directory
├── external/            # External data
├── temp/               # Temporary files
├── pyproject.toml      # Project configuration
├── requirements.txt    # Project dependencies
└── uv.lock            # UV package manager lock file
```

## Prerequisites

- Python 3.12 or higher
- UV package manager
- Git

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/react-fast-v9.git
   cd react-fast-v9
   ```

2. **Set Up Python Environment**:
   ```bash
   # Install UV if not already installed
   pip install uv

   # Create and activate virtual environment
   uv venv
   source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
   ```

3. **Install Dependencies**:
   ```bash
   # Install production dependencies
   uv pip install -r requirements.txt

   # Install development dependencies
   uv pip install -e ".[dev]"
   ```

## Development Workflow

1. **Running the Application**:
   ```bash
   # Development mode with auto-reload
   uvicorn src.main:app --reload --port 8080
   ```

2. **Code Structure Guidelines**:
   - Implement use cases in `src/1_application/`
   - Place domain models in `src/2_domain/`
   - Add external services in `src/3_infrastructure/`
   - Define API endpoints in `src/4_interfaces/`
   - Shared functions and config in `src/5_shared/`

## Architecture Principles

1. **Hexagonal Architecture**:
   - Domain layer is independent of external concerns
   - Use ports (interfaces) to define interactions
   - Implement adapters for external services

2. **Domain-Driven Design**:
   - Focus on business domain models
   - Use ubiquitous language in code
   - Implement bounded contexts

3. **SOLID Principles**:
   - Single Responsibility Principle
   - Open/Closed Principle
   - Liskov Substitution Principle
   - Interface Segregation Principle
   - Dependency Inversion Principle

## Contributing

1. **Branch Strategy**:
   - Create feature branches from `main`
   - Use descriptive branch names
   - Follow the pattern: `feature/`, `bugfix/`, or `hotfix/`

2. **Code Quality**:
   - Write unit tests for new features
   - Maintain code coverage
   - Follow PEP 8 style guide
   - Document public APIs

3. **Pull Request Process**:
   - Create detailed PR descriptions
   - Include test coverage
   - Request code review
   - Ensure CI passes

## Environment Variables

Create a `.env` file in the root directory with the following structure:
```env
APP_ENV=development
DEBUG=True
API_VERSION=v1
```

## API Documentation

- Access Swagger UI at `http://localhost:8000/docs`
- Access ReDoc at `http://localhost:8000/redoc`

## Troubleshooting

1. **Common Issues**:
   - If UV installation fails, ensure Python 3.12+ is installed
   - For dependency conflicts, check `uv.lock` file
   - Clear `.venv` and reinstall if environment is corrupted

2. **Debug Mode**:
   - Set `DEBUG=True` in `.env`
   - Check logs in console output
   - Use FastAPI debug mode for detailed error messages
