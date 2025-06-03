# User Guide for Data Science App

## Overview

This guide provides instructions for setting up, using, and contributing to the Data Science App. The application is designed to efficiently process and serve large datasets (1M-80M rows) in a local-first environment.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/datascience-app.git
   cd datascience-app
   ```

2. **Set Up a Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Dependencies**:
   ```bash
   poetry install
   or
   poetry install --no-root
   ```
	
## Usage

1. **Run the Application**:
   ```bash
   poetry run uvicorn main:app --reload
   ```
   or

   ```bash
   uv run uvicorn src.main:app --host 0.0.0.0 --port 8080
   ```


2. **Access the API**:
   - Open your browser and navigate to `http://localhost:8000/docs` to view the API documentation.

3. **Fetch Data**:
   - Use the provided endpoints to fetch and validate data from configured sources.


## 📋 Module Descriptions

### 🎯 Domain Layer (`src/domain/`)
The core business logic and rules of the application.

### 🔄 Application Layer (`src/application/`)
Orchestrates domain logic and coordinates between layers.


### 🛠️ Infrastructure Layer (`src/infrastructure/`)
Implements domain interfaces with external technologies.

### 🌐 Interface Layer (`src/interfaces/`)
Handles HTTP requests/responses and API documentation.

### 🔧 Shared Layer (`src/shared/`)
Common utilities, configurations, and cross-cutting concerns.

### 🧪 Frontend Client (`frontend/`)
Simulates real client interactions for testing and development.

## Contributing

1. **Fork the Repository**: Create your own fork of the repository.
2. **Create a Branch**: Create a new branch for your feature or bug fix.
3. **Make Changes**: Implement your changes and ensure they adhere to the project's coding standards.
4. **Submit a Pull Request**: Once your changes are complete, submit a pull request for review.

---
