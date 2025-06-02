# Well Production API

A high-performance FastAPI application for managing millions of well production records using hexagonal architecture, DDD principles, and DuckDB primary storage with on-demand CSV export.

## 🏗️ App Architecture

This project follows **Hexagonal Architecture** (Ports and Adapters) with **Domain-Driven Design** principles:

```
react-fast-v9/
│
├── src/                                    # Main Application Source
│   ├── __init__.py                        # Package initializer
│   ├── main.py                           # FastAPI application entry point
│   ├── favicon.ico                       # Application favicon
│   │
│   ├── domain/                           # DOMAIN LAYER (Core Business Logic)
│   │   ├── __init__.py                   # Domain package initializer
│   │   ├── entities/                     # Domain Entities
│   │   │   ├── __init__.py               # Entities package
│   │   │   └── well_production.py        # WellProduction entity with business logic
│   │   ├── value_objects/                # Immutable Value Objects
│   │   │   └── __init__.py               # Value objects package
│   │   ├── repositories/                 # Repository Interfaces (Ports)
│   │   │   ├── __init__.py               # Repository package
│   │   │   ├── well_production_repository.py  # Repository interface
│   │   │   └── ports.py                  # Repository port definitions
│   │   └── ports/                        # Domain Ports (for future use)
│   │       └── __init__.py               # Ports package
│   │
│   ├── application/                      # APPLICATION LAYER (Use Cases & Services)
│   │   ├── __init__.py                   # Application package initializer
│   │   └── services/                     # Application Services
│   │       ├── __init__.py               # Services package
│   │       ├── base.py                   # Base service class
│   │       ├── well_production_service.py        # Core well production service
│   │       ├── well_production_import_service.py # Import service implementation
│   │       ├── well_production_query_service.py  # Query service implementation
│   │       ├── external_api_service.py   # External API service
│   │       ├── fetchers.py               # Data fetching utilities
│   │       └── wells_service.py          # Wells service wrapper
│   │
│   ├── infrastructure/                   # INFRASTRUCTURE LAYER (External Adapters)
│   │   ├── __init__.py                   # Infrastructure package initializer
│   │   ├── repositories/                 # Repository Implementations (Adapters)
│   │   │   ├── __init__.py               # Repository implementations package
│   │   │   ├── well_production_repository_impl.py      # CSV implementation
│   │   │   └── duckdb_well_production_repository.py    # DuckDB implementation with CSV export
│   │   ├── db/                          # Database Configurations
│   │   │   ├── __init__.py               # DB package
│   │   │   └── duckdb_repo.py            # DuckDB repository implementation
│   │   ├── external/                     # External Service Adapters
│   │   │   ├── __init__.py               # External package
│   │   │   └── pandas_csv_exporter.py    # CSV export functionality
│   │   ├── operations/                   # Data Operations & SQL
│   │   │   ├── __init__.py               # Operations package
│   │   │   └── wells.sql                 # SQL queries for well operations
│   │   └── adapters/                     # Infrastructure Adapters
│   │       └── __init__.py               # Adapters package
│   │
│   ├── interfaces/                       # INTERFACE LAYER (API Controllers)
│   │   ├── __init__.py                   # Interfaces package initializer
│   │   └── api/                          # REST API Controllers
│   │       ├── __init__.py               # API package
│   │       ├── well_production_routes.py # FastAPI routes for well production
│   │       ├── schemas.py                # Pydantic schemas for API
│   │       ├── mappers.py                # Data mappers for API layer
│   │       └── dependencies.py           # API dependency injection
│   │
│   ├── shared/                           # SHARED LAYER (Common Utilities)
│   │   ├── __init__.py                   # Shared package initializer
│   │   ├── dependencies.py               # Dependency injection configuration
│   │   ├── exceptions.py                 # Custom exception classes
│   │   ├── responses.py                  # Standard API response models
│   │   ├── job_manager.py                # Background job management
│   │   ├── batch_processor.py            # Batch processing utilities
│   │   ├── config/                       # Configuration Management
│   │   │   ├── __init__.py               # Config package
│   │   │   ├── settings.py               # Application settings
│   │   │   └── datasets_config.py        # Dataset configuration
│   │   └── utils/                        # Utility Functions
│   │       ├── __init__.py               # Utils package
│   │       └── sql_loader.py             # SQL file loading utility
│   │
│   ├── api/                              # Additional API Components
│   │   └── __init__.py                   # API package
│   │
│   └── temp/                             # Temporary Processing Files
│       └── (temporary files)
│
├── frontend/                             # API CLIENT FOR TESTING
│   ├── __init__.py                       # Frontend package initializer
│   ├── requirements.txt                  # Frontend dependencies
│   ├── README.md                        # Frontend documentation
│   ├── client.py                        # Comprehensive API client
│   ├── test_health.py                   # Health endpoint test
│   ├── test_import.py                   # Import endpoint test
│   ├── test_stats.py                    # Stats endpoint test
│   ├── test_well_59806.py               # Specific well test
│   ├── test_field_8908.py               # Specific field test
│   └── downloads/                       # Downloaded files storage
│       └── (generated CSV files)
│
├── external/                            # EXTERNAL DATA
│   └── mocked_response.json            # Sample well production data
│
├── data/                               # GENERATED DATA STORAGE
│   └── wells_production.duckdb         # DuckDB database file
│
├── temp/                               # TEMPORARY FILES
│   └── (temporary processing files)
│
├── downloads/                          # DOWNLOAD STORAGE
│   └── (on-demand CSV exports)
│
├── logs/                               # APPLICATION LOGS
│   └── wells_api.log                   # Application log file
│
├── tests/                              # TEST SUITE
│   └── (test files)
│
├── requirements.txt                     # Python Dependencies
├── pyproject.toml                      # Project Configuration
├── uv.lock                            # UV Package Lock File
├── run.py                             # Application runner script
├── jobs.json                          # Job configuration
├── README.md                          # Project Documentation
├── developer_guide.md                 # Development Guidelines
├── user_guide.md                      # User Documentation
└── SQL_QUERIES_DESIGN.md             # SQL Design Documentation
```

## 🚀 Features

- **DuckDB Primary Storage**: Fast analytics with on-demand CSV export
- **High Performance**: Optimized for millions of records
- **Bulk Operations**: Efficient batch processing
- **Professional API**: RESTful endpoints with proper error handling
- **Health Checks**: Production-ready monitoring
- **Comprehensive Logging**: File and console logging with rotation
- **Background Jobs**: Asynchronous job processing
- **Batch Processing**: Efficient data processing utilities
- **Dependency Injection**: Clean dependency management
- **Exception Handling**: Comprehensive error handling system

## 📊 Storage Strategy

### DuckDB (Primary)
- **Purpose**: High-performance analytics, queries, and data storage
- **Benefits**: 
  - Columnar storage for fast aggregations
  - SQL interface for complex queries
  - Optimized for OLAP workloads
  - Handles millions of records efficiently
  - Fast import from JSON data sources

### CSV (On-Demand Export)
- **Purpose**: Data export when needed for downloads
- **Benefits**:
  - Universal format compatibility
  - Easy data sharing
  - Human-readable
  - Generated only when requested for downloads

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd react-fast-v9
   ```

2. **Install dependencies**:
   ```bash
   pip install uv
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -r requirements.txt
   ```

3. **Run the application**:
   ```bash
   uvicorn src.main:app --reload --port 8080
   ```

   Or use the provided runner script:
   ```bash
   python run.py
   ```

## 📚 API Endpoints

### Core Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information and available endpoints |
| `GET` | `/health` | Health check with service status |
| `GET` | `/docs` | Interactive API documentation |

### Well Production Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/wells/import` | Import well data from JSON with filters |
| `GET` | `/api/v1/wells/import/trigger` | Simple import trigger endpoint |
| `GET` | `/api/v1/wells/download` | Download CSV export |
| `GET` | `/api/v1/wells/stats` | Get database statistics |
| `GET` | `/api/v1/wells/well/{well_code}` | Get specific well data |
| `GET` | `/api/v1/wells/field/{field_code}` | Get all wells in a field |

## 🔧 Usage Examples

### Import Data
```bash
curl -X POST http://localhost:8080/api/v1/wells/import
```

### Simple Import Trigger
```bash
curl http://localhost:8080/api/v1/wells/import/trigger
```

### Download CSV
```bash
curl -O http://localhost:8080/api/v1/wells/download
```

### Get Statistics
```bash
curl http://localhost:8080/api/v1/wells/stats
```

### Query Specific Well
```bash
curl http://localhost:8080/api/v1/wells/well/59806
```

### Query Field Data
```bash
curl http://localhost:8080/api/v1/wells/field/8908
```

## 🏭 Production Considerations

### Performance Optimizations
- **Bulk Operations**: Uses batch processing for large datasets
- **Connection Pooling**: Efficient database connection management
- **Async Processing**: Non-blocking I/O operations with FastAPI
- **Indexing**: Optimized database indexes for common queries
- **Background Jobs**: Asynchronous job processing system

### Scalability
- **Local-First**: Designed for local deployment with massive datasets
- **Memory Efficient**: Streaming processing for large files
- **Fast DuckDB Storage**: Optimized columnar storage for analytics
- **Batch Processing**: Configurable batch sizes for optimal performance

### Monitoring & Observability
- **Health Checks**: `/health` endpoint for monitoring
- **Structured Logging**: File and console logging with different levels
- **Error Handling**: Comprehensive error responses with proper HTTP status codes
- **Job Tracking**: Background job status monitoring

### Configuration Management
- **Environment Variables**: Configurable via environment variables
- **Settings Management**: Centralized configuration with Pydantic
- **CORS Configuration**: Configurable CORS settings for production
- **Database Paths**: Configurable data storage locations

## 🔒 Data Schema

### Well Production Entity
```python
@dataclass
class WellProduction:
    field_code: int
    field_name: str
    well_code: int
    well_reference: str
    well_name: str
    production_period: str
    days_on_production: int
    oil_production_kbd: float
    gas_production_mmcfd: float
    liquids_production_kbd: float
    water_production_kbd: float
    data_source: str
    source_data: str
    partition_0: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
```

## 🧪 Testing

Run the application and test endpoints:

```bash
# Start the server
uvicorn src.main:app --reload --port 8080

# Test import
curl -X POST http://localhost:8080/api/v1/wells/import

# Check statistics
curl http://localhost:8080/api/v1/wells/stats

# Download data
curl -O http://localhost:8080/api/v1/wells/download

# Test specific well
curl http://localhost:8080/api/v1/wells/well/59806

# Test field data
curl http://localhost:8080/api/v1/wells/field/8908
```

### Frontend Testing
Use the provided frontend test clients:

```bash
cd frontend
python test_health.py
python test_import.py
python test_stats.py
python test_well_59806.py
python test_field_8908.py
```

## 📁 Data Files

- **Input**: `external/mocked_response.json` - Sample well production data
- **Output**: `data/wells_prod.csv` - Exported CSV file
- **Database**: `data/wells_production.duckdb` - DuckDB file
- **SQL**: `src/infrastructure/operations/wells.sql` - Database queries
- **Logs**: `logs/wells_api.log` - Application logs

## 🔧 Configuration

### Environment Variables
- `APP_ENV`: Application environment (development/production)
- `APP_DEBUG`: Debug mode flag
- `API_BASE_URL`: External API base URL
- `API_KEY`: External API key
- `DATA_ROOT_DIR_NAME`: Data directory name
- `DUCKDB_FILENAME`: DuckDB file name
- `CSV_EXPORT_FILENAME`: CSV export file name
- `CORS_ALLOWED_ORIGINS`: Allowed CORS origins

### Key Configuration Files
- `src/shared/config/settings.py`: Application settings
- `src/shared/config/datasets_config.py`: Dataset configurations
- `src/shared/dependencies.py`: Dependency injection setup
- `pyproject.toml`: Project metadata and dependencies

## 🤝 Contributing

1. Follow the hexagonal architecture patterns
2. Maintain separation of concerns between layers
3. Use dependency injection for loose coupling
4. Write descriptive commit messages
5. Ensure proper error handling and logging
6. Add comprehensive tests for new features
7. Update documentation for API changes

## 📄 License

This project is licensed under the MIT License.

## 🔗 Related Documentation

- [Developer Guide](developer_guide.md) - Detailed development instructions
- [User Guide](user_guide.md) - End-user documentation
- [SQL Queries Design](SQL_QUERIES_DESIGN.md) - Database design documentation
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Pydantic Documentation](https://docs.pydantic.dev/) 