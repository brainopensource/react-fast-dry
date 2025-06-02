# Well Production API

A high-performance FastAPI application for managing millions of well production records using hexagonal architecture, DDD principles, and dual storage (DuckDB + CSV).

## 🏗️ App Architecture

This project follows **Hexagonal Architecture** (Ports and Adapters) with **Domain-Driven Design** principles:

react-fast-v9/
│
├── src/                                    # Main Application Source
│   ├── __init__.py                        # Package initializer
│   ├── main.py                           # FastAPI application entry point
│   │
│   ├── domain/                           # DOMAIN LAYER (Core Business Logic)
│   │   ├── __init__.py                   # Domain package initializer
│   │   ├── entities/                     # Domain Entities
│   │   │   ├── __init__.py               # Entities package
│   │   │   └── well_production.py        # WellProduction entity with business logic
│   │   ├── value_objects/                # Immutable Value Objects
│   │   │   ├── __init__.py               # Value objects package
│   │   │   └── source_data.py            # SourceData value object
│   │   ├── repositories/                 # Repository Interfaces (Ports)
│   │   │   ├── __init__.py               # Repository package
│   │   │   └── well_production_repository.py  # Repository interface
│   │   ├── events/                       # Domain Events (for future use)
│   │   │   └── __init__.py               # Events package
│   │   └── aggregates/                   # Domain Aggregates (for future use)
│   │       └── __init__.py               # Aggregates package
│   │
│   ├── application/                      # APPLICATION LAYER (Use Cases & Services)
│   │   ├── __init__.py                   # Application package initializer
│   │   ├── use_cases/                    # Application Use Cases
│   │   │   ├── __init__.py               # Use cases package
│   │   │   └── import_well_production.py # Import use case implementation
│   │   ├── services/                     # Application Services
│   │   │   └── __init__.py               # Services package
│   │   ├── dtos/                         # Data Transfer Objects (empty - using entities)
│   │   │   └── __init__.py               # DTOs package
│   │   └── interfaces/                   # Application Interfaces
│   │       └── __init__.py               # Interfaces package
│   │
│   ├── infrastructure/                   # INFRASTRUCTURE LAYER (External Adapters)
│   │   ├── __init__.py                   # Infrastructure package initializer
│   │   ├── repositories/                 # Repository Implementations (Adapters)
│   │   │   ├── __init__.py               # Repository implementations package
│   │   │   ├── well_production_repository_impl.py      # CSV implementation
│   │   │   ├── duckdb_well_production_repository.py    # DuckDB implementation
│   │   │   └── composite_well_production_repository.py # Composite (CSV + DuckDB)
│   │   ├── db/                          # Database Configurations
│   │   │   └── __init__.py               # DB package
│   │   ├── external/                     # External Service Adapters
│   │   │   └── __init__.py               # External package
│   │   ├── operations/                   # Data Operations
│   │   │   └── __init__.py               # Operations package
│   │   └── cache/                        # Caching Layer
│   │       └── __init__.py               # Cache package
│   │
│   ├── interfaces/                       # INTERFACE LAYER (API Controllers)
│   │   ├── __init__.py                   # Interfaces package initializer
│   │   ├── api/                          # REST API Controllers
│   │   │   ├── __init__.py               # API package
│   │   │   └── well_production_routes.py # FastAPI routes for well production
│   │   └── middleware/                   # API Middleware
│   │       └── __init__.py               # Middleware package
│   │
│   └── shared/                           # SHARED LAYER (Common Utilities)
│       ├── __init__.py                   # Shared package initializer
│       ├── utils/                        # Utility Functions
│       │   └── __init__.py               # Utils package
│       └── config/                       # Configuration Management
│           └── __init__.py               # Config package
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
│   ├── wells_prod.csv                  # CSV export file
│   └── wells_production.duckdb         # DuckDB database file
│
├── temp/                               # TEMPORARY FILES
│   └── (temporary processing files)
│
├── requirements.txt                     # Python Dependencies
├── README.md                           # Project Documentation
├── developer_guide.md                 # Development Guidelines
├── user_guide.md                      # User Documentation
├── pyproject.toml                     # Project Configuration
└── uv.lock                            # UV Package Lock File

## 🚀 Features

- **Dual Storage**: DuckDB for fast analytics + CSV for compatibility
- **High Performance**: Optimized for millions of records
- **Bulk Operations**: Efficient batch processing
- **Professional API**: RESTful endpoints with proper error handling
- **Health Checks**: Production-ready monitoring
- **Logging**: Comprehensive logging system

## 📊 Storage Strategy

### DuckDB (Primary)
- **Purpose**: High-performance analytics and queries
- **Benefits**: 
  - Columnar storage for fast aggregations
  - SQL interface for complex queries
  - Optimized for OLAP workloads
  - Handles millions of records efficiently

### CSV (Secondary)
- **Purpose**: Data export and compatibility
- **Benefits**:
  - Universal format compatibility
  - Easy data sharing
  - Human-readable
  - Backup format

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
   uvicorn src.main:app --reload --port 8000
   ```

## 📚 API Endpoints

### Core Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information and available endpoints |
| `GET` | `/health` | Health check |
| `GET` | `/docs` | Interactive API documentation |

### Well Production Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/wells/import` | Import well data from JSON |
| `GET` | `/api/v1/wells/download` | Download CSV export |
| `GET` | `/api/v1/wells/stats` | Get database statistics |
| `GET` | `/api/v1/wells/well/{well_code}` | Get specific well data |
| `GET` | `/api/v1/wells/field/{field_code}` | Get all wells in a field |

## 🔧 Usage Examples

### Import Data
```bash
curl -X POST http://localhost:8000/api/v1/wells/import
```

### Download CSV
```bash
curl -O http://localhost:8000/api/v1/wells/download
```

### Get Statistics
```bash
curl http://localhost:8000/api/v1/wells/stats
```

### Query Specific Well
```bash
curl http://localhost:8000/api/v1/wells/well/59806
```

## 🏭 Production Considerations

### Performance Optimizations
- **Bulk Operations**: Uses batch processing for large datasets
- **Connection Pooling**: Efficient database connection management
- **Async Processing**: Non-blocking I/O operations
- **Indexing**: Optimized database indexes for common queries

### Scalability
- **Local-First**: Designed for local deployment with massive datasets
- **Memory Efficient**: Streaming processing for large files
- **Concurrent Storage**: Parallel writes to DuckDB and CSV

### Monitoring
- **Health Checks**: `/health` endpoint for monitoring
- **Logging**: Structured logging with different levels
- **Error Handling**: Comprehensive error responses

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
uvicorn src.main:app --reload

# Test import
curl -X POST http://localhost:8000/api/v1/wells/import

# Check statistics
curl http://localhost:8000/api/v1/wells/stats

# Download data
curl -O http://localhost:8000/api/v1/wells/download
```

## 📁 Data Files

- **Input**: `external/mocked_response.json` - Sample well production data
- **Output**: `data/wells_prod.csv` - Exported CSV file
- **Database**: `data/wells_production.duckdb` - DuckDB file

## 🤝 Contributing

1. Follow the hexagonal architecture patterns
2. Maintain separation of concerns between layers
3. Write descriptive commit messages
4. Ensure proper error handling
5. Add logging for important operations

## 📄 License

This project is licensed under the MIT License.

## 🔗 Related Documentation

- [Developer Guide](developer_guide.md) - Detailed development instructions
- [User Guide](user_guide.md) - End-user documentation
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [DuckDB Documentation](https://duckdb.org/docs/) 