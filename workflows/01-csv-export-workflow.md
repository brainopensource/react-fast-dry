# CSV Export Workflow - GET /api/v1/wells/download

This workflow shows the complete flow for exporting well production data as CSV, from API endpoint to frontend download.

## Endpoint-Driven Flow Diagram

```mermaid
graph TD
    A[Frontend: Tkinter App<br/>app.py] -->|HTTP GET Request| B[API Endpoint<br/>GET /api/v1/wells/download]
    
    B -->|Route Handler| C[well_production_routes.py<br/>download_well_production__]
    
    C -->|Dependency Injection| D[WellProductionQueryService<br/>provide_well_production_query_service__]
    
    C -->|Call Service Method| E[WellProductionQueryService<br/>export_to_csv__]
    
    E -->|Repository Call| F[DuckDBWellProductionRepository<br/>export_to_csv__]
    
    F -->|SQL Query Execution| G[DuckDB Database<br/>wells_production.duckdb]
    
    G -->|Data Retrieval| H[Batch Processing<br/>100k records per batch]
    
    H -->|Write CSV| I[File System<br/>downloads/wells_prod.csv]
    
    I -->|File Path Return| J[Repository Response<br/>CSV file path]
    
    J -->|Service Response| K[Query Service Result<br/>File metadata + path]
    
    K -->|HTTP Response| L[FastAPI FileResponse<br/>Content-Type: text/csv]
    
    L -->|File Download| M[Frontend: File Download<br/>User saves CSV file]
    
    M -->|UI Update| N[Success Notification<br/>Download completed]

    %% Error Handling Flow
    F -.->|Database Error| O[ApplicationException<br/>Database connection issues]
    E -.->|Validation Error| P[ValidationException<br/>Invalid parameters]
    C -.->|Exception Handling| Q[ResponseBuilder.error__<br/>Standardized error response]
    Q -.->|Error Response| R[Frontend: Error Display<br/>User notification]

    %% Performance Monitoring
    C -->|Request Tracking| S[Request ID Generation<br/>X-Request-ID header]
    C -->|Timing Decorator| T[async_timed<br/>Execution time tracking]
    
    %% Styling
    classDef frontend fill:#e1f5fe
    classDef api fill:#f3e5f5
    classDef service fill:#e8f5e8
    classDef repository fill:#fff3e0
    classDef database fill:#fce4ec
    classDef error fill:#ffebee
    
    class A,M,N,R frontend
    class B,C,L api
    class D,E,K service
    class F,J repository
    class G,H,I database
    class O,P,Q error
```

## Detailed Method Flow

### 1. API Endpoint Entry Point
- **File**: `src/interfaces/api/well_production_routes.py`
- **Method**: `download_well_production__`
- **Decorators**: `@router.get()`, `@async_timed`
- **Dependencies**: `WellProductionQueryService`

### 2. Service Layer Processing
- **File**: `src/application/services/well_production_query_service.py`
- **Method**: `export_to_csv()`
- **Functionality**: Coordinates CSV export process
- **Error Handling**: Wraps repository calls with service-level exception handling

### 3. Repository Layer Data Access
- **File**: `src/infrastructure/repositories/duckdb_well_production_repository.py`
- **Method**: `export_to_csv()`
- **Key Features**:
  - Batch processing (100k records per batch)
  - Memory management (6GB limit)
  - Parallel processing (4 threads)
  - Temporary file handling

### 4. Database Operations
- **Technology**: DuckDB
- **File**: `data/wells_production.duckdb`
- **Query Execution**: Bulk SELECT with optimized pagination
- **Performance**: Streaming results to avoid memory overflow

### 5. Frontend Response Handling
- **File**: `frontend/app.py`
- **Class**: `ApiModel`
- **Method**: `download_csv()`
- **UI Updates**: Progress indication and download completion notification

## Performance Characteristics

- **Batch Size**: 100,000 records per batch
- **Memory Limit**: 6GB allocation
- **Thread Count**: 4 parallel threads
- **File Format**: CSV with headers
- **Download Location**: User-selectable via file dialog

## Error Scenarios

1. **Database Connection Failure**: Returns ApplicationException
2. **File System Errors**: Handles permissions and disk space issues
3. **Memory Exhaustion**: Automatic batch size reduction
4. **Network Timeouts**: Frontend timeout handling (30 seconds)
5. **Validation Errors**: Parameter validation at API level
