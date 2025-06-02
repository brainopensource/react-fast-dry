## Development Plan: Integrating dbt for Advanced Calculations

This plan focuses on setting up dbt to handle complex batch calculations, with the results being available to your FastAPI application.

**Overall Goals:**

*   Establish a dbt project alongside your FastAPI application.
*   Define dbt models for the specified calculations, using DuckDB as the data warehouse.
*   Integrate dbt's batch-processed results into the FastAPI application for serving.
*   Maintain real-time capabilities for aspects not suited to batch processing (e.g., live ML predictions if needed, though dbt can prepare data for ML model *training*).

---

### **Phase 1: dbt Project Setup & Initial Configuration**

**(Estimated Time: 1-2 days)**

1.  **Install dbt and dbt-duckdb Adapter:**
    *   Add `dbt-core` and `dbt-duckdb` to your project's development dependencies (e.g., in `requirements-dev.txt` or `pyproject.toml` for Poetry/PDM).
    *   Ensure your virtual environment is set up correctly.

2.  **Initialize dbt Project:**
    *   Create a new directory for your dbt project, e.g., `dbt_project/` at the root of your `react-fast-v9/` workspace.
    *   Run `dbt init your_dbt_project_name` inside this directory.
    *   Familiarize yourself with the generated dbt project structure (`models/`, `seeds/`, `tests/`, `dbt_project.yml`, `profiles.yml`).

3.  **Configure `profiles.yml` for DuckDB:**
    *   This file (typically in `~/.dbt/` or you can specify a project-local path) tells dbt how to connect to your DuckDB.
    *   **Example `profiles.yml` entry:**
        ```yaml
        your_dbt_project_name:
          target: dev
          outputs:
            dev:
              type: duckdb
              path: ../data/wells_production.duckdb # Relative path to your DuckDB file from dbt_project dir
              # You might need to add extensions if your calculations require them
              # extensions:
              #   - httpfs
              #   - parquet
        ```
    *   Ensure the path to `wells_production.duckdb` is correct relative to where `dbt run` will be executed (i.e., from within the `dbt_project` directory).

4.  **Configure `dbt_project.yml`:**
    *   Update project name, profile, and model/seed/test paths if necessary.
    *   Define default materializations if desired (e.g., tables or views).

5.  **Create First dbt Source:**
    *   Define your existing `well_production` table as a dbt source.
    *   Create `dbt_project/models/sources/sources.yml`:
        ```yaml
        version: 2

        sources:
          - name: main_app_db
            description: "Source DuckDB database from the FastAPI application."
            database: your_duckdb_file_name_if_different_or_remove_if_path_is_enough # Usually path in profiles.yml is sufficient
            schema: main # DuckDB's default schema
            tables:
              - name: well_production
                description: "Raw well production data."
                # Optionally, add column descriptions and tests here later
        ```

6.  **Create First Staging Model:**
    *   Create `dbt_project/models/staging/stg_well_production.sql`:
        ```sql
        -- models/staging/stg_well_production.sql
        SELECT
            field_code,
            field_name,
            well_code,
            well_reference,
            well_name,
            production_period, -- Ensure this is a DATE or TIMESTAMP for time-based calcs
            CAST(production_period AS DATE) as production_date, -- Example: ensure date type
            days_on_production,
            oil_production_kbd,
            gas_production_mmcfd,
            liquids_production_kbd,
            water_production_kbd,
            data_source,
            source_data,
            partition_0,
            created_at,
            updated_at
            -- Add any basic cleaning or type casting needed before transformations
        FROM {{ source('main_app_db', 'well_production') }}
        ```
    *   Run `dbt run --select stg_well_production` to test. This will create a view or table (based on default/config) in your DuckDB.

---

### **Phase 2: Implement Core Batch Transformations in dbt Models**

**(Estimated Time: 3-5 days, depending on complexity familiarity)**

For each calculation, you'll create new SQL models in dbt. The focus here is on the *structure and flow*, not the exact SQL for now.

1.  **Cumulative Sums:**
    *   Create `dbt_project/models/intermediate/int_production_cumsum.sql`:
        ```sql
        -- models/intermediate/int_production_cumsum.sql
        WITH source_data AS (
            SELECT * FROM {{ ref('stg_well_production') }}
        )
        SELECT
            *,
            -- Pseudo-code for cumsum, actual SQL will use window functions
            -- SUM(oil_production_kbd) OVER (PARTITION BY well_code ORDER BY production_date) as cumulative_oil_kbd
            '# call calculate_cumsum(oil_production_kbd, well_code, production_date)' AS cumulative_oil_kbd_placeholder,
            '# call calculate_cumsum(gas_production_mmcfd, well_code, production_date)' AS cumulative_gas_mmcfd_placeholder
            -- ... other cumulative sums for different curves
        FROM source_data
        ```

2.  **Declining Curves:**
    *   Create `dbt_project/models/intermediate/int_declining_curves.sql`:
        ```sql
        -- models/intermediate/int_declining_curves.sql
        WITH production_data AS (
            SELECT * FROM {{ ref('int_production_cumsum') }} -- or stg_well_production if cumsum not needed here
        )
        SELECT
            *,
            -- Pseudo-code, actual implementation might involve UDFs or complex SQL
            '# call calculate_decline_curve_parameters(well_code, production_date, oil_production_kbd)' AS decline_params_placeholder,
            '# call generate_decline_forecast(well_code, decline_params_placeholder)' AS oil_decline_forecast_placeholder
        FROM production_data
        ```
    *   *Note: Complex algorithms might be better implemented as Python UDFs registered in DuckDB, or by running a Python script that dbt can orchestrate (using `dbt run-operation` or pre/post hooks).*\n\n3.  **ML Model Trendlines (Preparation & Post-processing):**
    *   dbt is not for running ML model training/inference directly. It's for preparing data *for* ML models and processing their *output*.
    *   Assume models are trained and can be called (e.g., via a Python script or a service).
    *   Create `dbt_project/models/intermediate/int_ml_trendlines_input.sql` (if specific features are needed):\n        ```sql
        -- models/intermediate/int_ml_trendlines_input.sql
        SELECT
            well_code,
            production_date,
            oil_production_kbd,
            cumulative_oil_kbd_placeholder -- from int_production_cumsum
            -- ... features needed by your ML model
        FROM {{ ref('int_production_cumsum') }} -- or another relevant model
        ```
    *   *External Step: Run your Python ML model using `int_ml_trendlines_input` as input, save predictions to a new DuckDB table (e.g., `ml_predictions_raw`).*
    *   Create `dbt_project/models/intermediate/int_ml_trendlines_output.sql` to integrate predictions:\n        ```sql
        -- models/intermediate/int_ml_trendlines_output.sql
        -- First, define ml_predictions_raw as a source or use dbt seeds if it's static output
        -- For this example, let's assume it's loaded into a table dbt can access
        WITH predictions AS (
            SELECT * FROM {{ source('main_app_db', 'ml_predictions_raw') }} -- Assuming predictions are loaded here
        ),
        base_data AS (
            SELECT * FROM {{ ref('int_declining_curves') }}
        )
        SELECT
            b.*,
            p.trendline_prediction_placeholder
        FROM base_data b
        LEFT JOIN predictions p ON b.well_code = p.well_code AND b.production_date = p.prediction_date -- Adjust join keys
        ```

4.  **Moving Averages (on declining curves):**
    *   Create `dbt_project/models/intermediate/int_moving_averages.sql`:\n        ```sql
        -- models/intermediate/int_moving_averages.sql
        WITH decline_forecasts AS (
            SELECT * FROM {{ ref('int_ml_trendlines_output') }} -- or int_declining_curves if ML trendlines are separate
        )
        SELECT
            *,
            -- Pseudo-code for moving average
            '# call calculate_moving_average(oil_decline_forecast_placeholder, well_code, production_date, 30_day_window)' AS ma_oil_forecast_30d_placeholder
        FROM decline_forecasts
        ```

5.  **Aggregations (Monthly & Yearly):**
    *   Create `dbt_project/models/marts/mart_production_monthly.sql`:\n        ```sql
        -- models/marts/mart_production_monthly.sql
        WITH base_production AS (
            SELECT * FROM {{ ref('int_moving_averages') }} -- or the latest relevant intermediate model
        )
        SELECT
            well_code,
            field_code,
            strftime(production_date, '%Y-%m-01') as production_month,
            '# aggregate sum(oil_production_kbd)' AS total_monthly_oil_placeholder,
            '# aggregate sum(gas_production_mmcfd)' AS total_monthly_gas_placeholder
            -- ... other aggregations
        FROM base_production
        GROUP BY 1, 2, 3
        ```
    *   Create `dbt_project/models/marts/mart_production_yearly.sql`: (Similar, grouping by year)

6.  **Revenue Calculations:**
    *   **Seed Data:** You'll need prices and currency tables. Use dbt seeds for this if they are relatively static CSVs.
        *   Create `dbt_project/seeds/prices.csv` and `dbt_project/seeds/currency_rates.csv`.
        *   Run `dbt seed` to load them into DuckDB.
    *   Create `dbt_project/models/marts/mart_revenue.sql`:\n        ```sql
        -- models/marts/mart_revenue.sql
        WITH monthly_production AS (
            SELECT * FROM {{ ref('mart_production_monthly') }}
        ),
        prices AS (
            SELECT * FROM {{ ref('prices') }} -- dbt seed
        ),
        currency_rates AS (
            SELECT * FROM {{ ref('currency_rates') }} -- dbt seed
        )
        SELECT
            mp.*,
            (mp.total_monthly_oil_placeholder * pr.oil_price_placeholder) as revenue_usd_placeholder,
            (mp.total_monthly_oil_placeholder * pr.oil_price_placeholder * cr.usd_to_local_rate_placeholder) as revenue_local_placeholder
        FROM monthly_production mp
        LEFT JOIN prices pr ON strftime(mp.production_month, '%Y-%m') = pr.price_month_placeholder -- Adjust join logic
        LEFT JOIN currency_rates cr ON strftime(mp.production_month, '%Y-%m') = cr.rate_month_placeholder -- Adjust join logic
        ```

7.  **Data Quality Checks (dbt Tests):**
    *   Add schema tests (`not_null`, `unique`, `accepted_values`, `relationships`) in `.yml` files next to your models.
    *   Create custom data tests (SQL queries that should return 0 rows) in the `tests/` directory.
    *   **Example `dbt_project/models/marts/marts.yml`:**
        ```yaml
        version: 2

        models:
          - name: mart_revenue
            description: "Calculated revenue per well per month."
            columns:
              - name: well_code
                tests:
                  - not_null
              - name: production_month
                tests:
                  - not_null
              - name: revenue_usd_placeholder
                tests:
                  - dbt_utils.not_negative # Requires dbt_utils package
        ```
    *   Run `dbt test` to execute these.

---

### **Phase 3: Workflow & FastAPI Integration**

**(Estimated Time: 2-3 days)**

1.  **dbt Run Scheduling/Triggering:**
    *   **Development:** Manually run `dbt run`, `dbt build` (runs models, tests, seeds, snapshots).
    *   **Batch Updates (Staging/Production):**
        *   Use a scheduler (OS-level like cron/Windows Task Scheduler, or a tool like Apache Airflow, Prefect, Dagster if complexity grows).
        *   The scheduler will execute `dbt build` periodically (e.g., nightly).

2.  **FastAPI Reading dbt-Generated Tables:**
    *   Your FastAPI application services will now query the `mart_` tables (or relevant intermediate tables) created by dbt in DuckDB.
    *   **Example in `WellProductionQueryService` (conceptual):**
        ```python
        # src/application/services/well_production_query_service.py
        # ...
        async def get_monthly_revenue(self, well_code: int, month: str) -> Optional[Dict]:
            # Assumes 'mart_revenue' table exists and is populated by dbt
            query = "SELECT * FROM mart_revenue WHERE well_code = ? AND production_month = ?"
            # Use your existing repository method to execute this query against DuckDB
            result = await self.repository.execute_custom_query(query, [well_code, month_start_date_iso(month)])
            return result[0] if result else None

        async def get_well_decline_forecast(self, well_code: int) -> List[Dict]:
            # Assumes 'int_declining_curves' or a mart model contains this
            query = "SELECT production_date, oil_decline_forecast_placeholder FROM int_declining_curves WHERE well_code = ? ORDER BY production_date"
            result = await self.repository.execute_custom_query(query, [well_code])
            return result
        ```
    *   The key is that FastAPI reads from tables that dbt *maintains*. The FastAPI app itself doesn't run dbt commands directly for request handling.

3.  **Handling ML Model Predictions:**
    *   **Option A (Batch Predictions via dbt orchestration):**
        1.  dbt prepares input data (e.g., `int_ml_trendlines_input`).
        2.  A script (run via dbt pre/post-hook or `dbt run-operation` calling a macro that uses `run_outside_dbt_python_script`) takes this data, calls the ML model, and writes predictions to a raw predictions table in DuckDB.
        3.  A subsequent dbt model (e.g., `int_ml_trendlines_output`) picks up these raw predictions and integrates them.
    *   **Option B (FastAPI for Real-time/Near Real-time Predictions):**
        1.  ML models are loaded and served by a separate FastAPI endpoint or service.
        2.  When new raw data comes in (or on demand), FastAPI can call the ML model.
        3.  Predictions can be stored back in a table that dbt can then source for further batch processing if needed.
    *   *Choose based on whether predictions need to be real-time or can be batch-generated.* For trendlines on historical data, batch is often fine.

4.  **Data Latency Considerations:**
    *   Users of your FastAPI app will see data as fresh as the last successful `dbt run`.
    *   Communicate this data latency to users if necessary.
    *   For more real-time needs, the FastAPI app would bypass dbt models and query the raw `well_production` table or use its own in-memory calculations with Polars as it does now. This is the "hybrid" part.

---

### **Phase 4: Advanced Considerations & Refinements**

**(Ongoing)**

1.  **Error Handling & Monitoring for dbt Runs:**
    *   Implement logging for dbt runs.
    *   Set up alerts for dbt run failures (especially if using a scheduler).
    *   dbt artifacts (like `manifest.json`, `run_results.json`) can be used for advanced monitoring.

2.  **Performance Optimization:**
    *   Configure dbt model materializations (table, view, incremental) appropriately. Use `incremental` for large tables that grow over time.
    *   Optimize DuckDB (e.g., indexing on columns frequently used in joins/filters, though DuckDB is good at this automatically).
    *   Review dbt model SQL for efficiency.

3.  **Managing dbt in Different Environments (dev, staging, prod):**
    *   Use dbt targets in `profiles.yml` to switch between different DuckDB instances or configurations if needed (e.g., a dev copy of the DB).

4.  **CI/CD for dbt:**
    *   Set up a CI pipeline to run `dbt compile`, `dbt run`, and `dbt test` on code changes to the dbt project.

---

## Why dbt for Heavy Calculations in a Data Science Context?

In a data science context involving millions of rows and potentially dozens of interconnected tables, relying solely on application-layer Python scripts or complex ORM queries for heavy, multi-stage calculations can become challenging. dbt offers several advantages for these scenarios:

1.  **Modularity and Reusability:** dbt encourages breaking down complex transformations into smaller, manageable, and reusable SQL models. Each model represents a specific transformation step (e.g., staging, intermediate processing, final mart). This is akin to functions in programming, making the data pipeline easier to understand, debug, and maintain.

2.  **Data Lineage and Dependency Management:** dbt automatically builds a directed acyclic graph (DAG) of your models based on `ref()` and `source()` functions. This provides clear data lineage, showing how data flows and transforms from source to final output. It simplifies understanding the impact of changes and debugging issues.

3.  **SQL-Centric Approach for Transformations:** SQL is often the most efficient language for set-based data transformations directly within the database. dbt leverages this by allowing data scientists and analysts to write transformations in SQL, which DuckDB (in this case) can execute efficiently.

4.  **Testing and Data Quality:** dbt has built-in support for data testing (schema tests, custom data tests). This is crucial for ensuring the reliability and accuracy of your data pipeline, especially when complex calculations are involved. You can define assertions about your data at each step.

5.  **Version Control and Collaboration:** dbt projects are typically managed under version control (like Git). This allows for collaborative development, tracking changes, and easier rollbacks, which are standard best practices in software and data engineering.

6.  **Idempotency and Materialization Strategies:** dbt models can be configured with different materialization strategies (views, tables, incremental models). Incremental models are particularly useful for large datasets, as they allow dbt to process only new or changed data, significantly speeding up run times for batch updates.

7.  **Separation of Concerns:** It separates the heavy lifting of data transformation (ETL/ELT) from the application logic (FastAPI). Your API remains lean and focused on serving data, while dbt handles the pre-computation of complex datasets.

8.  **Scalability of Analytic Logic:** As the number of calculations, data sources, or the complexity of transformations grows, dbt provides a structured framework to manage this complexity, which might become unwieldy if implemented purely in Python scripts scattered across an application.

9.  **Documentation:** dbt allows you to document your models, columns, and tests directly within your project. This documentation can be automatically generated and served as a website, making the data pipeline more understandable for the entire team.

By offloading these complex, batch-oriented calculations to dbt, your FastAPI application can remain responsive, focusing on serving pre-processed, analytics-ready data from DuckDB. This hybrid approach leverages the strengths of both tools: FastAPI for real-time interactions and dbt for robust, maintainable, and testable batch data transformations.