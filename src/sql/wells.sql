-- name: init
CREATE TABLE IF NOT EXISTS wells_production (
    field_code INTEGER,
    field_name TEXT,
    well_code INTEGER,
    well_reference TEXT,
    well_name TEXT,
    production_period TEXT, -- ISO string, can be changed to TIMESTAMP if needed
    days_on_production INTEGER,
    oil_production_kbd DOUBLE,
    gas_production_mmcfd DOUBLE,
    liquids_production_kbd DOUBLE,
    water_production_kbd DOUBLE,
    data_source TEXT,
    source_data TEXT,
    partition_0 TEXT
);

-- name: insert
INSERT INTO wells_production SELECT * FROM temp_df;

-- name: search
SELECT * FROM wells_production WHERE well_name ILIKE $name;

-- name: get_by_code_period
SELECT * FROM wells_production WHERE well_code = $code AND production_period = $period;
