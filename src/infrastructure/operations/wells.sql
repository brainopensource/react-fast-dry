-- name: create_table
CREATE TABLE IF NOT EXISTS well_production (
    field_code INTEGER NOT NULL,
    _field_name VARCHAR NOT NULL,
    well_code INTEGER NOT NULL,
    _well_reference VARCHAR NOT NULL,
    well_name VARCHAR NOT NULL,
    production_period VARCHAR NOT NULL,
    days_on_production INTEGER NOT NULL,
    oil_production_kbd DOUBLE NOT NULL,
    gas_production_mmcfd DOUBLE NOT NULL,
    liquids_production_kbd DOUBLE NOT NULL,
    water_production_kbd DOUBLE NOT NULL,
    data_source VARCHAR NOT NULL,
    source_data VARCHAR NOT NULL,
    partition_0 VARCHAR NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (well_code, field_code, production_period)
);

-- name: create_indexes
CREATE INDEX IF NOT EXISTS idx_field_code ON well_production(field_code);
CREATE INDEX IF NOT EXISTS idx_well_code ON well_production(well_code);
CREATE INDEX IF NOT EXISTS idx_production_period ON well_production(production_period);
CREATE INDEX IF NOT EXISTS idx_composite_key ON well_production(well_code, field_code, production_period);

-- name: get_by_well_code
SELECT * FROM well_production WHERE well_code = ? ORDER BY production_period DESC;

-- name: get_by_field_code
SELECT * FROM well_production WHERE field_code = ?;

-- name: get_all
SELECT * FROM well_production ORDER BY well_code, field_code, production_period;

-- name: count_all
SELECT COUNT(*) FROM well_production;

-- name: insert_single
INSERT OR REPLACE INTO well_production (
    field_code, _field_name, well_code, _well_reference, well_name,
    production_period, days_on_production, oil_production_kbd,
    gas_production_mmcfd, liquids_production_kbd, water_production_kbd,
    data_source, source_data, partition_0, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: insert_bulk
INSERT OR REPLACE INTO well_production (
    field_code, _field_name, well_code, _well_reference, well_name,
    production_period, days_on_production, oil_production_kbd,
    gas_production_mmcfd, liquids_production_kbd, water_production_kbd,
    data_source, source_data, partition_0, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: search_by_name
SELECT * FROM well_production WHERE well_name ILIKE ?;

-- name: get_by_code_and_period
SELECT * FROM well_production WHERE well_code = ? AND production_period = ?;

-- name: check_exists
SELECT COUNT(*) FROM well_production WHERE well_code = ? AND field_code = ? AND production_period = ?;

-- name: get_existing_records
SELECT well_code, field_code, production_period FROM well_production 
WHERE (well_code, field_code, production_period) IN (VALUES (?, ?, ?));

-- name: insert_from_temp
INSERT INTO well_production SELECT * FROM temp_df;
