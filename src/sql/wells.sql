-- name: init
CREATE TABLE IF NOT EXISTS wells_production (
    well_code TEXT,
    well_name TEXT,
    field_code TEXT,
    field_name TEXT,
    prod_oil_kbd DOUBLE,
    prod_gas_mcf DOUBLE,
    prod_water_kbd DOUBLE,
    production_period DATE
);

-- name: insert
INSERT INTO wells_production SELECT * FROM temp_df;

-- name: search
SELECT * FROM wells_production WHERE well_name ILIKE :name;

-- name: get_by_code_period
SELECT * FROM wells_production WHERE well_code = :code AND production_period = :period;
