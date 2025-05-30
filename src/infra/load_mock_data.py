import json
import duckdb

def load_mock_data():
    # Load the JSON data
    with open('external/mocked_response.json', 'r') as file:
        data = json.load(file)
    
    # Extract the records from the JSON data
    records = data['value']
    
    # Transform the records to match the wells_production table schema
    transformed_records = []
    for record in records:
        transformed_record = {
            'well_code': record['well_code'],
            'well_name': record['well_name'],
            'field_code': record['field_code'],
            'field_name': record['_field_name'],
            'prod_oil_kbd': record['oil_production_kbd'],
            'prod_gas_mcf': record['gas_production_mmcfd'],
            'prod_water_kbd': record['water_production_kbd'],
            'production_period': record['production_period'].split('T')[0],
            'days_on_production': record['days_on_production'],
            'liquids_production_kbd': record['liquids_production_kbd'],
            'data_source': record['data_source'],
            'source_data': record['source_data'],
            'partition_0': record['partition_0']
        }
        transformed_records.append(transformed_record)
    
    # Connect to the DuckDB database
    con = duckdb.connect('data/duckdb.db')
    
    # Create a temporary table to hold the transformed data
    con.execute("""
        CREATE TEMP TABLE temp_df (
            well_code TEXT,
            well_name TEXT,
            field_code TEXT,
            field_name TEXT,
            prod_oil_kbd DOUBLE,
            prod_gas_mcf DOUBLE,
            prod_water_kbd DOUBLE,
            production_period DATE,
            days_on_production INTEGER,
            liquids_production_kbd DOUBLE,
            data_source TEXT,
            source_data TEXT,
            partition_0 TEXT
        )
    """)
    
    # Insert the transformed data into the temporary table
    con.execute("INSERT INTO temp_df VALUES", transformed_records)
    
    # Insert the data from the temporary table into the wells_production table
    con.execute("INSERT INTO wells_production SELECT * FROM temp_df")
    
    # Close the connection
    con.close()

if __name__ == "__main__":
    load_mock_data()
