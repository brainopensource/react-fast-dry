#!/usr/bin/env python3
"""
Load Test Data Generator - Simplified for Speed

This script generates random records based on the mocked_response.json schema
for load testing and performance evaluation.

Usage:
    python create_json_load.py --records 1000 --output load_test_data.json
"""

import json
import random
import argparse
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any
import os


class LoadTestDataGenerator:
    """Generates random records following the mocked_response.json schema."""
    
    def __init__(self):
        # Fixed source data as requested
        self.SOURCE_DATA_FIXED = """{"Year":"1946","Month_Year":"01/1946","State":"Bahia","Basin":"Recôncavo","Field":"CANDEIAS","Well":"1-C-1-BA","Type":"Terra","Instalation":"Não Informado","Oil_Production_M3":"37,5","Cond_Production_M3":"0","Ass_Gas_Prod_MM3":"0","Non_Ass_Gas_Prod_MM3":"0","Water_Production_M3":"0","Gas_Injection_MM3":null,"Water_injec_sec_recovery_M3":null,"Water_Injec_discard_M3":null,"CO2_Injec_MM3":null,"N_Inject_MM3":null,"Steam_Injec_Ton":null,"Polym_Injec_M3":null,"Other_fluids_injec_M3":null,"Location":"Onshore"}"""
        
        # Data source
        self.data_source = "Brazil - Agência Nacional do Petróleo (ANP)"

    def generate_record(self) -> Dict[str, Any]:
        """Generate a single random record following the schema."""
        field_number = random.randint(1, 500)
        field_name = f'Field_number_{field_number}'
        field_code = field_number
        well_code = random.randint(1, 1000)
        well_reference = f"{random.randint(1, 5)}-{field_name[0].upper()}-{random.randint(1, 5)}-BA"
        
        # Simple date generation
        year = random.randint(1946, 2024)
        month = random.randint(1, 12)
        production_period = f"{year}-{month:02d}-01T00:00:00+00:00"
        
        # Simple production values
        oil_production_kbd = round(random.uniform(0.0001, 1.0), 10)
        gas_production_mmcfd = round(random.uniform(0, 10.0), 7) if random.random() > 0.6 else 0
        liquids_production_kbd = round(random.uniform(0, 0.5), 8) if random.random() > 0.8 else 0
        water_production_kbd = round(random.uniform(0, 2.0), 8) if random.random() > 0.5 else 0
        days_on_production = random.randint(24, 31)
        
        return {
            "field_code": field_code,
            "_field_name": field_name,
            "well_code": well_code,
            "_well_reference": well_reference,
            "well_name": well_reference,
            "production_period": production_period,
            "days_on_production": days_on_production,
            "oil_production_kbd": oil_production_kbd,
            "gas_production_mmcfd": gas_production_mmcfd,
            "liquids_production_kbd": liquids_production_kbd,
            "water_production_kbd": water_production_kbd,
            "data_source": self.data_source,
            "source_data": self.SOURCE_DATA_FIXED,
            "partition_0": "latest"
        }

    def generate_dataset(self, num_records: int) -> Dict[str, Any]:
        """Generate a complete dataset with the specified number of records."""
        records = [self.generate_record() for _ in range(num_records)]
        
        return {
            "@odata.context": "https://data.com/query-internal/",
            "value": records
        }

    def save_to_file(self, dataset: Dict[str, Any], filename: str) -> None:
        """Save the dataset to a JSON file."""
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, indent=2, ensure_ascii=False)


def main():
    """Main function to handle command line arguments and generate data."""
    parser = argparse.ArgumentParser(
        description="Generate random records for load testing"
    )
    
    parser.add_argument(
        '--records', '-r',
        type=int,
        default=1000,
        help='Number of records to generate (default: 1000)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='load_test_data.json',
        help='Output filename (default: load_test_data.json)'
    )
    
    parser.add_argument(
        '--seed', '-s',
        type=int,
        help='Random seed for reproducible results'
    )
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
    
    if args.records <= 0:
        return 1
    
    generator = LoadTestDataGenerator()
    dataset = generator.generate_dataset(args.records)
    generator.save_to_file(dataset, args.output)
    
    return 0


if __name__ == "__main__":
    exit(main()) 