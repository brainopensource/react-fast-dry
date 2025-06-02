#!/usr/bin/env python3
"""
Script to deduplicate JSON data using Polars.

This script removes duplicate records from a JSON file based on the combination of:
- well_code
- field_code  
- production_period

The input file is expected to be the mocked_response_backup.json file.
"""

import json
import polars as pl
from pathlib import Path
import argparse
import sys
from typing import Dict, Any


def load_json_data(file_path: Path) -> Dict[str, Any]:
    """
    Load JSON data from file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary containing the JSON data
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {file_path}: {e}")
        sys.exit(1)


def deduplicate_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove duplicates from the JSON data using Polars.
    
    Duplicates are identified by the combination of:
    - well_code
    - field_code
    - production_period
    
    Args:
        data: Dictionary containing the JSON data with 'value' key
        
    Returns:
        Dictionary with deduplicated data
    """
    if 'value' not in data:
        print("Error: JSON data must contain a 'value' key with the records array.")
        sys.exit(1)
    
    records = data['value']
    
    if not records:
        print("Warning: No records found in the data.")
        return data
    
    # Convert to Polars DataFrame
    df = pl.DataFrame(records)
    
    # Check if required columns exist
    required_columns = ['well_code', 'field_code', 'production_period']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        print(f"Available columns: {df.columns}")
        sys.exit(1)
    
    # Get initial count
    initial_count = len(df)
    print(f"Initial record count: {initial_count}")
    
    # Remove duplicates based on the specified columns
    # Keep the first occurrence of each duplicate group
    df_deduplicated = df.unique(subset=required_columns, keep='first')
    
    # Get final count
    final_count = len(df_deduplicated)
    duplicates_removed = initial_count - final_count
    
    print(f"Final record count: {final_count}")
    print(f"Duplicates removed: {duplicates_removed}")
    
    # Convert back to list of dictionaries
    deduplicated_records = df_deduplicated.to_dicts()
    
    # Create new data structure with deduplicated records
    result = data.copy()
    result['value'] = deduplicated_records
    
    return result


def save_json_data(data: Dict[str, Any], file_path: Path) -> None:
    """
    Save JSON data to file.
    
    Args:
        data: Dictionary containing the JSON data
        file_path: Path where to save the file
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Deduplicated data saved to: {file_path}")
    except Exception as e:
        print(f"Error saving file {file_path}: {e}")
        sys.exit(1)


def main():
    """Main function to run the deduplication process."""
    parser = argparse.ArgumentParser(
        description="Deduplicate JSON data using Polars",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deduplicate_json.py
  python deduplicate_json.py --input custom_input.json --output custom_output.json
  python deduplicate_json.py --dry-run
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        default='../external/mocked_response_backup.json',
        help='Input JSON file path (default: ../external/mocked_response_backup.json)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output JSON file path (default: input_file_deduplicated.json)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show deduplication results without saving the output file'
    )
    
    args = parser.parse_args()
    
    # Resolve input file path
    input_path = Path(args.input)
    if not input_path.is_absolute():
        # Make path relative to the script location
        script_dir = Path(__file__).parent
        input_path = script_dir / input_path
    
    # Resolve output file path
    if args.output:
        output_path = Path(args.output)
    else:
        # Create default output filename
        input_stem = input_path.stem
        output_path = input_path.parent / f"{input_stem}_deduplicated.json"
    
    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print("-" * 50)
    
    # Load data
    data = load_json_data(input_path)
    
    # Deduplicate
    deduplicated_data = deduplicate_data(data)
    
    # Save results (unless dry run)
    if not args.dry_run:
        save_json_data(deduplicated_data, output_path)
    else:
        print("\nDry run completed - no file was saved.")


if __name__ == "__main__":
    main() 