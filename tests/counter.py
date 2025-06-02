#!/usr/bin/env python3
"""
Script to count the number of dictionaries inside the "value" key 
from JSON files that may contain malformed escape sequences.
"""

import json
import os
import re
from pathlib import Path


def fix_json_escape_sequences(json_text):
    """
    Fix common JSON escape sequence issues.
    
    Args:
        json_text (str): The JSON text with potential escape issues
        
    Returns:
        str: JSON text with fixed escape sequences
    """
    # Fix invalid escape sequences like \ô, \ã, etc.
    # Replace them with properly escaped versions
    json_text = re.sub(r'\\([^"\\\/bfnrt])', r'\\\\\\1', json_text)
    return json_text


def count_dicts_with_regex(file_path):
    """
    Count dictionaries using regex pattern matching as a fallback.
    
    Args:
        file_path (str): Path to the JSON file
        
    Returns:
        int: Estimated number of dictionaries in the "value" array
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Find the "value" array section
        value_match = re.search(r'"value"\s*:\s*\[(.*)\]', content, re.DOTALL)
        if not value_match:
            print("Could not find 'value' array in file")
            return 0
        
        value_content = value_match.group(1)
        
        # Count opening braces that start dictionary objects
        # This is a simple heuristic - count { that are followed by "
        dict_pattern = r'\{\s*"'
        matches = re.findall(dict_pattern, value_content)
        
        return len(matches)
        
    except Exception as e:
        print(f"Error in regex counting: {e}")
        return 0


def count_dicts_line_by_line(file_path):
    """
    Count dictionaries by processing the file line by line.
    
    Args:
        file_path (str): Path to the JSON file
        
    Returns:
        int: Number of dictionaries found
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        in_value_array = False
        dict_count = 0
        brace_level = 0
        
        for line in lines:
            line = line.strip()
            
            # Check if we're entering the value array
            if '"value"' in line and '[' in line:
                in_value_array = True
                continue
            
            if in_value_array:
                # Count opening braces for new dictionaries
                if line.startswith('{'):
                    dict_count += 1
                
                # Check if we're exiting the value array
                if ']' in line and brace_level == 0:
                    break
                
                # Track brace levels to handle nested structures
                brace_level += line.count('{') - line.count('}')
        
        return dict_count
        
    except Exception as e:
        print(f"Error in line-by-line counting: {e}")
        return 0


def count_dicts_in_value(json_file_path):
    """
    Count the number of dictionaries inside the "value" key of a JSON file.
    Uses multiple strategies to handle malformed JSON.
    
    Args:
        json_file_path (str): Path to the JSON file
        
    Returns:
        int: Number of dictionaries in the "value" list
    """
    # Strategy 1: Try standard JSON parsing
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        if "value" not in data:
            print("Error: 'value' key not found in JSON file")
            return 0
        
        value_list = data["value"]
        
        if not isinstance(value_list, list):
            print("Error: 'value' is not a list")
            return 0
        
        dict_count = sum(1 for item in value_list if isinstance(item, dict))
        print("✓ Successfully parsed JSON using standard method")
        return dict_count
        
    except json.JSONDecodeError as e:
        print(f"⚠ Standard JSON parsing failed: {e}")
        print("Trying alternative methods...")
        
        # Strategy 2: Try fixing escape sequences
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            fixed_content = fix_json_escape_sequences(content)
            data = json.loads(fixed_content)
            
            if "value" in data and isinstance(data["value"], list):
                dict_count = sum(1 for item in data["value"] if isinstance(item, dict))
                print("✓ Successfully parsed JSON after fixing escape sequences")
                return dict_count
                
        except Exception as e2:
            print(f"⚠ Fixed JSON parsing also failed: {e2}")
        
        # Strategy 3: Use regex-based counting
        print("Trying regex-based counting...")
        regex_count = count_dicts_with_regex(json_file_path)
        if regex_count > 0:
            print(f"✓ Regex method found {regex_count} dictionaries")
            return regex_count
        
        # Strategy 4: Use line-by-line counting
        print("Trying line-by-line counting...")
        line_count = count_dicts_line_by_line(json_file_path)
        if line_count > 0:
            print(f"✓ Line-by-line method found {line_count} dictionaries")
            return line_count
        
        print("❌ All counting methods failed")
        return 0
        
    except FileNotFoundError:
        print(f"Error: File '{json_file_path}' not found")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 0


def get_sample_data_info(json_file_path):
    """
    Try to get sample information about the data structure.
    
    Args:
        json_file_path (str): Path to the JSON file
    """
    try:
        # Try to extract just the first few lines of the value array
        with open(json_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Find the start of the first dictionary in the value array
        value_start = content.find('"value"')
        if value_start == -1:
            return
        
        first_dict_start = content.find('{', value_start)
        if first_dict_start == -1:
            return
        
        # Extract a sample of the first dictionary (first 500 chars)
        sample = content[first_dict_start:first_dict_start + 500]
        
        # Try to extract field names using regex
        field_pattern = r'"([^"]+)"\s*:'
        fields = re.findall(field_pattern, sample)
        
        if fields:
            print(f"\nSample fields found in dictionaries:")
            print(f"Fields: {fields[:10]}...")  # Show first 10 fields
            
    except Exception as e:
        print(f"Could not extract sample data info: {e}")


def main():
    """Main function to run the counter script."""
    current_dir = Path(__file__).parent
    project_root = current_dir.parent
    json_file_path = project_root / "external" / "mocked_response_100K.json"
    
    print(f"Reading JSON file: {json_file_path}")
    print("=" * 60)
    
    if not json_file_path.exists():
        print(f"Error: File '{json_file_path}' does not exist")
        return
    
    # Count the dictionaries
    count = count_dicts_in_value(json_file_path)
    
    print("\n" + "=" * 60)
    print(f"RESULTS:")
    print(f"Number of dictionaries in 'value' key: {count}")
    
    # Try to get additional information
    get_sample_data_info(json_file_path)
    
    # File size info
    try:
        file_size = json_file_path.stat().st_size
        print(f"\nFile size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
    except Exception as e:
        print(f"Could not get file size: {e}")


if __name__ == "__main__":
    main() 