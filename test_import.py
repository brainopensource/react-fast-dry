#!/usr/bin/env python3
"""
Test script to identify import issues step by step.
"""

import sys
import traceback

def test_import(module_name, description):
    """Test importing a module and report results."""
    try:
        print(f"Testing {description}...")
        __import__(module_name)
        print(f"✅ {description} imported successfully")
        return True
    except Exception as e:
        print(f"❌ {description} failed: {str(e)}")
        traceback.print_exc()
        return False

def main():
    """Run import tests."""
    print("=== Import Test Suite ===\n")
    
    tests = [
        ("src.shared.config.settings", "Settings"),
        ("src.shared.config.schemas", "Schemas"),
        ("src.domain.ports.repository", "Repository Ports"),
        ("src.domain.ports.services", "Service Ports"),
        ("src.shared.generic_dependencies", "Generic Dependencies"),
        ("src.interfaces.api.generic_dataset_router", "Generic Router"),
        ("src.main", "Main Application"),
    ]
    
    passed = 0
    total = len(tests)
    
    for module_name, description in tests:
        if test_import(module_name, description):
            passed += 1
        print()
    
    print(f"=== Results: {passed}/{total} tests passed ===")
    
    if passed == total:
        print("🎉 All imports successful! The application should start.")
    else:
        print("💥 Some imports failed. Fix these issues before starting the server.")

if __name__ == "__main__":
    main() 