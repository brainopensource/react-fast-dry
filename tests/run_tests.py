#!/usr/bin/env python3
"""
Test runner script for the Well Production API tests.

This script provides convenient commands to run different types of tests
with proper setup and configuration.
"""

import sys
import subprocess
import argparse
from pathlib import Path


def run_command(cmd, description="Running command"):
    """Run a command and handle the output."""
    print(f"\n🔄 {description}...")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"✅ {description} completed successfully!")
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"❌ Command not found: {cmd[0]}")
        print("Make sure pytest is installed: pip install -r tests/requirements.txt")
        return False


def run_all_tests():
    """Run all tests with coverage."""
    cmd = [
        "pytest", "tests/",
        "-v",
        "--cov=src",
        "--cov-report=term-missing",
        "--cov-report=html"
    ]
    return run_command(cmd, "Running all tests with coverage")


def run_api_tests():
    """Run API tests only."""
    cmd = ["pytest", "tests/api/", "-v"]
    return run_command(cmd, "Running API tests")


def run_integration_tests():
    """Run integration tests only."""
    cmd = ["pytest", "tests/integration/", "-v"]
    return run_command(cmd, "Running integration tests")


def run_unit_tests():
    """Run unit tests only."""
    cmd = ["pytest", "tests/unit/", "-v"]
    return run_command(cmd, "Running unit tests")


def run_quick_tests():
    """Run a quick subset of tests for development."""
    cmd = [
        "pytest", "tests/api/test_health.py", 
        "tests/api/test_wells.py::TestWellEndpoints::test_import_wells_endpoint",
        "-v"
    ]
    return run_command(cmd, "Running quick test suite")


def run_with_markers(marker):
    """Run tests with specific markers."""
    cmd = ["pytest", "tests/", "-v", "-m", marker]
    return run_command(cmd, f"Running tests with marker '{marker}'")


def check_dependencies():
    """Check if test dependencies are installed."""
    print("🔍 Checking test dependencies...")
    
    try:
        import pytest
        print(f"✅ pytest {pytest.__version__} is installed")
    except ImportError:
        print("❌ pytest is not installed")
        print("Install with: pip install -r tests/requirements.txt")
        return False
    
    try:
        import httpx
        print(f"✅ httpx {httpx.__version__} is installed")
    except ImportError:
        print("❌ httpx is not installed")
        return False
    
    try:
        from fastapi.testclient import TestClient
        print("✅ FastAPI TestClient is available")
    except ImportError:
        print("❌ FastAPI TestClient is not available")
        return False
    
    return True


def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(description="Run Well Production API tests")
    parser.add_argument(
        "test_type",
        nargs="?",
        choices=["all", "api", "integration", "unit", "quick"],
        default="all",
        help="Type of tests to run (default: all)"
    )
    parser.add_argument(
        "--marker", "-m",
        help="Run tests with specific marker"
    )
    parser.add_argument(
        "--check-deps",
        action="store_true",
        help="Check if test dependencies are installed"
    )
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="Install test dependencies"
    )
    
    args = parser.parse_args()
    
    # Change to the project root directory
    project_root = Path(__file__).parent.parent
    import os
    os.chdir(project_root)
    
    print("🧪 Well Production API Test Runner")
    print("=" * 50)
    
    if args.install_deps:
        cmd = ["pip", "install", "-r", "tests/requirements.txt"]
        if run_command(cmd, "Installing test dependencies"):
            print("✅ Test dependencies installed successfully!")
        else:
            print("❌ Failed to install test dependencies")
            return 1
    
    if args.check_deps:
        if check_dependencies():
            print("✅ All test dependencies are installed")
        else:
            print("❌ Some test dependencies are missing")
            print("Run with --install-deps to install them")
            return 1
    
    # Check dependencies before running tests
    if not check_dependencies():
        print("\n❌ Missing test dependencies. Run with --install-deps to install them.")
        return 1
    
    # Run tests based on the specified type
    success = False
    
    if args.marker:
        success = run_with_markers(args.marker)
    elif args.test_type == "all":
        success = run_all_tests()
    elif args.test_type == "api":
        success = run_api_tests()
    elif args.test_type == "integration":
        success = run_integration_tests()
    elif args.test_type == "unit":
        success = run_unit_tests()
    elif args.test_type == "quick":
        success = run_quick_tests()
    
    if success:
        print("\n🎉 Tests completed successfully!")
        if args.test_type == "all":
            print("📊 Coverage report generated in htmlcov/index.html")
        return 0
    else:
        print("\n💥 Tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 