#!/usr/bin/env python3
"""
Production-ready runner for the Wells Production Data API.
This script provides different modes for running the application.
"""
import os
import sys
import argparse
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/wells_api.log", mode="a")
        ]
    )

def run_development():
    """Run in development mode with auto-reload"""
    import uvicorn
    from src.main import app
    
    setup_logging("DEBUG")
    
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)
    
    print("🚀 Starting Wells Production API in DEVELOPMENT mode...")
    print("📊 API Documentation: http://localhost:8000/docs")
    print("🔍 Health Check: http://localhost:8000/health")
    print("📈 Statistics: http://localhost:8000/api/v1/stats")
    
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"],
        log_level="debug",
        access_log=True
    )

def run_production():
    """Run in production mode"""
    import uvicorn
    from src.main import app
    
    setup_logging("INFO")
    
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)
    
    print("🏭 Starting Wells Production API in PRODUCTION mode...")
    print("📊 API Documentation: http://localhost:8000/docs")
    print("🔍 Health Check: http://localhost:8000/health")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True,
        workers=1  # Single worker for now, can be increased based on needs
    )

def health_check():
    """Perform a health check of the running API"""
    import httpx
    import time
    
    url = "http://localhost:8000/health"
    max_attempts = 3
    
    print("🔍 Performing health check...")
    
    for attempt in range(max_attempts):
        try:
            response = httpx.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                print("✅ Health check PASSED!")
                print(f"   Status: {data.get('status', 'unknown')}")
                print(f"   Database: {data.get('database', 'unknown')}")
                print(f"   Version: {data.get('version', 'unknown')}")
                return True
            else:
                print(f"❌ Health check failed with status {response.status_code}")
        except httpx.ConnectError:
            print(f"🔄 Attempt {attempt + 1}/{max_attempts}: API not responding...")
            if attempt < max_attempts - 1:
                time.sleep(2)
        except Exception as e:
            print(f"❌ Health check error: {e}")
    
    print("❌ Health check FAILED after all attempts!")
    return False

def test_stats():
    """Test the statistics endpoint"""
    import httpx
    
    url = "http://localhost:8000/api/v1/stats"
    
    print("📈 Testing statistics endpoint...")
    
    try:
        response = httpx.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            print("✅ Statistics endpoint working!")
            print(f"   Total records: {data.get('total_records', 'unknown')}")
            print(f"   Active wells: {data.get('active_wells', 'unknown')}")
            print(f"   External API status: {data.get('external_api_status', 'unknown')}")
            return True
        else:
            print(f"❌ Statistics failed with status {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"❌ Statistics test error: {e}")
    
    return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Wells Production Data API Runner")
    parser.add_argument(
        "command",
        choices=["dev", "prod", "health", "stats", "test"],
        help="Command to run"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    if args.command == "dev":
        run_development()
    elif args.command == "prod":
        run_production()
    elif args.command == "health":
        success = health_check()
        sys.exit(0 if success else 1)
    elif args.command == "stats":
        success = test_stats()
        sys.exit(0 if success else 1)
    elif args.command == "test":
        print("🧪 Running comprehensive tests...")
        health_success = health_check()
        stats_success = test_stats()
        overall_success = health_success and stats_success
        print(f"🎯 Overall test result: {'✅ PASSED' if overall_success else '❌ FAILED'}")
        sys.exit(0 if overall_success else 1)

if __name__ == "__main__":
    main() 