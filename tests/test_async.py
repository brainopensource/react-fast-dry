import requests
import time
import threading

def test_health():
    """Test health endpoint while import is running"""
    for i in range(10):
        try:
            start = time.time()
            resp = requests.get('http://127.0.0.1:8080/health', timeout=5)
            duration = time.time() - start
            print(f"Health check {i+1}: {duration:.2f}s - Status: {resp.status_code}")
        except Exception as e:
            print(f"Health check {i+1}: ERROR - {e}")
        time.sleep(1)

def test_import_trigger():
    """Test import trigger endpoint for wells_production dataset"""
    try:
        print("Testing import trigger...")
        start = time.time()
        resp = requests.get('http://127.0.0.1:8080/api/v1/wells_production/import/trigger', timeout=10)
        duration = time.time() - start
        print(f"Import trigger response time: {duration:.2f}s")
        print(f"Status code: {resp.status_code}")
        print(f"Response: {resp.json()}")
        
        if resp.status_code == 200:
            data = resp.json()
            job_id = data.get('data', {}).get('job_id')
            if job_id:
                print(f"Job ID received: {job_id}")
                return job_id
    except Exception as e:
        print(f"Import trigger ERROR: {e}")
    return None

def test_job_status(job_id):
    """Test job status endpoint"""
    if not job_id:
        return
    
    for i in range(5):
        try:
            start = time.time()
            resp = requests.get(f'http://127.0.0.1:8080/api/v1/wells_production/import/status/{job_id}', timeout=5)
            duration = time.time() - start
            print(f"Status check {i+1}: {duration:.2f}s - {resp.json()}")
        except Exception as e:
            print(f"Status check {i+1}: ERROR - {e}")
        time.sleep(2)

def test_datasets_list():
    """Test the datasets list endpoint"""
    try:
        print("Testing datasets list...")
        start = time.time()
        resp = requests.get('http://127.0.0.1:8080/api/v1/datasets', timeout=5)
        duration = time.time() - start
        print(f"Datasets list response time: {duration:.2f}s")
        print(f"Status code: {resp.status_code}")
        print(f"Available datasets: {resp.json()}")
    except Exception as e:
        print(f"Datasets list ERROR: {e}")

if __name__ == "__main__":
    print("=== Testing Async Behavior ===")
    
    # Test 0: Check available datasets
    test_datasets_list()
    
    # Test 1: Trigger import and get job ID
    job_id = test_import_trigger()
    
    # Test 2: Start health checks in background
    health_thread = threading.Thread(target=test_health)
    health_thread.daemon = True
    health_thread.start()
    
    # Test 3: Check job status
    test_job_status(job_id)
    
    print("=== Test Complete ===") 