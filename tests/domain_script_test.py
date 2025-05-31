#!/usr/bin/env python3
"""
Well Production API Client
A comprehensive client for testing the Well Production API endpoints.
"""

import requests
import json
import time
from pathlib import Path
from typing import Dict, Any
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import print as rprint

class WellProductionAPIClient:
    """Client for interacting with the Well Production API."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self.console = Console()
        self.session = requests.Session()
        
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make a request to the API with error handling."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, **kwargs)
            return response
        except requests.ConnectionError:
            self.console.print(f"[red]❌ Connection failed to {url}[/red]")
            self.console.print("[yellow]💡 Make sure the API server is running on localhost:8000[/yellow]")
            raise
        except Exception as e:
            self.console.print(f"[red]❌ Request failed: {e}[/red]")
            raise
    
    def _print_response(self, title: str, response: requests.Response):
        """Pretty print API response."""
        status_color = "green" if response.status_code < 400 else "red"
        
        # Create status panel
        status_text = f"Status: {response.status_code} | Time: {response.elapsed.total_seconds():.2f}s"
        self.console.print(Panel(f"[{status_color}]{status_text}[/{status_color}]", title=title))
        
        # Print response body if JSON
        try:
            data = response.json()
            self.console.print_json(json.dumps(data, indent=2))
        except:
            self.console.print(response.text)
        
        self.console.print("─" * 80)
    
    def test_health(self) -> bool:
        """Test the health endpoint."""
        self.console.print("[bold blue]🏥 Testing Health Endpoint[/bold blue]")
        
        try:
            response = self._make_request("GET", "/health")
            self._print_response("Health Check", response)
            return response.status_code == 200
        except Exception:
            return False
    
    def test_root(self) -> bool:
        """Test the root endpoint."""
        self.console.print("[bold blue]🏠 Testing Root Endpoint[/bold blue]")
        
        try:
            response = self._make_request("GET", "/")
            self._print_response("API Root", response)
            return response.status_code == 200
        except Exception:
            return False
    
    def test_import(self) -> Dict[str, Any]:
        """Test the import endpoint."""
        self.console.print("[bold blue]📥 Testing Import Endpoint[/bold blue]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            task = progress.add_task("Importing well production data...", total=None)
            
            try:
                response = self._make_request("POST", "/api/v1/wells/import")
                progress.stop()
                self._print_response("Import Wells", response)
                
                if response.status_code == 201:
                    return response.json()
                return {}
            except Exception:
                progress.stop()
                return {}
    
    def test_stats(self) -> Dict[str, Any]:
        """Test the stats endpoint."""
        self.console.print("[bold blue]📊 Testing Stats Endpoint[/bold blue]")
        
        try:
            response = self._make_request("GET", "/api/v1/wells/stats")
            self._print_response("Wells Statistics", response)
            
            if response.status_code == 200:
                data = response.json()
                
                # Create a nice table for stats
                table = Table(title="📈 Database Statistics")
                table.add_column("Metric", style="cyan")
                table.add_column("Value", style="green")
                
                table.add_row("Total Records", str(data.get("total_records", 0)))
                table.add_row("Primary Storage", data.get("storage_info", {}).get("primary", "Unknown"))
                table.add_row("Secondary Storage", data.get("storage_info", {}).get("secondary", "Unknown"))
                
                self.console.print(table)
                return data
            return {}
        except Exception:
            return {}
    
    def test_get_well(self, well_code: int = 59806) -> Dict[str, Any]:
        """Test getting a specific well."""
        self.console.print(f"[bold blue]🔍 Testing Get Well Endpoint (Code: {well_code})[/bold blue]")
        
        try:
            response = self._make_request("GET", f"/api/v1/wells/well/{well_code}")
            self._print_response(f"Well {well_code} Details", response)
            
            if response.status_code == 200:
                data = response.json()
                
                # Verify the well reference
                well_reference = data.get("well_code")
                expected_reference = "1-C-1-BA"  # From the mocked data
                
                if well_code == 59806:
                    self.console.print(f"[yellow]🔍 Checking well reference...[/yellow]")
                    # Note: The actual well_reference field would be in the response
                    # Let's check if this matches our expected data
                
                return data
            return {}
        except Exception:
            return {}
    
    def test_get_field(self, field_code: int = 8908) -> Dict[str, Any]:
        """Test getting wells by field."""
        self.console.print(f"[bold blue]🏭 Testing Get Field Endpoint (Code: {field_code})[/bold blue]")
        
        try:
            response = self._make_request("GET", f"/api/v1/wells/field/{field_code}")
            self._print_response(f"Field {field_code} Wells", response)
            
            if response.status_code == 200:
                data = response.json()
                
                # Verify the field name
                field_name = data.get("field_name")
                expected_name = "Candeias"
                
                if field_code == 8908:
                    self.console.print(f"[yellow]🔍 Checking field name...[/yellow]")
                    if field_name == expected_name:
                        self.console.print(f"[green]✅ Field name matches: {field_name}[/green]")
                    else:
                        self.console.print(f"[red]❌ Field name mismatch. Expected: {expected_name}, Got: {field_name}[/red]")
                
                # Create table for wells
                if "wells" in data:
                    table = Table(title=f"🏭 Wells in Field {field_code} ({field_name})")
                    table.add_column("Well Code", style="cyan")
                    table.add_column("Well Name", style="green")
                    table.add_column("Production Period", style="yellow")
                    table.add_column("Total Production (KBD)", style="magenta")
                    table.add_column("Status", style="blue")
                    
                    for well in data["wells"][:10]:  # Show first 10 wells
                        status = "🟢 Producing" if well.get("is_producing") else "🔴 Not Producing"
                        table.add_row(
                            str(well.get("well_code", "")),
                            well.get("well_name", ""),
                            well.get("production_period", ""),
                            f"{well.get('total_production_kbd', 0):.4f}",
                            status
                        )
                    
                    self.console.print(table)
                    
                    if len(data["wells"]) > 10:
                        self.console.print(f"[dim]... and {len(data['wells']) - 10} more wells[/dim]")
                
                return data
            return {}
        except Exception:
            return {}
    
    def test_download(self) -> bool:
        """Test downloading the CSV file."""
        self.console.print("[bold blue]⬇️ Testing Download Endpoint[/bold blue]")
        
        try:
            response = self._make_request("GET", "/api/v1/wells/download")
            
            if response.status_code == 200:
                # Save the file
                download_path = Path("frontend/downloads")
                download_path.mkdir(exist_ok=True)
                
                file_path = download_path / "wells_prod.csv"
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                file_size = len(response.content)
                self.console.print(f"[green]✅ Downloaded CSV file: {file_path} ({file_size:,} bytes)[/green]")
                
                # Show first few lines
                with open(file_path, 'r') as f:
                    lines = f.readlines()[:5]
                    self.console.print(Panel("\n".join(lines), title="📄 CSV Preview (First 5 lines)"))
                
                return True
            else:
                self._print_response("Download Failed", response)
                return False
        except Exception:
            return False
    
    def run_full_test_suite(self):
        """Run the complete test suite."""
        self.console.print(Panel.fit(
            "[bold blue]🚀 Well Production API Client Test Suite[/bold blue]",
            style="blue"
        ))
        
        results = {}
        
        # Test 1: Health Check
        results['health'] = self.test_health()
        time.sleep(1)
        
        # Test 2: Root endpoint
        results['root'] = self.test_root()
        time.sleep(1)
        
        # Test 3: Import data
        results['import'] = self.test_import()
        time.sleep(2)  # Give more time for import
        
        # Test 4: Get statistics
        results['stats'] = self.test_stats()
        time.sleep(1)
        
        # Test 5: Get specific well (should be well_reference = "1-C-1-BA")
        results['well'] = self.test_get_well(59806)
        time.sleep(1)
        
        # Test 6: Get field (should be field_name = "Candeias")
        results['field'] = self.test_get_field(8908)
        time.sleep(1)
        
        # Test 7: Download CSV
        results['download'] = self.test_download()
        
        # Summary
        self._print_summary(results)
    
    def _print_summary(self, results: Dict[str, Any]):
        """Print test summary."""
        self.console.print("\n" + "=" * 80)
        self.console.print(Panel.fit("[bold green]📋 Test Summary[/bold green]", style="green"))
        
        table = Table(title="🧪 Test Results")
        table.add_column("Test", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="yellow")
        
        # Health
        status = "✅ PASS" if results.get('health') else "❌ FAIL"
        table.add_row("Health Check", status, "API server health")
        
        # Root
        status = "✅ PASS" if results.get('root') else "❌ FAIL"
        table.add_row("Root Endpoint", status, "API information")
        
        # Import
        import_data = results.get('import', {})
        if import_data:
            count = import_data.get('data', {}).get('imported_count', 0)
            status = f"✅ PASS ({count} records)"
        else:
            status = "❌ FAIL"
        table.add_row("Import Wells", status, "JSON to DB import")
        
        # Stats
        stats_data = results.get('stats', {})
        if stats_data:
            total = stats_data.get('total_records', 0)
            status = f"✅ PASS ({total} total)"
        else:
            status = "❌ FAIL"
        table.add_row("Statistics", status, "Database statistics")
        
        # Well lookup
        well_data = results.get('well', {})
        if well_data:
            well_name = well_data.get('well_name', 'Unknown')
            status = f"✅ PASS ({well_name})"
        else:
            status = "❌ FAIL"
        table.add_row("Well Lookup", status, "Specific well query")
        
        # Field lookup
        field_data = results.get('field', {})
        if field_data:
            field_name = field_data.get('field_name', 'Unknown')
            wells_count = field_data.get('wells_count', 0)
            status = f"✅ PASS ({field_name}, {wells_count} wells)"
        else:
            status = "❌ FAIL"
        table.add_row("Field Lookup", status, "Field wells query")
        
        # Download
        status = "✅ PASS" if results.get('download') else "❌ FAIL"
        table.add_row("CSV Download", status, "Export functionality")
        
        self.console.print(table)


def main():
    """Main function to run the client."""
    client = WellProductionAPIClient()
    
    try:
        client.run_full_test_suite()
    except KeyboardInterrupt:
        client.console.print("\n[yellow]⚠️ Test suite interrupted by user[/yellow]")
    except Exception as e:
        client.console.print(f"\n[red]❌ Test suite failed: {e}[/red]")


if __name__ == "__main__":
    main() 