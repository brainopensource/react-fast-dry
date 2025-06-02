import tkinter as tk
from tkinter import ttk, messagebox
import requests
import threading
import json
import time

# --- Model Component ---
class ApiModel:
    """
    Manages all data interactions with the FastAPI backend.
    Responsible for making API requests and handling raw responses.
    """
    BASE_URL = "http://127.0.0.1:8080" # Base URL for your FastAPI server

    def _make_request(self, method, endpoint, data=None):
        """Helper to make HTTP requests and handle common errors."""
        url = f"{self.BASE_URL}{endpoint}"
        try:
            if method == 'GET':
                response = requests.get(url, timeout=30)
            elif method == 'POST':
                response = requests.post(url, json=data, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Return status code and response content for all cases
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = response.text

            return response.status_code, response_data
            
        except requests.exceptions.ConnectionError:
            error_msg = f"Could not connect to FastAPI server at {self.BASE_URL}. Please ensure the server is running."
            return None, error_msg
        except requests.exceptions.Timeout:
            error_msg = "The request to the server timed out."
            return None, error_msg
        except requests.exceptions.HTTPError as e:
            error_msg = f"Server responded with an error: {e.response.status_code} - {e.response.text}"
            return e.response.status_code, error_msg
        except requests.exceptions.RequestException as e:
            error_msg = f"An unexpected error occurred: {e}"
            return None, error_msg
        except ValueError as e:
            error_msg = str(e)
            return None, error_msg
        except Exception as e:
            error_msg = f"An unexpected error occurred: {e}"
            return None, error_msg    # Removed wells, fields, and production trends methods

    def trigger_wells_import(self):
        """Triggers the wells import process via the API."""
        return self._make_request('GET', '/api/v1/wells/import/trigger')

    def get_import_status(self, job_id):
        """Gets the status of an import job by job_id."""
        return self._make_request('GET', f'/api/v1/wells/import/status/{job_id}')

    def get_well_by_code(self, well_code):
        """Fetches well data by well code from the API."""
        return self._make_request('GET', f'/api/v1/wells/well/{well_code}')


# --- View Component ---
class HomeTabFrame(ttk.Frame):
    """View for the Home tab."""
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()

    def create_widgets(self):
        ttk.Label(self, text="Welcome to the Well Production Monitoring App!",
                  font=("Helvetica", 16, "bold")).pack(pady=20)
        ttk.Label(self, text="Use the tabs above to explore wells, production data, and manage imports.").pack(pady=10)
        ttk.Label(self, text="Developed with Tkinter and FastAPI.").pack(pady=5)

class SyncTriggerTabFrame(ttk.Frame):
    """View for the Sync Trigger tab."""
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.status_label = None
        self.text_widget = None

        self.create_widgets()

    def create_widgets(self):
        # Header
        ttk.Label(self, text="Trigger Wells Data Import:", font=("Helvetica", 14, "bold")).pack(pady=20)
        
        # Control frame
        control_frame = ttk.Frame(self)
        control_frame.pack(pady=10)
        
        ttk.Button(control_frame, text="Trigger Import Now", command=self.controller.trigger_wells_import).pack(pady=10)
        
        # Status label
        self.status_label = ttk.Label(control_frame, text="Ready to trigger import.")
        self.status_label.pack(pady=10)
        
        # Response frame
        response_frame = ttk.LabelFrame(self, text="API Response", padding=10)
        response_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Text widget for displaying response
        text_frame = ttk.Frame(response_frame)
        text_frame.pack(fill='both', expand=True)

        self.text_widget = tk.Text(text_frame, wrap=tk.WORD, height=15, font=("Consolas", 10))
        self.text_widget.pack(side='left', fill='both', expand=True)

        # Scrollbar for text widget
        scrollbar = ttk.Scrollbar(text_frame, command=self.text_widget.yview)
        scrollbar.pack(side='right', fill='y')
        self.text_widget.configure(yscrollcommand=scrollbar.set)
        
        # Initial message
        self.text_widget.insert(tk.END, "Response will be shown here after triggering import.\n")
        self.text_widget.config(state=tk.DISABLED)

    def update_status(self, message, is_error=False):
        """Updates the status message displayed to the user."""
        if self.status_label:
            color = "red" if is_error else "blue"
            self.status_label.config(text=message, foreground=color)

    def append_response(self, text):
        """Appends text to the response text widget."""
        self.text_widget.config(state=tk.NORMAL)
        self.text_widget.insert(tk.END, text)
        self.text_widget.see(tk.END)  # Scroll to bottom
        self.text_widget.config(state=tk.DISABLED)

    def set_response(self, text):
        """Sets the text in the response text widget."""
        self.text_widget.config(state=tk.NORMAL)
        self.text_widget.delete(1.0, tk.END)
        self.text_widget.insert(tk.END, text)
        self.text_widget.config(state=tk.DISABLED)


class WellLookupTabFrame(ttk.Frame):
    """View for the Well Lookup tab."""
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.status_label = None
        self.entry_widget = None
        self.treeview = None
        self.info_text = None

        self.create_widgets()

    def create_widgets(self):
        # Header
        ttk.Label(self, text="Well Lookup by Code:", font=("Helvetica", 14, "bold")).pack(pady=20)
        
        # Input frame
        input_frame = ttk.Frame(self)
        input_frame.pack(pady=10, fill='x', padx=20)
        
        ttk.Label(input_frame, text="Well Code:").pack(side='left', padx=5)
        self.entry_widget = ttk.Entry(input_frame, width=20)
        self.entry_widget.pack(side='left', padx=5)
        
        search_button = ttk.Button(input_frame, text="Search Well", command=self.controller.search_well_by_code)
        search_button.pack(side='left', padx=5)
        
        # Bind Enter key to search
        self.entry_widget.bind('<Return>', lambda event: self.controller.search_well_by_code())
        
        # Status label
        self.status_label = ttk.Label(input_frame, text="Enter a well code and click Search")
        self.status_label.pack(side='left', padx=20)
        
        # Info frame for metadata
        info_frame = ttk.LabelFrame(self, text="Well Information", padding=10)
        info_frame.pack(fill='x', padx=10, pady=5)
        
        self.info_text = tk.Text(info_frame, height=4, font=("Consolas", 9))
        self.info_text.pack(fill='x')
        self.info_text.config(state=tk.DISABLED)
        
        # Treeview frame
        tree_frame = ttk.LabelFrame(self, text="Production Records", padding=10)
        tree_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Treeview for displaying production data
        tree_container = ttk.Frame(tree_frame)
        tree_container.pack(fill='both', expand=True)

        self.treeview = ttk.Treeview(tree_container, show="headings")
        self.treeview.pack(side='left', fill='both', expand=True)

        # Scrollbars for treeview
        v_scrollbar = ttk.Scrollbar(tree_container, orient="vertical", command=self.treeview.yview)
        v_scrollbar.pack(side='right', fill='y')
        self.treeview.configure(yscrollcommand=v_scrollbar.set)

        h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.treeview.xview)
        h_scrollbar.pack(side='bottom', fill='x')
        self.treeview.configure(xscrollcommand=h_scrollbar.set)

    def get_well_code_input(self):
        """Get the well code from the input field."""
        if self.entry_widget:
            return self.entry_widget.get().strip()
        return ""

    def clear_input(self):
        """Clear the input field."""
        if self.entry_widget:
            self.entry_widget.delete(0, tk.END)

    def update_status(self, message, is_error=False):
        """Updates the status message displayed to the user."""
        if self.status_label:
            color = "red" if is_error else "blue"
            self.status_label.config(text=message, foreground=color)

    def display_well_info(self, well_data):
        """Display general well information."""
        if not self.info_text:
            return
            
        self.info_text.config(state=tk.NORMAL)
        self.info_text.delete(1.0, tk.END)
        
        if well_data and 'data' in well_data:
            data = well_data['data']
            info_text = f"Well Code: {data.get('well_code', 'N/A')}\n"
            info_text += f"Records Found: {data.get('records_found', 0)}\n"
            info_text += f"Message: {well_data.get('message', 'N/A')}\n"
            info_text += f"Response Time: {well_data.get('metadata', {}).get('execution_time_ms', 'N/A')} ms"
            
            self.info_text.insert(tk.END, info_text)
        
        self.info_text.config(state=tk.DISABLED)

    def display_production_data(self, well_data):
        """Display production data in the treeview."""
        # Clear existing data
        if self.treeview:
            for item in self.treeview.get_children():
                self.treeview.delete(item)

        if not well_data or 'data' not in well_data or not well_data['data'].get('wells'):
            self.update_status("No production data to display", is_error=True)
            self.treeview["columns"] = []
            return

        wells = well_data['data']['wells']
        
        # Define columns for production data display
        columns = [
            'well_name', 'field_name', 'production_period', 'oil_production_kbd',
            'gas_production_mmcfd', 'liquids_production_kbd', 'water_production_kbd',
            'total_production_kbd', 'is_producing', 'days_on_production', 'data_source'
        ]
        
        self.treeview["columns"] = columns

        # Configure column headers and widths
        column_configs = {
            'well_name': ('Well Name', 120),
            'field_name': ('Field', 100),
            'production_period': ('Production Period', 150),
            'oil_production_kbd': ('Oil (kbd)', 100),
            'gas_production_mmcfd': ('Gas (mmcfd)', 100),
            'liquids_production_kbd': ('Liquids (kbd)', 100),
            'water_production_kbd': ('Water (kbd)', 100),
            'total_production_kbd': ('Total (kbd)', 100),
            'is_producing': ('Producing', 80),
            'days_on_production': ('Days Prod.', 80),
            'data_source': ('Data Source', 200)
        }

        for col in columns:
            title, width = column_configs.get(col, (col.replace('_', ' ').title(), 100))
            self.treeview.heading(col, text=title, anchor=tk.W)
            self.treeview.column(col, width=width, anchor=tk.W)

        # Insert data rows
        for well in wells:
            production = well.get('production_data', {})
            metadata = well.get('metadata', {})
            
            values = [
                well.get('well_name', ''),
                well.get('field_name', ''),
                metadata.get('production_period', ''),
                production.get('oil_production_kbd', 0),
                production.get('gas_production_mmcfd', 0),
                production.get('liquids_production_kbd', 0),
                production.get('water_production_kbd', 0),
                production.get('total_production_kbd', 0),
                'Yes' if production.get('is_producing', False) else 'No',
                metadata.get('days_on_production', 0),
                metadata.get('data_source', '')
            ]
            
            self.treeview.insert('', 'end', values=values)

        self.update_status(f"Successfully loaded {len(wells)} production records", is_error=False)

    def clear_display(self):
        """Clear all displayed data."""
        if self.treeview:
            for item in self.treeview.get_children():
                self.treeview.delete(item)
            self.treeview["columns"] = []
        
        if self.info_text:
            self.info_text.config(state=tk.NORMAL)
            self.info_text.delete(1.0, tk.END)
            self.info_text.config(state=tk.DISABLED)


class AppView(tk.Tk):
    """
    Main Application View.
    Responsible for creating the main window and managing tab views.
    """
    def __init__(self, controller):
        super().__init__()
        self.controller = controller
        self.title("Well Production Frontend (MVC)")
        self.geometry("1200x800")
        print("AppView initialized")

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)

        self.home_tab = HomeTabFrame(self.notebook)
        self.notebook.add(self.home_tab, text='Home')

        self.sync_tab = SyncTriggerTabFrame(self.notebook, self.controller)
        self.notebook.add(self.sync_tab, text='Sync Trigger')

        self.well_lookup_tab = WellLookupTabFrame(self.notebook, self.controller)
        self.notebook.add(self.well_lookup_tab, text='Well Lookup')

        # Link controller to views for explicit communication
        print("Setting view references in AppView")
        self.controller.set_view_references(self.sync_tab, self.well_lookup_tab)
        print("View references set in AppView")


# --- Controller Component ---
class AppController:
    """
    Acts as the intermediary between the Model and the View.
    Handles user input from the View, interacts with the Model for data,
    and updates the View based on Model responses.
    """
    def __init__(self):
        self.model = ApiModel()
        print("AppController initialized")
        
        # Initialize references to specific tab views - will be set by AppView
        self.sync_view = None
        self.well_lookup_view = None
        print("Controller view references initialized to None")
        
        # Create the view AFTER initializing the references
        self.view = AppView(self)
        print("AppView instantiated in AppController")

    def set_view_references(self, sync_view, well_lookup_view):
        """Sets references to the specific tab views for direct updates."""
        print("Setting view references in AppController")
        self.sync_view = sync_view
        self.well_lookup_view = well_lookup_view
        print(f"Sync view set: {self.sync_view is not None}")
        print(f"Well Lookup view set: {self.well_lookup_view is not None}")
        print("View references set in AppController")

    def run(self):
        """Starts the Tkinter application main loop."""
        self.view.mainloop()

    # --- Enhanced Controller methods for Sync Trigger Tab ---
    def trigger_wells_import(self):
        """Initiates the wells import process using both trigger and status endpoints."""
        print("Triggering import in AppController")
        if self.sync_view:
            self.sync_view.update_status("Starting import process...", is_error=False)
            self.sync_view.set_response("Starting import process...\n")
            threading.Thread(target=self._perform_import_trigger_and_status, daemon=True).start()
        else:
            print("self.sync_view is None in AppController")

    def _perform_import_trigger_and_status(self):
        """Perform the import trigger request and then check status."""
        
        def update_ui(message):
            if self.sync_view:
                self.sync_view.append_response(message)
        
        # Step 1: Trigger import
        self.view.after(0, lambda: update_ui("=== STEP 1: Triggering Import ===\n"))
        self.view.after(0, lambda: update_ui("Sending request to /api/v1/wells/import/trigger\n\n"))
        
        status_code, result = self.model.trigger_wells_import()
        
        if status_code is not None:
            if status_code == 200 and isinstance(result, dict):
                # Extract job_id from trigger response
                job_id = result.get('data', {}).get('job_id')
                
                # Format trigger response
                trigger_response = f"✅ Import triggered successfully!\n"
                trigger_response += f"Status Code: {status_code}\n"
                trigger_response += f"Job ID: {job_id}\n"
                trigger_response += f"Message: {result.get('message', 'N/A')}\n"
                trigger_response += f"Response: {json.dumps(result, indent=2)}\n\n"
                
                self.view.after(0, lambda: update_ui(trigger_response))
                self.view.after(0, lambda: self.sync_view.update_status(f"Import triggered! Job ID: {job_id}", is_error=False))
                
                if job_id:
                    # Step 2: Wait a moment and check status
                    self.view.after(0, lambda: update_ui("=== STEP 2: Checking Import Status ===\n"))
                    self.view.after(0, lambda: update_ui("Waiting 1 second before checking status...\n"))
                    
                    time.sleep(1)  # Wait a moment for the job to start
                    
                    self.view.after(0, lambda: update_ui(f"Sending request to /api/v1/wells/import/status/{job_id}\n\n"))
                    
                    status_code_2, status_result = self.model.get_import_status(job_id)
                    
                    if status_code_2 is not None:
                        if status_code_2 == 200 and isinstance(status_result, dict):
                            status_data = status_result.get('data', {})
                            import_status = status_data.get('status', 'unknown')
                            
                            status_response = f"✅ Status check successful!\n"
                            status_response += f"Status Code: {status_code_2}\n"
                            status_response += f"Current Import Status: {import_status}\n"
                            status_response += f"Job ID: {status_data.get('job_id', 'N/A')}\n"
                            status_response += f"Created At: {status_data.get('created_at', 'N/A')}\n"
                            status_response += f"Response: {json.dumps(status_result, indent=2)}\n\n"
                            

                            self.view.after(0, lambda: update_ui(status_response))
                            self.view.after(0, lambda: self.sync_view.update_status(f"Status: {import_status}", is_error=False))
                        else:
                            error_response = f"❌ Status check failed!\n"
                            error_response += f"Status Code: {status_code_2}\n"
                            error_response += f"Error: {status_result}\n\n"
                            
                            self.view.after(0, lambda: update_ui(error_response))
                            self.view.after(0, lambda: self.sync_view.update_status(f"Status check failed: HTTP {status_code_2}", is_error=True))
                    else:
                        error_response = f"❌ Status check connection error!\n"
                        error_response += f"Error: {status_result}\n\n"
                        
                        self.view.after(0, lambda: update_ui(error_response))
                        self.view.after(0, lambda: self.sync_view.update_status("Status check connection failed", is_error=True))
                else:
                    error_msg = "❌ No job_id returned from trigger request!\n\n"
                    self.view.after(0, lambda: update_ui(error_msg))
                    self.view.after(0, lambda: self.sync_view.update_status("No job_id returned", is_error=True))
            else:
                # Trigger failed
                error_response = f"❌ Import trigger failed!\n"
                error_response += f"Status Code: {status_code}\n"
                error_response += f"Error: {result}\n\n"
                
                self.view.after(0, lambda: update_ui(error_response))
                self.view.after(0, lambda: self.sync_view.update_status(f"Trigger failed: HTTP {status_code}", is_error=True))
        else:
            # Connection or other error
            error_response = f"❌ Connection error during trigger!\n"
            error_response += f"Error: {result}\n\n"
            
            self.view.after(0, lambda: update_ui(error_response))
            self.view.after(0, lambda: self.sync_view.update_status("Connection failed", is_error=True))

    # --- Controller methods for Well Lookup Tab ---
    def search_well_by_code(self):
        """Initiates the well search by code and updates the well lookup view."""
        if not self.well_lookup_view:
            print("Error: well_lookup_view is None")
            return
            
        well_code = self.well_lookup_view.get_well_code_input()
        
        if not well_code:
            self.well_lookup_view.update_status("Please enter a well code", is_error=True)
            return
            
        # Validate that well_code is numeric (assuming well codes are numbers)
        try:
            int(well_code)
        except ValueError:
            self.well_lookup_view.update_status("Well code must be a number", is_error=True)
            return
            
        self.well_lookup_view.clear_display()
        self.well_lookup_view.update_status(f"Searching for well {well_code}...")
        threading.Thread(target=self._fetch_well_by_code, args=(well_code,), daemon=True).start()

    def _fetch_well_by_code(self, well_code):
        """Fetch well data by code from API and update the view."""
        status_code, data = self.model.get_well_by_code(well_code)
        
        def update_ui():
            if not self.well_lookup_view:
                print("Error: well_lookup_view is None in _fetch_well_by_code")
                return
                
            if status_code and status_code == 200:
                self.well_lookup_view.display_well_info(data)
                self.well_lookup_view.display_production_data(data)
            elif status_code:
                self.well_lookup_view.update_status(f"API returned status {status_code}: {data}", is_error=True)
                self.well_lookup_view.clear_display()
            else:
                self.well_lookup_view.update_status(f"Error: {data}", is_error=True)
                self.well_lookup_view.clear_display()
        
        self.view.after(0, update_ui)


if __name__ == "__main__":
    controller = AppController()
    controller.run()
