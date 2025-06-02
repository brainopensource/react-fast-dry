import tkinter as tk
from tkinter import messagebox, filedialog
import customtkinter as ctk
import requests
import threading
import json
import time
import os

# Set CustomTkinter appearance
ctk.set_appearance_mode("dark")  # Dark theme
ctk.set_default_color_theme("blue")  # Blue theme for buttons

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

    def download_csv(self, endpoint, save_path, progress_callback=None):
        """Download CSV file from endpoint with progress callback."""
        # Ensure download directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        url = f"{self.BASE_URL}{endpoint}"
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                total = int(r.headers.get('content-length', 0))
                downloaded = 0
                chunk_size = 8192
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if progress_callback and total:
                                progress_callback(downloaded / total * 100)
            return r.status_code, save_path
        except requests.exceptions.RequestException as e:
            return None, str(e)


# --- View Component ---
class HomeTabFrame(ctk.CTkFrame):
    """View for the Home tab."""
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()

    def create_widgets(self):
        ctk.CTkLabel(self, text="Welcome to the Well Production Monitoring App!",
                     font=ctk.CTkFont(size=20, weight="bold")).pack(pady=30)
        ctk.CTkLabel(self, text="Use the tabs above to explore wells, production data, and manage imports.",
                     font=ctk.CTkFont(size=14)).pack(pady=15)
        ctk.CTkLabel(self, text="Developed with CustomTkinter and FastAPI.",
                     font=ctk.CTkFont(size=12)).pack(pady=10)

class SyncTriggerTabFrame(ctk.CTkFrame):
    """View for the Sync Trigger tab."""
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.status_label = None
        self.text_widget = None
        self.latest_job_id = None
        self.check_status_button = None
        self.create_widgets()

    def create_widgets(self):
        # Header
        ctk.CTkLabel(self, text="Trigger Wells Data Import:", 
                     font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)
        
        # Control frame
        control_frame = ctk.CTkFrame(self)
        control_frame.pack(pady=15, padx=20, fill='x')
        
        # Step 1: Trigger Import button
        ctk.CTkButton(control_frame, text="Trigger Import Now", 
                      command=self.controller.trigger_wells_import,
                      height=40, font=ctk.CTkFont(size=14, weight="bold")).pack(side='left', padx=10, pady=15)
        
        # Step 2: Check Status button
        self.check_status_button = ctk.CTkButton(control_frame, text="Check Import Status", 
                      command=self.controller.check_import_status,
                      height=40, font=ctk.CTkFont(size=14, weight="bold"), state="disabled")
        self.check_status_button.pack(side='left', padx=10, pady=15)
        
        # Status label
        self.status_label = ctk.CTkLabel(control_frame, text="Ready to trigger import.",
                                         font=ctk.CTkFont(size=12))
        self.status_label.pack(side='left', padx=20, pady=10)
        
        # Response frame
        response_frame = ctk.CTkFrame(self)
        response_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Header for response frame
        ctk.CTkLabel(response_frame, text="API Response", 
                     font=ctk.CTkFont(size=14, weight="bold")).pack(pady=10)

        # Text widget for displaying response
        text_frame = ctk.CTkFrame(response_frame)
        text_frame.pack(fill='both', expand=True, padx=10, pady=10)

        self.text_widget = ctk.CTkTextbox(text_frame, height=300, font=ctk.CTkFont(family="Consolas", size=10))
        self.text_widget.pack(fill='both', expand=True, padx=5, pady=5)
        # Initial message
        self.text_widget.insert("0.0", "Response will be shown here after triggering import.\n")

    def update_status(self, message, is_error=False):
        """Updates the status message displayed to the user."""
        if self.status_label:
            color = "#ff4444" if is_error else "#4a9eff"
            self.status_label.configure(text=message, text_color=color)

    def append_response(self, text):
        """Appends text to the response text widget."""
        if self.text_widget:
            self.text_widget.insert("end", text)
            self.text_widget.see("end")  # Scroll to bottom

    def set_response(self, text):
        """Sets the text in the response text widget."""
        if self.text_widget:
            self.text_widget.delete("0.0", "end")
            self.text_widget.insert("0.0", text)

    def set_latest_job_id(self, job_id):
        self.latest_job_id = job_id
        if self.check_status_button:
            if job_id:
                self.check_status_button.configure(state="normal")
            else:
                self.check_status_button.configure(state="disabled")

    def get_latest_job_id(self):
        return self.latest_job_id


class WellLookupTabFrame(ctk.CTkFrame):
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
        ctk.CTkLabel(self, text="Well Lookup by Code:", 
                     font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)
        
        # Input frame
        input_frame = ctk.CTkFrame(self)
        input_frame.pack(pady=15, fill='x', padx=20)
        
        ctk.CTkLabel(input_frame, text="Well Code:", 
                     font=ctk.CTkFont(size=12, weight="bold")).pack(side='left', padx=10, pady=10)
        self.entry_widget = ctk.CTkEntry(input_frame, width=150, height=35)
        self.entry_widget.pack(side='left', padx=10, pady=10)
        
        search_button = ctk.CTkButton(input_frame, text="Search Well", 
                                      command=self.controller.search_well_by_code,
                                      height=35, font=ctk.CTkFont(size=12, weight="bold"))
        search_button.pack(side='left', padx=10, pady=10)
        
        # Bind Enter key to search
        self.entry_widget.bind('<Return>', lambda event: self.controller.search_well_by_code())
        
        # Status label
        self.status_label = ctk.CTkLabel(input_frame, text="Enter a well code and click Search",
                                         font=ctk.CTkFont(size=11))
        self.status_label.pack(side='left', padx=20, pady=10)
        
        # Info frame for metadata
        info_frame = ctk.CTkFrame(self)
        info_frame.pack(fill='x', padx=20, pady=10)
        
        ctk.CTkLabel(info_frame, text="Well Information", 
                     font=ctk.CTkFont(size=14, weight="bold")).pack(pady=5)
        
        self.info_text = ctk.CTkTextbox(info_frame, height=80, font=ctk.CTkFont(family="Consolas", size=10))
        self.info_text.pack(fill='x', padx=10, pady=5)
        
        # Treeview frame - keeping ttk.Treeview as CustomTkinter doesn't have a replacement
        import tkinter.ttk as ttk
        tree_frame = ctk.CTkFrame(self)
        tree_frame.pack(fill='both', expand=True, padx=20, pady=10)

        ctk.CTkLabel(tree_frame, text="Production Records", 
                     font=ctk.CTkFont(size=14, weight="bold")).pack(pady=5)        # Treeview for displaying production data
        tree_container = ctk.CTkFrame(tree_frame)
        tree_container.pack(fill='both', expand=True, padx=10, pady=5)

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
            self.entry_widget.delete(0, "end")

    def update_status(self, message, is_error=False):
        """Updates the status message displayed to the user."""
        if self.status_label:
            color = "#ff4444" if is_error else "#4a9eff"
            self.status_label.configure(text=message, text_color=color)

    def display_well_info(self, well_data):
        """Display general well information."""
        if not self.info_text:
            return
            
        self.info_text.delete("0.0", "end")
        
        if well_data and 'data' in well_data:
            data = well_data['data']
            info_text = f"Well Code: {data.get('well_code', 'N/A')}\n"
            info_text += f"Records Found: {data.get('records_found', 0)}\n"
            info_text += f"Message: {well_data.get('message', 'N/A')}\n"
            info_text += f"Response Time: {well_data.get('metadata', {}).get('execution_time_ms', 'N/A')} ms"
            
            self.info_text.insert("0.0", info_text)

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
            self.info_text.delete("0.0", "end")


class DownloadTabFrame(ctk.CTkFrame):
    """View for downloading CSV files."""
    DEFAULT_FILENAME = "wells_production_monthly.csv"

    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.save_path = None
        self.progress = None
        self.status_label = None
        self.path_entry = None  # show selected path
        self.create_widgets()

    def create_widgets(self):
        # Header
        ctk.CTkLabel(self, text="Download CSV Data", 
                     font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)
        
        # Path selection frame
        path_frame = ctk.CTkFrame(self)
        path_frame.pack(pady=15, fill='x', padx=20)
        
        ctk.CTkLabel(path_frame, text="Save Location:", 
                     font=ctk.CTkFont(size=12, weight="bold")).pack(anchor='w', padx=10, pady=(10, 5))
        
        path_input_frame = ctk.CTkFrame(path_frame)
        path_input_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        # Set default path to current working directory + default filename
        import os
        default_dir = os.getcwd()
        default_path = os.path.join(default_dir, self.DEFAULT_FILENAME)
        self.save_path = default_path
        
        self.path_entry = ctk.CTkEntry(path_input_frame, height=35, font=ctk.CTkFont(size=11))
        self.path_entry.pack(side='left', fill='x', expand=True, padx=(0, 10))
        self.path_entry.delete(0, "end")
        self.path_entry.insert(0, default_path)
        self.path_entry.configure(state="disabled")
        
        ctk.CTkButton(path_input_frame, text='Browse...', command=self.browse_folder,
                      width=100, height=35).pack(side='right')

        # Download section
        download_frame = ctk.CTkFrame(self)
        download_frame.pack(pady=15, fill='x', padx=20)
        
        ctk.CTkButton(download_frame, text='Download CSV', 
                      command=self.controller.trigger_wells_download,
                      height=40, font=ctk.CTkFont(size=14, weight="bold")).pack(pady=15)
        # Progress bar
        self.progress = ctk.CTkProgressBar(download_frame, width=400, height=20)
        self.progress.pack(pady=10)
        self.progress.set(0)  # Initialize to 0%
        
        # Status label
        self.status_label = ctk.CTkLabel(download_frame, text='Ready to download',
                                         font=ctk.CTkFont(size=12))
        self.status_label.pack(pady=10)

    def browse_folder(self):
        folder_path = filedialog.askdirectory(title='Select Folder to Save CSV')
        if folder_path and self.path_entry:
            import os
            full_path = os.path.join(folder_path, self.DEFAULT_FILENAME)
            self.save_path = full_path
            self.path_entry.configure(state="normal")
            self.path_entry.delete(0, "end")
            self.path_entry.insert(0, full_path)
            self.path_entry.configure(state="disabled")

    def update_status(self, message, is_error=False):
        """Updates the status message displayed to the user."""
        if self.status_label:
            color = "#ff4444" if is_error else "#4a9eff"
            self.status_label.configure(text=message, text_color=color)

    def update_progress(self, progress_value):
        """Updates the progress bar with the given value (0-1)."""
        if self.progress:
            # CustomTkinter progress bar expects values between 0 and 1
            self.progress.set(progress_value / 100.0)


class AppView(ctk.CTk):
    """
    Main Application View with sidebar navigation for dashboard look.
    Responsible for creating the main window and managing tab views.
    """
    def __init__(self, controller):
        super().__init__()
        self.controller = controller
        self.title("Well Production Frontend (MVC)")
        self.geometry("1200x800")
        print("AppView initialized")

        # Main layout: sidebar (left) and content (right)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

        # Sidebar
        self.sidebar = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nswe")
        self.sidebar.grid_rowconfigure(4, weight=1)  # Pushes buttons to top

        # Sidebar buttons
        self.btn_home = ctk.CTkButton(self.sidebar, text="Home", command=lambda: self.show_frame("home"), height=50, font=ctk.CTkFont(size=15, weight="bold"))
        self.btn_home.grid(row=0, column=0, sticky="ew", padx=20, pady=(40, 10))
        self.btn_sync = ctk.CTkButton(self.sidebar, text="Sync Trigger", command=lambda: self.show_frame("sync"), height=50, font=ctk.CTkFont(size=15, weight="bold"))
        self.btn_sync.grid(row=1, column=0, sticky="ew", padx=20, pady=10)
        self.btn_lookup = ctk.CTkButton(self.sidebar, text="Well Lookup", command=lambda: self.show_frame("lookup"), height=50, font=ctk.CTkFont(size=15, weight="bold"))
        self.btn_lookup.grid(row=2, column=0, sticky="ew", padx=20, pady=10)
        self.btn_download = ctk.CTkButton(self.sidebar, text="Download", command=lambda: self.show_frame("download"), height=50, font=ctk.CTkFont(size=15, weight="bold"))
        self.btn_download.grid(row=3, column=0, sticky="ew", padx=20, pady=10)

        # Main content area
        self.content = ctk.CTkFrame(self, corner_radius=10)
        self.content.grid(row=0, column=1, sticky="nswe", padx=10, pady=10)
        self.content.grid_rowconfigure(0, weight=1)
        self.content.grid_columnconfigure(0, weight=1)

        # Create tab frames (but don't pack them yet)
        self.home_tab = HomeTabFrame(self.content)
        self.sync_tab = SyncTriggerTabFrame(self.content, self.controller)
        self.well_lookup_tab = WellLookupTabFrame(self.content, self.controller)
        self.download_tab = DownloadTabFrame(self.content, self.controller)

        self.frames = {
            "home": self.home_tab,
            "sync": self.sync_tab,
            "lookup": self.well_lookup_tab,
            "download": self.download_tab
        }

        for frame in self.frames.values():
            frame.grid(row=0, column=0, sticky="nswe")
            frame.grid_remove()

        self.show_frame("home")  # Show home by default

        # Link controller to views for explicit communication
        print("Setting view references in AppView")
        self.controller.set_view_references(self.sync_tab, self.well_lookup_tab, self.download_tab)
        print("View references set in AppView")

    def show_frame(self, name):
        for key, frame in self.frames.items():
            if key == name:
                frame.grid()
            else:
                frame.grid_remove()


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
        self.download_view = None
        print("Controller view references initialized to None")
        
        # Create the view AFTER initializing the references
        self.view = AppView(self)
        print("AppView instantiated in AppController")

    def set_view_references(self, sync_view, well_lookup_view, download_view):
        """Sets references to the specific tab views for direct updates."""
        print("Setting view references in AppController")
        self.sync_view = sync_view
        self.well_lookup_view = well_lookup_view
        self.download_view = download_view
        print(f"Sync view set: {self.sync_view is not None}")
        print(f"Well Lookup view set: {self.well_lookup_view is not None}")
        print(f"Download view set: {self.download_view is not None}")
        print("View references set in AppController")

    def run(self):
        """Starts the Tkinter application main loop."""
        self.view.mainloop()

    # --- Sync Trigger Tab: Step 1 ---
    def trigger_wells_import(self):
        print("Triggering import in AppController (Step 1)")
        if self.sync_view:
            self.sync_view.update_status("Starting import process...", is_error=False)
            self.sync_view.set_response("Starting import process...\n")
            threading.Thread(target=self._perform_import_trigger, daemon=True).start()
        else:
            print("self.sync_view is None in AppController")

    def _perform_import_trigger(self):
        def update_ui(message):
            if self.sync_view:
                self.sync_view.append_response(message)
        self.view.after(0, lambda: update_ui("=== STEP 1: Triggering Import ===\n"))
        self.view.after(0, lambda: update_ui("Sending request to /api/v1/wells/import/trigger\n\n"))
        status_code, result = self.model.trigger_wells_import()
        if status_code is not None:
            if status_code == 200 and isinstance(result, dict):
                job_id = result.get('data', {}).get('job_id')
                trigger_response = f"✅ Import triggered successfully!\n"
                trigger_response += f"Status Code: {status_code}\n"
                trigger_response += f"Job ID: {job_id}\n"
                trigger_response += f"Message: {result.get('message', 'N/A')}\n"
                trigger_response += f"Response: {json.dumps(result, indent=2)}\n\n"
                self.view.after(0, lambda: update_ui(trigger_response))
                if self.sync_view:
                    self.view.after(0, lambda: self.sync_view.update_status(f"Import triggered! Job ID: {job_id}", is_error=False))
                    self.view.after(0, lambda: self.sync_view.set_latest_job_id(job_id))
            else:
                error_response = f"❌ Import trigger failed!\n"
                error_response += f"Status Code: {status_code}\n"
                error_response += f"Error: {result}\n\n"
                self.view.after(0, lambda: update_ui(error_response))
                self.view.after(0, lambda: self.sync_view.update_status(f"Trigger failed: HTTP {status_code}", is_error=True))
        else:
            error_response = f"❌ Connection error during trigger!\n"
            error_response += f"Error: {result}\n\n"
            self.view.after(0, lambda: update_ui(error_response))
            self.view.after(0, lambda: self.sync_view.update_status("Connection failed", is_error=True))

    # --- Sync Trigger Tab: Step 2 ---
    def check_import_status(self):
        if not self.sync_view:
            print("self.sync_view is None in AppController (check_import_status)")
            return
        job_id = self.sync_view.get_latest_job_id()
        if not job_id:
            self.sync_view.append_response("❌ No job_id available. Please trigger import first.\n")
            self.sync_view.update_status("No job_id available. Trigger import first.", is_error=True)
            return
        self.sync_view.update_status(f"Checking status for job_id: {job_id}", is_error=False)
        threading.Thread(target=self._perform_import_status, args=(job_id,), daemon=True).start()

    def _perform_import_status(self, job_id):
        def update_ui(message):
            if self.sync_view:
                self.sync_view.append_response(message)
        self.view.after(0, lambda: update_ui(f"=== STEP 2: Checking Import Status for job_id {job_id} ===\n"))
        self.view.after(0, lambda: update_ui(f"Sending request to /api/v1/wells/import/status/{job_id}\n\n"))
        status_code, status_result = self.model.get_import_status(job_id)
        if status_code is not None:
            if status_code == 200 and isinstance(status_result, dict):
                status_data = status_result.get('data', {})
                import_status = status_data.get('status', 'unknown')
                status_response = f"✅ Status check successful!\n"
                status_response += f"Status Code: {status_code}\n"
                status_response += f"Current Import Status: {import_status}\n"
                status_response += f"Job ID: {status_data.get('job_id', 'N/A')}\n"
                status_response += f"Created At: {status_data.get('created_at', 'N/A')}\n"
                status_response += f"Response: {json.dumps(status_result, indent=2)}\n\n"
                self.view.after(0, lambda: update_ui(status_response))
                self.view.after(0, lambda: self.sync_view.update_status(f"Status: {import_status}", is_error=False))
            else:
                error_response = f"❌ Status check failed!\n"
                error_response += f"Status Code: {status_code}\n"
                error_response += f"Error: {status_result}\n\n"
                self.view.after(0, lambda: update_ui(error_response))
                self.view.after(0, lambda: self.sync_view.update_status(f"Status check failed: HTTP {status_code}", is_error=True))
        else:
            error_response = f"❌ Status check connection error!\n"
            error_response += f"Error: {status_result}\n\n"
            self.view.after(0, lambda: update_ui(error_response))
            self.view.after(0, lambda: self.sync_view.update_status("Status check connection failed", is_error=True))

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
    
    # --- Controller methods for Download Tab ---
    def trigger_wells_download(self):
        if not self.download_view:
            print("Error: download_view is None")
            return
            
        path = getattr(self.download_view, 'save_path', None)
        if not path:
            self.download_view.update_status("Please select a save location first", is_error=True)
            return
        self.download_view.update_status("Starting download...", is_error=False)
        self.download_view.update_progress(0)
        threading.Thread(target=self._perform_download, daemon=True).start()

    def _perform_download(self):
        if not self.download_view:
            print("Error: download_view is None in _perform_download")
            return
            
        save_path = self.download_view.save_path
        def progress_cb(p):
            if self.download_view:
                self.view.after(0, lambda: self.download_view.update_progress(p))
                
        status, result = self.model.download_csv('/api/v1/wells/download', save_path, progress_cb)
        if self.download_view:
            if status == 200:
                self.view.after(0, lambda: self.download_view.update_status(f"Download complete: {result}", is_error=False))
            else:
                self.view.after(0, lambda: self.download_view.update_status(f"Download failed: {result}", is_error=True))


if __name__ == "__main__":
    controller = AppController()
    controller.run()
