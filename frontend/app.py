import tkinter as tk
from tkinter import ttk, messagebox
import requests
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
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
            return None, error_msg

    def get_wells_data(self):
        """Fetches wells data from the API."""
        return self._make_request('GET', '/api/v1/wells')

    def get_fields_data(self):
        """Fetches fields data from the API."""
        return self._make_request('GET', '/api/v1/fields')

    def get_production_trends_data(self):
        """Fetches production trend data from the API."""
        return self._make_request('GET', '/api/v1/production/trends')

    def trigger_wells_import(self):
        """Triggers the wells import process via the API."""
        return self._make_request('GET', '/api/v1/wells/import/trigger')

    def get_import_status(self, job_id):
        """Gets the status of an import job by job_id."""
        return self._make_request('GET', f'/api/v1/wells/import/status/{job_id}')


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

class TablesTabFrame(ttk.Frame):
    """View for the Tables tab."""
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.treeview = None
        self.current_table_data = None
        self.status_label = None

        self.create_widgets()

    def create_widgets(self):
        # Frame for buttons
        button_frame = ttk.Frame(self)
        button_frame.pack(pady=10, fill='x')

        ttk.Button(button_frame, text="Load Wells", command=self.controller.load_wells_table).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Load Fields", command=self.controller.load_fields_table).pack(side='left', padx=5)

        # Status label for showing loading/error messages
        self.status_label = ttk.Label(self, text="Ready to load data", foreground="blue")
        self.status_label.pack(pady=5)

        # Treeview for displaying data
        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill='both', expand=True, padx=10, pady=10)

        self.treeview = ttk.Treeview(tree_frame, show="headings")
        self.treeview.pack(side='left', fill='both', expand=True)

        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.treeview.yview)
        vsb.pack(side='right', fill='y')
        self.treeview.configure(yscrollcommand=vsb.set)

        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.treeview.xview)
        hsb.pack(side='bottom', fill='x')
        self.treeview.configure(xscrollcommand=hsb.set)

    def show_info(self, message, is_error=False):
        """Display status information to the user."""
        if self.status_label:
            color = "red" if is_error else "blue"
            self.status_label.config(text=message, foreground=color)

    def display_data(self, data, data_type="items"):
        """Displays data in the Treeview."""
        # Clear existing data
        for item in self.treeview.get_children():
            self.treeview.delete(item)

        if not data:
            self.show_info(f"No {data_type} data received from the API.", is_error=True)
            # Clear columns if no data
            self.treeview["columns"] = []
            return

        # Infer columns from the first item (assuming all items have same keys)
        columns = list(data[0].keys())
        self.treeview["columns"] = columns

        for col in columns:
            self.treeview.heading(col, text=col.replace('_', ' ').title(), anchor=tk.W)
            self.treeview.column(col, width=100, anchor=tk.W)

        for item in data:
            values = [item.get(col, '') for col in columns]
            self.treeview.insert('', 'end', values=values)
        
        self.current_table_data = data
        self.show_info(f"Successfully loaded {len(data)} {data_type} records", is_error=False)

class PlotsTabFrame(ttk.Frame):
    """View for the Plots tab."""
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.canvas = None
        self.fig = None
        self.status_label = None

        self.create_widgets()

    def create_widgets(self):
        # Control frame
        control_frame = ttk.Frame(self)
        control_frame.pack(pady=10, fill='x')

        ttk.Button(control_frame, text="Load Production Trends", command=self.controller.load_production_plots).pack(side='left', padx=5)
        
        # Status label
        self.status_label = ttk.Label(control_frame, text="Ready to load plots", foreground="blue")
        self.status_label.pack(side='left', padx=20)

        # Frame for the Matplotlib canvas
        self.plot_frame = ttk.Frame(self)
        self.plot_frame.pack(fill='both', expand=True, padx=10, pady=10)

    def show_info(self, message, is_error=False):
        """Display status information to the user."""
        if self.status_label:
            color = "red" if is_error else "blue"
            self.status_label.config(text=message, foreground=color)

    def display_plot(self, dates, oil_production, title="Production Trends"):
        """Displays a Matplotlib plot."""
        # Clear previous plot if exists
        if self.canvas:
            self.canvas.get_tk_widget().destroy()
            plt.close(self.fig)

        self.fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(dates, oil_production, marker='o', linestyle='-')
        ax.set_title(title)
        ax.set_xlabel("Date")
        ax.set_ylabel("Oil Production")
        ax.grid(True)
        self.fig.autofmt_xdate()

        self.canvas = FigureCanvasTkAgg(self.fig, master=self.plot_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        self.show_info(f"Successfully loaded production trends plot", is_error=False)

    def clear_plot(self):
        if self.canvas:
            self.canvas.get_tk_widget().destroy()
            plt.close(self.fig)
            self.canvas = None
            self.fig = None

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

        self.tables_tab = TablesTabFrame(self.notebook, self.controller)
        self.notebook.add(self.tables_tab, text='Tables')

        self.plots_tab = PlotsTabFrame(self.notebook, self.controller)
        self.notebook.add(self.plots_tab, text='Plots')

        self.sync_tab = SyncTriggerTabFrame(self.notebook, self.controller)
        self.notebook.add(self.sync_tab, text='Sync Trigger')

        # Link controller to views for explicit communication
        print("Setting view references in AppView")
        self.controller.set_view_references(self.tables_tab, self.plots_tab, self.sync_tab)
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
        self.tables_view = None
        self.plots_view = None
        self.sync_view = None
        print("Controller view references initialized to None")
        
        # Create the view AFTER initializing the references
        self.view = AppView(self)
        print("AppView instantiated in AppController")

    def set_view_references(self, tables_view, plots_view, sync_view):
        """Sets references to the specific tab views for direct updates."""
        print("Setting view references in AppController")
        self.tables_view = tables_view
        self.plots_view = plots_view
        self.sync_view = sync_view
        print(f"Tables view set: {self.tables_view is not None}")
        print(f"Plots view set: {self.plots_view is not None}")
        print(f"Sync view set: {self.sync_view is not None}")
        print("View references set in AppController")

    def run(self):
        """Starts the Tkinter application main loop."""
        self.view.mainloop()

    # --- Controller methods for Tables Tab ---
    def load_wells_table(self):
        """Initiates loading wells data and updates the tables view."""
        if self.tables_view:
            self.tables_view.display_data([])  # Clear current display
            self.tables_view.show_info("Loading wells data...")
            # Run API call in a separate thread to keep UI responsive
            threading.Thread(target=self._fetch_wells_data, daemon=True).start()
        else:
            print("Error: tables_view is None")

    def _fetch_wells_data(self):
        """Fetch wells data from API and update the view."""
        status_code, data = self.model.get_wells_data()
        
        def update_ui():
            if not self.tables_view:
                print("Error: tables_view is None in _fetch_wells_data")
                return
                
            if status_code and status_code == 200 and isinstance(data, list):
                self.tables_view.display_data(data, "wells")
            elif status_code:
                self.tables_view.show_info(f"API returned status {status_code}: {data}", is_error=True)
                self.tables_view.display_data([], "wells")
            else:
                self.tables_view.show_info(f"Error: {data}", is_error=True)
                self.tables_view.display_data([], "wells")
        
        self.view.after(0, update_ui)

    def load_fields_table(self):
        """Initiates loading fields data and updates the tables view."""
        if self.tables_view:
            self.tables_view.display_data([])  # Clear current display
            self.tables_view.show_info("Loading fields data...")
            threading.Thread(target=self._fetch_fields_data, daemon=True).start()
        else:
            print("Error: tables_view is None")

    def _fetch_fields_data(self):
        """Fetch fields data from API and update the view."""
        status_code, data = self.model.get_fields_data()
        
        def update_ui():
            if not self.tables_view:
                print("Error: tables_view is None in _fetch_fields_data")
                return
                
            if status_code and status_code == 200 and isinstance(data, list):
                self.tables_view.display_data(data, "fields")
            elif status_code:
                self.tables_view.show_info(f"API returned status {status_code}: {data}", is_error=True)
                self.tables_view.display_data([], "fields")
            else:
                self.tables_view.show_info(f"Error: {data}", is_error=True)
                self.tables_view.display_data([], "fields")
        
        self.view.after(0, update_ui)

    # --- Controller methods for Plots Tab ---
    def load_production_plots(self):
        """Initiates loading production trend data and updates the plots view."""
        if self.plots_view:
            self.plots_view.clear_plot()  # Clear existing plot
            self.plots_view.show_info("Loading production trends...")
            threading.Thread(target=self._fetch_production_plots_data, daemon=True).start()
        else:
            print("Error: plots_view is None")

    def _fetch_production_plots_data(self):
        """Fetch production plots data from API and update the view."""
        status_code, data = self.model.get_production_trends_data()
        
        def update_ui():
            if not self.plots_view:
                print("Error: plots_view is None in _fetch_production_plots_data")
                return
                
            if status_code and status_code == 200:
                if isinstance(data, dict) and 'dates' in data and 'oil_production' in data:
                    self.plots_view.display_plot(data['dates'], data['oil_production'])
                else:
                    self.plots_view.show_info("Invalid data format received from API", is_error=True)
            elif status_code:
                self.plots_view.show_info(f"API returned status {status_code}: {data}", is_error=True)
            else:
                self.plots_view.show_info(f"Error: {data}", is_error=True)
        
        self.view.after(0, update_ui)

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


if __name__ == "__main__":
    # Ensure Tkinter is initialized on the main thread
    app_controller = AppController()
    app_controller.run()
