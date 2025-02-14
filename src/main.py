
from matplotlib import pyplot as plt
import os
import pandas as pd
from shiny import App, ui, render, reactive
from eda import EDA
from FileCleaner import FileCleaner
from DatabaseManager import DatabaseManager
import webbrowser


# Instantiate EDA class
eda = EDA()

# Define the actual directory paths
aqi_directory = "data/daily_aqi"
temp_directory = "data/daily_temp"
ozone_directory = "data/daily_ozone"
pm25_directory = "data/daily_pm2.5"
pm10_directory = "data/daily_pm10"
no2_directory = "data/daily_no2"
so2_directory = "data/daily_so2"
co_directory = "data/daily_co"

# Instantiate FileCleaner class
file_cleaner = FileCleaner(
    aqi_directory, temp_directory, ozone_directory, 
    pm25_directory, pm10_directory, no2_directory, 
    so2_directory, co_directory
)


# Check if the database file exists
db_file_path = 'air.db'

if not os.path.exists(db_file_path):
    # Clean the data if database doesn't exist
    file_cleaner.clean_all_files()
    
    # Instantiate DatabaseManager class
    db_manager = DatabaseManager(db_file_path)
    
    # Load cleaned data into the database if database doesn't exist
    db_manager.load_all_data()
else:
    print("Database already exists.")


# Define table options
table_options = ["AQIdata", "temperatures", "ozone", "co", "so2", "no2", "pm2.5", "pm10"]

# Define the UI
app_ui = ui.page_fluid(
    ui.h2("Air Quality Dashboard"),
    ui.row(
        ui.column(3,
            ui.input_select(
                "selected_table",
                "Select a Table",
                table_options
            ),
            ui.input_select(
                "selected_state",
                "Select a State",
                choices = []
            ),
            ui.input_select(
                "selected_cbsa",
                "Select a CBSA (Optional)",
                choices = [],
                selected = None
            ),
            ui.input_numeric(
                "rows_to_show",
                "Number of rows to display",
                value=10,
                min=1,
                max=100
            )
        ),
        ui.column(9,
            ui.h3("Data Preview"),
            ui.output_data_frame("filtered_data"),
            ui.h3("Summary Statistics"),
            ui.output_text_verbatim("summary_stats"),
            ui.h3("Spatial Heatmap"),
            ui.output_ui("heatmap"),
            ui.h3("Correlation Matrix"),
            ui.output_ui("correlation_matrix")
        )
    )
)

# Define the server logic
def server(input, output, session):
    
    @reactive.Calc
    def get_base_data():
        """Load the current table's data"""
        selected_table = input.selected_table()
        if selected_table:
            return eda.load_data(selected_table)
        return pd.DataFrame()

    @reactive.Effect
    @reactive.event(input.selected_table)
    def clear_selections():
        """Clear state and CBSA selections when table changes"""
        ui.update_select("selected_state", choices=[])
        ui.update_select("selected_cbsa", choices=[])
    
    @reactive.Effect
    @reactive.event(input.selected_table)
    def update_state_options():
        """Update state options based on the selected table."""
        # Get the current state before updating
        current_state = input.selected_state()
        
        df = get_base_data()
        if not df.empty:
            if "CBSA" in df.columns:
                df['State'] = df['CBSA'].apply(lambda x: x.split(', ')[-1])
            elif "CBSA Name" in df.columns:
                df['State'] = df['CBSA Name'].apply(lambda x: x.split(', ')[-1])
            
            states = sorted(df['State'].dropna().unique().tolist())
            
            # Update the state selection list
            ui.update_select("selected_state", choices=states)
            
            # Restore the previously selected state if it exists in the new list
            if current_state in states:
                ui.update_select("selected_state", selected=current_state)
            else:
                ui.update_select("selected_state", selected=None)  # Default if previous state is invalid


    @reactive.Effect
    @reactive.event(input.selected_state, input.selected_table)
    def update_cbsa_options():
        """Update CBSA options based on selected state and table"""
        df = get_base_data()
        selected_state = input.selected_state()
        
        if not df.empty and selected_state:
            if "CBSA" in df.columns:
                df = df[df['CBSA'].str.endswith(selected_state)]
                cbsa_column = "CBSA"
            elif "CBSA Name" in df.columns:
                df = df[df['CBSA Name'].str.endswith(selected_state)]
                cbsa_column = "CBSA Name"
            
            cbsa_options = sorted(df[cbsa_column].dropna().unique().tolist())
            ui.update_select("selected_cbsa", 
                           choices=["All CBSAs"] + cbsa_options,
                           selected="All CBSAs")
        else:
            ui.update_select("selected_cbsa", choices=[], selected=None)


    @reactive.Calc
    def get_filtered_data():
        """Get filtered data based on all selections"""
        df = get_base_data()
        selected_state = input.selected_state()
        selected_cbsa = input.selected_cbsa()
        
        if df.empty or not selected_state:
            return pd.DataFrame()
            
        # Filter by state
        if "CBSA" in df.columns:
            df = df[df['CBSA'].str.endswith(selected_state)]
            cbsa_column = "CBSA"
        elif "CBSA Name" in df.columns:
            df = df[df['CBSA Name'].str.endswith(selected_state)]
            cbsa_column = "CBSA Name"
            
        # Filter by CBSA if specifically selected
        if selected_cbsa and selected_cbsa != "All CBSAs":
            df = df[df[cbsa_column] == selected_cbsa]
                
        return df
    

    @output
    @render.data_frame
    def filtered_data():
        df = get_filtered_data()
        if not df.empty:
            return df.head(input.rows_to_show())
        return pd.DataFrame()

    @output
    @render.text
    def summary_stats():
        df = get_filtered_data()
        if not df.empty:
            return str(df.describe())
        return "No data selected"
    


    @output
    @render.ui
    def heatmap():
        """Generate and display the spatial heatmap"""
        df = get_filtered_data()
        selected_state = input.selected_state()
        dataset_name = input.selected_table()

        if df.empty or selected_state is None:
            return ui.HTML("<p>No data available to generate heatmap.</p>")

        # Generate the heatmap Plotly figure
        fig = eda.plot_spatial_heatmap(df, dataset_name, selected_state)

        if fig is None:
            # Handle cases where heatmap cannot be generated
            return ui.HTML("<p>Spatial heatmaps are not available for AQIdata due to lack of latitude and longitude data.</p>")

        # Convert the figure to an HTML div string
        fig_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

        # Return the HTML string to embed the figure
        return ui.HTML(fig_html)
    

    @output
    @render.ui
    def correlation_matrix():
        """Generate and display the correlation matrix as a Plotly heatmap."""
        df = get_filtered_data()
        selected_state = input.selected_state()

        if df.empty or selected_state is None:
            return ui.HTML("<p>No data available to generate correlation matrix.</p>")

        # Generate the correlation matrix plotly figure
        fig = eda.analyze_correlations(selected_state)

        # Convert the Plotly figure to an HTML div string
        fig_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

        # Return the HTML string to embed the figure in the UI
        return ui.HTML(fig_html)

# Create the app
app = App(app_ui, server)

if __name__ == "__main__":
    webbrowser.open_new("http://127.0.0.1:8000")
    app.run(port=8000)
