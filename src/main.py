import shiny
import pandas as pd
from shiny import App, ui, render, reactive
from eda import EDA
import webbrowser

# Instantiate your EDA class
eda = EDA()

# Define table options
table_options = ["AQIdata", "temperatures", "ozone", "co", "so2", "no2", "pm25", "pm10"]

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
            ui.output_text_verbatim("summary_stats")
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
    def update_state_options():
        """Update state options based on current table"""
        df = get_base_data()
        if not df.empty:
            if "CBSA" in df.columns:
                df['State'] = df['CBSA'].apply(lambda x: x.split(', ')[-1])
            elif "CBSA Name" in df.columns:
                df['State'] = df['CBSA Name'].apply(lambda x: x.split(', ')[-1])
            states = sorted(df['State'].dropna().unique().tolist())
            ui.update_select("selected_state", choices=states)

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
    
# Create the app
app = App(app_ui, server)

if __name__ == "__main__":
    webbrowser.open_new("http://127.0.0.1:8000")
    app.run(port=8000)
