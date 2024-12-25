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

# import shiny
# import pandas as pd
# from shiny import App, ui, render, reactive
# from eda import EDA
# import webbrowser

# # Instantiate your EDA class
# eda = EDA()

# # Define table options
# table_options = ["AQIdata", "temperatures", "ozone", "co", "so2", "no2", "pm25", "pm10"]

# # Define the UI
# app_ui = ui.page_fluid(
#     ui.h2("Air Quality Investigation Dashboard"),
#     ui.row(
#         ui.column(3,
#             ui.input_select(
#                 "selected_table",
#                 "Select a Table",
#                 table_options
#             ),
#             ui.input_select(
#                 "selected_state",
#                 "Select a State",
#                 choices = []
#             ),
#             ui.input_select(
#                 "selected_cbsa",
#                 "Select a CBSA (Optional)",
#                 choices = [],
#                 selected = None
#             ),
#             ui.input_numeric(
#                 "rows_to_show",
#                 "Number of rows to display",
#                 value=10,
#                 min=1,
#                 max=100
#             )
#         ),
#         ui.column(9,
#             ui.h3("Data Preview"),
#             ui.output_data_frame("filtered_data"),
#             ui.h3("Summary Statistics"),
#             ui.output_text_verbatim("summary_stats")
#         )
#     )
# )

# # Define the server logic
# def server(input, output, session):

#     @reactive.Effect
#     def update_table_options():
#         selected_table = input.selected_table()
#         if selected_table:
#             # Clear state and CBSA selections when table changes
#             ui.update_select("selected_state", choices=[])
#             ui.update_select("selected_cbsa", choices=[])

#     @reactive.Effect
#     def update_state_options():
#         selected_table = input.selected_table()
#         if selected_table:
#             df = eda.load_data(selected_table)
#             if "CBSA" in df.columns:
#                 df['State'] = df['CBSA'].apply(lambda x: x.split(', ')[-1])
#             elif "CBSA Name" in df.columns:
#                 df['State'] = df['CBSA Name'].apply(lambda x: x.split(', ')[-1])
#             states = df['State'].dropna().unique().tolist()
#             states.sort()
#             ui.update_select("selected_state", choices=states)

#     @reactive.Effect
#     def update_cbsa_options():
#         selected_table = input.selected_table()
#         selected_state = input.selected_state()
#         if selected_table and selected_state:
#             df = eda.load_data(selected_table)
#             if "CBSA" in df.columns:
#                 df = df[df['CBSA'].str.endswith(selected_state)]
#             elif "CBSA Name" in df.columns:
#                 df = df[df['CBSA Name'].str.endswith(selected_state)]
#             cbsa_options = df["CBSA"].dropna().unique().tolist()
#             cbsa_options.sort()
#             ui.update_select("selected_cbsa", 
#                            choices=["All CBSAs"] + cbsa_options,
#                            selected="All CBSAs")
#         else:
#             ui.update_select("selected_cbsa", choices=[], selected=None)

#     @reactive.Calc
#     def get_filtered_data():
#         selected_table = input.selected_table()
#         selected_state = input.selected_state()
#         selected_cbsa = input.selected_cbsa()
        
#         if not selected_table or not selected_state:
#             return pd.DataFrame()
            
#         df = eda.load_data(selected_table)
        
#         # Filter by state
#         if "CBSA" in df.columns:
#             df = df[df['CBSA'].str.endswith(selected_state)]
#         elif "CBSA Name" in df.columns:
#             df = df[df['CBSA Name'].str.endswith(selected_state)]
            
#         # Filter by CBSA if specifically selected
#         if selected_cbsa and selected_cbsa != "All CBSAs":
#             if "CBSA" in df.columns:
#                 df = df[df["CBSA"] == selected_cbsa]
#             elif "CBSA Name" in df.columns:
#                 df = df[df["CBSA Name"] == selected_cbsa]
                
#         return df

#     @output
#     @render.data_frame
#     def filtered_data():
#         df = get_filtered_data()
#         if not df.empty:
#             return df.head(input.rows_to_show())
#         return pd.DataFrame()

#     @output
#     @render.text
#     def summary_stats():
#         df = get_filtered_data()
#         if not df.empty:
#             return str(df.describe())
#         return "No data selected"
    
# # Create the app
# app = App(app_ui, server)

# if __name__ == "__main__":
#     webbrowser.open_new("http://127.0.0.1:8000")
#     app.run(port=8000)





















# import pandas as pd
# import numpy as np 
# import matplotlib.pyplot as plt
# import os
# from eda import EDA

# if __name__ == '__main__':
#     eda = EDA()

#     while True:
#         dataset_choice = eda.get_dataset_choice()
#         df = eda.load_data(dataset_choice)

#         if dataset_choice == 'AQIdata':
#             state_name = eda.get_state_choice(dataset_choice)
#             df = eda.filter_by_state(df, state_name)

#         elif dataset_choice != 'AQIdata':  # Handle state selection for non-AQIdata tables
#             state_name = eda.get_state_choice(dataset_choice)
#             # ! normalize state name to abbreviation
#             df = eda.filter_by_state(df, state_name)
        
#         if df.empty:
#             print("No data available to plot.")
#             continue

#         # eda.print_summary_stats(df, dataset_choice)

#         # print(f"This is the dataset: {dataset_choice}")
#         # print(df.nunique())

#         # # Call the function for paginated plotting
#         # eda.plot_facet_grid_paginated(df, dataset_name=dataset_choice, regions_per_page=4)

#         # # Call the function for interactive plotting
#         # eda.plot_interactive_facet_grid(df, dataset_name=dataset_choice)

#         # # Call the function for spatial heatmap plotting
#         # eda.plot_spatial_heatmap(df, dataset_name=dataset_choice, state_name=state_name)

#         # Call the function for correlation matrix plotting
#         eda.analyze_correlations(state_name)

#         # # Overlay Map
#         # eda.create_overlay(state_name=state_name)


#         prompt = input("Do you want to analyze another dataset? (yes/no): ").lower()
#         if prompt != 'yes':
#             break
    
#     eda.close_connection()



