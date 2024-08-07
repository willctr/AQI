import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import os
from eda import EDA

if __name__ == '__main__':
    eda = EDA()

    while True:
        dataset_choice = eda.get_dataset_choice()
        df = eda.load_data(dataset_choice)

        if dataset_choice == 'AQIdata':
            state_name = eda.get_state_choice(dataset_choice)
            df = eda.filter_by_state(df, state_name)

        elif dataset_choice != 'AQIdata':  # Handle state selection for non-AQIdata tables
            state_name = eda.get_state_choice(dataset_choice)
            # ! normalize state name to abbreviation
            df = eda.filter_by_state(df, state_name)
        
        if df.empty:
            print("No data available to plot.")
            continue

        eda.print_summary_stats(df, dataset_choice)

        print(f"This is the dataset: {dataset_choice}")
        print(df.nunique())

        # Call the function for paginated plotting
        # eda.plot_facet_grid_paginated(df, dataset_name=dataset_choice, regions_per_page=4)

        # Call the function for interactive plotting
        eda.plot_interactive_facet_grid(df, dataset_name=dataset_choice)

        # Call the function for spatial heatmap plotting
        eda.plot_spatial_heatmap(df, dataset_name=dataset_choice, state_name=state_name)

        # Call the function for correlation matrix plotting
        eda.analyze_correlations(state_name)

        prompt = input("Do you want to analyze another dataset? (yes/no): ").lower()
        if prompt != 'yes':
            break
    
    eda.close_connection()
