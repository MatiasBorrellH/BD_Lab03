import pandas as pd
import os
import json


# Data loader:
def idealista_loader():
    """
    Loads and combines all JSON files from the 'datasets/idealista' folder into a single Pandas DataFrame.
    
    This function scans through all JSON files in the idealista dataset directory, reads each file,
    and combines them into a unified DataFrame. It handles both list-type JSONs and nested JSON objects
    using json_normalize for the latter.
    
    Returns:
        pandas.DataFrame: A combined DataFrame containing data from all JSON files in the directory.
                          Returns the DataFrame with all records from all loaded JSON files.
    
    Raises:
        Exception: Prints error message if any file cannot be read but continues processing other files.
    
    Note:
        The function expects JSON files to be in 'datasets/idealista' relative to the current working directory.
    """
    # Relative path to the folder from the notebook
    folder = 'datasets/idealista'
    dataframes = []

    # Loop through all .json files in the folder
    for file in os.listdir(folder):
        if file.lower().endswith('.json'):
            full_path = os.path.join(folder, file)
        
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # If the JSON contains a list of records
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    else:
                        df = pd.json_normalize(data)

                    dataframes.append(df)

            except Exception as e:
                print(f"Error reading {file}: {e}")

# Combine all DataFrames into one
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        print("Combined DataFrame created with shape:", final_df.shape)
    else:
        print("No JSON files were successfully loaded.")

# Preview the result
    final_df.head()
    return final_df