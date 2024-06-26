import pandas as pd
from sys import argv
import re
import os

# Get folder path from command line argument
folder_path = re.sub(r'\/$', '', argv[1])

# Validate the folder path
if not os.path.isdir(folder_path):
    print(f"Error: The folder '{folder_path}' does not exist.")
    exit(1)

# Define the output file path explicitly within the folder
output_file_path = os.path.join(folder_path, 'combined_output.csv')

# Get names of CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Check if there are any CSV files in the folder
if not csv_files:
    print(f"No CSV files found in the folder '{folder_path}'.")
    exit(1)

# Read each CSV file and store the dataframes in a list
dfs = []
for csv in csv_files:
    df = pd.read_csv(os.path.join(folder_path, csv), header=0)
    dfs.append(df)

# Combine dataframes into one dataframe
if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    
    # Write the combined dataframe to the output file
    final_df.to_csv(output_file_path, index=False, mode="w")
    
    # # Delete each original file
    # for csv in csv_files:
    #     os.remove(os.path.join(folder_path, csv))
    #     crc_file = os.path.join(folder_path, "." + csv + ".crc")
    #     if os.path.exists(crc_file):
    #         os.remove(crc_file)
    
    print(f"Combined CSV files have been saved to '{output_file_path}' and original files have been deleted.")
else:
    print("No dataframes to combine.")