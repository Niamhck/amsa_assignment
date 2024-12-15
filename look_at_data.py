import os
import netCDF4 as nc

# Define the path to the folder containing .nc files
folder_path = "/Users/niamhcallinankeenan/Downloads/amsa_data"

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".nc"):
        file_path = os.path.join(folder_path, filename)
        print(f"Inspecting file: {filename}")
        
        # Open the .nc file and print a summary
        with nc.Dataset(file_path, 'r') as dataset:
            print(dataset)  # Prints metadata and structure
            print("\n")
            