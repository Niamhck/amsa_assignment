# Full Code for Data Preprocessing
#----------------------------------

# Needed to change the format from .nc (for storing climate data) to .csv
# Filter out data for Belgium and the Netherlands
# Aggregate the data into quarterly format
# Drop unnecessary columns - double cloud cover and precipitation columns
# Print the dataset statistics for the BYOD assignment

import os
import xarray as xr
import pandas as pd

# Step 1: Process NetCDF files and save to CSV
data_folder = "/Users/niamhcallinankeenan/Downloads/climate_data/files_for_analysis"
output_csv = "benelux_combined_data_filtered.csv"

# Latitude and longitude bounds for Belgium and the Netherlands
lat_min, lat_max = 49, 53.5  
lon_min, lon_max = 2.5, 7.5  

merged_data = None

for file in os.listdir(data_folder):
    if file.endswith(".nc"):
        file_path = os.path.join(data_folder, file)
        print(f"Processing file: {file}")  # Log progress
        
        with xr.open_dataset(file_path, chunks={'time': 10}) as ds:
            ds_subset = ds.sel(latitude=slice(lat_min, lat_max), longitude=slice(lon_min, lon_max))
            
            for variable_name in ds_subset.data_vars:
                print(f"  Processing variable: {variable_name}")
                data_array = ds_subset[variable_name]  
                
                df = data_array.to_dataframe().reset_index()
                
                if "time" in df.columns:
                    df["time"] = pd.to_datetime(df["time"])
                    df["year"] = df["time"].dt.year
                    df["month"] = df["time"].dt.month
                    df = df.drop(columns=["time"])

                df = df[df["year"] < 2024]
                
                df = df[["year", "month", "latitude", "longitude", variable_name]].rename(columns={variable_name: f"{variable_name}_{file}"})
                
                if merged_data is None:
                    merged_data = df
                else:
                    merged_data = pd.merge(merged_data, df, on=["year", "month", "latitude", "longitude"], how="outer")


merged_data.to_csv(output_csv, index=False)
print(f"Data has been processed and saved to '{output_csv}'.")

# Step 2: Aggregate the climate data into quarterly format and drop unnecessary columns
df = pd.read_csv(output_csv)
df['quarter'] = ((df['month'] - 1) // 3) + 1
df = df.drop(columns=['month'])
intermediate_csv = "benelux_without_month.csv"
df.to_csv(intermediate_csv, index=False)

columns_to_drop = ['cloud-cover_monthly-mean', 'precipitation_monthly-mean']
df = df.drop(columns=columns_to_drop, errors='ignore') 
print(f"Columns dropped: {columns_to_drop}")

quarterly_data = (
    df.groupby(['year', 'quarter', 'latitude', 'longitude'])
    .mean(numeric_only=True) 
    .reset_index()
)

final_csv = "benelux_combined_quarterly_updated.csv"
quarterly_data.to_csv(final_csv, index=False)
print(f"Final quarterly dataset saved to '{final_csv}'.")

# Step 3: Print dataset statistics for the BYOD assignment
print(f"Final dataset dimensions: {quarterly_data.shape}")
print(f"Number of data points: {len(quarterly_data)}")
print("Column titles:")
print(quarterly_data.columns.tolist())
