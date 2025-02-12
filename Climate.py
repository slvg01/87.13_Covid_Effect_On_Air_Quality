import cdsapi
import zipfile
import xarray as xr
import pandas as pd
import glob
import os

# upload the data from the API
dataset = "sis-ecde-climate-indicators"
request = {
    "variable": [
        "mean_temperature",
        "tropical_nights",
        "hot_days",
        "warmest_three_day_period",
        "heatwave_days",
        "frost_days",
        "daily_maximum_temperature",
        "daily_minimum_temperature",
        "maximum_consecutive_five_day_precipitation",
        "extreme_precipitation_total",
        "frequency_of_extreme_precipitation",
        "consecutive_dry_days",
        "duration_of_meteorological_droughts",
        "magnitude_of_meteorological_droughts",
        "days_with_high_fire_danger",
        "mean_wind_speed",
        "extreme_wind_speed_days",
        "fire_weather_index"
    ],
    "origin": "reanalysis",
    "temporal_aggregation": ["yearly"],
    "spatial_aggregation": "gridded",
    #"spatial_aggregation": "regional_layer",
    #"regional_layer": ["eea_38"],
    "other_parameters": [
        "max",
        "mean",
        "min",
        "30_c",
        "35_c",
        "40_c"
    ]
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()


#unpack the zip file of .nc file 
zip_path = "b3c589af62a506076bea6d11b376e2b1.zip"  
extract_path = "climate_data"  
with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extract_path)

nc_files = glob.glob(f"{extract_path}/*.nc")
print(f"Extracted files: {nc_files}")



#list the unzipped .nc files 
nc_files = glob.glob(f"{extract_path}/*.nc")  


#loop sur les files pour transfer des .nf  sur dataframe
dfs = []  
for file in nc_files:
    ds = xr.open_dataset(file)  # Load NetCDF file
    df = ds.to_dataframe().reset_index()  
    df["indicator"] = os.path.basename(file).replace(".nc", "")  # Add filename as indicator column
    dfs.append(df)

# Merge all DataFrames
final_df = pd.concat(dfs, ignore_index=True)

# # Reorder/clean the DF a bit
col_to_move = final_df.columns[4]  
col_data = final_df[col_to_move]  
final_df = final_df.drop(columns=[col_to_move])
final_df = final_df.drop(columns=['eea_38', 'realization'])
final_df.insert(0, col_to_move, col_data)

print(final_df.head())  

final_df.to_csv("climate_data/climate_output.csv", index=False)
