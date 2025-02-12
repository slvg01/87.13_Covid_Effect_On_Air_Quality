import cdsapi
import zipfile
import xarray as xr
import pandas as pd
import glob
import os
import requests

# Replace with your actual OpenAQ API key
API_KEY = "01946b8515c545443cdcd262a884a88dab1be54962aad37f4f93c3420cc49844"

# Define API endpoints (specific to New Delhi)
GEOBOX_DATA = [-10.5,36,9.6, 51.1]
COUNTRY_DATA = [23,79,60,22,67,27]
COUNTRY_DATA2 = ["NO","GB","BE","FR","SP","MA"]
LOCATION_ID = 8118  # New Delhi station ID
SENSOR_ID = 3917  # Ozone sensor (for New Delhi)
PARAMETER_ID = 2  # PM2.5 parameter

geobox_url = f"https://api.openaq.org/v3/locations?bbox={GEOBOX_DATA[0]},{GEOBOX_DATA[1]},{GEOBOX_DATA[2]},{GEOBOX_DATA[3]}&limit=1000"
location_url = f"https://api.openaq.org/v3/locations/{LOCATION_ID}"
sensor_url = f"https://api.openaq.org/v3/sensors/{SENSOR_ID}/days/yearly?limit=1000"
pm25_url = f"https://api.openaq.org/v3/parameters/{PARAMETER_ID}/latest?limit=1000"
countries_url = f"https://api.openaq.org/v3/countries"

# Headers with API key
headers = {"X-API-Key": API_KEY}

# Function to fetch data
def fetch_data(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# Fetch data
geobox_data = fetch_data(geobox_url)
sensor_data = fetch_data(sensor_url)
pm25_data = fetch_data(pm25_url)
countries_data = fetch_data(countries_url)

# Convert "results" field from JSON response into Pandas DataFrame

'''if geobox_data:
    geobox_df = pd.DataFrame(geobox_data.get("results",[]))
    print(geobox_df.columns)
    geobox_df["latitude"]=geobox_df["coordinates"].apply(lambda x: x['latitude'])
    geobox_df["longitude"]=geobox_df["coordinates"].apply(lambda x: x['longitude'])
    geobox_df = geobox_df[["id", "name", "locality", "country", "latitude", "longitude", "datetimeFirst", "datetimeLast"]]
    print(geobox_df)
    geobox_df.to_csv('pollution_data/locations')


if countries_data:
    countries_df = pd.DataFrame(countries_data.get("results",[]))
    print(countries_df.columns)
    print(countries_df.head())
    countries_df.to_csv('pollution_data/country_list.csv', index=False)'''

if sensor_data:
    sensor_df = pd.DataFrame(sensor_data.get("results", []))
    print("\nSensor Yearly Averages (New Delhi - Ozone Sensor):")
    print(sensor_df)

    #print(sensor_df[["year", "average"]])

'''if pm25_data:
    pm25_df = pd.DataFrame(pm25_data.get("results", []))

    # Filter only New Delhi data (by checking location name)
    #pm25_df = pm25_df[pm25_df["location"] == "New Delhi"]
    
    print("\nLatest PM2.5 Values (New Delhi):")
    print(pm25_df)
    #print(pm25_df[["location", "value", "unit", "date.utc"]])

# OPTIONAL: Merge data for analysis (if needed)
if not location_df.empty and not pm25_df.empty:
    combined_df = location_df.merge(pm25_df, how="inner", left_on="name", right_on="location")
    print("\nCombined Data for New Delhi:")
    print(combined_df.head())'''

