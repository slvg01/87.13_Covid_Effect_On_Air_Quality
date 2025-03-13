import requests
import pandas as pd
import time
import logging




# ======= LOGGING CONFIG GLOBAL PARAMTERS DEFINITION  =======


MAX_RETRIES = 2  # Max attemt in case of extraction failure
RETRY_DELAY = 10  # Delay between each retry in case of failure (seconds)
API_KEY = "01946b8515c545443cdcd262a884a88dab1be54962aad37f4f93c3420cc49844"
HEADERS = {"X-API-Key": API_KEY, "Accept-Charset": "utf-8"}



    

# ======= FUNCTIONS  DEFINITION  =======


def extract_data(url):
    """Extract API URL data """
    params = {"limit": 1000, "page": 1}
    all_results = []

    while True:
        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code == 429:  
            raise Exception("Too Many Requests (429)")
           
        if response.status_code != 200:  # Manage others HTTP errors
            raise Exception(f"Erreur HTTP {response.status_code}: {response.text}")

        data = response.json()
        results = data.get("results", [])
        all_results.extend(results)

        if len(results) < params["limit"]:  
            break

        params["page"] += 1

    return all_results


def extract_with_retry(url, max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Gère les erreurs et applique un retry en cas d'erreur 429"""
    retries = 0

    while retries < max_retries:
        try:
            return extract_data(url)  
        except Exception as e:
            error_message = str(e)

            if "Too Many Requests" in error_message or "429" in error_message:
                time.sleep(delay)  # Pause entre les tentatives
                retries += 1
                
                
            else:
                return None #  others errors are fatal > out 

    return None  # max retries reached without success > out 





# ======= GETTING THE LOCATION DATAFRAME =======


# Getting worldwide sensor locations
url= "https://api.openaq.org/v3/locations"
locations_raw = extract_with_retry(url)

df_locations_raw = pd.DataFrame(locations_raw)
df_locations_raw.rename(columns={'id': 'location_id', 'name': 'location_name'}, inplace=True)



# Unpack countries and coordinates and time inside the Dataframe
df_country = df_locations_raw['country'].apply(pd.Series)
df_coordinates = df_locations_raw['coordinates'].apply(pd.Series)
df_DatetimeFirst = df_locations_raw['datetimeFirst'].apply(pd.Series)
df_DatetimeLast = df_locations_raw['datetimeLast'].apply(pd.Series)

df_country.columns= ['country_id', 'country_code', 'country_name']
df_coordinates.columns= ['latitude', 'longitude']
df_DatetimeFirst.columns= ['datetimeFirst.utc', 'datetimeFirst.local']
df_DatetimeLast.columns= ['datetimeLast.utc', 'datetimeLast.local']
df_locations_unpacked=pd.concat([df_locations_raw,df_country, df_coordinates, df_DatetimeFirst, df_DatetimeLast],axis=1)


# Clean the Dataframe
df_locations_unpacked.drop(['owner', 'provider', 'isMobile', 'isMonitor', 'instruments', 'licenses', 'bounds', 'distance', 'country', 'coordinates', 'datetimeFirst.utc', 'datetimeLast.utc'], axis=1, inplace=True,errors='ignore')
df_locations_clean = df_locations_unpacked



# Unpack  the 'sensors' list of sensors vertically
df_exploded = df_locations_clean.explode("sensors")



# Expand the nested dictionary structure of the 'sensors' column horizontally
sensors_normalized = pd.json_normalize(df_exploded['sensors'])


# Aggregate both Dataframes horizontally
df_exploded = pd.concat([df_exploded.reset_index(drop=True), sensors_normalized], axis=1)



# Clean the DataFrame 
df_exploded["sensor_id"] = df_exploded["id"]
df_exploded["sensor_name"] = df_exploded["parameter.name"]
df_exploded["parameter_id"] = df_exploded["parameter.id"]
df_exploded["units"] = df_exploded["parameter.units"]
df_exploded["parameter_name"] = df_exploded["parameter.displayName"]
df_exploded["datetimeFirst_local"] = df_exploded["datetimeFirst.local"]
df_exploded["datetimeLast_local"] = df_exploded["datetimeLast.local"]
df_exploded.drop(['sensors', 'id', 'name', 'parameter.id' ,'parameter.name', 'parameter.units', 'parameter.displayName', 'datetimeFirst.local', 'datetimeLast.local'], axis=1, inplace=True)
df_location_final = df_exploded




# ========== RESTRICT GEO IF NEEDED & GET SENSOR URL LIST TO FETCH ===========


# restric to luxembourg geography
df_location_final_be = df_location_final[df_location_final['country_code']=='BE']




# Generate URLs list
urls_list = [f"https://api.openaq.org/v3/sensors/{sensor_id}/days/monthly" for sensor_id in df_location_final_be['sensor_id']]





# ======= LOOP ON URLs LIST TO EXTRACT SENSOR DATA =======

data_df = pd.DataFrame()


for counter, url in enumerate(urls_list, start=1):
    sensor_id = int(url.split("/")[-3])
    sensors_data = extract_with_retry(url)

    if sensors_data is None:
        continue  

  
    # Add sendor ID to the data
    for record in sensors_data:
        record["sensor_id"] = sensor_id

    # Dataframe conversion and appending
    data_df = pd.concat([data_df, pd.DataFrame(sensors_data)], ignore_index=True)
    logging.info(f"✅ URL {counter}, for sensor id {sensor_id} >> Data extracted sucessfully :)")


    # Impose time delay to avoud API limit (60 call/s and 2K calls/h)
    time.sleep(0.65)  




# ======= DATA FINAL TRANSFORMATION =======

# Unpack Summary column
if 'summary' in data_df.columns:
    summary_df = pd.json_normalize(data_df['summary'])
    data_df = pd.concat([data_df, summary_df], axis=1).drop(columns=['summary'], errors='ignore')


# Unpack Period column (time data) 
if 'period' in data_df.columns:
    data_df['year'] = data_df['period'].apply(
        lambda x: pd.to_datetime(x['datetimeFrom']['local']).strftime('%Y') if isinstance(x, dict) and 'datetimeFrom' in x else None
    )
    data_df['month'] = data_df['period'].apply(
        lambda x: pd.to_datetime(x['datetimeFrom']['local']).strftime('%m') if isinstance(x, dict) and 'datetimeFrom' in x else None
    )
    data_df['day'] = data_df['period'].apply(
        lambda x: pd.to_datetime(x['datetimeFrom']['local']).strftime('%d') if isinstance(x, dict) and 'datetimeFrom' in x else None
    )


# Unpack coverage column
if 'coverage' in data_df.columns:
    coverage_df = pd.json_normalize(data_df['coverage'])
    data_df = pd.concat([data_df, coverage_df], axis=1).drop(columns=['coverage'], errors='ignore')


# Kill useless columns
cols_to_drop = ['flagInfo', 'period', 'parameter', 'coordinates', 'expectedInterval', 'observedInterval',
                'datetimeFrom.utc', 'datetimeTo.utc', 'q02', 'q25', 'q75', 'q98']
data_df.drop(columns=cols_to_drop, errors='ignore', inplace=True)