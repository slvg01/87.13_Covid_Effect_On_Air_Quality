import requests
import pandas as pd
from IPython.display import display
import time
import logging


# voir ce que signikfie les location is monitor  et is mobile pour les exlcure ,~
# exclure les sans data de début mais avant test des station pour voir. 
# faire un scope plus réduit  >> qq pays seulement 

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Définition des paramètres globaux
MAX_RETRIES = 3  # Nombre max de tentatives en cas d'échec
RETRY_DELAY = 2  # Délai entre chaque tentative (en secondes)
API_KEY = "01946b8515c545443cdcd262a884a88dab1be54962aad37f4f93c3420cc49844"
headers = {"X-API-Key": API_KEY, 'Accept-Charset': 'utf-8'}



def extract_data (url): 

    params = {
        "limit": 1000,
        "page": 1
    }

    all_results = []

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            break

        data = response.json()
        results = data.get('results', [])
        all_results.extend(results)

        if len(results) < params['limit']:
            break

        params['page'] += 1

        #if params['page'] > 1:  
            #break
            

    return all_results 





# Fonction pour extraire les données avec gestion des erreurs et retry
def fetch_with_retry(url, max_retries=MAX_RETRIES, delay=RETRY_DELAY):

    """Tente d'extraire les données d'une URL avec une gestion d'erreur en cas de dépassement de quota."""
    retries = 0
    while retries < max_retries:
        try:
            return extract_data(url)  # Fonction d'extraction des données
        except Exception as e:
            error_message = str(e)
            if "Many Requests" in error_message or "429" in error_message:
                retries += 1
                logging.warning(f"URL échouée (tentative {retries}/{max_retries}): {url} -> {error_message}")
                time.sleep(delay)  # Attente avant de réessayer
            else:
                logging.error(f"Erreur non récupérable sur {url}: {error_message}")
                break  # Sort de la boucle en cas d'autre erreur
    return None  # Retourne None si toutes les tentatives ont échoué




# Getting worldwide sensor locations
url= "https://api.openaq.org/v3/locations"
locations_raw = extract_data(url)

df_locations_raw = pd.DataFrame(locations_raw)
df_locations_raw.rename(columns={'id': 'location_id', 'name': 'location_name'}, inplace=True)
df_locations_raw.to_csv('pollution_data/1_locations_raw.csv', index=True)



#Unpack countries and coordinates
df_country = df_locations_raw['country'].apply(pd.Series)
df_coordinates = df_locations_raw['coordinates'].apply(pd.Series)
df_DatetimeFirst = df_locations_raw['datetimeFirst'].apply(pd.Series)
df_Datetimelast = df_locations_raw['datetimeLast'].apply(pd.Series)

df_country.columns= ['country_id', 'country_code', 'country_name']
df_coordinates.columns= ['latitude', 'longitude']
df_DatetimeFirst.columns= ['datetimeFirst.utc', 'datetimeFirst.local']
df_Datetimelast.columns= ['datetimeLast.utc', 'datetimeLast.local']
df_locations_unpacked=pd.concat([df_locations_raw,df_country, df_coordinates, df_DatetimeFirst, df_Datetimelast],axis=1)


# Pack & clean 
df_locations_unpacked.drop(['owner', 'provider', 'isMobile', 'isMonitor', 'instruments', 'licenses', 'bounds', 'distance', 'country', 'coordinates', 'datetimeFirst.utc', 'datetimeLast.utc'], axis=1, inplace=True,errors='ignore')
df_locations_clean = df_locations_unpacked
df_locations_clean.to_csv('pollution_data/2_locations_clean.csv', index=True)



# Unpack  the 'sensors' list of sensors : vertically
df_exploded = df_locations_clean.explode("sensors")
df_exploded.to_csv('pollution_data/3_exploded.csv', index=True)




# Expand the nested dictionary structure of the 'sensors' column
sensors_normalized = pd.json_normalize(df_exploded['sensors'])



# Concatenate all dataframe
df_exploded = pd.concat([df_exploded.reset_index(drop=True), sensors_normalized], axis=1)
df_exploded.to_csv('pollution_data/4_exploded.csv', index=True)

# Clean the DataFrame 
df_exploded["sensor_id"] = df_exploded["id"]
df_exploded["sensor_name"] = df_exploded["parameter.name"]
df_exploded["parameter_id"] = df_exploded["parameter.id"]
df_exploded["units"] = df_exploded["parameter.units"]
df_exploded["parameter_name"] = df_exploded["parameter.displayName"]
df_exploded["datetimeFirst_local"] = df_exploded["datetimeFirst.local"]
df_exploded["datetimeLast_local"] = df_exploded["datetimeLast.local"]
df_exploded.drop(['sensors', 'id', 'name', 'parameter.id' ,'parameter.name', 'parameter.units', 'parameter.displayName', 'datetimeFirst', 'datetimeLast'], axis=1, inplace=True)


#df_exploded.reset_index(drop=True, inplace=True)
df_location_final = df_exploded
df_location_final.to_csv('pollution_data/4_location_final.csv', index=True)
print(f'location_final lenght : {len(df_location_final)}')


# restric to belgium geography
df_location_final_belgium = df_location_final[df_location_final['country_code']=='BE']
df_location_final_belgium.to_csv('pollution_data/5_location_final_belgium.csv', index=True)
print(f'location_final_belgium_length :  {len(df_location_final_belgium)}')

'''
# restric to luxembourg geography
df_location_final_lux = df_location_final[df_location_final['country_code']=='LU']
df_location_final_lux.to_csv('pollution_data/5_location_final_lux.csv', index=True)
print(f'location_final_lux length :  {len(df_location_final_lux)}')
'''


''' 
# restric to france geography
df_location_final_france = df_location_final[df_location_final['country_code']=='FR']
df_location_final_france.to_csv('pollution_data/5_location_final_france.csv', index=True)
print(f'location_final_france length :  {len(df_location_final_france)}')
'''


# generate URLs list
urls_list = [f"https://api.openaq.org/v3/sensors/{sensor_id}/days/monthly" for sensor_id in df_location_final_belgium['sensor_id']]
print(f'length url list : {len(urls_list)}')

"""

# Extract monthly measurements sensor data from URLs
data_df = pd.DataFrame()
counter = 0
for url in urls_list: 
    sensor_id = int(url.split("/")[-3])
    sensors_data = extract_data(url)
    counter += 1
    
    time.sleep(0.01)
    
    data = []
    
    # Iterate through the extracted data and append sensor_id to each record
    for record in sensors_data:
        record["sensor_id"] = sensor_id
        data.append(record)
    
    # Convert the list to a DataFrame and 
    data_df_url = pd.DataFrame(data)
    
    # Append the current URL's data to the main DataFrame
    data_df = pd.concat([data_df, data_df_url], ignore_index=True)
    print(f"URL {counter} >> data extracted ")


# Unpack the 'summary' column
if 'summary' in data_df.columns:
    summary_df = pd.json_normalize(data_df['summary'])
    data_df = pd.concat([data_df, summary_df], axis=1).drop('summary', axis=1)



# Extract year,  month, day frol local time column 
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

else:
    print("The 'period' column is missing in the DataFrame.")



# unpack the coverage column 
coverage_df = pd.json_normalize(data_df['coverage'])
final_data_df = pd.concat([data_df, coverage_df], axis=1).drop('coverage', axis=1)
final_data_df.drop(['flagInfo', 'period', 'parameter', 'coordinates', 'expectedInterval', 'observedInterval', 'datetimeFrom.utc', 'datetimeTo.utc','q02', 'q25', 'q75', 'q98'], axis=1, inplace=True, errors='ignore')
print(len(data_df))
final_data_df.to_csv('pollution_data/6_sensor_data_final.csv', index=True)"""




# Initialisation du DataFrame principal
data_df = pd.DataFrame()

# Boucle sur les URLs
for counter, url in enumerate(urls_list, start=1): 
    sensor_id = int(url.split("/")[-3])
    sensors_data = fetch_with_retry(url)

    if sensors_data is None:
        logging.error(f"Abandon de l'URL après {MAX_RETRIES} tentatives: {url}")
        continue  # Passe à l'URL suivante si toutes les tentatives ont échoué
    
    time.sleep(0.01)  # Pause entre chaque requête pour éviter les limites d'API

    # Ajout de l'ID du capteur aux données
    for record in sensors_data:
        record["sensor_id"] = sensor_id

    # Conversion en DataFrame et ajout au DataFrame principal
    data_df = pd.concat([data_df, pd.DataFrame(sensors_data)], ignore_index=True)
    logging.info(f"URL {counter} >> Données extraites avec succès")

# Décompactage des colonnes JSON si elles existent
if 'summary' in data_df.columns:
    data_df = pd.concat([data_df, pd.json_normalize(data_df['summary'])], axis=1).drop(columns=['summary'], errors='ignore')

if 'period' in data_df.columns:
    data_df['year'] = data_df['period'].apply(lambda x: pd.to_datetime(x['datetimeFrom']['local']).strftime('%Y') if isinstance(x, dict) else None)
    data_df['month'] = data_df['period'].apply(lambda x: pd.to_datetime(x['datetimeFrom']['local']).strftime('%m') if isinstance(x, dict) else None)
    data_df['day'] = data_df['period'].apply(lambda x: pd.to_datetime(x['datetimeFrom']['local']).strftime('%d') if isinstance(x, dict) else None)
else:
    logging.warning("La colonne 'period' est absente du DataFrame.")

# Décompactage de la colonne 'coverage' si elle existe
if 'coverage' in data_df.columns:
    data_df = pd.concat([data_df, pd.json_normalize(data_df['coverage'])], axis=1).drop(columns=['coverage'], errors='ignore')

# Suppression des colonnes inutiles
cols_to_drop = ['flagInfo', 'period', 'parameter', 'coordinates', 'expectedInterval', 'observedInterval',
                'datetimeFrom.utc', 'datetimeTo.utc', 'q02', 'q25', 'q75', 'q98']
data_df.drop(columns=cols_to_drop, errors='ignore', inplace=True)

# Export des données finales
data_df.to_csv('pollution_data/6_sensor_data_final.csv', index=True)
logging.info(f"Extraction terminée avec {len(data_df)} lignes enregistrées.")

