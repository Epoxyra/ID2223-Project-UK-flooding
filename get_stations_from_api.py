import requests
import json
import os

def get_station_measures(station_reference):
    url = f"http://environment.data.gov.uk/flood-monitoring/id/stations/{station_reference}/measures"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json().get("items", [])
    else:
        print(f"Erreur lors de la requête pour la station {station_reference} :", response.status_code)
        return []

def write_measures_to_json(station_reference, measures):
    if not os.path.exists("measures_from_Thames_stations"):
        os.makedirs("measures_from_Thames_stations")

    json_filename = f"measures_from_Thames_stations/{station_reference}_measures.json"

    with open(json_filename, mode='w', encoding='utf-8') as json_file:
        json.dump(measures, json_file, indent=2)

    print(f"Les mesures de la station {station_reference} ont été écrites dans {json_filename}.")

# Lire les références de station à partir du fichier texte
with open("Thames_station_reference.txt", "r") as file:
    station_references = [line.strip() for line in file if line.strip()]

for station_reference in station_references:
    measures = get_station_measures(station_reference)
    write_measures_to_json(station_reference, measures)
