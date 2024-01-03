import requests
import csv

url = "https://environment.data.gov.uk/flood-monitoring/id/stations"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    items = data.get("items", [])

    # Filtrer les stations avec riverName égal à "River Thames"
    thames_stations = [station for station in items if station.get("riverName") == "River Thames"]

    # Nom du fichier CSV de sortie
    csv_filename = "thames_stations.csv"

    # Collecter tous les champs présents dans les stations
    all_fields = set()
    for thames_station in thames_stations:
        all_fields.update(thames_station.keys())

    # Écrire les données dans le fichier CSV
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = list(all_fields)
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Écrire l'en-tête du fichier CSV
        writer.writeheader()

        # Écrire les données des stations dans le fichier CSV avec un retour à la ligne entre chaque station
        for thames_station in thames_stations:
            writer.writerow(thames_station)

    print(f"Les données ont été écrites dans {csv_filename}.")
else:
    print("Erreur lors de la requête :", response.status_code)
