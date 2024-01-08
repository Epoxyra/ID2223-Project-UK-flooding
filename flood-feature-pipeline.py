import os
import modal
import requests

LOCAL=True

if LOCAL == False:
   stub = modal.Stub("flood_3_hours")
   image = modal.Image.debian_slim().pip_install(["hopsworks"]) 

   @stub.function(image=image, schedule=modal.Period(hours=3), secret=modal.Secret.from_name("id2223"))
   def f():
       g()


def get_water_level():
    url = f"http://environment.data.gov.uk/flood-monitoring/id/stations/2904TH/measures"
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response and retrieve the "items" list
        data = response.json().get("items", [])

        # Iterate over each measure in the data
        for measure in data:
            # Check if the measure has the qualifier "Stage" and the unit "mASD"
            if measure.get("qualifier") == "Stage" and measure.get("unitName") == "mASD":
                # Retrieve the value of the latest reading and return it
                water_level = measure.get("latestReading", {}).get("value")
                return water_level

        # If no valid measure is found, return None or any suitable default value
        return None
    else:
        print(f"Error fetching API request", response.status_code)
        return -1


def get_severity_level():
    # API endpoint URL for flood warnings
    url = "http://environment.data.gov.uk/flood-monitoring/id/floods"

    # Make a GET request to the API
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response and retrieve the "items" list
        data = response.json().get("items", [])

        # Iterate over each item in the data
        for item in data:
            # Check if the item has the desired floodAreaID
            if item.get("floodAreaID") == "061FWF23HammCrt":
                # Retrieve and return the severityLevel
                return item.get("severityLevel")

        # If the zone is not found, return None or any suitable default value
        return None
    else:
        # Print an error message if the API request fails
        print(f"Error fetching API request. Status code: {response.status_code}")
        return None

def g():
    import hopsworks
    import pandas as pd

    project = hopsworks.login()
    fs = project.get_feature_store()

    water_level = get_water_level()
    flood_warning = get_severity_level()
    
    df = pd.DataFrame({ "value": water_level, "flood_warning": flood_warning })

    quality_fg = fs.get_feature_group(name="flood",version=1)
    quality_fg.insert(df)

if __name__ == "__main__":
    if LOCAL == True :
        g()
    else:
        stub.deploy("flood_3_hours")
        with stub.run():
            f()
