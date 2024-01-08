import os
import modal
import requests

LOCAL=True

if LOCAL == False:
   stub = modal.Stub("flood_3_hours")
   image = modal.Image.debian_slim().pip_install(["hopsworks"]) 

   @stub.function(image=image, schedule=modal.Period(hours=3), secret=modal.Secret.from_name("HOPSWORKS_API_KEY"))
   def f():
       g()


def get_water_level():
    url = f"http://environment.data.gov.uk/flood-monitoring/id/stations/2904TH/measures"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json().get("items", [])
        # how to get the water level?
    else:
        print(f"Error fetching API request", response.status_code)
        return -1

def get_flood_warning():
    url = f"http://environment.data.gov.uk/flood-monitoring/id/floods"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json().get("items", [])
        # how to get the flood warning?
    else:
        print(f"Error fetching API request", response.status_code)
        return -1

def g():
    import hopsworks
    import pandas as pd

    project = hopsworks.login()
    fs = project.get_feature_store()

    water_level = get_water_level()
    flood_warning = get_flood_warning()
    
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
