import os
import modal

LOCAL = True

if LOCAL == False:
    stub = modal.Stub()
    hopsworks_image = modal.Image.debian_slim().pip_install(
        ["hopsworks", "joblib", "sklearn", "dataframe-image"])


    @stub.function(image=hopsworks_image, schedule=modal.Period(days=1),
                   secret=modal.Secret.from_name("HOPSWORKS_API_KEY"))
    def f():
        g()


def g():
    import pandas as pd
    import hopsworks
    import joblib
    import datetime
    from PIL import Image
    from datetime import datetime
    import dataframe_image as dfi
    from sklearn.metrics import confusion_matrix
    from matplotlib import pyplot
    import requests

    project = hopsworks.login()
    fs = project.get_feature_store()

    mr = project.get_model_registry()
    model = mr.get_model("flood_model", version=1)
    model_dir = model.download()
    model = joblib.load(model_dir + "/flood_model.pkl")
    feature_view = fs.get_feature_view(name="flood", version=1)
    batch_data = feature_view.get_batch_data()

    y_pred = model.predict(batch_data)
    # print(y_pred)
    offset = 1
    flood_alert = y_pred[y_pred.size - offset]
    flood_url = "https://raw.githubusercontent.com/Epoxyra/ID2223-Project-UK-flooding/main/images/" + flood_alert + ".png"
    print("flood predicted: " + flood_alert)
    img = Image.open(requests.get(flood_url, stream=True).raw)
    img.save("./latest_flood.png")
    dataset_api = project.get_dataset_api()
    dataset_api.upload("./latest_flood.png", "Resources/images", overwrite=True)

    flood_fg = fs.get_feature_group(name="flood", version=1)
    df = flood_fg.read()
    # print(df)
    label = df.iloc[-offset]["severitylevel"]
    label_url = "https://raw.githubusercontent.com/Epoxyra/id2223_lab1_flood/main/images/" + label + ".jpg"
    print("flood actual: " + label)
    img = Image.open(requests.get(label_url, stream=True).raw)
    img.save("./actual_flood.png")
    dataset_api.upload("./actual_flood.png", "Resources/images", overwrite=True)

    monitor_fg = fs.get_or_create_feature_group(name="flood_predictions",
                                                version=1,
                                                primary_key=["datetime"],
                                                description="flood risk forecast"
                                                )

    now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    data = {
        'prediction': [flood_alert],
        'label': [label],
        'datetime': [now],
    }
    monitor_df = pd.DataFrame(data)
    monitor_fg.insert(monitor_df, write_options={"wait_for_job": False})

    history_df = monitor_fg.read()
    # Add our prediction to the history, as the history_df won't have it -
    # the insertion was done asynchronously, so it will take ~1 min to land on App
    history_df = pd.concat([history_df, monitor_df])

    df_recent = history_df.tail(4)
    dfi.export(df_recent, './df_recent.png', table_conversion='matplotlib')
    dataset_api.upload("./df_recent.png", "Resources/images", overwrite=True)


if __name__ == "__main__":
    if LOCAL == True:
        g()
    else:
        with stub.run():
            f()

