import zipfile
from datetime import datetime, timedelta

import requests
import numpy as np
import pandas as pd
import streamlit as st
import geopandas as gpd
import pydeck as pdk

from inference import(
    load_batch_of_features_from_store,
    load_predictions_from_store
)

from paths import DATA_DIR
from plot import plot_one_sample

st.set_page_config(layout="wide")

current_date = pd.to_datetime(datetime.utcnow()).floor(freq='h')


st.title(f'Taxi demand predictions')
st.header(f'{current_date} UTC')

progress_bar = st.sidebar.header("Working progress")
progress_bar = st.sidebar.progress(0)
N_STEPS = 6

def load_shape_data_file() -> gpd.geodataframe.GeoDataFrame:
    URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    response = requests.get(URL)
    path = DATA_DIR / f'taxi_zones.zip'

    if response.status_code == 200:
        open(path, 'wb').write(response.content)
    else:
        raise Exception(f'{URL} is not available')
    
    #we unzip it
    
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(DATA_DIR / 'taxi_zones')

    #geopandas dataframe
    return gpd.read_file(DATA_DIR / 'taxi_zones/taxi_zones.shp').to_crs('epsg:4326')

@st.cache_data
def _load_batch_of_features_from_store(current_date: datetime) -> pd.DataFrame:
    return load_batch_of_features_from_store(current_date)

#this cache data speeds up the process if the data has already been loaded
@st.cache_data
def _load_predictions_from_store(
    from_pickup_hour: datetime,
    to_pickup_hour: datetime
) -> pd.DataFrame: 
    return load_predictions_from_store(from_pickup_hour, to_pickup_hour)

with st.spinner(text="Downloading shape file to plot taxi zones"):
    geo_df = load_shape_data_file()
    st.sidebar.write('Shape file was downloaded (Done)')
    progress_bar.progress(1/N_STEPS)

with st.spinner(text="Fetching model predictionns from the store"):
    predictions_df = _load_predictions_from_store(
        from_pickup_hour=current_date-timedelta(hours=1),
        to_pickup_hour=current_date
    )

    st.sidebar.write('Model predictions arrived (Done)')
    progress_bar.progress(2/N_STEPS)

print('=====================')
print(current_date -timedelta(hours=1) )
print(current_date)
print('=====================')
print(predictions_df.head(10), 'data?')

#here we are checking the predictions for the current hour have already been computed
#and are available
next_hour_predictions_ready = \
    False if predictions_df[predictions_df['pickup_hour'].dt.strftime('%Y-%m-%d %H:%M:%S') == (current_date).strftime('%Y-%m-%d %H:%M:%S')].empty else True
prev_hour_predictions_ready = \
    False if predictions_df[predictions_df['pickup_hour'].dt.strftime('%Y-%m-%d %H:%M:%S') == (current_date - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')].empty else True


print('-----------------------')
# print(predictions_df['pickup_hour'].dt.strftime('%Y-%m-%d %H:%M:%S').head(10) == current_date - timedelta(hours=1))
# print('-----------------------')
# print(current_date - timedelta(hours=1))

if next_hour_predictions_ready:
    predictions_df = predictions_df[predictions_df['pickup_hour'].dt.strftime('%Y-%m-%d %H:%M:%S') == (current_date).strftime('%Y-%m-%d %H:%M:%S')]
elif prev_hour_predictions_ready:
    predictions_df = predictions_df[predictions_df['pickup_hour'].dt.strftime('%Y-%m-%d %H:%M:%S') == (current_date - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')]
    current_date = current_date - timedelta(hours=1)
    st.subheader('The most recent data is not yet available. Using last hour predictions')
else:
    raise Exception('Features are not available for the last 2 hours. Is your feature pipeline up and running?')

print(predictions_df)


with st.spinner(text="Preparing data to plot"):
    def pseuocolor(val, minval, maxval, startcolor, stopcolor):
        f = float(val-minval) / (maxval-minval)
        return tuple(f*(b-a)+a for (a,b) in zip(startcolor, stopcolor))
    
    df = pd.merge(geo_df, predictions_df, right_on='pickup_location_id', left_on='LocationID', how='inner')

    BLACK, GREEN = (0, 0, 0), (0, 255, 0)
    df['color_scaling'] = df['predicted_demand']
    max_pred, min_pred = df['color_scaling'].max(), df['color_scaling'].min()
    df['fill_color'] = df['color_scaling'].apply(lambda x: pseuocolor(x, min_pred, max_pred, BLACK, GREEN))
    progress_bar.progress(3/N_STEPS)

with st.spinner(text="Generating NYC Map"):
    INITIAL_VIEW_STATE = pdk.ViewState(
        latitude=40.7831,
        longitude=-73.9712,
        zoom=11,
        max_zoom=16,
        pitch=45,
        bearing=0
    )

    geojson = pdk.Layer(
        "GeoJsonLayer",
        df,
        opacity=0.25,
        stroked=False,
        filled=True,
        extruded=False,
        wireframe=True,
        get_elevation=10,
        get_fill_color="fill_color",
        get_line_color=[255, 255, 255],
        auto_highlight=True,
        pickable=True,
    )

    tooltip = {"html": "<b>Zone:</b> [{LocationID}]{zone} <br /> <b>Predicted rides:</b> {predicted_demand}"}

    r = pdk.Deck(
        layers=[geojson],
        initial_view_state=INITIAL_VIEW_STATE,
        tooltip=tooltip
    )

    st.pydeck_chart(r)
    progress_bar.progress(4/N_STEPS)

with st.spinner(text="Fetching batch of features used in the last run"):
    features_df = _load_batch_of_features_from_store(current_date)
    st.sidebar.write("Inference features fetched from the store")
    progress_bar.progress(5/N_STEPS)

with st.spinner(text="Plotting time-series data"):
    row_indeces = np.argsort(predictions_df['predicted_demand'].values)[::-1]
    n_to_plot = 10

    for row_id in row_indeces[:n_to_plot]:
        fig = plot_one_sample(
            features=features_df,
            targets=predictions_df['predicted_demand'],
            example_id=row_id,
            predictions=pd.Series(predictions_df['predicted_demand'])
        )
        st.plotly_chart(fig, theme='streamlit', use_container_width=True, width=1000)

    progress_bar.progress(6/N_STEPS)