from pathlib import Path
from datetime import datetime, timedelta
import requests
from typing import Optional, List, Tuple
import pandas as pd
import numpy as np
from tqdm import tqdm
from paths import RAW_DATA_DIR

def download_file_of_raw_data(year: int, month: int) -> Path:
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    res = requests.get(url)

    if res.status_code == 200:
        path = RAW_DATA_DIR / f'rides_{year}-{month:02d}.parquet'
        open(path, "wb").write(res.content)
        return path
    else:
        raise Exception(f'{url} is not available.')

# removes all the incorrect data from specific period
# e.g removes all unwanted months from february
def validate_raw_data(
        rides: pd.DataFrame,
        year: int,
        month: int
) -> pd.DataFrame:
    # this_month_start = f'{year}-{month:02d}-01'
    # next_month_start = f'{year}-{month:02d}-01' if month < 12 else f'{year+1}-01-01'
    # rides = rides[rides.pickup_datetime >= this_month_start]
    # rides = rides[rides.pickup_datetime < next_month_start]

    # return rides

    rides = rides[rides.pickup_datetime.dt.year == year]
    return rides

#loads raw data from storage or download it from website and then loading it into pandas dataframe
def load_raw_data(
    year: int,
    months: Optional[List[int]] = None
) -> pd.DataFrame:

    rides = pd.DataFrame()
    
    if months is None:
        months = list(range(1, 13))
    elif isinstance(months, int):
        months = [months]

    #if file is already here dont download it
    for month in months:
        local_file = RAW_DATA_DIR / f'rides_{year}-{month:02d}.parquet'
        if not local_file.exists():
            try:
                print(f'Downloading file {year}-{month:02d}')
                download_file_of_raw_data(year, month)
            except:
                print(f'{year}-{month:02d} file is not available')
                continue
        else:
            print(f'File {year}-{month:02d} was already in local storage') 

        # load the file into Pandas
        rides_one_month = pd.read_parquet(local_file)
        

        # rename columns and take only the ones we need
        rides_one_month = rides_one_month[['tpep_pickup_datetime', 'PULocationID']]
        rides_one_month.rename(columns={
            'tpep_pickup_datetime': 'pickup_datetime',
            'PULocationID': 'pickup_location_id',
        }, inplace=True)
        

        # validate the file
        rides_one_month = validate_raw_data(rides_one_month, year, month)

        # append to existing data
        
        rides = pd.concat([rides, rides_one_month], ignore_index=True)
        # rides = rides.append(rides_one_month removed in pandas 2.0
    
    rides = rides[['pickup_datetime', 'pickup_location_id']]

    return rides

#add rows that have no rides
def add_missing_slots(ts_data: pd.DataFrame) -> pd.DataFrame:
    location_ids = range(1, ts_data['pickup_location_id'].max() + 1)

    full_range = pd.date_range(ts_data['pickup_hour'].min(),
                               ts_data['pickup_hour'].max(),
                               freq='H')
    output = pd.DataFrame()
    for location_id in tqdm(location_ids):

        # keep only rides for this 'location_id'
        ts_data_i = ts_data.loc[ts_data.pickup_location_id == location_id, ['pickup_hour', 'rides']]
        
        if ts_data_i.empty:
            # add a dummy entry with a 0
            ts_data_i = pd.DataFrame.from_dict([
                {'pickup_hour': ts_data['pickup_hour'].max(), 'rides': 0}
            ])

        ts_data_i.set_index('pickup_hour', inplace=True)
        ts_data_i.index = pd.DatetimeIndex(ts_data_i.index)
        ts_data_i = ts_data_i.reindex(full_range, fill_value=0)
        
        # add back `location_id` columns
        ts_data_i['pickup_location_id'] = location_id

        output = pd.concat([output, ts_data_i])
    
    # move the pickup_hour from the index to a dataframe column
    output = output.reset_index().rename(columns={'index': 'pickup_hour'})
    
    return output


def transform_raw_data_into_ts_data(
    rides: pd.DataFrame
) -> pd.DataFrame:
    rides['pickup_hour'] = rides['pickup_datetime'].dt.floor('H')
    agg_rides = rides.groupby(['pickup_hour', 'pickup_location_id']).size().reset_index()
    agg_rides.rename(columns={0: 'rides'}, inplace=True)

    agg_rides_all_slots = add_missing_slots(agg_rides)

    return agg_rides_all_slots

def transform_ts_data_into_features_and_target(
    ts_data: pd.DataFrame,
    input_seq_len: int,
    step_size: int
) -> pd.DataFrame:
    """
    Slices and transposes data from time-series format into a (features, target)
    format that we can use to train Supervised ML models
    """
    assert set(ts_data.columns) == {'pickup_hour', 'rides', 'pickup_location_id'}

    location_ids = ts_data['pickup_location_id'].unique()
    features = pd.DataFrame()
    targets = pd.DataFrame()
    
    for location_id in tqdm(location_ids):
        
        # keep only ts data for this `location_id`
        ts_data_one_location = ts_data.loc[
            ts_data.pickup_location_id == location_id, 
            ['pickup_hour', 'rides']
        ]

        # pre-compute cutoff indices to split dataframe rows
        indices = get_cutoff_indices_features_and_target(
            ts_data_one_location,
            input_seq_len,
            step_size
        )

        # slice and transpose data into numpy arrays for features and targets
        n_examples = len(indices)
        x = np.ndarray(shape=(n_examples, input_seq_len), dtype=np.float32)
        y = np.ndarray(shape=(n_examples), dtype=np.float32)
        pickup_hours = []
        
        for i, idx in enumerate(indices):
            x[i, :] = ts_data_one_location.iloc[idx[0]:idx[1]]['rides'].values
            y[i] = ts_data_one_location.iloc[idx[1]:idx[2]]['rides'].values
            pickup_hours.append(ts_data_one_location.iloc[idx[1]]['pickup_hour'])

        # numpy -> pandas
        features_one_location = pd.DataFrame(
            x,
            columns=[f'rides_previous_{i+1}_hour' for i in reversed(range(input_seq_len))]
        )
        features_one_location['pickup_hour'] = pickup_hours
        features_one_location['pickup_location_id'] = location_id

        # numpy -> pandas
        targets_one_location = pd.DataFrame(y, columns=[f'target_rides_next_hour'])

        # concatenate results
        features = pd.concat([features, features_one_location])
        targets = pd.concat([targets, targets_one_location])

    features.reset_index(inplace=True, drop=True)
    targets.reset_index(inplace=True, drop=True)

    print(len(features))

    return features, targets['target_rides_next_hour']

def get_cutoff_indices_features_and_target(
    data: pd.DataFrame,
    input_seq_len: int,
    step_size: int
    ) -> list:

        stop_position = len(data) - 1
        
        # Start the first sub-sequence at index position 0
        subseq_first_idx = 0
        subseq_mid_idx = input_seq_len
        subseq_last_idx = input_seq_len + 1
        indices = []
        
        while subseq_last_idx <= stop_position:
            indices.append((subseq_first_idx, subseq_mid_idx, subseq_last_idx))
            subseq_first_idx += step_size
            subseq_mid_idx += step_size
            subseq_last_idx += step_size

        return indices