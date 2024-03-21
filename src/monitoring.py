from datetime import datetime, timedelta
from argparse import ArgumentParser

import pandas as pd

import config as config

from config import FEATURE_GROUP_PREDICTIONS_METADATA, FEATURE_GROUP_METADATA
from feature_store_api import get_or_create_feature_group, get_feature_store

def load_predictions_and_actual_values_from_store(
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:

    # 2 feature groups we need to merge
    predictions_fg = get_or_create_feature_group(FEATURE_GROUP_PREDICTIONS_METADATA)
    actuals_fg = get_or_create_feature_group(FEATURE_GROUP_METADATA)

    # query to join the 2 features groups by `pickup_hour` and `pickup_location_id`
    from_ts = int(from_date.timestamp() * 1000)
    to_ts = int(to_date.timestamp() * 1000)
    query = predictions_fg.select_all() \
        .join(actuals_fg.select(['pickup_location_id', 'pickup_hour', 'rides']),
              on=['pickup_hour', 'pickup_location_id'], prefix=None) \
        .filter(predictions_fg.pickup_hour >= from_ts) \
        .filter(predictions_fg.pickup_hour <= to_ts)
    
    # breakpoint()

    # create the feature view `config.FEATURE_VIEW_MONITORING` if it does not
    # exist yet
    feature_store = get_feature_store()
    try:
        # create feature view as it does not exist yet
        feature_store.create_feature_view(
            name='monitoring_feature_view1',
            version=config.MONITORING_FV_VERSION,
            query=query
        )
    except:
        print('Feature view already existed. Skip creation.') #fix

    # feature view
    monitoring_fv = feature_store.get_feature_view(
        name='monitoring_feature_view1',
        version=config.MONITORING_FV_VERSION
    )
    
    # fetch data form the feature view
    # fetch predicted and actual values for the last 30 days
    monitoring_df = monitoring_fv.get_batch_data(
        start_time=from_date - timedelta(days=7),
        end_time=to_date + timedelta(days=7),
    )

    # filter data to the time period we are interested in
    pickup_ts_from = int(from_date.timestamp() * 1000)
    pickup_ts_to = int(to_date.timestamp() * 1000)
    monitoring_df = monitoring_df[monitoring_df.pickup_ts.between(pickup_ts_from, pickup_ts_to)]

    return monitoring_df

if __name__ == '__main__':

    # parse command line arguments
    parser = ArgumentParser()
    parser.add_argument('--from_date',
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
                        help='Datetime argument in the format of YYYY-MM-DD HH:MM:SS')
    parser.add_argument('--to_date',
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
                        help='Datetime argument in the format of YYYY-MM-DD HH:MM:SS')
    args = parser.parse_args()


    monitoring_df = load_predictions_and_actual_values_from_store()