import pandas as pd
from sklearn.preprocessing import FunctionTransformer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline, Pipeline

import lightgbm as lgb

def average_rides_last_4_weeks(x: pd.DataFrame) -> pd.DataFrame:
    """
    Adds one column with the average rides from
    - 7 days ago
    - 14 days ago
    - 21 days ago
    - 28 days ago
    """
    x['average_rides_last_4_weeks'] = 0.25*(
        x[f'rides_previous_{7*24}_hour'] + \
        x[f'rides_previous_{2*7*24}_hour'] + \
        x[f'rides_previous_{3*7*24}_hour'] + \
        x[f'rides_previous_{4*7*24}_hour']
    )
    return x


class TemporalFeaturesEngineer(BaseEstimator, TransformerMixin):
    """
    Scikit-learn data transformation that adds 2 columns
    - hour
    - day_of_week
    and removes the `pickup_hour` datetime column.
    """
    def fit(self, x, y=None):
        return self
    
    def transform(self, x, y=None):
        
        x_ = x.copy()
        
        # Generate numeric columns from datetime
        x_["hour"] = x_['pickup_hour'].dt.hour
        x_["day_of_week"] = x_['pickup_hour'].dt.dayofweek
        
        return x_.drop(columns=['pickup_hour'])

def get_pipeline(**hyperparams) -> Pipeline:

    # sklearn transform
    add_feature_average_rides_last_4_weeks = FunctionTransformer(
        average_rides_last_4_weeks, validate=False)
    
    # sklearn transform
    add_temporal_features = TemporalFeaturesEngineer()

    # sklearn pipeline
    return make_pipeline(
        add_feature_average_rides_last_4_weeks,
        add_temporal_features,
        lgb.LGBMRegressor(**hyperparams)
    )