from typing import Optional, List
from dataclasses import dataclass

import hsfs
import hopsworks

import config as config


def get_feature_store() -> hsfs.feature_store.FeatureStore:

    project = hopsworks.login(
        project=config.HOPSWORKS_PROJECT_NAME,
        api_key_value=config.HOPSWORKS_API_KEY
    )
    return project.get_feature_store()

# TODO: remove this function, and use get_or_create_feature_group instead
def get_feature_group(
    name: str,
    version: Optional[int] = 1
    ) -> hsfs.feature_group.FeatureGroup:

    return get_feature_store().get_feature_group(
        name=name,
        version=version,
    )

# def get_or_create_feature_group(
#     feature_group_metadata: FeatureGroupConfig
# ) -> hsfs.feature_group.FeatureGroup:

#     return get_feature_store().get_or_create_feature_group(
#         name=feature_group_metadata.name,
#         version=feature_group_metadata.version,
#         description=feature_group_metadata.description,
#         primary_key=feature_group_metadata.primary_key,
#         event_time=feature_group_metadata.event_time,
#         online_enabled=feature_group_metadata.online_enabled
#     )

# def get_or_create_feature_view(
#     feature_view_metadata: FeatureViewConfig
# ) -> hsfs.feature_view.FeatureView:

#     # get pointer to the feature store
#     feature_store = get_feature_store()

#     # get pointer to the feature group
#     # from src.config import FEATURE_GROUP_METADATA
#     feature_group = feature_store.get_feature_group(
#         name=feature_view_metadata.feature_group.name,
#         version=feature_view_metadata.feature_group.version
#     )

#     # create feature view if it doesn't exist
#     try:
#         feature_store.create_feature_view(
#             name=feature_view_metadata.name,
#             version=feature_view_metadata.version,
#             query=feature_group.select_all()
#         )
#     except:
#         logger.info("Feature view already exists, skipping creation.")
    
#     # get feature view
#     feature_store = get_feature_store()
#     feature_view = feature_store.get_feature_view(
#         name=feature_view_metadata.name,
#         version=feature_view_metadata.version,
#     )

#     return feature_view