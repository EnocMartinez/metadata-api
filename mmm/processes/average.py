#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 19/2/24
"""
from mmm import MetadataCollector
import rich
from mmm.common import load_fields_from_dict
from mmm.metadata_collector import get_sensor_deployments
from mmm.data_sources.api import Datastream


def average_process(sensor: dict, process: dict, parameters: dict, mc: MetadataCollector, obs_props_ids: dict,
                    sensor_id: int, thing_id: int, url: str, fois: dict, update=True):
    """
    Register the Datastreams for an average process
    """

    sensor_name = sensor["#id"]
    sensor_deployments = get_sensor_deployments(mc, sensor_name)
    for station, deployment_time in sensor_deployments:
        station_doc = mc.get_document("stations", station)
        period = parameters["period"]
        for var in sensor["variables"]:
            varname = var["@variables"]
            if "ignore" in parameters.keys() and varname in parameters["ignore"]:
                rich.print(f"[yellow]Average ignores {varname}...")
                continue
            units = var["@units"]
            data_type = var["dataType"]
            obs_prop_id = obs_props_ids[varname]
            if var["dataType"] == "timeseries" or var["dataType"] == "profiles":  # creating raw_data timeseries
                ds_name = f"{station}:{sensor_name}:{varname}:{data_type}:{period}"
                ds_full_data_name = f"{station}:{sensor_name}:{varname}:{data_type}:full"
                units_doc = mc.get_document("units", units)
                ds_units = load_fields_from_dict(units_doc, ["name", "symbol", "definition"])
                properties = {
                    "fullData": False,
                    "dataType": data_type,
                    "averagePeriod": period,
                    "averageFrom": ds_full_data_name,
                    "defaultFeatureOfInterest": fois[station_doc["defaults"]["@programmes"]]
                }

                ds = Datastream(ds_name, ds_name, ds_units, thing_id, obs_prop_id, sensor_id, properties=properties)
                ds.register(url, update=update, verbose=True)
