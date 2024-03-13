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
from mmm.data_sources.api import Datastream


def inference_process(sensor: dict, process: dict, mc: MetadataCollector, obs_props_ids: dict, sensor_id: int,
                      thing_id: int, url: str, update=True):
    """
    Registers the Datastreams for Object Detection inference. The output is expected to be an integer number of
    detections.
    """

    __required_fields = ["variable_names", "name"]
    for k in __required_fields:
        if k not in process.keys():
            rich.print(f"[red]ERROR, expected key {k} in inference configuration")

    station = sensor["deployment"]["@stations"]
    sensor_name = sensor["#id"]

    rich.print(f"Registering inference Datastreams for {sensor_name}")

    variables = mc.get_documents("variables")
    classes = {}  # key taxa name (standard_name),
    rich.print("checking that all object detection classes have been already registered...")
    for detection_class in process["variable_names"]:
        if detection_class in process["ignore"]:
            continue
        found = False
        for var in variables:

            if var["standard_name"] == detection_class:
                classes[detection_class] = var
                found = True
        if not found:
            rich.print(f"[red]ERROR, variable {detection_class} not found ")

    # Now, let's register the datastreams

    # First a datastream where all the detections with probabilities will be generated
    obs_prop_id = obs_props_ids["FATX"]
    units_doc = mc.get_document("units", "dimensionless")

    name = f"{station}:{sensor_name}:fish_abundance:{process['name']}"
    description = f"Fish abundance detected from the pictures from camera {sensor_name} at {station}"
    properties = {
        "fullData": True,
        "dataType": "inference",
        "modelName": process["name"],
        "weights": process["weights"],
        "trainingConfig": process["trainingConfig"],
        "trainingData": process["trainingData"]
    }

    ds_units = load_fields_from_dict(units_doc, ["name", "symbol", "definition"])
    ds = Datastream(name, description, ds_units, thing_id, obs_prop_id, sensor_id, properties=properties,
                    observation_type="OM_Observation")  # generic observation type, will be used to store json data
    ds.register(url, update=update, verbose=True)

    # Now register species one by one
    units_doc = mc.get_document("units", "counts")
    for taxa_name, variable in classes.items():
        if taxa_name in process["ignore"]:
            rich.print(f"Ignoring {taxa_name}...")

        taxa_normalized = taxa_name.lower().replace(" ", "_").replace(".", "")
        name = f"{station}:{sensor_name}:{taxa_normalized}:detections"
        description = f"Detections of {taxa_name} in pictures from camera {sensor_name} at {station}"
        properties = {
            "fullData": True,
            "dataType": "detections",
            "modelName": process["name"],
            "algorithm": process["algorithm"],
            "weights": process["weights"],
            "trainingConfig": process["trainingConfig"],
            "trainingData": process["trainingData"]
        }
        obs_prop_id = obs_props_ids[variable["#id"]]
        ds_units = load_fields_from_dict(units_doc, ["name", "symbol", "definition"])
        ds = Datastream(name, description, ds_units, thing_id, obs_prop_id, sensor_id, properties=properties,
                        observation_type="OM_CountObservation")
        ds.register(url, update=update, verbose=True)
