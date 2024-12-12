#!/usr/bin/env python3
"""
Generates all periodic datasets for every dataset with at least one sensor deployed

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""

from argparse import ArgumentParser
import yaml
from mmm import setup_log
from mmm.metadata_collector import init_metadata_collector
from generate_dataset import generate_dataset
import rich

if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False,
                           default="secrets.yaml")
    argparser.add_argument("services", help="Another argument", type=str, nargs="+")
    argparser.add_argument("-t", "--data-types", help="Valid data types", type=str, nargs="+")

    args = argparser.parse_args()
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    log = setup_log("all_datasets")
    mc = init_metadata_collector(secrets, log)
    datasets = mc.get_documents("datasets")

    # Get a list of active sensors
    active_sensors = []
    for sensor in mc.get_documents("sensors"):
        sensor_id = sensor["#id"]
        station, deployment_time, active = mc.get_last_sensor_deployment(sensor_id)
        if active:
            active_sensors.append(sensor_id)

    rich.print(active_sensors)
    for dataset in datasets:
        dataset_id = dataset["#id"]
        log.info(f"Processing dataset {dataset_id}")

        active_dataset = False
        for sensor_id in dataset["@sensors"]:
            if sensor_id in active_sensors:
                rich.print(f"   found active sensor '{sensor_id}'")
                active_dataset = True

        if not active_dataset:
            rich.print(f"[yellow] Dataset {dataset_id} does not have any active sensor")
            continue

        if dataset["dataType"] not in args.data_types:
            rich.print(f"[yellow]Skipping dataset {dataset_id} with type '{dataset['dataType']}'")
            continue

        rich.print(f"[green]Processing dataset {dataset_id} with type '{dataset['dataType']}'")

        for service_name, service in dataset["export"].items():

            if service_name not in args.services:
                continue

            if "period" not in service.keys() or service["period"] == "none" or not service["period"]:
                continue

            rich.print(f"[magenta]{dataset_id} {service_name}")
            generate_dataset(dataset_id, service_name, "", "", args.secrets,  current=True, log=log)

