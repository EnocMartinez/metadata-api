#!/usr/bin/env python3
"""
This script loads bulk data into SensorThings Databsase

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez#upc.edu
license: MIT
created: 21/09/2023
"""
from argparse import ArgumentParser
from mmm import MetadataCollector, CkanClient, propagate_mongodb_to_sensorthings
from mmm.metadata_collector import init_metadata_collector
import yaml
import rich

from mmm.core import load_fields_from_dict, bulk_load_data

if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets.yaml")
    argparser.add_argument("file", help="Data file", type=str)
    argparser.add_argument("sensor_id", help="Sensor ID", type=str)
    argparser.add_argument("-a", "--average", help="Averaged data (period must be specified)", type=str, default="")
    argparser.add_argument("-d", "--detections", help="Detections data", action="store_true")
    argparser.add_argument("-t", "--timeseries", help="Timeseries data", action="store_true")
    argparser.add_argument("-p", "--profiles", help="Profile data", action="store_true")
    argparser.add_argument("-i", "--inference", help="Inference data", action="store_true")
    argparser.add_argument("-f", "--files", help="Files data (register the paths)", action="store_true")
    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    mc = init_metadata_collector(secrets)

    psql_conf = load_fields_from_dict(secrets["sensorthings"], ["database", "user", "host", "port", "password"])
    url = secrets["sensorthings"]["url"]

    if int(args.timeseries) + int(args.profiles) + int(args.detections) + int(args.inference) + int(args.files) != 1:
        raise ValueError("ONE data type must be selected")

    if args.profiles:
        data_type = "profiles"
    elif args.detections:
        data_type = "detections"
    elif args.timeseries:
        data_type = "timeseries"
    elif args.inference:
        data_type = "inference"
    elif args.files:
        data_type = "files"
    else:
        raise ValueError(f"Unimplemented type!")

    rich.print(f"[cyan]Bulk load data from sensor {args.sensor_id} file {args.file}")
    bulk_load_data(args.file, psql_conf, mc, url, args.sensor_id, data_type, average=args.average)



