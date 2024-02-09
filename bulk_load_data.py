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
from mmm import SensorthingsDbConnector
import yaml

from mmm.core import load_fields_from_dict, bulk_load_data

if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets.yaml")
    argparser.add_argument("file", help="Data file", type=str)
    argparser.add_argument("sensor_id", help="Sensor ID", type=str)
    argparser.add_argument("-a", "--average", help="Averaged data (period must be specified)", type=str, default="")
    argparser.add_argument("-p", "--profile", help="Profile data", action="store_true")
    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    mc = init_metadata_collector(secrets)

    psql_conf = load_fields_from_dict(secrets["sensorthings"], ["database", "user", "host", "port", "password"])
    url = secrets["sensorthings"]["url"]

    if args.profile:
        data_type = "profiles"
    elif args.average:
        data_type = "average"
    else:
        data_type = "timeseries"

    bulk_load_data(args.file, psql_conf, mc, url, args.sensor_id, data_type, average=args.average)



