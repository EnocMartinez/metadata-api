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
from mmm.sensorthings.db import SensorThingsDbConnector
import yaml

from mmm.core import load_fields_from_dict
import rich
if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets-local.yaml")
    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    mc = MetadataCollector(secrets["mongodb"]["connection"], secrets["mongodb"]["database"])

    psql_conf = load_fields_from_dict(secrets["sensorthings"], ["database", "user", "host", "port", "password"])
    db = SensorThingsDbConnector(psql_conf)
    obs_size = db.value_from_query('SELECT pg_total_relation_size(\'"OBSERVATIONS"\') AS table_size_bytes;')
    obs_count = db.value_from_query('SELECT count(*) from "OBSERVATIONS";')

    ts_count = db.value_from_query('SELECT count(*) from timeseries;')
    ts_size = db.value_from_query("SELECT hypertable_size('timeseries');")

    pr_count = db.value_from_query('SELECT count(*) from profiles;')
    pr_size = db.value_from_query("SELECT hypertable_size('profiles');")

    rich.print(f"OBSERVATIONS size {obs_size/1e6:.02f} MB for {obs_count} lines")
    rich.print(f"timeseries   size {ts_size /1e6:.02f} MB for {ts_count} lines")
    rich.print(f"profiles     size {pr_size /1e6:.02f} MB for {pr_count} lines")


