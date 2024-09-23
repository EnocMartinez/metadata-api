#!/usr/bin/env python3
"""
This script updates data from the metadata database and registers it to CKAN

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez#upc.edu
license: MIT
created: 21/09/2023
"""
from argparse import ArgumentParser
from mmm import MetadataCollector, CkanClient, propagate_metadata_to_sensorthings, setup_log, init_data_collector
import yaml
from mmm.metadata_collector import init_metadata_collector

if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets.yaml")
    argparser.add_argument("-c", "--collections", help="Only use certain collections (comma-separated list)", default="all")

    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
    collections = args.collections.split(",")
    log = setup_log("MCtoSTA")
    dc = init_data_collector(secrets, log)

    url = secrets["sensorthings"]["url"]
    auth = ()
    if "api_user" in secrets["sensorthings"].keys() and "api_password" in secrets["sensorthings"].keys():
        auth = (secrets["sensorthings"]["api_user"],secrets["sensorthings"]["api_password"])

    propagate_metadata_to_sensorthings(dc, collections, url, auth=auth)
