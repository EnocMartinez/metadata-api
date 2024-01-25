#!/usr/bin/env python3
"""
This script updates data from MongoDB and registers it to CKAN

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez#upc.edu
license: MIT
created: 21/09/2023
"""
from argparse import ArgumentParser
from mmm import MetadataCollector, CkanClient, propagate_mongodb_to_ckan
import yaml

from mmm.metadata_collector import init_metadata_collector

if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets-test.yaml")
    argparser.add_argument("-c", "--collections", help="Only use certain collections (comma-separated list)", default="")

    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
    collections = args.collections.split(",")
    mc = init_metadata_collector(secrets)

    proj = secrets["ckan"]["project_logos"]
    org = secrets["ckan"]["organization_logos"]

    ckan = CkanClient(mc, secrets["ckan"]["url"], secrets["ckan"]["api_key"])
    propagate_mongodb_to_ckan(mc, ckan, collections)
