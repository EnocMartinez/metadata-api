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
from mmm import MetadataCollector, CkanClient, propagate_mongodb_to_sensorthings
import yaml


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets-local.yaml")
    argparser.add_argument("-c", "--collections", help="Only use certain collections (comma-separated list)", default="all")

    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
    collections = args.collections.split(",")
    mc = MetadataCollector(secrets["mongodb"]["connection"], secrets["mongodb"]["database"])
    url = secrets["sensorthings"]["url"]
    propagate_mongodb_to_sensorthings(mc, collections, url)
