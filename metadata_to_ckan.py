#!/usr/bin/env python3
"""
This script updates data from metadata database and registers it to CKAN

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez#upc.edu
license: MIT
created: 21/09/2023
"""
from argparse import ArgumentParser
from mmm import CkanClient, propagate_metadata_to_ckan
import yaml

from mmm.common import run_over_ssh, run_subprocess
from mmm.metadata_collector import init_metadata_collector
import rich

if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets.yaml")
    argparser.add_argument("-c", "--collections", help="Only use certain collections (comma-separated list)", default="")

    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
    collections = args.collections.split(",")
    if not args.collections:
        collections = ["datasets", "organizations"]
    else:
        collections = args.collections.split(",")

    mc = init_metadata_collector(secrets)

    proj = secrets["ckan"]["project_logos"]
    org = secrets["ckan"]["organization_logos"]

    # ckan_host = secrets["ckan"]["host"]
    # rich.print("Getting ckan key in a quick-and-dirty way")
    # cmd = "docker exec ckan ckan user token add ckan_admin tk1 | tail -n 1 | sed 's/\t//g' >  ~/ckan.key"
    # run_over_ssh(ckan_host, cmd)
    # run_subprocess(f"scp {ckan_host}:~/ckan.key .")
    #
    # with open("ckan.key") as f:
    #     key = f.read()
    # rich.print(f"CKAN key {key}")

    ckan = CkanClient(mc, secrets["ckan"]["url"], secrets["ckan"]["api_key"])
    propagate_metadata_to_ckan(mc, ckan, collections)
