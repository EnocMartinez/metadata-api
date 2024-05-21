#!/usr/bin/env python3
"""
Script that takes data from a SensorThings database and generates  NetCDF data

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""

from argparse import ArgumentParser
from mmm import DataCollector, setup_log, CkanClient
import yaml
import rich
import pandas as pd
from stadb import SensorThingsApiDB

from mmm.fileserver import FileServer
from mmm.metadata_collector import init_metadata_collector
import os


def calculate_time_intervals(time_start: str, time_end: str, periodicity=""):
    """
    Exports the dataset from the appropriate data source.

    :param dataset_id:
    :param periodicity:
    :return:
    """
    __valid_periods = ["day", "month", "year"]
    intervals = []  # list of periods to export as (tstart, tend)
    tstart = pd.Timestamp(time_start, tz="utc")
    tend = pd.Timestamp(time_end, tz="utc")
    if periodicity:
        assert(periodicity in __valid_periods)
        a = tstart
        b = tstart
        while b < tend:
            rich.print(f"incrementing '{periodicity}'...")
            rich.print(f"Before {b}")
            if periodicity == "day":
                b = b + pd.DateOffset(days=1)
            elif periodicity == "month":
                b = b + pd.DateOffset(months=1)
            elif periodicity == "year":
                b = b + pd.DateOffset(years=1)
            rich.print(f"After  {b}")
            intervals.append((a, b))
            a = b
    else:
        intervals.append((tstart, tend))

    rich.print("exporting the following intervals:")
    for s, e in intervals:
        rich.print(f"From '{s}' to '{e}'")

    return intervals


def generate_dataset(dataset_id: str, time_start: str, time_end: str, out_folder: str, secrets, periodicity="",
                     ckan=False, force=False, format:str= "") -> list:
    """
    Generate a dataset following the configuration in the MongoDB dataset register.
    :param dataset_id: id of the dataset register
    :param time_start:
    :param time_end:
    :param out_folder: output folder where the datasets will be generated. If null, use 'dataset_id' as folder name.
    :param secrets: secrets.yaml file
    :param periodicity: "day", "month" or "year" to split the data in yearly, monthly or daily files. If not set all data will be in the same file
    :param csv_file: If set, instead of using datasource, get the data from csv_files
    :return: list of filenames
    """

    if not out_folder:
        rich.print("using dataset_id as output folder")
        out_folder = os.path.join("datasets", dataset_id)

    with open(secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    log = setup_log("sta_to_emso")
    mc = init_metadata_collector(secrets)
    if ckan:
        ckan = CkanClient(mc, secrets["ckan"]["url"], secrets["ckan"]["api_key"])

    dc = DataCollector(secrets, log, mc=mc)

    intervals = calculate_time_intervals(time_start, time_end, periodicity=periodicity)
    datasets = []
    for tstart, tend in intervals:
        dataset = dc.generate(dataset_id, tstart, tend, out_folder, ckan=ckan, format=format, force=force)
        datasets.append(dataset)
    return datasets


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("dataset_id", help="Dataset ID", type=str)
    argparser.add_argument("-o", "--output", help="Folder where datasets will be stored", type=str, default="")
    argparser.add_argument("-c", "--ckan", help="Export to CKAN", action="store_true")
    argparser.add_argument("-F", "--force", help="Overwrite any existing dataset", action="store_true")
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False,
                           default="secrets.yaml")
    argparser.add_argument("-t", "--time-range", help="Time range with ISO notation, like 2022-01-01/2023-01-01",
                           type=str, required=False, default="2022-01-01/2023-01-01")
    argparser.add_argument("-p", "--period", help="period to generate files, 'day', 'month' or 'year'. If not set a "
                                                  "single big file will be generated", type=str,
                           required=False, default="")

    argparser.add_argument("-f", "--format",type=str,  required=False, default="",
                           help="Suggest format such as netcdf, csv, etc. May not work for all datasets")

    args = argparser.parse_args()
    tstart, tend = args.time_range.split("/")

    generate_dataset(args.dataset_id, tstart, tend,  args.output, args.secrets, periodicity=args.period, ckan=args.ckan,
                     format=args.format, force=args.force)

