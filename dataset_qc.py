#!/usr/bin/env python3
"""
Script to apply QualityControl procedures to a dataset

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
"""

from mmm import init_metadata_collector
from argparse import ArgumentParser
import yaml
import json

from mmm.data_manipulation import open_csv
from mmm.quality_control import dataset_qc

if __name__ == "__main__":
    # Adding command line options #
    argparser = ArgumentParser()
    argparser.add_argument("sensor", type=str, help="Sensor name", default="")
    argparser.add_argument("-o", "--output", dest="output", type=str, required=False, help="Output file to store the curated dataset")
    argparser.add_argument("-i", "--input", dest="input", required=True, help="CSV dataset to be processed", default="")
    argparser.add_argument("--time-range", dest="time_range", required=False, default="", help="Inject only this time range (t1/t2) to SensorThings, e.g.2020-01-01/2020-01-02")
    argparser.add_argument("-l", "--limits", help="set the limits as a dict: -l {\"temperature\": [12,40]}", default="{}")
    argparser.add_argument("-t", "--tests", help="comma-separated list of tests", default="aggreggate")
    argparser.add_argument("-p", "--parallel", help="Applies multiprocessing to QC to speed it up", action="store_true")
    argparser.add_argument("-V", "--variables", dest="variables", default="", help="list of variables (separated by comma) to be plotted/saved during (default ALL)")
    argparser.add_argument("-A", "--qc-all", dest="qc_show_all", default=False, action="store_true", help="Prints all QC tests")
    argparser.add_argument("-S", "--save-qc", dest="save_qc", default="", type=str, help="Store the result of the QC tests as images")
    argparser.add_argument("-X", "--show-qc", dest="show_qc", default=False, action="store_true", help="Plots all QC tests")
    argparser.add_argument("-s", "--secrets", type=str, help="JSON secrets file with secrets connection info", default="secrets.yaml")

    args = argparser.parse_args()

    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    print(f"Loading {args.secrets} file...")
    mc = init_metadata_collector(secrets)

    limits = json.loads(args.limits)

    df = open_csv(args.input, time_range=args.time_range)
    print(df)

    variables = []
    if args.qc_show_all:
        variables = list(df.columns)

    elif args.variables:
        variables = args.variables.split(",")

    tests = args.tests.split(",")

    df = dataset_qc(mc, args.sensor, df, show=args.show_qc, save=args.save_qc, varlist=variables, limits=limits,
                    tests=args.tests, paralell=args.parallel)
    print(df)
    if args.output:
        df.to_csv(args.output)