#!/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 09/09/24
"""
import os.path
from argparse import ArgumentParser
import pandas as pd
import json
from mmm.common import file_list, setup_log, timestamp_from_filename, run_subprocess, run_over_ssh
from mmm import init_metadata_collector, init_data_collector, bulk_load_data
import yaml
import rich


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("path", help="Dataset ID", type=str)
    argparser.add_argument("sensor_name", help="Service name (e.g. ERDDAP, CKAN, etc.)", type=str)
    argparser.add_argument("destination", help="Destination path in the fileserver", type=str)
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False,
                           default="secrets.yaml")
    argparser.add_argument("-f", "--foi", help="Feature of Interest to be used", type=str, default="")
    argparser.add_argument("-d", "--dry-run", help="Do not perform any changes", action="store_true")
    args = argparser.parse_args()
    files = file_list(args.path)

    # Looking for sensor
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
    log = setup_log("file_ingestor")
    mc = init_metadata_collector(secrets, log=log)
    dc = init_data_collector(secrets, log=log)
    log.info(f"Getting sensor_id for sensor '{args.sensor_name}'")
    sensor_id = dc.sta.value_from_query(f'select "ID" from "SENSORS" where "NAME" = \'{args.sensor_name}\';')
    log.info(f"Sensor ID for '{args.sensor_name}' = {sensor_id}")
    log.info(f"Getting files datastreams")
    q = f'''
        select "ID" from "DATASTREAMS" 
            where "SENSOR_ID" = {sensor_id} and
            "PROPERTIES"->>'dataType' = 'files';
    '''
    datastream_id = dc.sta.value_from_query(q)
    log.info(f"Datastream for files in sensor {args.sensor_name} is {datastream_id}")

    if args.foi:
        if str(args.foi).isdecimal():
            foi_id = args.foi
            foi_name = dc.sta.value_from_query(f'select "NAME" from "FEATURES" where "ID" = \'{foi_id}\';')
        else:
            foi_name = args.foi
            foi_id = dc.sta.value_from_query(f'select "ID" from "FEATURES" where "NAME" = \'{foi_name}\';')

        log.info(f"Using user-defined FOI: {foi_name} id={foi_id}")
    else:
        log.info(f"No FOI provided, trying to get default FeatureOfInterest")
        props = dc.sta.datastream_properties[datastream_id]
        if "defaultFeatureOfInterest" in props.keys():
            foi_id = props["defaultFeatureOfInterest"]
            foi_name = dc.sta.value_from_query(f'select "NAME" from "FEATURES" where "ID" = \'{foi_id}\';')
        else:
            raise ValueError(f"Default FeatureOfInterest not specified in datastream {datastream_id}")

    # Now we have all we need, sensor_id, datastream_id and foi_id, let's create the CSV file

    data = {
        "timestamp": [],
        "results": [],
        "datastream_id": [],
        "foi_id": [],
        "parameters": []
    }
    for file in files:
        data["timestamp"].append(timestamp_from_filename(file))
        new_filename = file.replace(args.path, args.destination)
        data["results"].append(dc.fileserver.path2url(new_filename))
        data["datastream_id"].append(datastream_id)
        data["foi_id"].append(foi_id)
        data["parameters"].append(json.dumps({"size": os.path.getsize(file)}))

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df.sort_index()
    rich.print(df)

    # Convert first and last timestamp to strings
    init = pd.to_datetime(df.index.values[0]).strftime("%Y%m%d-%H%M%S")
    end = pd.to_datetime(df.index.values[-1]).strftime("%Y%m%d-%H%M%S")
    csv_filename = args.sensor_name + "_" + init + "_to_" + end + ".csv"
    if args.dry_run:
        exit()

    log.info(f"Creating {csv_filename}...")
    df.to_csv(csv_filename)

    tar_filename = os.path.join("/tmp", csv_filename.replace(".csv", ".tar.gz"))

    # Now crate the tar file
    oldir = os.getcwd()
    os.chdir(args.path)
    log.info("Creating tar file (this may take a while)...")
    files_to_compress = " ".join(os.listdir())
    run_subprocess(f"tar -zcf {tar_filename} {files_to_compress}")
    dest_tarfile = os.path.join(args.destination, tar_filename)
    dc.fileserver.send_file(args.destination, tar_filename, indexed=False)
    log.info("Removing local tar file")
    os.remove(tar_filename)
    tar_basename = os.path.basename(tar_filename)
    log.info("Extracting remote tar file (this may take a while)...")
    run_over_ssh(dc.fileserver.host, f"cd {args.destination} && tar -zxf {tar_basename}")

    log.info("Removing remote tar file")
    run_over_ssh(dc.fileserver.host, f"cd {args.destination} && rm {tar_basename}")

    os.chdir(oldir)

    bulk_load_data(csv_filename, secrets["sensorthings"], "", args.sensor_name, "files")