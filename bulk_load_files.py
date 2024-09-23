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
from mmm import init_metadata_collector, init_data_collector, bulk_load_data, DataCollector
import yaml
import logging


def bulk_load_files(dc: DataCollector, files: list, path: str, sensor_name: str, destination: str, foi_id: int,
                    log: logging.Logger):
    data = {
        "timestamp": [],
        "results": [],
        "datastream_id": [],
        "foi_id": [],
        "parameters": []
    }
    for file in files:
        data["timestamp"].append(timestamp_from_filename(file))
        new_filename = file.replace(path, destination)
        data["results"].append(dc.fileserver.path2url(new_filename))
        data["datastream_id"].append(datastream_id)
        data["foi_id"].append(foi_id)
        data["parameters"].append(json.dumps({"size": os.path.getsize(file)}))

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df.sort_index()

    # Convert first and last timestamp to strings
    init = pd.to_datetime(df.index.values[0]).strftime("%Y%m%d-%H%M%S")
    end = pd.to_datetime(df.index.values[-1]).strftime("%Y%m%d-%H%M%S")
    csv_filename = sensor_name + "_" + init + "_to_" + end + ".csv"

    log.info(f"Creating {csv_filename}...")
    df.to_csv(csv_filename)

    tar_filename = os.path.join("/tmp", csv_filename.replace(   ".csv", ".tar.gz"))

    # Now crate the tar file
    oldir = os.getcwd()
    os.chdir(path)
    log.info(f"Creating tar file {tar_filename} (this may take a while)...")
    # Remove the relative path from the files
    if path[-1] != "/":
        path += "/"
    relative_path_files = [f.replace(path, "") for f in files]

    files_to_compress = " ".join(relative_path_files)
    run_subprocess(f"tar -zcf {tar_filename} {files_to_compress}")


    # Create the folder based on the first file in the array
    log.info("Creating folders...")
    dest_path = dc.fileserver.url2path(data["results"][0])
    run_over_ssh(dc.fileserver.host, f"mkdir -p {os.path.dirname(dest_path)}", fail_exit=True)

    log.info("Sending file over ssh..")
    dc.fileserver.send_file(destination, tar_filename, indexed=False)
    log.info("Removing local tar file")
    os.remove(tar_filename)

    log.info("Extracting remote tar file (this may take a while)...")
    tar_basename = os.path.basename(tar_filename)
    run_over_ssh(dc.fileserver.host, f"cd {destination} && tar -zxf {tar_basename}", fail_exit=True)

    log.info("Removing remote tar file")
    run_over_ssh(dc.fileserver.host, f"cd {destination} && rm {tar_basename}", fail_exit=True)
    os.chdir(oldir)
    bulk_load_data(csv_filename, secrets["sensorthings"], "", sensor_name, "files")



if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("path", help="Dataset ID", type=str)
    argparser.add_argument("sensor_name", help="Service name (e.g. ERDDAP, CKAN, etc.)", type=str)
    argparser.add_argument("destination", help="Destination path in the fileserver", type=str)
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False,
                           default="secrets.yaml")
    argparser.add_argument("-f", "--foi", help="Feature of Interest to be used", type=str, default="")
    argparser.add_argument("-d", "--dry-run", help="Do not perform any changes", action="store_true")
    argparser.add_argument("-n", "--split", help="Split files into N-length arrays", type=int, default=2000)
    args = argparser.parse_args()
    all_files = file_list(args.path)
    all_files = sorted(all_files)

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

    # Split files into smaller arrays
    i = 0
    sub_arrays = []
    while i < len(all_files):
        n = min(i+args.split, len(all_files))
        sub_arrays.append(all_files[i:n])
        i = n
    log.info(f"Splitting array {len(all_files)} into arrays of length {args.split}")

    iterations = 1
    for files in sub_arrays:
        log.info(f" ---> Iteration {iterations} of {len(sub_arrays)}, processing {len(files)} files")
        bulk_load_files(dc, list(files), args.path, args.sensor_name, args.destination, foi_id, log)
        iterations += 1



