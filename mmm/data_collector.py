#!/usr/bin/env python3
"""
This file implements the DataCollector, a class implementing generic data access

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 30/11/22
"""
import pandas as pd
from .metadata_collector import MetadataCollector
from .data_sources.sensorthings import SensorthingsDbConnector
import rich
import os
import json

class DataCollector:
    def __init__(self, mc: MetadataCollector, sta: SensorthingsDbConnector=None):
        """
        Constructor
        :param mc: MetadataCollecotr object
        """
        self.mc = mc

        # Define datasources
        self.sta = sta

    def generate(self, dataset_id: str, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder: str) -> str:
        """
        Generates a dataset based on its configuration stored in MongoDB
        :param dataset_id: #id of the dataset
        :param outfile: output filename
        :return: Dataset file
        """
        os.makedirs(out_folder, exist_ok=True)
        conf = self.mc.get_document("datasets", dataset_id)
        if conf["dataSource"] == "sensorthingsdb":
            dataset = self.netcdf_from_sta(conf, time_start, time_end, out_folder)
        else:
            raise ValueError(f"data_source='{conf['data_source']}' not implemented")
        return dataset


    def netcdf_from_sta(self, conf, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder):
        """
        Generates a NetCDF file from a SensorThings Database
        :param conf:
        :return:
        """
        rich.print("[cyan]Generating NetCDF from a SensorThings API file")
        rich.print("processing data_source_options...")

        filename = conf["#id"]  + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".nc"
        filename = os.path.join(out_folder, filename)

        station = self.mc.get_document("stations", conf["@stations"])
        variables = []  # by default all variables will be used
        if "@variables" in conf.keys():
            variables = conf["@variables"]

        options = conf["dataSourceOptions"]
        dataframes = []
        metadata = []

        for sensor_id in conf["@sensors"]:
            sensor = self.mc.get_document("sensors", sensor_id)
            rich.print(f"Getting data for sensor '{sensor_id}' from '{time_start}' to '{time_end}'...")
            df = self.sta.get_dataset(sensor["#id"], station["#id"], options, time_start, time_end, variables=variables)
            m = self.metadata_harmonizer_conf(conf, sensor, station, variables, tstart=time_start, tend=time_end)
            dataframes.append(df)
            metadata.append(m)
        call_generator(dataframes, metadata, output=filename)
        return filename

    def metadata_harmonizer_conf(self, dataset, sensor: dict, station: dict, variable_ids: list, os_data_mode="R",
                            os_data_type="OceanSITES time-series data", tstart="", tend="") -> dict:
        """
        This method returns the configuration required by the Metadata Harmonizer tool from the MongoDB
        :param dataset: sensor dict from MongoDB database
        :param sensor: sensor dict from MongoDB database
        :param station: station dict from MongoDB database
        :param variable_ids: list of variables to be included in the dataset
        :param os_data_mode: OceansSITES data mode, can be 'R' (real-time), 'P' (provisional, 'D' (delayed) or 'M' (mixed)
        :param os_data_type: OceanSITES data type, probably by default time-series data
        :return:
        """

        if not variable_ids:  # By default, use ALL variables
            variable_ids = [dic["@variables"] for dic in sensor["variables"]]

        variables = [self.mc.get_document("variables", v) for v in variable_ids]

        pi, _ = self.mc.get_contact_by_role(dataset, "ProjectLeader")
        owner, _ = self.mc.get_contact_by_role(station, "owner")

        projects = self.mc.get_funding_projects(sensor["#id"])

        # global attributes
        gl = {
            "*title": dataset["title"],
            "*summary": dataset["summary"],
            "*institution_edmo_code": owner["EDMO"].split("/")[-1],  # just the code, not the full URL
            #"$site_code": "OBSEA (seafloor)",
            "$emso_facility": "",
            "*source": station["platformType"]["label"],
            "$data_type": os_data_type,
            "$data_mode": os_data_mode,
            "*principal_investigator": pi["name"],
            "*principal_investigator_email": pi["email"],
        }

        if "emsoFacility" in station.keys():
            gl["$emso_facility"] = station["emsoFacility"]

        # Create dictionary where var_id is the key and the value is the units doc
        units = {}
        for var_id in variable_ids:
            for var in sensor["variables"]:
                if var["@variables"] == var_id:
                    units[var_id] = self.mc.get_document("units", var["@units"])

        var_metadata = {}
        for variable in variables:
            var_id = variable["#id"]
            var_metadata[var_id] = {
                "*long_name": variable["description"],
                "*sdn_parameter_uri": variable["definition"],
                "~sdn_uom_uri": units[var_id]["definition"],
                "~standard_name": variable["standard_name"]
            }

        sensor_metadata = {
            "*sensor_model_uri": sensor["model"]["definition"],
            "*sensor_serial_number": sensor["serialNumber"],
            "$sensor_mount": "mounted_on_fixed_structure",
            "$sensor_orientation": "upward"
        }
        coordinates = {
            "depth": station["deployment"]["coordinates"]["depth"],
            "latitude": station["deployment"]["coordinates"]["latitude"],
            "longitude": station["deployment"]["coordinates"]["longitude"]
        }

        # Now build to document
        return {
            "global": gl,
            "variables": var_metadata,
            "sensor": sensor_metadata,
            "coordinates": coordinates
        }

def call_generator(dataframes, metadata, output="output.nc", generator_path="../metadata-harmonizer/generator.py"):
    """
    Dump dataframes and metadata to temporal files and call generator.py from the metadata-harmonizer project
    :param dataframes:
    :param metadata:
    :param output:
    :param generator_path:
    :return:
    """
    assert(len(dataframes) == len(metadata))
    csv_files = []
    meta_files = []
    for i in range(len(dataframes)):
        csv_name = f".data_{i}.csv"
        meta_name = f".meta_{i}.min.json"
        csv_files.append(csv_name)
        meta_files.append(meta_name)

        dataframes[i].to_csv(csv_name)
        with open(meta_name, "w") as f:
            json.dump(metadata[i], f, indent=2)

    data = " ".join(csv_files)
    meta = " ".join(meta_files)

    command = f"python3 {generator_path} --data {data} --metadata {meta} -o {output}"
    rich.print(f"[purple]Running command: {command}")
    ret = os.system(command)
    if ret != 0:
        raise ValueError("Could not generate dataset")

    rich.print("Removing temporal files...", end="")
    [os.remove(f) for f in csv_files]
    [os.remove(f) for f in meta_files]
    rich.print("[green]ok!")

