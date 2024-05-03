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

from .common import run_subprocess
from .data_manipulation import open_csv, merge_dataframes_by_columns
from .metadata_collector import MetadataCollector
import rich
import os
import json
from stadb import SensorThingsApiDB


class DataCollector:
    def __init__(self, mc: MetadataCollector, sta: SensorThingsApiDB = None):
        """
        Constructor
        :param mc: MetadataCollecotr object
        """
        self.mc = mc

        # Define datasources
        self.sta = sta

    def generate(self, dataset_id: str, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder: str,
                 csv_file: str = "") -> str:
        """
        Generates a dataset based on its configuration stored in MongoDB
        :param dataset_id: #id of the dataset
        :param outfile: output filename
        :return: Dataset file
        """
        os.makedirs(out_folder, exist_ok=True)
        conf = self.mc.get_document("datasets", dataset_id)

        if "constraints" in conf.keys() and "timeRange" in conf["constraints"].keys():
            time_start = pd.Timestamp(conf["constraints"]["timeRange"].split("/")[0])
            time_end = pd.Timestamp(conf["constraints"]["timeRange"].split("/")[1])
            rich.print(f"[yellow]WARNING: Exporting constraints of the datset! from {time_start} to {time_end}")

        if csv_file:
            return self.netcdf_from_csv(conf, out_folder, csv_file)

        if conf["dataSource"] == "sensorthings":
            dataset = self.netcdf_from_sta(conf, time_start, time_end, out_folder)

        elif conf["dataSource"] == "fileserver":
            dataset = self.zip_from_fileserver(conf, time_start, time_end, out_folder)
        else:
            raise ValueError(f"data_source='{conf['dataSource']}' not implemented")

        if csv_file:
            pass
        elif "export" in conf.keys():
            host = conf["export"]["host"]
            path = conf["export"]["path"]
            path = os.path.join(path, dataset_id)
            rich.print(f"Delivering dataset {dataset} to {host}:{path}")
            run_subprocess(["ssh", host, f"mkdir -p {path}"])
            run_subprocess(["rsync", dataset, f"{host}:{path}"])
        else:
            rich.print(f"[yellow]WARNING! dataset {dataset_id} doesn't have export options!")
        return dataset

    def netcdf_from_csv(self, conf, out_folder, csv_file):
        df = open_csv(csv_file, format=True)
        rich.print(df)
        time_start = pd.Timestamp(df.index.values[0])
        time_end = pd.Timestamp(df.index.values[-1])
        rich.print("[purple]Generating NetCDF from a CSV file")

        filename = conf["#id"] + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".nc"
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

            m = self.metadata_harmonizer_conf(conf, sensor, station, variables, tstart=time_start, tend=time_end)
            dataframes.append(df)
            metadata.append(m)

        df = open_csv(csv_file)
        dataframes = [df]
        if len(conf["@sensors"]) != 1:
            # create dummy csv files with just the header for the metadata harmonizer
            dummydata = {"timestamp": [], "depth": []}
            for v in df.columns:
                dummydata[v] = []
            dummydf = pd.DataFrame(dummydata)
            dataframes.append(dummydf)
            rich.print(dummydf)

        call_generator(dataframes, metadata, output=filename)

    def netcdf_from_sta(self, conf, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder, csv_file=""):
        """
        Generates a NetCDF file from a SensorThings Database
        :param conf:
        :return:
        """
        rich.print("[cyan]Generating NetCDF from a SensorThings API file")
        rich.print("processing data_source_options...")

        filename = conf["#id"] + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".nc"
        filename = os.path.join(out_folder, filename)

        station = self.mc.get_document("stations", conf["@stations"])
        data_type = conf["dataType"]

        try:
            full_data = conf["dataSourceOptions"]["fullData"]
        except KeyError:
            rich.print("[red]dataSourceOptions/fullData not found in dataset configuration!")
            raise KeyError("dataSourceOptions/fullData not found in dataset configuration!")

        station_name = station["#id"]
        variables = []  # by default all variables will be used
        if "@variables" in conf.keys():
            variables = conf["@variables"]

        dataframes = []  # list with a dataframe per variable
        metadata = []    # list of a metadata dict per variable

        for sensor_name in conf["@sensors"]:
            rich.print(f"[orange1]Getting datastreams from station={station_name} and sensor={sensor_name} fullData={full_data}")
            rich.print(f"[orange1]dataType='{data_type}'")
            sensor = self.mc.get_document("sensors", sensor_name)

            # Get the THING_ID from SensorThings based on the Station name
            thing_id = self.sta.value_from_query(
                f'select "ID" from "THINGS" where "NAME" = \'{station_name}\';'
            )
            sensor_id = self.sta.value_from_query(
                f'select "ID" from "SENSORS" where "NAME" = \'{sensor_name}\';'
            )
            # Super query that returns all varname and datastream_id  for one station-sensor combination
            # Results are stored as a DataFrame
            query = f'''select 
                    "OBS_PROPERTIES"."NAME" as varname, 
                    "DATASTREAMS"."ID" as datastream_id                    
                from  
                    "DATASTREAMS"
                left join 
                    "OBS_PROPERTIES"
                on 
                    "DATASTREAMS"."OBS_PROPERTY_ID" = "OBS_PROPERTIES"."ID"
                where 
                    "DATASTREAMS"."SENSOR_ID" = {sensor_id} and "DATASTREAMS"."THING_ID" = {thing_id} 
                    and "DATASTREAMS"."PROPERTIES"->>'dataType' = '{data_type}'
                    and ("DATASTREAMS"."PROPERTIES"->>'fullData')::boolean = {full_data}                    
                '''
            if not full_data:
                # if we are dealing with an average, we need to make sure that the average period matches
                avg_period = conf["dataSourceOptions"]["averagePeriod"]
                query += f'\r\n\t\t and "DATASTREAMS"."PROPERTIES"->>\'averagePeriod\' = \'{avg_period}\''

            query += ";"
            datastreams = self.sta.dataframe_from_query(query)
            sensor_dataframes = []
            for idx, ds in datastreams.iterrows():
                # ds is a dict with 'varname', 'datastream_id' and 'data_type'
                datastream_id = ds["datastream_id"]
                varname = ds["varname"]
                if variables and varname not in variables:
                    rich.print(f"[yellow]Ignoring variable {varname}")
                    continue
                rich.print(f"[purple]Getting {sensor_name} {varname} from {time_start} to {time_end}")
                # Query all data from the datastream_id during the time range and assign proper variable name
                if full_data:
                    q = (
                        f'''
                        select timestamp, value as "{varname}", qc_flag as "{varname + "_QC"}" 
                        from timeseries 
                        where datastream_id = {datastream_id}
                        and timestamp between \'{time_start}\' and \'{time_end}\';                     
                        '''
                    )
                else:
                    q = (
                        f'''
                        select
                            "PHENOMENON_TIME_START" as timestamp,
                            "RESULT_NUMBER" as "{varname}",
                            "RESULT_QUALITY"->>'qc_flag' as "{varname + "_QC"}",
                            "RESULT_QUALITY"->>'stdev' as "{varname + "_STD"}"
                        from
                            "OBSERVATIONS"
                        where
                            "DATASTREAM_ID" = 42
                            and "PHENOMENON_TIME_START" between \'{time_start}\' and \'{time_end}\';
                                      
                                                '''
                    )
                df = self.sta.dataframe_from_query(q, debug=True)
                sensor_dataframes.append(df)
            df = merge_dataframes_by_columns(sensor_dataframes)
            df = df.set_index("timestamp")
            rich.print(f"[cyan]{sensor_name} data from {time_start} to {time_end}")
            print(df)
            dataframes.append(df)
            m = self.metadata_harmonizer_conf(conf, sensor, station, variables, tstart=time_start, tend=time_end)
            metadata.append(m)

        call_generator(dataframes, metadata, output=filename)
        return filename

    def zip_from_fileserver(self, conf, time_start, time_end, out_folder) -> str:
        rich.print("[yellow]WARNING! zipping all data in server! slicing not yet implemented!")
        source_path = conf["dataSourceOptions"]["path"]
        dataset_id = conf["#id"]
        dest_path = os.path.join(conf["export"]["path"], dataset_id)
        filename = dataset_id + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".zip"
        filename = os.path.join(dest_path, filename)

        cmd = f"zip -r {filename} {source_path}"
        rich.print(cmd)
        run_subprocess(["ssh", conf["export"]["host"], f"mkdir -p {dest_path}"])
        rich.print(f'ssh {conf["export"]["host"]} {cmd}')
        run_subprocess(["ssh", conf["export"]["host"], cmd])

        rich.print("[green]done!")

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
            rich.print(f"[yellow]looking for {var_id}...")
            found = False
            for var in sensor["variables"]:
                if var["@variables"] == var_id:
                    units[var_id] = self.mc.get_document("units", var["@units"])
                    found = True
            if not found:
                raise LookupError(f"variable {var_id} not found in sensor {sensor['#id']}!")

        var_metadata = {}
        for variable in variables:
            var_id = variable["#id"]
            var_metadata[var_id] = {
                "*long_name": variable["description"],
                "*sdn_parameter_uri": variable["definition"],
                "~sdn_uom_uri": units[var_id]["definition"],
                "~standard_name": variable["standard_name"],
                "~standard_name_uri": variable["standard_name"]
            }

        sensor_metadata = {
            "*sensor_model_uri": sensor["model"]["definition"],
            "*sensor_serial_number": sensor["serialNumber"],
            "$sensor_mount": "mounted_on_fixed_structure",
            "$sensor_orientation": "upward"
        }

        latitude, longitude, depth = self.mc.get_station_position(station["#id"], tstart)
        coordinates = {
            "depth": depth,
            "latitude": latitude,
            "longitude": longitude
        }

        # Now build to document
        d = {
            "global": gl,
            "variables": var_metadata,
            "sensor": sensor_metadata,
            "coordinates": coordinates
        }
        return d


def call_generator(dataframes, metadata, output="output.nc", generator_path="../metadata-harmonizer/generator.py"):
    """
    Dump dataframes and metadata to temporal files and call generator.py from the metadata-harmonizer project
    :param dataframes:
    :param metadata:
    :param output:
    :param generator_path:
    :return:
    """
    assert (len(dataframes) == len(metadata))

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


def get_dataset(sta: SensorThingsApiDB, sensor: str, station_name: str, options: dict, time_start: pd.Timestamp,
                time_end: pd.Timestamp,
                variables: list = []) -> pd.DataFrame:
    """
    Gets all data from a sensor deployed at a certain station from time_start to time_end. If variables is specified
    only a subset of variables will be returned.

    options is a dict with "rawData" key (true/false) and if False, averagePeriod MUST be specified like:
        {"rawData": false, "averagePeriod": "30min"}
            OR
        {"rawData": true}

    :param sensor: sensor name
    :param station: station name
    :param time_start:
    :param time_end:
    :param options: dict with rawData and optionally averagePeriod.
    :param variables: list of variable names
    :return:
    """
    # Make sure options are correct
    assert "fullData" in options.keys()
    if not options["rawData"]:
        assert ("averagePeriod" in options.keys())

    raw_data = options["rawData"]

    if variables:
        rich.print(f"Keeping only variables: {variables}")

    sensor_id = sta.sensor_id_name[sensor]
    station_id = sta.thing_id_name[station_name]
    # Get all datastreams for a sensor
    datastreams = sta.get_sensor_datastreams(sensor_id)
    # Keep only datastreams at a specific station
    datastreams = datastreams[datastreams["thing_id"] == station_id]

    # Drop datastreams with variables that are not in the list
    if variables:
        variable_ids = [sta.obs_prop_name_id[v] for v in variables]
        rich.print(f"variables {variable_ids}")
        # keep only variables in list
        all_variable_ids = np.unique(datastreams["obs_prop_id"].values)
        remove_this = [var_id for var_id in all_variable_ids if var_id not in variable_ids]
        for remove_id in remove_this:
            datastreams = datastreams[datastreams["obs_prop_id"] != remove_id]  # keep others

    # Keep only datastreams that match data type
    remove_idxs = []
    for idx, row in datastreams.iterrows():
        if raw_data:
            if not row["properties"]["rawSensorData"]:
                remove_idxs.append(idx)
        else:  # Keep only those variables that are not raw_data and whose period matches the data_type
            if row["properties"]["rawSensorData"]:  # Do not keep raw data
                remove_idxs.append(idx)
            elif row["properties"]["averagePeriod"] != options[
                "averagePeriod"]:  # do not keep data with different period
                remove_idxs.append(idx)

    datastreams = datastreams.drop(remove_idxs)

    # Query data for every datastream
    data = []
    for idx, row in datastreams.iterrows():
        datastream_id = row["id"]
        obs_prop_id = row["obs_prop_id"]
        df = sta.dataframe_from_datastream(datastream_id, time_start, time_end)

        varname = sta.obs_prop_id_name[obs_prop_id]
        rename = {"value": varname, "qc_flag": varname + "_QC"}
        if "stdev" in df.columns:
            rename["stdev"] = varname + "_SD"
        df = df.rename(columns=rename)
        data.append(df)
    return merge_dataframes_by_columns(data)
