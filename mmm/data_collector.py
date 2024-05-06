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

        # check the dataset constraints
        if "constraints" in conf.keys() and "timeRange" in conf["constraints"].keys():
            ctime_start = pd.Timestamp(conf["constraints"]["timeRange"].split("/")[0])
            ctime_end = pd.Timestamp(conf["constraints"]["timeRange"].split("/")[1])

            if ctime_start > time_start:
                time_start = ctime_start
                rich.print(f"[yellow]WARNING: Dataset constraint Forces start time to {ctime_start}")
            if ctime_end < time_end:
                time_end = ctime_end
                rich.print(f"[yellow]WARNING: Dataset constraint Forces end time to {ctime_end}")

        requires_export = True

        if csv_file:
            return self.netcdf_from_csv(conf, out_folder, csv_file)

        if conf["dataSource"] == "sensorthings":
            dataset = self.netcdf_from_sta(conf, time_start, time_end, out_folder)

        elif conf["dataSource"] == "filesystem":
            dataset = self.zip_from_filesystem(conf, time_start, time_end, out_folder)
            # datasets created from filesystem are already created on the remote server, no export needed
            requires_export = False
        else:
            raise ValueError(f"data_source='{conf['dataSource']}' not implemented")

        if csv_file:
            pass

        elif requires_export and "export" in conf.keys():
            host = conf["export"]["host"]
            path = conf["export"]["path"]
            path = os.path.join(path, dataset_id)
            rich.print(f"Delivering dataset {dataset} to {host}:{path}")
            run_subprocess(["ssh", host, f"mkdir -p {path}"])
            run_subprocess(["rsync", dataset, f"{host}:{path}"])
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
                    # Query the regular OBSERVATIONS table
                    q = (f'''
                        select
                            "PHENOMENON_TIME_START" as timestamp,
                            "RESULT_NUMBER" as "{varname}",
                            "RESULT_QUALITY"->>'qc_flag' as "{varname + "_QC"}",
                            "RESULT_QUALITY"->>'stdev' as "{varname + "_STD"}"
                        from
                            "OBSERVATIONS"
                        where
                            "DATASTREAM_ID" = {datastream_id}
                            and "PHENOMENON_TIME_START" between \'{time_start}\' and \'{time_end}\';
                    ''')
                df = self.sta.dataframe_from_query(q, debug=False)
                sensor_dataframes.append(df)
            df = merge_dataframes_by_columns(sensor_dataframes)
            df = df.set_index("timestamp")
            rich.print(f"[cyan]{sensor_name} data from {time_start} to {time_end}")
            dataframes.append(df)
            m = self.metadata_harmonizer_conf(conf, sensor, station, variables, tstart=time_start, tend=time_end)
            metadata.append(m)

        call_generator(dataframes, metadata, output=filename)
        return filename

    def zip_from_filesystem(self, conf, time_start, time_end, out_folder) -> str:
        """
        Compresses all files in the fileserver into a zip file.

        Required options:
            dataSourceOptions:
               url: URL used to register the data
               path: filesystem raw path (base path for the URL)
               host: remote server
        """
        # First step, get all files indexed in the SensorThings database
        datastream_ids = []
        baseurl = conf["dataSourceOptions"]["url"]
        basepath = conf["dataSourceOptions"]["path"]

        for sensor in conf["@sensors"]:
            sensor_id = self.sta.sensor_id_name[sensor]
            # Get the Datastream ID of the files
            df = self.sta.dataframe_from_query(f'''
            select
                "ID" from "DATASTREAMS" 
            where 
                "PROPERTIES"->>'dataType' = 'files'
                and "SENSOR_ID" = {sensor_id}; 
            ''', debug=False)
            files_ids = df["ID"].values  # convert dataframe to list

            for i in files_ids:
                datastream_ids.append(str(i))

        # Now let's query for all registered files in the database matching the datastreams
        df = self.sta.dataframe_from_query(f'''
        select "RESULT_STRING" as files from "OBSERVATIONS"             
        where            
            "DATASTREAM_ID" IN ({", ".join(datastream_ids)})
            and "PHENOMENON_TIME_START" between \'{time_start}\' and \'{time_end}\';
        ''', debug=False)

        files = list(df["files"].values)  # List of all files to be compressed

        # Now convert the file URL to a filesystem path

        files = [f.replace(baseurl, basepath) for f in files]        
        # source_path = conf["dataSourceOptions"]["path"]
        dataset_id = conf["#id"]
        dest_path = os.path.join(conf["export"]["path"], dataset_id)
        filename = dataset_id + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".zip"
        filename = os.path.join(dest_path, filename)


        rich.print(f"creating folders...", end="")
        run_subprocess(["ssh", conf["export"]["host"], f"mkdir -p {dest_path}"])
        rich.print("[green]done!")

        cmd = f"zip -r {filename} {' '.join(files)}"
        rich.print(f"creating zip file with {len(files)} (this may take a while)...", end="")
        run_subprocess(["ssh", conf["export"]["host"], cmd])
        rich.print("[green]done!")

        return filename

    def metadata_harmonizer_conf(self, dataset, sensor: dict, station: dict, variable_ids: list,
                                 default_data_mode="real-time", os_data_type="OceanSITES time-series data",
                                 tstart="", tend="") -> dict:
        """
        This method returns the configuration required by the Metadata Harmonizer tool from the MongoDB
        :param dataset: sensor dict from MongoDB database
        :param sensor: sensor dict from MongoDB database
        :param station: station dict from MongoDB database
        :param variable_ids: list of variables to be included in the dataset
        :param default_data_mode: Default data mode
        :param os_data_type: OceanSITES data type, probably by default time-series data
        :return:
        """

        if not variable_ids:  # By default, use ALL variables
            variable_ids = [dic["@variables"] for dic in sensor["variables"]]


        variables = [self.mc.get_document("variables", v) for v in variable_ids]

        # Get minimum info (PI and owner)
        pi, _ = self.mc.get_contact_by_role(dataset, "ProjectLeader")
        owner, _ = self.mc.get_contact_by_role(station, "owner")

        # Put all people involved in an array
        people = []
        roles = []
        for c in dataset["contacts"]:
            role = c["role"]
            if "@organizations" in c.keys():
                rich.print(f"[yellow]WARNING: skipping institution '{c['@organizations']}'")
                continue
            name = self.mc.get_document("people", c["@people"])["name"]
            people.append(name)
            roles.append(role)

        for c in dataset["contacts"]:
            role = c["role"]
            if "@organizations" in c.keys():
                rich.print(f"[yellow]WARNING: skipping institution '{c['@organizations']}'")
                continue
            name = self.mc.get_document("people", c["@people"])["name"]
            people.append(name)
            roles.append(role)

        # Put funding information

        project_names = []
        project_codes= []
        if "funding" in dataset.keys():
            for project_id in dataset["funding"]["@projects"]:
                project = self.mc.get_document("projects", project_id)
                project_names.append(project["acronym"])
                project_codes.append(project["funding"]["grantId"])

        # Get the OceanSITES Data Mode
        data_mode = default_data_mode
        if "dataMode" in dataset.keys():
            data_mode = dataset["dataMode"]
        data_mode_dict = {"real-time": "R", "delayed": "D", "mixed": "M", "provisional": "P"}
        dm = data_mode_dict[data_mode]

        # global attributes
        gl = {
            "*title": dataset["title"],
            "*summary": dataset["summary"],
            "*institution_edmo_code": owner["EDMO"].split("/")[-1],  # just the code, not the full URL
            "$emso_facility": "None",
            "~network": "None",
            "*source": station["platformType"]["label"],
            "$data_type": os_data_type,
            "$data_mode": dm,
            "*principal_investigator": pi["name"],
            "*principal_investigator_email": pi["email"],
            "funding_project_names": project_names,
            "funding_project_codes": project_codes
        }

        if "emsoFacility" in station.keys():
            gl["$emso_facility"] = station["emsoFacility"]
            if station["emsoFacility"] != "None":
                gl["~network"] = "EMSO"

        # Create dictionary where var_id is the key and the value is the units doc
        units = {}
        for var_id in variable_ids:
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

