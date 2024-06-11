#!/usr/bin/env python3
"""
This file implements the DataCollector, a class implementing generic data access

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 30/11/22
"""
import logging

import pandas as pd
from .data_sources import SensorThingsApiDB
from .ckan import CkanClient
from .common import run_subprocess, check_url, detect_common_path, run_over_ssh
from .data_manipulation import open_csv, merge_dataframes_by_columns, merge_dataframes
from .metadata_collector import MetadataCollector, init_metadata_collector
from .fileserver import FileServer
import rich
import os
import json
import emso_metadata_harmonizer as mh


def init_data_collector(secrets: dict, log: logging.Logger, mc: MetadataCollector = None, sta: SensorThingsApiDB = None):
    return DataCollector(secrets, log, mc=mc, sta=sta)


class DataCollector:
    def __init__(self, secrets: dict, log, mc: MetadataCollector = None, sta: SensorThingsApiDB = None):

        if not mc:
            self.mc = init_metadata_collector(secrets)
        else:
            self.mc = mc

        if not sta:
            staconf = secrets["sensorthings"]
            self.sta = SensorThingsApiDB(staconf["host"], staconf["port"], staconf["database"], staconf["user"],
                                         staconf["password"], log, timescaledb=True)
        else:
            self.sta = sta
        self.fileserver = FileServer(secrets["fileserver"], log)

    def generate(self, dataset_id: str, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder: str,
                 ckan: CkanClient, force=False, format="") -> str:
        """
        Generates a dataset based on its configuration stored in MongoDB
        :param dataset_id: #id of the dataset
        :param outfile: output filename
        :return: Dataset file
        """
        os.makedirs(out_folder, exist_ok=True)
        conf = self.mc.get_document("datasets", dataset_id)

        if type(time_start) is str:
            time_start = pd.Timestamp(time_start)
        if type(time_end) is str:
            time_end = pd.Timestamp(time_end)

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

        if conf["dataSource"] == "sensorthings":
            if format.lower() == "csv":
                dataset_file, dataset_url, file_type = self.csv_from_sta(conf, time_start, time_end, out_folder, force)
            elif format.lower() in ["nc", "netcdf"]:
                dataset_file, dataset_url, file_type = self.netcdf_from_sta(conf, time_start, time_end, out_folder, force)
            else:
                raise ValueError(f"Unknwon dataSource format '{format}'")

        elif conf["dataSource"] == "filesystem":
            dataset_file, dataset_url, file_type = self.zip_from_filesystem(conf, time_start, time_end, out_folder, force)

        else:
            raise ValueError(f"data_source='{conf['dataSource']}' not implemented")

        if not check_url(dataset_url):
            raise ValueError(f"Dataset URL not reachable: {dataset_url}")

        # Now, let's register the file as a "resource" in a CKAN "package"
        if ckan:
            rich.print(f"[cyan]Publishing dataset as CKAN resource")
            if not dataset_url:
                raise ValueError("Datset URL not set, has the dataset been exported?")
            packages = ckan.get_package_list()
            # Package ID should be the dataset ID in lowercase
            package_id = conf["#id"].lower()

            if package_id not in packages:
                raise ValueError(f"Datset {package_id} not published in CKAN!")

            # Make filename without extension the resource ID
            resource_id = os.path.basename(dataset_file).replace(".", "_")

            start_date = time_start.strftime("%Y-%m-%d")
            end_date = time_end.strftime("%Y-%m-%d")

            description = f"{file_type} data from {start_date} to {end_date}"
            name = f"{file_type} data from {start_date} to {end_date}"

            r = ckan.resource_create(
                package_id,
                resource_id,
                description,
                name,
                resource_url=dataset_url,
                format=file_type,
            )
            rich.print(f"[cyan]Registered!")
            rich.print(r)

        return dataset_file, dataset_url

    def dataframe_from_sta(self, conf: dict, station: dict, sensor: dict, time_start: pd.Timestamp,
                           time_end: pd.Timestamp):
        if conf["dataType"] == "timeseries":
            return self.dataframe_from_sta_timeseries(conf, station, sensor, time_start, time_end)
        elif conf["dataType"] == "detections":
            return self.dataframe_from_sta_detections(conf, station, sensor, time_start, time_end)
        else:
            raise ValueError(f"Unimplemented data type {conf['dataType']}")

    def dataframe_from_sta_detections(self, conf: dict, station: dict, sensor: dict, time_start: pd.Timestamp,
                                      time_end: pd.Timestamp):
        """
        Returns a DataFrame for a specific Sensor in a specific time interval
        """

        data_type = conf["dataType"]
        sensor_name = sensor["#id"]
        station_name = station["#id"]

        variables = []  # by default all variables will be used
        if "@variables" in conf.keys():
            variables = conf["@variables"]

        rich.print(f"[orange1]Getting datastreams from station={station_name} sensor={sensor_name} fullData={full_data}")
        rich.print(f"[orange1]dataType='{data_type}'")

        # Get the THING_ID from SensorThings based on the Station name
        thing_id = self.sta.value_from_query(
            f'select "ID" from "THINGS" where "NAME" = \'{station_name}\';'
        )
        sensor_id = self.sta.value_from_query(
            f'select "ID" from "SENSORS" where "NAME" = \'{sensor_name}\';'
        )

        # select * from "DATASTREAMS"
        # 	where "SENSOR_ID" = (select "ID" from "SENSORS" where "NAME" = 'IPC608_8B64_165')
        # 	and "PROPERTIES"->>'modelName' = 'YOLOv8l_18sp_2361img'
        # 	and "PROPERTIES"->>'dataType' = 'detections'
        # 	and "PROPERTIES"->>'standardName' = 'Coris julis'
        # ;

        # Super query that returns all varname and datastream_id  for one station-sensor combination
        # Results are stored as a DataFrame
        query = f'''select 
                "OBS_PROPERTIES"."NAME" as varname, 
                "DATASTREAMS"."ID" as datastream_id                    
            from  
                "DATASTREAMS"
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
        df = df.sort_index(ascending=True)
        return df


    def dataframe_from_sta_timeseries(self, conf: dict, station: dict, sensor: dict, time_start: pd.Timestamp,
                                      time_end: pd.Timestamp):
        """
        Returns a DataFrame for a specific Sensor in a specific time interval
        """

        data_type = conf["dataType"]
        sensor_name = sensor["#id"]
        station_name = station["#id"]

        variables = []  # by default all variables will be used
        if "@variables" in conf.keys():
            variables = conf["@variables"]

        try:
            full_data = conf["dataSourceOptions"]["fullData"]
        except KeyError:
            rich.print("[red]dataSourceOptions/fullData not found in dataset configuration!")
            raise KeyError("dataSourceOptions/fullData not found in dataset configuration!")

        rich.print(
            f"[orange1]Getting datastreams from station={station_name} and sensor={sensor_name} fullData={full_data}")
        rich.print(f"[orange1]dataType='{data_type}'")

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
        df = df.sort_index(ascending=True)
        return df

    def netcdf_from_sta(self, conf, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder, force):
        """
        Generates a NetCDF file from a SensorThings Database
        :param conf:
        :return:
        """
        rich.print("[cyan]Generating NetCDF from a SensorThings API file")
        rich.print("processing data_source_options...")
        dataset_id = conf["#id"]
        filename = dataset_id + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".nc"
        local_file = os.path.join(out_folder, filename)
        remote_path = os.path.join(conf["export"]["path"], dataset_id)

        # Run send_file with dry_run to get the path and not sending the file
        dataset_url = self.fileserver.send_file(remote_path, local_file, dry_run=True)
        if not force and check_url(dataset_url):
            rich.print(f"[yellow]Dataset already exists! URL: {dataset_url}")
            return filename, dataset_url, "NetCDF"

        station = self.mc.get_document("stations", conf["@stations"])

        variables = []  # by default all variables will be used
        if "@variables" in conf.keys():
            variables = conf["@variables"]

        dataframes = []  # list with a dataframe per variable
        metadata = []  # list of a metadata dict per variable

        for sensor_name in conf["@sensors"]:
            sensor = self.mc.get_document("sensors", sensor_name)
            df = self.dataframe_from_sta(conf, station, sensor, time_start, time_end)
            rich.print(f"[cyan]{sensor_name} data from {time_start} to {time_end}")
            dataframes.append(df)
            m = self.metadata_harmonizer_conf(conf, sensor, station, variables, tstart=time_start, tend=time_end)
            metadata.append(m)

        rich.print(f"[orange3]Creating file {local_file}")

        call_dataset_generator(dataframes, metadata, output=local_file)

        rich.print("Delivering NetCDF dataset file ...")
        dataset_url = self.fileserver.send_file(remote_path, local_file)
        # We should return (file, url), but we don't have url yet
        return filename, dataset_url, "NetCDF"

    def csv_from_sta(self, conf, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder, force):
        """
        Generates a CSV file from a SensorThings Database
        :param conf:
        :return:
        """
        rich.print("[cyan]Generating CSV from a SensorThings API file")
        rich.print("processing data_source_options...")
        dataset_id = conf["#id"]
        filename = dataset_id + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".csv"
        local_file = os.path.join(out_folder, filename)
        remote_path = os.path.join(conf["export"]["path"], dataset_id)
        remote_file = os.path.join(remote_path, filename)
        dataset_url = self.fileserver.path2url(remote_file)

        if not force and check_url(dataset_url):
            rich.print(f"[yellow]Dataset already exists! URL: {dataset_url}")
            return filename, dataset_url, "CSV"

        station = self.mc.get_document("stations", conf["@stations"])
        dataframes = []  # list with a dataframe per variable

        for sensor_name in conf["@sensors"]:
            sensor = self.mc.get_document("sensors", sensor_name)
            df = self.dataframe_from_sta(conf, station, sensor, time_start, time_end)

            if len(conf["@sensors"]) > 1:
                df["SENSOR"] = sensor_name

            rich.print(f"[cyan]{sensor_name} data from {time_start} to {time_end}")
            dataframes.append(df)
        df = merge_dataframes(dataframes)
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        df.to_csv(local_file)

        rich.print("Delivering CSV dataset file ...")
        self.fileserver.send_file(remote_path, local_file)

        return filename, dataset_url, "CSV"

    def zip_from_filesystem(self, conf, time_start, time_end, out_folder, force) -> str:
        """
        Compresses all files in the fileserver into a zip file.

        Required options:
            dataSourceOptions:
               path: filesystem raw path (base path for the URL)
        """

        dataset_id = conf["#id"]
        dest_path = os.path.join(conf["export"]["path"], dataset_id)
        filename = dataset_id + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".zip"
        filename = os.path.join(dest_path, filename)
        filename_local = os.path.join("/tmp", filename)

        # First step, get all files indexed in the SensorThings database
        datastream_ids = []

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

        if len(files) < 1:
            rich.print(f"[red]No files to be zipped!")
            exit()

        # Now convert the file URLs to filesystem paths
        files = [self.fileserver.url2path(f) for f in files]

        # Try to remove path until the sensor name
        basepath = detect_common_path(files)
        rich.print(f"Base path for all files is {basepath}")
        # If common prefix goes beyond the sensor name, shorten it, we want to keep the sensor name
        if sensor.lower() not in basepath.lower():
            idx = basepath.lower().find(sensor.lower())
            basepath = basepath[:idx]
        rich.print(f"Final base path for all files is {basepath}")

        # Erase basepath, so we keep the basepath
        files = [f.replace(basepath, "") for f in files]

        # Make sure that relative paths are not interpreted as absolute now
        for i in range(len(files)):
            if files[i].startswith("/"):
                files[i] = files[i][1:]

        file_type = files[0].split(".")[-1]

        dataset_url = self.fileserver.path2url(filename)
        if not force and check_url(dataset_url):
            rich.print(f"[yellow]Dataset already exists! URL: {dataset_url}")
            return filename, dataset_url, file_type

        rich.print(f"creating folders...", end="")
        run_over_ssh(conf["export"]["host"], f"mkdir -p {dest_path}", fail_exit=True)
        rich.print("[green]done!")

        # If the command is too long it cannot be sent via ssh and will raise an OSError, we create a temporal script
        # with the command and send it to the host
        script_name = os.path.basename(filename).split(".")[0] + ".sh"
        rich.print(f"[blue]Creating zip script {script_name}...")

        cmd = "#!/bin/bash\n"
        cmd += "echo 'Auto-generated script from MMAPI, compressing files into a zip file'\n"
        cmd += f"cd {basepath}\n"
        cmd += f"mkdir -p {os.path.dirname(filename)}\n"
        cmd += f"zip -r {filename} {' '.join(files)}\n"

        with open(script_name, "w") as f:
            f.write(cmd)  # write the command to the script
        os.chmod(script_name, 0o775)
        rich.print(f"[blue]Delivering zip script...")
        script_dest = os.path.join(f"/var/tmp/{script_name}")
        self.fileserver.send_file("/var/tmp", script_name, indexed=False)

        rich.print(f"creating zip file with {len(files)} files, this may take a while...", end="")
        run_over_ssh(conf["export"]["host"], script_dest)
        rich.print("[green]done!")

        rich.print("Removing local script...")
        os.remove(script_name)
        rich.print("Removing remote script...")
        run_over_ssh(conf["export"]["host"], f"rm -f {script_dest}")

        return filename, dataset_url, file_type

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
        project_codes = []
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


def call_dataset_generator(dataframes: list, metadata: list, output="output.nc"):
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


    mh.generate_dataset(csv_files, meta_files, output=output)

