#!/usr/bin/env python3
"""
This file implements the DataCollector, a class implementing generic data access and delivery.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 30/11/22
"""
import logging

import emso_metadata_harmonizer.metadata
import jsonschema
import pandas as pd
from .data_sources import SensorThingsApiDB
from .ckan import CkanClient
from .common import run_subprocess, check_url, detect_common_path, run_over_ssh, LoggerSuperclass, assert_types, \
    assert_type
from .data_manipulation import open_csv, merge_dataframes_by_columns, merge_dataframes, calculate_time_intervals
from .metadata_collector import MetadataCollector, init_metadata_collector
from .fileserver import FileServer
from .dataset import DataExporter
import rich
import os
import json
import emso_metadata_harmonizer as mh
from .schemas import dataset_exporter_conf, dataset_exporter_formats
from mmm import DatasetObject


def init_data_collector(secrets: dict, log: logging.Logger, mc: MetadataCollector = None,
                        sta: SensorThingsApiDB = None):
    return DataCollector(secrets, log, mc=mc, sta=sta)


class DataCollector(LoggerSuperclass):
    """
    This class implements all methods to collect data from Databases and FileSystems, generate datasets and
    deliver them to the proper service.
    """

    def __init__(self, secrets: dict, log, mc: MetadataCollector = None, sta: SensorThingsApiDB = None):
        self.log = log
        LoggerSuperclass.__init__(self, log, "DC")
        if not mc:
            self.mc = init_metadata_collector(secrets, log=log)
        else:
            self.mc = mc

        if not sta:
            staconf = secrets["sensorthings"]
            self.sta = SensorThingsApiDB(staconf["host"], staconf["port"], staconf["database"], staconf["user"],
                                         staconf["password"], log, timescaledb=True)
        else:
            self.sta = sta
        self.fileserver = FileServer(secrets["fileserver"], log)

        self.emso = None  # by default, do not initialize emso metadata

    def dataset_filename(self, dataset: dict, fmt: str, tstart: pd.Timestamp, tend: pd.Timestamp,
                         tmp_folder="temp") -> str:
        """
        Based on the configuration, generate the dataset filename
        """
        assert type(dataset) is dict
        assert type(tstart) is pd.Timestamp
        assert type(tend) is pd.Timestamp

        # convert from dataset_exporter_formats to real extensions
        extensions = {
            "netcdf": ".nc",
            "csv": ".csv",
            "zip": ".zip"
        }

        dataset_id = dataset["#id"]
        os.makedirs(tmp_folder, exist_ok=True)
        filename = dataset_id + "_" + tstart.strftime("%Y%m%d") + "_" + tend.strftime("%Y%m%d") + extensions[fmt]
        return os.path.join(tmp_folder, filename)

    def generate_dataset(self, dataset: str | dict, service_name: str, time_start: pd.Timestamp|str,
                         time_end: pd.Timestamp|str, fmt: str = "") -> list:
        """
        Generate a dataset.
        :return: list of datasets
        """
        assert_type(service_name, str)
        assert_types(dataset, [dict, str])
        assert_types(time_start, [pd.Timestamp, str])
        assert_types(time_end, [pd.Timestamp, str])

        if type(dataset) is str:
            conf = self.mc.get_document("datasets", dataset)
        else:
            conf = dataset

        if type(time_start) is str:
            time_start = pd.Timestamp(time_start)
        if type(time_end) is str:
            time_end = pd.Timestamp(time_end)

        if time_start > time_end:
            raise ValueError(f"Time start={time_start} greater than time end={time_end}")

        # Check if we need to create a single file or a tree of smaller files:
        if conf["export"][service_name]["period"] == "none":
            d = self.generate_dataset_file(conf, service_name, time_start, time_end, fmt=fmt)
            return [d]
        else:
            return self.generate_dataset_tree(conf, service_name, time_start, time_end, fmt=fmt)

    def generate_dataset_tree(self,  dataset: dict, service_name: str, time_start: pd.Timestamp, time_end: pd.Timestamp,
                              fmt: str = ""):
        assert_type(service_name, str)
        assert_types(dataset, [dict, str])
        assert_type(time_start, pd.Timestamp)
        assert_type(time_end, pd.Timestamp)
        conf = dataset

        if service_name not in conf["export"].keys():
            raise ValueError(f"Dataset {conf['#id']} doesn't have export configuration for service '{service_name}'")

        service = conf["export"][service_name]

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

        # Get the period
        intervals = calculate_time_intervals(time_start, time_end, service["period"])

        datasets = []
        for tstart, tend in intervals:
            try:
                d = self.generate_dataset_file(conf, service_name, tstart, tend, fmt=fmt)
            except ValueError:
                # no data found for this period
                continue
            datasets.append(d)
        return datasets

    def generate_dataset_file(self, dataset: dict, service_name: str, time_start: pd.Timestamp,
                              time_end: pd.Timestamp, fmt: str = "") -> DatasetObject:
        """
        Generates a dataset based on its configuration stored in Metadata DB
        :param dataset: #id of the dataset
        :param service_name: Name of the service that will be used to export the dataset
        :param time_start: dataset time start
        :param time_end: dataset time end
        :param fmt: overwrite original format (e.g. csv instead of netcdf)
        :return: Dataset file
        """
        assert_type(dataset, dict)
        assert_type(service_name, str)
        assert_type(time_start, pd.Timestamp)
        assert_type(time_end, pd.Timestamp)
        conf = dataset
        # Convert service ID to dict
        if service_name not in conf["export"].keys():
            raise ValueError(f"Dataset {conf['#id']} doesn't have export configuration for service '{service_name}'")
        service = conf["export"][service_name]

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
        # Generate the dataset filename
        if not fmt:
            fmt = service["format"]
        else:
            assert fmt in dataset_exporter_formats, f"Format '{fmt}' not allowed"
        if fmt == "csv":
            filename = self.csv_from_sta(conf, time_start, time_end)
        elif fmt == "netcdf":
            filename = self.netcdf_from_sta(conf, time_start, time_end)
        elif fmt == "zip":
            filename = self.zip_from_filesystem(conf, time_start, time_end)
        else:
            raise ValueError(f"Unknown dataSource format '{fmt}'")
        obj = DatasetObject(conf, filename, service_name, time_start, time_end, fmt, self.log)
        return obj

    def dataframe_from_sta(self, conf: dict, station: dict, sensor: dict, time_start: pd.Timestamp,
                           time_end: pd.Timestamp) -> pd.DataFrame:
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
        df = df.rename(columns={"timestamp": "TIME"})
        df = df.set_index("TIME")
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
        df = df.rename(columns={"timestamp": "TIME"})
        df = df.set_index("TIME")
        df = df.sort_index(ascending=True)
        return df

    def netcdf_from_sta(self, conf, time_start: pd.Timestamp, time_end: pd.Timestamp):
        """
        Creates a NetCDF file according to the configuration
        :param conf:
        :param time_start: time start
        :param time_end: time end
        :param station: to minimize query time, let the option to pass the station
        :return: generated NetCDF filename
        """
        station = self.mc.get_document("stations", conf["@stations"])
        variables = []  # by default all variables will be used
        if "@variables" in conf.keys():
            variables = conf["@variables"]

        dataframes = []  # list with a dataframe per variable
        metadata = []    # list of a metadata dict per variable

        for sensor_name in conf["@sensors"]:
            self.info(f"Getting {sensor_name} data from {time_start} to {time_end}")
            sensor = self.mc.get_document("sensors", sensor_name)
            df = self.dataframe_from_sta(conf, station, sensor, time_start, time_end)
            dataframes.append(df)
            # Get the real time start/time end
            m = self.metadata_harmonizer_conf(conf, sensor, station, variables, tstart=time_start, tend=time_end)
            metadata.append(m)

        if all([df.empty for df in dataframes]):
            self.warning(f"ALL dataframes from {time_start} to {time_end} are empty!, skpping")
            raise ValueError("No data")

        self.info("Generating filename...")
        filename = self.dataset_filename(conf, "netcdf", time_start, time_end)
        self.info("Calling NetCDF wrapper...")
        filename = self.call_dataset_generator(dataframes, metadata, output=filename)
        self.info(f"Dataset {filename} generated!")
        return filename

    def csv_from_sta(self, conf, time_start: pd.Timestamp, time_end: pd.Timestamp):
        """
        Generates a CSV file from a SensorThings Database
        """
        filename = self.dataset_filename(conf, "csv", time_start, time_end)
        station = self.mc.get_document("stations", conf["@stations"])
        dataframes = []  # list with a dataframe per variable

        for sensor_name in conf["@sensors"]:
            sensor = self.mc.get_document("sensors", sensor_name)
            df = self.dataframe_from_sta(conf, station, sensor, time_start, time_end)
            if len(conf["@sensors"]) > 1:
                df["SENSOR_ID"] = sensor_name
            dataframes.append(df)

        df = merge_dataframes(dataframes)
        df.to_csv(filename)
        return filename

    def zip_from_filesystem(self, conf, time_start, time_end) -> str:
        """
        Compresses all files in the fileserver into a zip file. Since millions of files can be compressed, a small
        bash script will be generated and transfered to the fileserver and executed there. Then the file will be
        transfered to the machine running MMAPI
        """

        # Create the dataset in /var/tmp
        remote_filename = self.dataset_filename(conf, "zip", time_start, time_end, tmp_folder="/var/tmp")
        self.info(f"Creating ZIP dataset, ID: {conf['#id']}, from {time_start} to {time_end}")

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
            raise ValueError(f"No files to be zipped!")
        elif len(files) == 1:
            rich.print(f"[yellow]Just one file!")
            raise ValueError(f"Just one file to be zipped?")

        # Now convert the file URLs to filesystem paths
        files = [self.fileserver.url2path(f) for f in files]

        # Try to remove path until the sensor name
        basepath = detect_common_path(files)
        self.debug(f"Base path for all files is {basepath}")
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

        # If the command is too long it cannot be sent via ssh and will raise an OSError, we create a temporal script
        # with the command and send it to the host
        script_name = os.path.basename(remote_filename).split(".")[0] + ".sh"
        self.info(f"Creating zip script {script_name}...")

        cmd = "#!/bin/bash\n"
        cmd += "echo 'Auto-generated script from MMAPI, compressing files into a zip file'\n"
        cmd += f"cd {basepath}\n"
        cmd += f"mkdir -p {os.path.dirname(remote_filename)}\n"
        cmd += f"zip -r {remote_filename} {' '.join(files)}\n"

        with open(script_name, "w") as f:
            f.write(cmd)  # write the command to the script
        os.chmod(script_name, 0o775)

        self.info(f"Delivering script...")
        script_dest = os.path.join(f"/var/tmp/{script_name}")
        self.fileserver.send_file("/var/tmp", script_name, indexed=False)

        self.info(f"Creating zip file with {len(files)} files, this may take a while...")
        run_over_ssh(self.fileserver.host, script_dest, fail_exit=True)

        # Now get the file
        self.info(f"get zip file from server...")
        filename = self.fileserver.recv_file(remote_filename, "tmpdata")

        # delete remote file
        run_over_ssh(self.fileserver.host, f"rm {remote_filename}")
        # delete local file
        os.remove(script_name)

        run_over_ssh(self.fileserver.host, f"rm {script_dest}")

        return filename

    def metadata_harmonizer_conf(self, dataset, sensor: dict, station: dict, variable_ids: list,
                                 default_data_mode="real-time", os_data_type="OceanSITES time-series data",
                                 tstart: str = "", tend: str = "") -> dict:
        """
        This method returns the configuration required by the Metadata Harmonizer tool from the Metadata DB
        :param dataset: sensor dict from Metadata DB database
        :param sensor: sensor dict from Metadata DB database
        :param station: station dict from Metadata DB database
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

    def upload_datafile_to_ckan(self, ckan, dataset: DatasetObject):
        """
        Takes a dataset in the FileServer and publish it to CKAN as a resource
        """
        assert type(ckan) is CkanClient
        assert type(dataset) is DatasetObject

        # The ID of the resource will be the filename in lower case with _<format>
        resource_id = os.path.basename(dataset.filename).replace(".", "_").lower()
        package_id = dataset.dataset_id.lower()

        name = f"{dataset.dataset_id} data from {dataset.tstart_str('%Y-%m-%d')} to {dataset.tend_str('%Y-%m-%d')}"
        description = f"Data in {dataset.fmt} format from {dataset.tstart_str()} to {dataset.tend_str()}"
        return ckan.resource_create(
            package_id,
            resource_id,
            description=description,
            name=name,
            format=dataset.fmt,
            resource_url=dataset.url)

    def call_dataset_generator(self, dataframes: list, metadata: list, output="output.nc"):
        """
        Dump dataframes and metadata to temporal files and calls the datasets generator
        :param dataframes:
        :param metadata:
        :param output:
        :return:
        """
        assert (len(dataframes) == len(metadata))
        if not self.emso:
            self.emso = emso_metadata_harmonizer.metadata.EmsoMetadata()
        dataframes = [df.reset_index() for df in dataframes]
        mh.generate_dataset(dataframes, metadata, output=output, emso_metadata=self.emso)
        return output
