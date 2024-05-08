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

from .ckan import CkanClient
from .common import run_subprocess, check_url
from .data_manipulation import open_csv, merge_dataframes_by_columns, merge_dataframes
from .metadata_collector import MetadataCollector, init_metadata_collector
from .fileserver import FileServer
import rich
import os
import json
from stadb import SensorThingsApiDB


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
        self.fileserver = FileServer(secrets["fileserver"])

    def generate(self, dataset_id: str, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder: str,
                 ckan: CkanClient, format="") -> str:
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

        if conf["dataSource"] == "sensorthings":

            if format.lower() == "csv":
                dataset_file, dataset_url, file_type = self.csv_from_sta(conf, time_start, time_end, out_folder)
            else:
                dataset_file, dataset_url, file_type = self.netcdf_from_sta(conf, time_start, time_end, out_folder)

        elif conf["dataSource"] == "filesystem":
            dataset_file, dataset_url, file_type = self.zip_from_filesystem(conf, time_start, time_end, out_folder)

        else:
            raise ValueError(f"data_source='{conf['dataSource']}' not implemented")

        if not dataset_url:
            if "export" in conf.keys():
                path = conf["export"]["path"]
                rich.print(f"this is the path {path}")
                path = os.path.join(path, dataset_id)  # append dataset_id to the path
                rich.print(f"new path {path}")
                dataset_url = self.fileserver.send_file(path, dataset_file)
            else:
                rich.print(f"[red]ERROR! export required, but no export parameters have been set")

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
            resource_id = os.path.basename(dataset_file).split(".")[0]

            start_date = time_start.strftime("%Y-%m-%d")
            end_date = time_end.strftime("%Y-%m-%d")

            description = f"data from {start_date} to {end_date}"
            name = f"data from {start_date} to {end_date}"

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

    def dataframe_from_sta(self, conf: dict, station: dict , sensor: dict, time_start: pd.Timestamp, time_end: pd.Timestamp):
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

        call_generator(dataframes, metadata, output=filename)

        # We should return (file, url), but we don't have url yet
        return filename, None, "NetCDF"

    def csv_from_sta(self, conf, time_start: pd.Timestamp, time_end: pd.Timestamp, out_folder, csv_file=""):
        """
        Generates a NetCDF file from a SensorThings Database
        :param conf:
        :return:
        """
        rich.print("[cyan]Generating CSV from a SensorThings API file")
        rich.print("processing data_source_options...")

        filename = conf["#id"] + "_" + time_start.strftime("%Y%m%d") + "_" + time_end.strftime("%Y%m%d") + ".csv"
        filename = os.path.join(out_folder, filename)

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
        print(df)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename)

        return filename, None, "CSV"

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

        file_type = files[0].split(".")[-1]

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

        dataset_url = self.fileserver.path2url(filename)

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
