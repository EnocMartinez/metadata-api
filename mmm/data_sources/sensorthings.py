#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""


from .postgresql import PgDatabaseConnector
from ..common import LoggerSuperclass, reverse_dictionary, dataframe_to_dict
import json
import pandas as pd
import numpy as np
from .timescaledb import TimescaleDB
from ..data_manipulation import merge_dataframes_by_columns
import rich


def varname_from_datastream(ds_name):
    """
    Extracts a variable name from a datastream name. The datastream name must follow the following pattern:
        <station>:<sensor>:<VARNAME>:<data_type>
    :param ds_name:
    :raises: SyntaxError if the patter doesn't match
    :return: variable name
    """
    splitted = ds_name.split(":")
    if len(splitted) != 4:
        raise SyntaxError(f"Datastream name {ds_name} doesn't have the expected format!")
    varname = splitted[2]
    return varname


def varname_from_datastream_dataframe(df, datastream_name):
    """
    Renames a dataframe from raw_data table (value, qc_flag) to variable names, e.g. if variable name TEMP, then
    value->TEMP qc_flag->TEMP_qc
    """
    varname = varname_from_datastream(datastream_name)
    varname_qc = varname + "_qc"
    varname_std = varname + "_std"
    df = df.rename(columns={"value": varname, "qc_flag": varname_qc, "stdev": varname_std})
    return df, varname


def varname_from_datastream(datastream_name):
    """
    Takes a raw data name (e.g. OBSEA:SBE37:PSAL:raw_data) to variable name (PSAL)
    """
    return datastream_name.split(":")[2]


class SensorthingsDbConnector(PgDatabaseConnector, LoggerSuperclass):
    def __init__(self, host, port, db_name, db_user, db_password, logger, timescaledb=False):
        """
        initializes  DB connector specific for SensorThings API database (FROST implementation)
        :param host:
        :param port:
        :param db_name:
        :param db_user:
        :param db_password:
        :param logger:
        """
        PgDatabaseConnector.__init__(self, host, port, db_name, db_user, db_password, logger)
        LoggerSuperclass.__init__(self, logger, "STA DB")
        self.info("Initialize database connector...")
        self.__sensor_properties = {}

        if timescaledb:
            self.timescale = TimescaleDB(self, logger)
        else:
            self.timescale = None

        # This dicts provide a quick way to get the relations between elements in the database without doing a query
        # they are initialized with __initialize_dicts()
        self.datastream_id_varname = {}      # key: datastream_id, value: variable name
        self.datastream_id_sensor_name = {}  # key: datastream_id, value: sensor name
        self.sensor_id_name = {}             # key: sensor id, value: name
        self.thing_id_name = {}             # key: sensor id, value: name
        self.datastream_name_id = {}         # key: datastream name, value: datastream id
        self.obs_prop_name_id = {}           # key: observed property name, value: observed property id

        # dictionaries where sensors key is name and value is ID
        self.__initialize_dicts()

        # dictionaries where key is ID and value is name
        self.sensor_name_id = reverse_dictionary(self.sensor_id_name)
        self.datastream_id_name = reverse_dictionary(self.datastream_name_id)
        #self.obs_prop_id_name = reverse_dictionary(self.obs_prop_name_id)
        self.thing_name_id = reverse_dictionary(self.thing_id_name)
        self.obs_prop_id_name = reverse_dictionary(self.obs_prop_name_id)
        self.datastream_properties = self.get_datastream_properties()

    def sensor_var_from_datastream(self, datastream_id):
        """
        Returns the sensor name and variable name from a datastream_id
        :param datastream_id: datastream ID
        :returns: a tuple of sensor_name, varname
        """
        sensor_name = self.datastream_id_sensor_name[datastream_id]
        varname = self.datastream_id_varname[datastream_id]
        return sensor_name, varname

    def __initialize_dicts(self):
        """
        Initialize the dicts used for quickly access to relations without querying the database
        """
        # DATASTREAM -> SENSOR relation
        query = """
            select "DATASTREAMS"."ID" as datastream_id, "SENSORS"."ID" as sensor_id, "SENSORS"."NAME" as sensor_name, 
            "DATASTREAMS"."NAME" as datastream_name
            from "DATASTREAMS", "SENSORS" 
            where "DATASTREAMS"."SENSOR_ID" = "SENSORS"."ID" order by datastream_id asc;"""
        df = self.dataframe_from_query(query)

        # key: datastream_id ; value: sensor name
        self.datastream_id_sensor_name = dataframe_to_dict(df, "datastream_id", "sensor_name")
        datastream_name_dict = dataframe_to_dict(df, "datastream_id", "datastream_name")

        # convert datastream name into variable name "OBSEA:SBE16:TEMP:raw_data -> TEMP
        self.datastream_id_varname = {key: name.split(":")[2] for key, name in datastream_name_dict.items()}

        # SENSOR ID -> SENSOR NAME
        self.sensor_id_name = self.get_sensors()  # key: sensor_id, value: sensor_name
        self.thing_id_name = self.get_things()  # key: sensor_id, value: sensor_name

        # DATASTREAM_NAME -> DATASTREAM_ID
        df = self.dataframe_from_query(f'select "ID", "NAME" from "DATASTREAMS";')
        self.datastream_name_id = dataframe_to_dict(df, "NAME", "ID")

        # OBS_PROPERTY NAME -> OBS_PROPERTY ID
        df = self.dataframe_from_query('select "ID", "NAME" from "OBS_PROPERTIES";')
        self.obs_prop_name_id = dataframe_to_dict(df, "NAME", "ID")

    def get_sensor_datastreams(self, sensor_id):
        """
        Returns a dataframe with all datastreams belonging to a sensor
        :param sensor_id: ID of a sensor
        :return: dataframe with datastreams ID, NAME and PROPERTIES
        """
        query = (f'select "ID" as id , "NAME" as name, "THING_ID" as thing_id, "OBS_PROPERTY_ID" AS obs_prop_id,'
                 f' "PROPERTIES" as properties from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};')
        df = self.dataframe_from_query(query)
        return df

    def get_sensor_qc_metadata(self, name: str):
        """
        Returns an object with all the necessary information to apply QC to all variables from the sensor
        :return:
        """
        sensor_id = self.sensor_id_name[name]
        data = {
            "name": name,
            "id": int(sensor_id),  # from numpy.int64 to regular int
            "variables": {}
        }
        query = 'select "ID" as id , "NAME" as name, "PROPERTIES" as properties ' \
                f'from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};'
        df = self.dataframe_from_query(query)
        for _, row in df.iterrows():
            ds_name = row["name"]
            properties = row["properties"]
            if "rawSensorData" not in properties.keys() or not properties["rawSensorData"]:
                # if rawSensorData = False of if there is no rawSensorData flag, just ignore this datastream
                self.debug(f"Ignoring datastream {ds_name}")
                continue
            self.info(f"Loading configuration for datastream {ds_name}")
            varname = varname_from_datastream(ds_name)

            qc_config = {}
            try:
                qc_config = properties["QualityControl"]["configuration"]
            except KeyError as e:
                self.warning(f"Quality Control for variable {varname} not found!")
                self.warning(f"KeyError: {e}")

            data["variables"][varname] = {
                "variable_name": varname,
                "datastream_name": ds_name,
                "datastream_id": int(self.datastream_name_id[ds_name]),
                "quality_control": qc_config
            }
        return data

    def get_qc_config(self, raw_sensor_data_flag="rawSensorData") -> dict:
        """
        Gets the QC config for all sensors from the database and stores it in a dictionary
        :return: dict with the configuration
        """
        sensors = {}

        for sensor, sensor_id in self.sensor_id_name.items():
            sensors[sensor] = {"datastreams": {}, "id": sensor_id, "name": sensor}

            # Select Datastreams belonging to a sensor and expand rawData flag and quality control config
            q = f'select "ID" as id, "NAME" as name, ("PROPERTIES"->>\'QualityControl\') as qc_config, ' \
                f'("PROPERTIES"->>\'{raw_sensor_data_flag}\')::BOOLEAN as is_raw_data ' \
                f' from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};'

            df = self.dataframe_from_query(q, debug=False)

            for index, row in df.iterrows():
                if not row["is_raw_data"]:
                    self.warning(f"[yellow] ignoring datastream {row['name']}")
                    continue

                sensors[sensor]["datastreams"][row["id"]] = {
                    "name": row["name"],
                    "qc_config": json.loads(row["qc_config"])
                }

    def get_sensors(self):
        """
        Returns a dictionary with sensor's names and their id, e.g. {"SBE37": 3, "AWAC": 5}
        :return: dictionary
        """
        df = self.dataframe_from_query('select "ID", "NAME" from "SENSORS";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_things(self):
        df = self.dataframe_from_query('select "ID", "NAME" from "THINGS";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_sensor_properties(self):
        """
        Returns a dictionary with sensor's names and their properties
        :return: dictionary
        """
        if self.__sensor_properties:
            return self.__sensor_properties
        df = self.dataframe_from_query('select "NAME", "PROPERTIES" from "SENSORS";')
        self.__sensor_properties = dataframe_to_dict(df, "NAME", "PROPERTIES")
        return self.__sensor_properties


    def get_datastream_sensor(self, fields=["ID", "SENSOR_ID"]):
        select_fields = ", ".join(fields)
        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "NAME", "SENSOR_ID")


    def get_datastream_properties(self, fields=["ID", "PROPERTIES"]):
        select_fields = f'"{fields[0]}"'
        for f in fields[1:]:
            select_fields += f', "{f}"'

        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "ID", "PROPERTIES")

    def get_last_datastream_timestamp(self, datastream_id):
        """
        Returns a timestamp (pd.Timestamp) with the last data point from a certain datastream. If there's no data
        return None or pd.Timestamp
        """

        properties = self.datastream_properties[datastream_id]
        if properties["rawSensorData"]:
            query = f"select timestamp from {self.raw_data_table} where datastream_id = {datastream_id} order by" \
                    f" timestamp desc limit 1;"
            row = self.dataframe_from_query(query)

        else:
            query = f'select "PHENOMENON_TIME_START" as timestamp from "OBSERVATIONS" where "DATASTREAM_ID" = {datastream_id} ' \
                    f'order by "PHENOMENON_TIME_START" desc limit 1;'
            row = self.dataframe_from_query(query)

        if row.empty:
            return None

        return row["timestamp"][0]

    def get_last_datastream_data(self, datastream_id, hours):
        """
        Gets the last N hours of data from a datastream
        :param datastream_id: ID of the datastream
        :param hours: get last X hours
        :return: dataframe with the data
        """
        query = f"select timestamp, value, qc_flag from raw_data where datastream_id = {datastream_id} " \
                f"and timestamp > now() - INTERVAL '{hours} hours' order by timestamp asc;"
        df = self.dataframe_from_query(query)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        return df


    def get_data(self, identifier, time_start: str, time_end: str):
        """
        Access the 0BSERVATIONS data table and exports all data between time_start and time_end
        :param identifier: datasream name (str) or datastream id (int)
        :param time_start: start time
        :param time_end: end time  (not included)
        """
        if type(identifier) == int:
            pass
        elif type(identifier) == str:  # if string, convert from name to ID
            identifier = self.datastream_name_id[identifier]

        query = f' select ' \
                f'    "PHENOMENON_TIME_START" AS timestamp, ' \
                f'    "RESULT_NUMBER" AS value,' \
                f'    ("RESULT_QUALITY" ->> \'qc_flag\'::text)::integer AS qc_flag,' \
                f'    ("RESULT_QUALITY" ->> \'stdev\'::text)::double precision AS stdev ' \
                f'from "OBSERVATIONS" ' \
                f'where "OBSERVATIONS"."DATASTREAM_ID" = {identifier} ' \
                f'and "PHENOMENON_TIME_START" >= \'{time_start}\' and  "PHENOMENON_TIME_START" < \'{time_end}\'' \
                f'order by timestamp asc;'

        df = self.dataframe_from_query(query)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        if not df.empty and np.isnan(df["stdev"].max()):
            self.debug(f"Dropping stdev for {self.datastream_id_name[identifier]}")
            del df["stdev"]
        return df.set_index("timestamp")

    def dataframe_from_datastream(self, datastream_id: int, time_start: pd.Timestamp, time_end: pd.Timestamp):
        """
        Takes the ID of a datastream and exports its data to a dataframe.
        """
        df = self.dataframe_from_query(f'select "PROPERTIES" from "DATASTREAMS" where "ID"={datastream_id};')
        if df.empty:
            raise ValueError(f"Datastream with ID={datastream_id} not found in database!")
        properties = df.values[0][0]  # only one value with the properties in JSON format
        # if the datastream has rawSensorData=True in properties, export from raw_data
        if self.timescale and properties["rawSensorData"]:
            df = self.timescale.get_raw_data(datastream_id, time_start, time_end)
        else:  # otherwise export from observations
            df = self.get_data(datastream_id, time_start, time_end)
        df, _ = varname_from_datastream_dataframe(df, self.datastream_id_name[datastream_id])
        return df

    def get_dataset(self, sensor:str, station_name:str, options: dict, time_start: pd.Timestamp, time_end:pd.Timestamp, variables: list=[])\
            -> pd.DataFrame:
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
        assert("rawData" in options.keys())
        if not options["rawData"]:
            assert("averagePeriod" in options.keys())

        raw_data = options["rawData"]

        if variables:
            rich.print(f"Keeping only variables: {variables}")

        sensor_id = self.sensor_id_name[sensor]
        station_id = self.thing_id_name[station_name]
        # Get all datastreams for a sensor
        datastreams = self.get_sensor_datastreams(sensor_id)
        # Keep only datastreams at a specific station
        datastreams = datastreams[datastreams["thing_id"] == station_id]

        # Drop datastreams with variables that are not in the list
        if variables:
            variable_ids = [self.obs_prop_name_id[v] for v in variables]
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
            else: # Keep only those variables that are not raw_data and whose period matches the data_type
                if row["properties"]["rawSensorData"]:  # Do not keep raw data
                    remove_idxs.append(idx)
                elif row["properties"]["averagePeriod"] != options["averagePeriod"]:  # do not keep data with different period
                    remove_idxs.append(idx)

        datastreams = datastreams.drop(remove_idxs)

        # Query data for every datastream
        data = []
        for idx, row in datastreams.iterrows():
            datastream_id = row["id"]
            obs_prop_id = row["obs_prop_id"]
          #  df = self.get_data(datastream_id, time_start, time_end)
            df = self.dataframe_from_datastream(datastream_id, time_start, time_end)

            varname = self.obs_prop_id_name[obs_prop_id]
            rename = {"value": varname, "qc_flag": varname + "_QC"}
            if "stdev" in df.columns:
                rename["stdev"] = varname  + "_SD"
            df = df.rename(columns=rename)
            data.append(df)
        return merge_dataframes_by_columns(data)

    def check_if_table_exists(self, view_name):
        """
        Checks if a view already exists
        :param view_name: database view to check if exists
        :return: True if exists, False if it doesn't
        """
        # Select all from information_schema
        query = "SELECT table_name FROM information_schema.tables"
        df = self.dataframe_from_query(query)
        table_names = df["table_name"].values
        if view_name in table_names:
            return True
        return False


