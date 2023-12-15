#!/usr/bin/env python3
"""
This file implements functions and classes to interact with the SensorThings Database directly.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 23/3/21
"""
import threading
import psycopg2
import pandas as pd
import numpy as np
import time
import os
from threading import Thread
import datetime
import gc
import json
import rich
from rich.progress import Progress

from mmm.sensorthings.api import sensorthings_post, sensorthings_get
from mmm.sensorthings.timescaledb import TimescaleDb
from mmm.data_manipulation import slice_dataframes
from mmm.parallelism import multiprocess


def check_if_inserted_api(url, datasetream_id, phenomenon_time):
    """
    Checks if in a specific time there is already an observation of a DatastreamID trhough the SensorThings API
    :return: The observation (if exists) or None if it does not exist
    """
    endpoint = "Datastreams(%d)" % datasetream_id + "/Observations"
    endpoint += "?$filter=resultTime eq %s" % phenomenon_time  # date matches
    obs = sensorthings_get(url, endpoint=endpoint)
    if obs["value"]:
        return obs["value"][0]
    return None


class SensorThingsDbConnector(object):
    def __init__(self, config: dict, debug=False, timeseries="timeseries", profiles="profiles"):
        """

        :param host:
        :param port:
        :param db_name:
        :param db_user:
        :param db_password:
        """
        rich.print("Connecting to SensorThings Database")

        for key in ["host", "database", "port", "user", "password"]:
            assert(key in config.keys())

        self.debug = debug

        self.host = config["host"]
        self.db_name = config["database"]
        self.db_password = config["password"]
        self.port = config["port"]
        self.db_user = config["user"]

        self.connection = psycopg2.connect(host=self.host, port=self.port, dbname=self.db_name, user=self.db_user,
                                           password=self.db_password)
        self.cursor = self.connection.cursor()

        self.__last_observation_index = -1

        self.__sta_ids = ["sensorthings", "sensorthings api", "sta"]
        self.__timescaledb_ids = ["timescale", "timescaledb", "timescale db"]

        self.tsdb = TimescaleDb(self, timeseries_table=timeseries, profiles_table=profiles)

        self.__unique_constraint_name = "OBSERVATIONS_UNIQUE_TIME_DATASTREAM"
        # self.add_unique_time_constraint()
        self.add_unique_name_constraint("SENSORS")
        self.add_unique_name_constraint("THINGS")
        self.add_unique_name_constraint("DATASTREAMS")
        self.add_unique_name_constraint("OBS_PROPERTIES")

    def exec_query(self, query, debug=False):
        """
        Wrapper to query with psycopg2
        :param cursor: DB cursor
        :param query as string
        :returns nothing
        """
        if debug:
            rich.print("[magenta]%s" % query)
        self.cursor.execute(query)
        self.connection.commit()

    def dataframe_from_query(self, query, debug=False):
        """
        Makes a query to the database using a cursor object and returns a DataFrame object
        with the reponse
        :param query: string with the query
        :returns DataFrame with the query result
        """
        self.exec_query(query, debug=debug)
        response = self.cursor.fetchall()
        colnames = [desc[0] for desc in self.cursor.description]  # Get the Column names
        return pd.DataFrame(response, columns=colnames)

    def dict_from_query(self, query, debug=False):
        self.exec_query(query, debug=debug)
        response = self.cursor.fetchall()
        if len(response) == 0:
            return {}
        elif len(response[0]) != 2:
            raise ValueError(f"Expected two fields in response, got {len(response[0])}")

        return {key: value for key, value in response}

    def value_from_query(self, query, debug=False):
        """
        Run a single value from a query
        """
        self.exec_query(query, debug=debug)
        response = self.cursor.fetchall()
        return response[0][0]

    def get_last_timestamp(self, table_name):
        query = f"select timestamp from {table_name} order by timestamp desc limit 1;"
        df = self.dataframe_from_query(query)
        if df.empty:
            raise ValueError("Empty response from database!")
        date = df["timestamp"][0]
        date += pd.to_timedelta("10s")
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_last_observation_id(self):
        """
        Gets last observation in database
        :return:
        """
        query = "SELECT \"ID\" FROM public.\"OBSERVATIONS\" ORDER BY \"ID\" DESC LIMIT 1"
        df = self.dataframe_from_query(query)
        #  Check if the table is empty
        if df.empty:
            return 0
        return int(df["ID"].values[0])

    def sql_copy_csv(self, filename, table="OBSERVATIONS", delimiter=","):
        """
        Execute a COPY query to copy from a local CSV file to a database
        :return:
        """
        query = "COPY public.\"%s\" FROM '%s' DELIMITER '%s' CSV HEADER;" % (table, filename, delimiter)
        rich.print(f"[purple]{query}")
        self.cursor.execute(query)
        self.connection.commit()

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

    def check_if_constraint_exists(self, constrain_name):
        query = f"SELECT 1 FROM pg_constraint WHERE conname = '{constrain_name}';"
        df = self.dataframe_from_query(query)
        if df.empty:
            return False
        return True

    def add_unique_time_constraint(self):
        """
        Adds a constraint to ensure that we do not have duplicated values
        """

        if self.check_if_constraint_exists(self.__unique_constraint_name):
            # already exists
            return None
        query = f'ALTER TABLE IF EXISTS public."OBSERVATIONS" ADD CONSTRAINT "{self.__unique_constraint_name}" UNIQUE ' \
                f'("PHENOMENON_TIME_START", "DATASTREAM_ID");'
        self.exec_query(query)

    def add_unique_name_constraint(self, table_name):
        """
        Adds a constraint to have unique "NAME" values in a table
        :param table_name: table to add constraint
        :return:
        """
        constraint_name = f"{table_name}_UNIQUE_NAME"
        if not self.check_if_constraint_exists(constraint_name):
            query = f'ALTER TABLE IF EXISTS public."{table_name}" ADD CONSTRAINT "{constraint_name}" UNIQUE ' \
                    f'("NAME");'
            self.exec_query(query)

    def disable_observations_triggers(self):
        """
        Disables all triggers on observation table
        """
        query = "ALTER TABLE public.\"OBSERVATIONS\" DISABLE TRIGGER ALL;"
        self.exec_query(query)

    def enable_observations_triggers(self):
        """
        Enables all triggers on observation's table
        :return:
        """
        query = "ALTER TABLE public.\"OBSERVATIONS\" ENABLE TRIGGER ALL;"
        self.exec_query(query)

    def drop_observations_constrains(self):
        """
        Drops all foreign key constraints in table OBSERVATIONs
        :return:
        """
        query = "ALTER TABLE  public.\"OBSERVATIONS\"  DROP CONSTRAINT IF EXISTS \"OBSERVATIONS_DATASTREAM_ID_FKEY\";"
        self.exec_query(query)
        query = "ALTER TABLE  public.\"OBSERVATIONS\"  DROP CONSTRAINT IF EXISTS \"OBSERVATIONS_FEATURE_ID_FKEY\";"
        self.exec_query(query)

        query = f"ALTER TABLE  public.\"OBSERVATIONS\"  DROP CONSTRAINT IF EXISTS " \
                f"\"{self.__unique_constraint_name}\";"
        self.exec_query(query)

    def create_observations_constrains(self):
        """
        Drops all foreing key constraints in table OBSERVATIONs
        :return:
        """
        query = "ALTER TABLE public.\"OBSERVATIONS\"   ADD CONSTRAINT \"OBSERVATIONS_DATASTREAM_ID_FKEY\" FOREIGN " \
                "KEY (\"DATASTREAM_ID\") REFERENCES public.\"DATASTREAMS\" (\"ID\") MATCH SIMPLE ON UPDATE CASCADE " \
                "ON DELETE CASCADE;"
        self.exec_query(query)

        query = "ALTER TABLE public.\"OBSERVATIONS\" ADD CONSTRAINT \"OBSERVATIONS_FEATURE_ID_FKEY\" FOREIGN KEY " \
                "(\"FEATURE_ID\") REFERENCES public.\"FEATURES\" (\"ID\") MATCH SIMPLE ON UPDATE CASCADE ON DELETE " \
                "CASCADE;"
        self.exec_query(query)

        self.add_unique_time_constraint()

    def drop_observations_index(self):
        """
        Drops all indexes in OBSERVATIONs table
        :return:
        """
        self.exec_query("DROP INDEX public.\"OBSERVATIONS_FEATURE_ID\";")
        self.exec_query("DROP INDEX public.\"OBSERVATIONS_DATASTREAM_ID\";")

    def create_observations_index(self):
        """
        Creates all indexes in OBSERVATIONs table
        :return:
        """
        self.exec_query("CREATE INDEX \"OBSERVATIONS_DATASTREAM_ID\" ON public.\"OBSERVATIONS\" USING btree"
                        " (\"DATASTREAM_ID\" ASC NULLS LAST) TABLESPACE pg_default;")
        self.exec_query("CREATE INDEX \"OBSERVATIONS_FEATURE_ID\" ON public.\"OBSERVATIONS\" USING btree  "
                        "(\"FEATURE_ID\" ASC NULLS LAST)TABLESPACE pg_default;")

    def format_csv_sta(self, df_in, column_mapper, filename, feature_id, avg_period: str = "", profile=False):
        """
        Takes a dataframe and arranges it accordingly to the OBSERVATIONS table from a SensorThings API, preparing the
        data to be inserted by a COPY statement
        :param df_in: input dataframe
        :param column_mapper: structure that maps datastreams with dataframe columns
        :param filename: name of the file to be generated
        :param feature_id: ID of the FeatureOfInterst
        :param avg_period: if set, the phenomenon time end will be timestamp + avg_period to generate a timerange.
                           used in averaged data.
        """
        init = False
        if self.__last_observation_index < 0:  # not initialized
            self.__last_observation_index = self.get_last_observation_id()

        for colname, datastream_id in column_mapper.items():
            if colname not in df_in.columns:
                continue

            df = df_in.copy(deep=True)
            quality_control = False
            stdev = False
            keep = ["timestamp", colname]
            if colname + "_qc" in df_in.columns:
                quality_control = True
                keep += [colname + "_qc"]

            if colname + "_std" in df_in.columns:
                stdev = True
                keep += [colname + "_std"]

            if profile:
                keep += ["depth"]

            df["timestamp"] = df.index.values
            for c in df.columns:
                if c not in keep:
                    del df[c]

            if df.empty:
                rich.print(f"[yellow]Got empty dataframe for {colname}")
                continue

            df["PHENOMENON_TIME_START"] = np.datetime_as_string(df["timestamp"], unit="s", timezone="UTC")

            if avg_period:  # if we have the average period
                df["PHENOMENON_TIME_END"] = np.datetime_as_string(df["timestamp"] + pd.to_timedelta(avg_period),
                                                                  unit="s", timezone="UTC")
            else:
                df["PHENOMENON_TIME_END"] = df["PHENOMENON_TIME_START"]
            df["RESULT_TIME"] = df["PHENOMENON_TIME_START"]
            df["RESULT_TYPE"] = 0
            df["RESULT_NUMBER"] = df[colname]
            df["RESULT_BOOLEAN"] = np.nan
            df["RESULT_JSON"] = np.nan
            df["RESULT_STRING"] = df[colname].astype(str)
            df["RESULT_QUALITY"] = "{\"qc_flag\": 2}"
            df["VALID_TIME_START"] = np.nan
            df["VALID_TIME_END"] = np.nan
            if profile:
                df["PARAMETERS"] = ""  # in case of profile we need to add depth as parameter
            else:
                df["PARAMETERS"] = np.nan
            df["DATASTREAM_ID"] = datastream_id
            df["FEATURE_ID"] = feature_id
            df["ID"] = np.arange(0, len(df.index.values), dtype=int) + self.__last_observation_index + 1
            self.__last_observation_index = df["ID"].values[-1]

            # Quality control and standard deviation
            for i in range(0, len(df.index.values)):
                qc_value = np.nan
                std_value = np.nan
                if stdev:
                    std_value = df[colname + "_std"].values[i]
                if qc_value:
                    qc_value = df[colname + "_qc"].values[i]
                if not np.isnan(qc_value) and stdev and not np.isnan(std_value):
                    # If we have QC and STD, put both
                    df["RESULT_QUALITY"].values[i] = "{\"qc_flag\": %d, \"stdev\": %f}" % \
                                                     (df[colname + "_qc"].values[i], df[colname + "_std"].values[i])
                elif not np.isnan(qc_value):
                    # If we only have QC, put it
                    if df[colname + "_qc"].values[i]:
                        df["RESULT_QUALITY"].values[i] = "{\"qc_flag\": %d}" % (df[colname + "_qc"].values[i])
                elif not np.isnan(std_value):
                    # If we only have STD, put it
                    if df[colname + "_std"].values[i]:
                        df["RESULT_QUALITY"].values[i] = "{\"stdev\": %d}" % (df[colname + "_std"].values[i])

            if profile:
                df["depth"] = df["depth"].values.astype(int)  # force conversion to integer
                for i in range(0, len(df.index.values)):
                    df["PARAMETERS"].values[i] = "{\"depth\": %d}" % (df["depth"].values[i])

            del df["timestamp"]
            del df[colname]
            if quality_control:
                del df[colname + "_qc"]
            if stdev:
                del df[colname + "_std"]
            if profile:
                del df["depth"]

            if not init:
                df_final = df
                init = True
            else:
                df_final = pd.concat([df_final, df])

        df_final.to_csv(filename, index=False)

    def format_timeseries_csv(self, df_in, column_mapper, filename):
        """
        Format from a regular dataframe to a Dataframe ready to be copied into a TimescaleDB simple table
        :param df_in:
        :param column_mapper:
        :return:
        """
        df_final = None
        init = False
        for colname, datastream_id in column_mapper.items():
            if colname not in df_in.columns:  # if column is not in dataset, just ignore this datastream
                continue
            df = df_in.copy(deep=True)
            keep = ["timestamp", colname, colname + "_qc"]
            df["timestamp"] = df.index.values
            df = df[keep]
            df = df.dropna(how="any")
            df["time"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df["datastream_id"] = datastream_id
            df = df.set_index("time")
            df = df.rename(columns={colname: "value", colname + "_qc": "qc_flag"})
            df["qc_flag"] = df["qc_flag"].values.astype(int)
            del df["timestamp"]
            if not init:
                df_final = df
                init = True
            else:
                df_final = pd.concat([df_final, df])
        df_final.to_csv(filename)
        del df_final
        gc.collect()

    def format_profile_csv(self, df_in, column_mapper, filename):
        """
        Format from a regular dataframe to a Dataframe ready to be copied into a TimescaleDB simple table
        :param df_in:
        :param column_mapper:
        :return:
        """
        df_final = None
        init = False
        for colname, datastream_id in column_mapper.items():
            if colname not in df_in.columns:  # if column is not in dataset, just ignore this datastream
                continue
            df = df_in.copy(deep=True)
            keep = ["timestamp", "depth", colname, colname + "_qc"]
            df["timestamp"] = df.index.values
            df = df[keep]
            df = df.dropna(how="any")
            df["time"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df["datastream_id"] = datastream_id
            df = df.set_index("time")
            df = df.rename(columns={colname: "value", colname + "_qc": "qc_flag"})
            df["qc_flag"] = df["qc_flag"].values.astype(int)
            del df["timestamp"]
            if not init:
                df_final = df
                init = True
            else:
                df_final = pd.concat([df_final, df])
        df_final.to_csv(filename)
        del df_final
        gc.collect()

    def dataframes_to_observations_csv(self, dataframes: list, column_mapper: dict, folder: str, feature_id: int, avg_period:str = "", profile=False):
        """
        Write dataframes into local csv files ready for sql copy following the syntax in table OBSERVATIONS
        """
        files = []
        i = 0
        for dataframe in dataframes:
            file = os.path.join(folder, f"observations_copy_{i:04d}.csv")
            i += 1
            self.format_csv_sta(dataframe, column_mapper, file, feature_id, avg_period=avg_period, profile=profile)
            files.append(file)

        return files

    def dataframes_to_timeseries_csv(self, dataframes: list, column_mapper: dict, folder: str):
        """
        Write dataframes into local csv files ready for sql copy following the syntax in table OBSERVATIONS
        """
        i = 0
        files = []
        for dataframe in dataframes:
            file = os.path.join(folder, f"timeseries_copy_{i:04d}.csv")
            i += 1
            rich.print(f"timeseries CSV {i:04d} of {len(dataframes)}")
            self.format_timeseries_csv(dataframe, column_mapper, file)
            files.append(file)
        return files

    def dataframes_to_profile_csv(self, dataframes: list, column_mapper: dict, folder: str):
        """
        Write dataframes into local csv files ready for sql copy following the syntax in table OBSERVATIONS
        """
        i = 0
        files = []
        for dataframe in dataframes:
            file = os.path.join(folder, f"profile_copy_{i:04d}.csv")
            i += 1
            rich.print(f"timeseries CSV {i:04d} of {len(dataframes)}")
            self.format_profile_csv(dataframe, column_mapper, file)
            files.append(file)
        return files

    def enable_all_triggers(self):
        """
        Disables/Enables triggers and indexes to speed up the data ingestion into the database
        :return:
        """
        rich.print("Enable observation triggers...")
        self.enable_observations_triggers()
        rich.print("Create observation constrains...", end="")
        t = time.time()
        self.create_observations_constrains()
        rich.print(f"took {time.time() - t} seconds")
        # rich.print("Creating indexes...")
        # pinit = time()
        # self.create_observations_index()
        # rich.print("Create indexes took %.02f seconds" % (time() - pinit))

    def disable_all_triggers(self):
        rich.print("disabling database triggers...")
        self.disable_observations_triggers()
        rich.print("Dropping observation constrains...")
        self.drop_observations_constrains()

    def inject_to_timeseries(self, df, datastreams, max_rows=100000, disable_triggers=False, tmp_folder="/tmp/sta_db_copy/data"):
        """
        Inject all data in df into the timeseries table via SQL copy
        """

        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows / len(datastreams))
        dataframes = slice_dataframes(df, max_rows=rows)
        files = self.dataframes_to_timeseries_csv(dataframes, datastreams, tmp_folder)
        rich.print("Generating all files took %0.02f seconds" % (time.time() - init))

        if disable_triggers:
            self.disable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to timeseries hypertable...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "timeseries")
                progress.advance(task1, advance=1)

        if disable_triggers:
            self.enable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        rich.print("[magenta]Inserting all via SQL COPY took %.02f seconds" % (time.time() - init))

    def inject_to_profiles(self, df, datastreams, max_rows=100000, disable_triggers=False, tmp_folder="/tmp/sta_db_copy/data"):
        """
        Inject all data in df into the timeseries table via SQL copy
        """

        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows / len(datastreams))
        dataframes = slice_dataframes(df, max_rows=rows)
        files = self.dataframes_to_profile_csv(dataframes, datastreams, tmp_folder)
        rich.print("Generating all files took %0.02f seconds" % (time.time() - init))

        if disable_triggers:
            self.disable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to timeseries hypertable...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "profiles")
                progress.advance(task1, advance=1)

        if disable_triggers:
            self.enable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        rich.print("[magenta]Inserting all via SQL COPY took %.02f seconds" % (time.time() - init))

    def inject_to_observations(self, df: pd.DataFrame, datastreams: dict, url: str, foi_id: int, avg_period: str,
                               max_rows=10000, disable_triggers=False, tmp_folder="/tmp/sta_db_copy/data",
                               profile=False):
        """
        Injects all data in a dataframe using SQL copy.
        """
        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())
        rich.print("POSTing the first row to get the Feature ID")

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows / len(datastreams))
        dataframes = slice_dataframes(df, max_rows=rows)

        files = self.dataframes_to_observations_csv(dataframes, datastreams, tmp_folder, foi_id, avg_period=avg_period, profile=profile)
        rich.print(f"Generating all files took {time.time() - init:0.02f} seconds")

        if disable_triggers:
            self.disable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to OBSERVATIONS table...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "OBSERVATIONS")
                progress.advance(task1, advance=1)

        if disable_triggers:
            self.enable_all_triggers()

        rich.print("Forcing PostgreSQL to update Observation ID...")
        self.exec_query("select setval('\"OBSERVATIONS_ID_seq\"', (select max(\"ID\") from \"OBSERVATIONS\") );")

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)


if __name__ == "__main__":
    pass
