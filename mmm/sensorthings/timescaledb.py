#!/usr/bin/env python3
"""
This file implements functions and classes to interact with the SensorThings Database directly.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 23/3/21
"""

import rich


class TimescaleDb:
    def __init__(self, sta_db_connector, timeseries_table="timeseries", profiles_table="profiles"):
        """
        Creates a TimescaleDB object, which adds timeseries capabilities to the SensorThings Database.
        Two hypertables are created:
            timeseries: regular timeseries data (timestamp, value, qc, datastream_id)
            profiles: depth-specific data (timestamp, depth, value, qc, datastream_id)
        """
        print("init SensorThingsDbConnector")

        self.db = sta_db_connector
        self.timeseries_hypertable = timeseries_table
        self.profiles_hypertable = profiles_table

        default_interval = "30days"

        if not self.db.check_if_table_exists(self.timeseries_hypertable):
            rich.print(f"[magenta]TimescaleDB, initializing {self.timeseries_hypertable} as a hypertable")
            self.create_timeseries_hypertable(timeseries_table, chunk_interval_time=default_interval)
            self.add_compression_policy(timeseries_table, policy="30d")

        if not self.db.check_if_table_exists(profiles_table):
            rich.print(f"[magenta]TimescaleDB, initializing {profiles_table} as a hypertable")
            self.create_profiles_hypertable(profiles_table, chunk_interval_time=default_interval)
            self.add_compression_policy(profiles_table, policy="30d")

    def create_timeseries_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a table with four parameters, the timestamp, the value, the qc_flag and aa datastream_id as foreing key
        :return:
        """
        if self.db.check_if_table_exists(name):
            rich.print("[yellow]WARNING: table already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION NOT NULL,   
        qc_flag smallint,
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        rich.print("creating table...")
        self.db.exec_query(query)
        rich.print("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def create_profiles_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a table with four parameters, the timestamp, the value, the qc_flag and aa datastream_id as foreing key
        :return:
        """
        if self.db.check_if_table_exists(name):
            rich.print("[yellow]WARNING: table already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        depth smallint,
        value DOUBLE PRECISION NOT NULL,   
        qc_flag INT8,
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, depth, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        rich.print("creating table...")
        self.db.exec_query(query)
        rich.print("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def stats(self, table, hypertable=False):
        print("\n\n===== %s ====" % table)
        query = "SELECT COUNT(timestamp) from %s;" % table
        print(self.dataframe_from_query(query))
        if not hypertable:
            query = "SELECT pg_size_pretty(pg_relation_size('%s'));" % table
            print(self.dataframe_from_query(query))
        else:
            query = "SELECT pg_size_pretty(hypertable_size('%s'));" % table
            print(self.dataframe_from_query(query))
            query = "SELECT total_chunks, pg_size_pretty(before_compression_total_bytes) as before, " \
                    "pg_size_pretty(after_compression_total_bytes) " \
                    "as after FROM hypertable_compression_stats('%s');" % table
            print(self.dataframe_from_query(query))

        print("\n\n")

    def add_compression_policy(self, table_name, policy="30d"):
        """
        Adds compression policy to a hypertable
        """

        query = f"ALTER TABLE {table_name} SET (timescaledb.compress, timescaledb.compress_orderby = " \
                "'timestamp DESC', timescaledb.compress_segmentby = 'datastream_id');"
        self.db.exec_query(query)
        query = f"SELECT add_compression_policy('{table_name}', INTERVAL '{policy}'); "
        self.db.exec_query(query)

    def compress_all(self, table_name, older_than="30days"):
        query = f"""
            SELECT compress_chunk(i, if_not_compressed => true) from 
            show_chunks(
            '{table_name}',
             now() - interval '{older_than}',
              now() - interval '100 years') i;
              """
        self.db.exec_query(query)

    def compression_stats(self, table) -> (float, float, float):
        """
        Returns compression stats
        :param table: hypertable name
        :returns: tuple like (MBytes before, MBytes after, compression ratio)
        """
        df = self.db.dataframe_from_query(f"SELECT * FROM hypertable_compression_stats('{table}');")
        bytes_before = df["before_compression_total_bytes"].values[0]
        bytes_after = df["after_compression_total_bytes"].values[0]
        if type(bytes_after) is type(None):
            return 0, 0, 0
        else:
            ratio = bytes_before/bytes_after
            return round(bytes_before/1e6,2), round(bytes_after/1e6,2), round(ratio, 2)


if __name__ == "__main__":
    pass
