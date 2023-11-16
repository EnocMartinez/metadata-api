#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""


from ..common import LoggerSuperclass, PRL
import pandas as pd


class TimescaleDB(LoggerSuperclass):
    def __init__(self, sta, hypertable_name: str, logger):
        LoggerSuperclass.__init__(self, logger, "TSDB", colour=PRL)
        self.hypertable = hypertable_name  # name of the hypertable with raw data
        self.sta = sta

    def get_raw_data(self, identifier: str, time_start: str, time_end: str)->pd.DataFrame:
        """
        Access the raw data table and exports all data between time_start and time_end
        :param identifier: datasream name (str) or datastream id (int)
        :param time_start: start time
        :param time_end: end time (not included)
        """
        if type(identifier) == int:
            pass
        elif type(identifier) == str:  # if string, convert from name to ID
            identifier = self.sta.datastream_name_id[identifier]
        query = f"select timestamp, value, qc_flag from {self.hypertable} where datastream_id = {identifier} and " \
                f"timestamp >= '{time_start}' and timestamp <'{time_end}' order by timestamp asc"
        df = self.sta.dataframe_from_query(query)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.set_index("timestamp")




