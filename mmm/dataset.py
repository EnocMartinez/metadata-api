#!/usr/bin/env python3
"""
Simple structure that stores dataset information

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 12/6/24
"""
import logging

import jsonschema
import pandas as pd
import os
from .schemas import dataset_exporter_conf, mmm_schemas
from .fileserver import FileServer, send_file
from emso_metadata_harmonizer import erddap_config
import time
from mmm.common import validate_schema


class DatasetObject:
    def __init__(self, conf: dict, filename: str, service_name: str, tstart: pd.Timestamp | str, tend: pd.Timestamp | str,
                 fmt: str):
        """
        This object contains all the metadata related to a dataset (or data file) and provides methods to deliver,
        update it.

        :param conf: Dataset configuration dict, must be compliant with dataset schema
        :param filename: file that contains the actual data
        :param service_name: the name of the service used to export the data. This service must be listed in conf
        :param tstart: time start
        :param tend: time end
        :param fmt: export format (by default format in conf)
        """
        assert type(conf) is dict
        validate_schema(conf, mmm_schemas["datasets"], [])

        # Convert strings
        if type(tstart) is str:
            tstart = pd.Timestamp(tstart)
        if type(tend) is str:
            tend = pd.Timestamp(tend)

        assert type(tstart) is pd.Timestamp
        assert type(tend) is pd.Timestamp

        self.filename = filename
        self.conf = conf
        self.dataset_id = conf["#id"]
        self.fmt = fmt
        self.tstart = tstart
        self.tend = tend
        self.url = ""

        self.ctime = pd.Timestamp(os.path.getctime(filename))
        self.size = os.path.getsize(filename)

        tfmt = "%Y-%m-%d"
        basename = os.path.basename(filename)
        self.data_object_id = basename + "_" + self.tstart_str(tfmt) + "_" + self.tstart_str(tfmt) + "_" + fmt
        self.service_name = service_name

        # Store the configuration for all export services, we don't know yet to which service the data object
        # will be delivered.
        config = conf["export"][service_name]
        self.exporter = DataExporter(config)

        self.delivered = False  # will be set to True once the data object has been sent

        self.erddap_configured = False
        self.erddap_dataset_id = ""

    def tstart_str(self, fmt="%Y-%m-%dT%H:%M:%SZ"):
        return self.tstart.strftime(fmt)

    def tend_str(self, fmt="%Y-%m-%dT%H:%M:%SZ"):
        return self.tend.strftime(fmt)

    def deliver(self, fileserver: FileServer = None):
        """
        Delivers a dataset to the export service as configured in __init__
        :param fileserver: FileServer to convert from filesystem tu public HTTP URL. If no URL is needed, leave it blank
        :return: URL (if fileserver is passed) or filesystem path
        """
        if fileserver:
            assert type(fileserver) is FileServer, f"expected type FileServer, got {type(fileserver)}"

        if self.delivered:
            raise ValueError("Dataset already delivered!")

        exporter = self.exporter
        self.url = exporter.deliver_dataset(self.filename, self.tstart, fileserver=fileserver)
        self.delivered = True
        return self.url

    def configure_erddap(self, datasets_xml, dataset_path):
        """
        Configure an ERDDAP dataset based on the dataset configuration and dataset NetCDF file. This only works for
        EMSO-compliant NetCDF files
        :param datasets_xml: path to the ERDDAPs datasets.xml file
        :param dataset_path: Path where the datasets will be stored. May differ from filesystem path since erddap is
                             dockerized.
        """
        if self.fmt != "netcdf":
            raise ValueError(f"Unimplemented ERDDAP configuration for dataset with type '{self.fmt}'")

        if self.service_name != "erddap":
            raise ValueError(f"ERDDAP exporter not configured for this dataset!")

        try:
            self.erddap_dataset_id = self.conf["identifier"]
        except KeyError:
            # If not set, use generic dataset_id
            self.erddap_dataset_id = self.dataset_id

        # configure erddap using the emso_metadata_harmonizer tool
        erddap_config(self.filename, self.erddap_dataset_id, dataset_path, datasets_xml_file=datasets_xml)
        self.erddap_configured = True

    def reload_erddap_dataset(self, big_parent_directory):
        """
        Creates a hardFlag to tell ERDDAP to reload a dataset ASAP
        :param big_parent_directory: ERDDAP's big parent directory
        :return: nothing
        """
        dataset_hard_flag = os.path.join(big_parent_directory, "hardFlag", self.dataset_id)
        with open(dataset_hard_flag, "w") as f:
            f.write("1")

        while os.path.exists(dataset_hard_flag):
            time.sleep(1)
            print("waiting for erddap to load the dataset...")

    def __repr__(self):
        """
        Return stats of the dataset object
        """
        string = ""
        string += f"=== DatasetObject {id(self)} ===\n"
        string += f"   DatasetID: {self.dataset_id}\n"
        string += f"       format: {self.fmt}\n"
        string += f"   time start: {self.tstart_str()}\n"
        string += f"     time end: {self.tend_str()}\n\n"
        string += f"-------- file --------\n"
        string += f"     filename: {self.filename}\n"
        string += f"         size: {self.size / (1024 * 1024):.02f} MB\n"
        string += f"          url: {self.url}\n"
        string += f"    delivered: {self.delivered}\n"
        string += f"-----------------------------------------"
        return string


class DataExporter:
    def __init__(self, conf):
        """
        Class to export datasets from a datasource and deliver them to the proper service
        """
        jsonschema.validate(conf, schema=dataset_exporter_conf)
        self.period = conf["period"]
        self.host = conf["host"]
        self.format = conf["format"]
        self.path = conf["path"]

    def deliver_dataset(self, filename, timestamp: pd.Timestamp, fileserver: FileServer, url_required=True)->str:
        """
        Takes a dataset (already generated) and delivers it according to the dataset's configuration
        :param filename: dataset to deliver
        :param timestamp: timestamp used to generate folders
        """
        # TODO: This only works if FileServer and ERDDAP are on the same VM
        if fileserver:
            assert type(fileserver) is FileServer, "Expected FileServer object"
            assert fileserver.host == self.host, "DataExporter and FileServer have different hosts, not implemented"

        assert type(filename) is str
        assert type(timestamp) is pd.Timestamp

        assert os.path.isfile(filename), f"Not a file: '{filename}"
        # First, construct the path
        path = self.path  # start with base path
        path = self.generate_path(path, self.period, timestamp)
        if fileserver:
            result = fileserver.send_file(path, filename, indexed=url_required)
        else:
            result = send_file(filename, path, self.host)
        return result

    @staticmethod
    def generate_path(path, period, timestamp):
        if not period:
            return path
        # Now, let's check the file tree level
        tree = ""
        if period == "daily":
            tree = timestamp.strftime("%Y/%m")
        elif period == "monthly":
            tree = timestamp.strftime("%Y")
        elif period == "yearly":
            pass
        elif period == "none":
            pass
        else:
            raise ValueError("This should never happen, schema not honored!")
        if tree:
            path = os.path.join(path, tree)
        return path