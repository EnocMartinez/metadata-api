#!/usr/bin/env python3
"""
Simple structure that stores dataset information

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 12/6/24
"""
import jsonschema
import pandas as pd
import os
from .schemas import dataset_exporter_conf
from .fileserver import FileServer


class DatasetObject:
    def __init__(self, conf, filename, tstart: pd.Timestamp | str, tend: pd.Timestamp | str, fmt: str):
        """
        Object that contains metadata about a dataset
        """
        assert type(conf) is dict
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
        self.exporters = {}

        # Store the configuration for all export services, we don't know yet to which service the data object
        # will be delivered.
        for key, config in conf["export"].items():
            self.exporters[key] = DataExporter(config)

        self.delivered = False  # will be set to True once the data object has been sent

    def tstart_str(self, fmt="%Y-%m-%dT%H:%M:%SZ"):
        return self.tstart.strftime(fmt)

    def tend_str(self, fmt="%Y-%m-%dT%H:%M:%SZ"):
        return self.tend.strftime(fmt)

    def deliver(self, service: str, fileserver: FileServer):
        """
        Deliver dataset to a service.
        """
        assert service in self.exporters.keys(), f"Service {service} not configured for dataset {self.dataset_id}"
        assert type(fileserver) is FileServer

        if self.delivered:
            raise ValueError("Dataset already delivered!")

        exporter = self.exporters[service]
        self.url = exporter.deliver_dataset(self.filename, self.tstart, fileserver)
        self.delivered = True
        return self.url

    def __repr__(self):
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
        self.file_tree_level = conf["fileTreeLevel"]

    def deliver_dataset(self, filename, timestamp: pd.Timestamp, fileserver: FileServer, url_required=True)->str:
        """
        Takes a dataset (already generated) and delivers it according to the dataset's configuration
        :param filename: dataset to deliver
        :param timestamp: timestamp used to generate folders
        """

        # TODO: This only works if FileServer and ERDDAP are on the same VM
        assert fileserver.host == self.host, "DataExporter and FileServer have different hosts, not implemented"
        assert type(filename) is str
        assert type(timestamp) is pd.Timestamp
        assert type(fileserver) is FileServer
        assert os.path.isfile(filename), f"Not a file: '{filename}"
        # First, construct the path
        path = self.path  # start with base path

        # Now, let's check the file tree level
        if self.file_tree_level == "daily":
            tree = timestamp.strftime("%Y/%m/%d")
        elif self.file_tree_level == "monthly":
            tree = timestamp.strftime("%Y/%m")
        elif self.file_tree_level == "yearly":
            tree = timestamp.strftime("%Y")
        elif self.file_tree_level == "none":
            tree = ""
        else:
            raise ValueError("This should never happen, schema not honored!")
        path = os.path.join(path, tree)

        result = fileserver.send_file(path, filename, indexed=url_required)
        return result