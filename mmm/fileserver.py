#!/usr/bin/env python3
"""
FileServer implementation, delivers files to the remote fileserver, where files are exposed via HTTP. Contains methods
to convert from paths to urls and vice-versa.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 23/3/21
"""

import os
import shutil
import rich
from .common import run_subprocess, LoggerSuperclass, BLU


def is_absolute_path(path):
    if path.startswith("/"):
        return True
    return False


class FileServer(LoggerSuperclass):
    def __init__(self, conf: dict, log):
        """
        Simple and stupid class that converts paths to urls and urls to paths. It assumes that an NGINX service
        with an auto-indexed folder is already set up.

        :param basepath: root path of the fileserver
        :param baseurl: root url of the fileserver
        :param host: hostname of the fileserver
        """
        for key in ["host", "basepath", "baseurl"]:
            assert key in conf.keys(), f"expected {key} in configuration"
        LoggerSuperclass.__init__(self, log, "FileSrv", colour=BLU)
        self.basepath = conf["basepath"]
        self.baseurl = conf["baseurl"]

        if self.baseurl[-1] != "/":
            self.baseurl += "/"

        if self.basepath[:2] is "./":
            self.basepath[2:]

        self.host = conf["host"]

        self.path_alias = []  # Links to the real path
        if "path_links" in conf.keys():
            self.path_links = conf["path_links"]
        else:
            self.path_links = []

        self.info("==== FileServer Initialized ====")
        self.info(f"  >> base path: {self.basepath}")
        self.info(f"  >>  base url: {self.baseurl}")
        self.info(f"  >>      host: {self.host}")

    def path2url(self, path: str):
        assert type(path) is str, "expected string"

        for link in self.path_links:
            # If there is a softlink to the path, replace with the real path
            if link in path:
                path = path.replace(link, self.basepath)
        if self.basepath not in path:
            raise ValueError(f"basepath {self.basepath} not found in path:'{path}'")
        return path.replace(self.basepath, self.baseurl)

    def url2path(self, url: str):
        assert type(url) is str, "expected string"
        assert url.startswith("http://") or url.startswith("https://"), f"URL not valid '{url}'"
        return url.replace(self.baseurl, self.basepath)

    def send_file(self, path: str, file: str):
        """
        Sends a file to the FileServer
        :param path: path to deliver the file
        :param file: filename
        :returns: URL of the files
        """
        assert type(path) is str, "path must be a string!"
        assert type(file) is str, "file must be a string!"
        assert os.path.exists(file), "file does not exist!"

        if not os.path.exists(file):
            raise ValueError(f"file {file} does not exist!")

        # If we are in the 'host' machine, simply copy it
        if is_absolute_path(path):
            dest_file = os.path.join(path, os.path.basename(file))
        else:
            dest_file = os.path.join(self.basepath, path, os.path.basename(file))

        if os.uname().nodename == self.host or self.host == "localhost":
            self.info(f"Local copy from {file} to {dest_file}")
            os.makedirs(os.path.dirname(dest_file), exist_ok=True)
            shutil.copy2(file, dest_file)
        else:
            # Creating folder (just in case)
            run_subprocess(["ssh", self.host, f"mkdir -p {path}"])
            # Run rsync process
            run_subprocess(["rsync", file, f"{self.host}:{dest_file}"])
            self.info(f"rsync from {file} to {self.host}:{dest_file}")

        return self.path2url(dest_file)
