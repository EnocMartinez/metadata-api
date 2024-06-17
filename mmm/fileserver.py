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
import socket
from .common import run_subprocess, LoggerSuperclass, BLU, run_over_ssh


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

        if self.basepath.startswith("./"):
            self.basepath = self.basepath[2:]

        if self.basepath[-1] != "/":
            self.basepath += "/"

        self.host = conf["host"]

        self.path_alias = []  # Links to the real path
        if "path_links" in conf.keys():
            self.path_links = conf["path_links"]
        else:
            self.path_links = []

        try:
            socket.gethostbyname(self.host)
        except socket.gaierror:
            raise ValueError(f"Host {self.host} could not be resolved")

    def path2url(self, path: str):
        assert type(path) is str, "expected string"

        if path.startswith("./"):
            path = path[2:]

        for link in self.path_links:
            # If there is a softlink to the path, replace with the real path
            if link in path:
                path = path.replace(link, self.basepath)
        if self.basepath not in path:
            raise ValueError(f"basepath {self.basepath} not found in path:'{path}'")

        url = path.replace(self.basepath, self.baseurl)

        # make sure we don't have double / like http://my.url/some//path ->   http://my.url/some/path
        protocol, route = url.split("://")
        url = protocol + "://" + route.replace("//", "/")
        return url

    def url2path(self, url: str):
        assert type(url) is str, "expected string"
        assert url.startswith("http://") or url.startswith("https://"), f"URL not valid '{url}'"
        assert url.startswith(self.baseurl), f"URL {url} does not start with baseurl: {self.baseurl}"
        return url.replace(self.baseurl, self.basepath)

    def send_file(self, path: str, file: str, dry_run=False, indexed=True):
        """
        Sends a file to the FileServer
        :param path: path to deliver the file
        :param file: filename
        :param http_indexed: If True, the file should be http indexed, which means that should be a the base path
        :returns: URL of the files
        """
        assert type(path) is str, "path must be a string!"
        assert type(file) is str, "file must be a string!"
        if not dry_run:
            assert os.path.exists(file), "file does not exist!"

        if file.startswith("./"):
            file = file[2:]
        if path.startswith("./"):
            path = path[2:]

        # If we are in the 'host' machine, simply copy it
        if is_absolute_path(path):
            dest_file = os.path.join(path, os.path.basename(file))

        else:
            if path.startswith(self.basepath):
                dest_file = os.path.join(path, os.path.basename(file))
            else:
                dest_file = os.path.join(self.basepath, path, os.path.basename(file))

        if not dry_run:
            if os.uname().nodename == self.host or self.host == "localhost":
                self.info(f"Local copy from {file} to {dest_file}")
                os.makedirs(os.path.dirname(dest_file), exist_ok=True)
                shutil.copy2(file, dest_file)
            else:
                # Creating folder (just in case)
                run_over_ssh(self.host, f"mkdir -p {path}", fail_exit=True)
                # Run rsync process
                run_subprocess(["rsync", file, f"{self.host}:{dest_file}"])
                self.info(f"rsync from {file} to {self.host}:{dest_file}")
        else:
            # Dry run, do nothing
            pass
        if indexed:
            return self.path2url(dest_file)
        return dest_file

    def recv_file(self, remote_file: str, folder: str):
        """
        Get a file from the fileserver
        :param remote_file: remote file (full path)
        :param folder: local folder where to store
        :returns: local filename
        """
        assert type(remote_file) is str, "path must be a string!"
        assert type(folder) is str, "file must be a string!"

        local_file = os.path.join(folder, os.path.basename(remote_file))
        if os.uname().nodename == self.host or self.host == "localhost":
            self.info(f"Local copy from {remote_file} to {folder}")
            os.makedirs(folder, exist_ok=True)
            shutil.copy2(remote_file, local_file)
        else:
            # Run rsync process
            run_subprocess(["rsync", f"{self.host}:{remote_file}", local_file])
            self.info(f"rsync from {self.host}:{remote_file} to {local_file}")

        return local_file
