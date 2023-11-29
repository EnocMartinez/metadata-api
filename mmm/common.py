#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 21/9/23
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler
import rich
import subprocess

# Color codes
GRN = "\x1B[32m"
RST = "\033[0m"
BLU = "\x1B[34m"
YEL = "\x1B[33m"
RED = "\x1B[31m"
MAG = "\x1B[35m"
CYN = "\x1B[36m"
WHT = "\x1B[37m"
NRM = "\x1B[0m"
PRL = "\033[95m"
RST = "\033[0m"


colors = [GRN, RST, BLU, YEL, RED, MAG, CYN, WHT, NRM, PRL, RST]

def setup_log(name, path="log", log_level="debug"):
    """
    Setups the logging module
    :param name: log name (.log will be appended)
    :param path: where the logs will be stored
    :param log_level: log level as string, it can be "debug, "info", "warning" and "error"
    """

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Check arguments
    if len(name) < 1 or len(path) < 1:
        raise ValueError("name \"%s\" not valid", name)
    elif len(path) < 1:
        raise ValueError("name \"%s\" not valid", name)

    # Convert to logging level
    if log_level == 'debug':
        level = logging.DEBUG
    elif log_level == 'info':
        level = logging.INFO
    elif log_level == 'warning':
        level = logging.WARNING
    elif log_level == 'error':
        level = logging.ERROR
    else:
        raise ValueError("log level \"%s\" not valid" % log_level)

    if not os.path.exists(path):
        os.makedirs(path)

    filename = os.path.join(path, name)
    if not filename.endswith(".log"):
        filename += ".log"
    print("Creating log", filename)
    print("name", name)

    logger = logging.getLogger()
    logger.setLevel(level)
    log_formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)-7s: %(message)s',
                                      datefmt='%Y/%m/%d %H:%M:%S')
    handler = TimedRotatingFileHandler(filename, when="midnight", interval=1, backupCount=7)
    handler.setFormatter(log_formatter)
    logger.addHandler(handler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(log_formatter)
    logger.addHandler(consoleHandler)

    logger.info("")
    logger.info(f"===== {name} =====")

    return logger


def file_list(dir_name) -> list:
    """ create a list of file and sub directories names in the given directory"""
    list_of_files = os.listdir(dir_name)
    all_files = list()
    for entry in list_of_files:
        full_path = os.path.join(dir_name, entry)
        if os.path.isdir(full_path):
            all_files = all_files + file_list(full_path)
        else:
            all_files.append(full_path)
    return all_files


class LoggerSuperclass:
    def __init__(self, logger: logging.Logger, name: str, colour=NRM):
        """
        SuperClass that defines logging as class methods adding a heading name
        """
        self.__logger_name = name
        self.__logger = logger
        if not logger:
            self.__logger = logging  # if not assign the generic module
        self.__log_colour = colour

    def warning(self, *args):
        mystr = YEL + "[%s] " % self.__logger_name + str(*args) + RST
        self.__logger.warning(mystr)

    def error(self, *args, exception=False):
        mystr = "[%s] " % self.__logger_name + str(*args)
        self.__logger.error(RED + mystr + RST)
        if exception:
            raise ValueError(mystr)

    def debug(self, *args):
        mystr = self.__log_colour + "[%s] " % self.__logger_name + str(*args) + RST
        self.__logger.debug(mystr)

    def info(self, *args):
        mystr = self.__log_colour + "[%s] " % self.__logger_name + str(*args) + RST
        self.__logger.info(mystr)


def reverse_dictionary(data):
    """
    Takes a dictionary and reverses key-value pairs
    :param data: any dict
    :return: reversed dictionary
    """
    return {value: key for key, value in data.items()}


def normalize_string(instring, lower_case=False):
    """
    This function takes a string and normalizes by replacing forbidden chars by underscores.The following chars
    will be replaced: : @ $ % & / + , ; and whitespace
    :param instring: input string
    :return: normalized string
    """
    forbidden_chars = [":", "@", "$", "%", "&", "/", "+", ",", ";", " ", "-"]
    outstring = instring
    for char in forbidden_chars:
        outstring = outstring.replace(char, "_")
    if lower_case:
        outstring = outstring.lower()
    return outstring

def dataframe_to_dict(df, key, value):
    """
    Takes two columns of a dataframe and converts it to a dictionary
    :param df: input dataframe
    :param key: column name that will be the key
    :param value: column name that will be the value
    :return: dict
    """

    keys = df[key]
    values = df[value]
    d = {}
    for i in range(len(keys)):
        d[keys[i]] = values[i]
    return d


def reverse_dictionary(data):
    """
    Takes a dictionary and reverses key-value pairs
    :param data: any dict
    :return: reversed dictionary
    """
    return {value: key for key, value in data.items()}


def run_subprocess(cmd: str, fail_exit=False):
    """
    Runs a command as a subprocess. If the process retunrs 0 returns True. Otherwise prints stderr and stdout and returns False
    :param cmd: command
    :return: True/False
    """
    if type(cmd) == list:
        cmd_list = cmd
    else:
        cmd_list = cmd.split(" ")
    proc = subprocess.run(cmd_list, capture_output=True)
    if proc.returncode != 0:
        rich.print(f"\n[red]ERROR while running command '{cmd}'")
        if proc.stdout:
            rich.print(f"subprocess stdout:")
            rich.print(f">[bright_black]    {proc.stdout.decode()}")
        if proc.stderr:
            rich.print(f"subprocess stderr:")
            rich.print(f">[bright_black] {proc.stderr.decode()}")

        if fail_exit:
            exit(1)

        return False
    return True
