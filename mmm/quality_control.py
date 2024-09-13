"""
This file provides a user-friendly interface for actions such as data curator, apply qc, resample dataset and more

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
created: 30/07/2024
"""

import time

import pandas as pd
import rich
import json
from mmm.data_manipulation import slice_dataframes, merge_dataframes
from mmm.parallelism import multiprocess
import gc
import matplotlib.pyplot as plt
import os
import numpy as np
from ioos_qc.config import QcConfig
from ioos_qc.results import collect_results
from mmm import MetadataCollector
from mmm.common import assert_type, assert_types

qc_flags = {
    "good": 1,
    "not_applied": 2,
    "suspicious": 3,
    "bad": 4,
    "missing": 9
}

__qc_colors = {
    "good": "green",
    "not_applied": "grey",
    "suspicious": "yellow",
    "bad": "red",
    "missing": "black"
}
__qc_sizes = {
    "good": 0.5,
    "not_applied": 1,
    "suspicious": 4,
    "bad": 4,
    "missing": 4
}


def open_qc_conf_file(file):
    """
    Opens a QC cnofiguration file (JSON) and processes it
    :param file:
    :return: dict with QC config
    """
    rich.print(f"[green]Opening QC conf file {file}")
    with open(file) as f:
        qc = json.load(f)

    for key, value in qc.items():
        if "same_as" in value.keys():
            rich.print(f"Using for {key} same QC conf as {value['same_as']}...")
            qc[key] = qc[value["same_as"]]

    return qc


def save_figure(fig, varname, test_name, df, folder="qc_output"):
    """
    Saves a figure
    Args:
        fig:
        varname:
        test_name:
        df:
        folder:

    Returns:

    """
    # dataframes should be sliced by month, so create a filename with YYYY-MM
    path = os.path.join(folder, varname, test_name)
    os.makedirs(path, exist_ok=True)
    date = np.datetime_as_string(df.index.values[0], unit="M")
    filename = varname + "_" + test_name + "_" + date + ".png"
    filename = os.path.join(path, filename)
    plt.legend(loc="lower right")
    fig.savefig(filename)


def show_qc_results(qc_results, varname, df, save="", limits={}, tests=[]):
    """
    Prepares plots to show the results. Crates some figures with matplotlib
    :param qc_results: structure with QC results
    :param varname: variable name
    :param df: dataframe with data
    :param save: flag to determine if the plot should be saved to a file
    :param limits: if set, set the y limits for the variable e.g. {"temperature": [10,40]}
    :return:
    """
    df_var = df.copy(deep=True)
    figures = []
    for var in df_var.columns:
        if var != varname:
            del df_var[var]
    for test_name in qc_results["qartod"].keys():
        df_var[test_name] = qc_results["qartod"][test_name].astype(np.int8)
        if test_name not in tests and "all" not in tests:
            continue
        fig, ax = plt.subplots(1, 1, figsize=(20, 10), sharex=False)
        figures.append(fig)
        for flag in qc_flags.keys():
            df_flag = df_var[df_var[test_name] == qc_flags[flag]]
            flag_count = len(df_flag.index.values)
            if flag_count <= 0:
                continue
            percent = 100 * flag_count / len(df_var.index.values)
            label = flag + " (%.02f%%, %d points)" % (percent, len(df_flag.index.values))

            ax.scatter(x=df_flag.index.values, y=df_flag[varname].values, color=__qc_colors[flag], marker='o',
                       s=__qc_sizes[flag], label=label)

        if varname in limits.keys():
            ax.set_ylim(limits[varname])
        ax.grid()
        ax.legend()
        ax.set_title(test_name)
        fig.canvas.manager.set_window_title(varname + "_" + test_name +
                                            "_" + df_var[test_name].index.values[0].astype(str)[:10])
        if save:
            save_figure(fig, varname, test_name, df_var, folder=save)
            plt.close(fig)

    if "good_data" in tests:
        # Plot with Good Data only
        fig, ax = plt.subplots(1, 1, figsize=(20, 10), sharex=False)
        fig.canvas.manager.set_window_title(varname + "_" + "good_data" +
                                            "_" + df_var[test_name].index.values[0].astype(str)[:10])
        figures.append(fig)
        df_good = df_var[df_var["aggregate"] == qc_flags["good"]]
        ax.scatter(x=df_good.index.values, y=df_good[varname].values, color="green", marker='o', s=0.5,
                   label=varname + " good data only")
        if varname in limits.keys():
            ax.set_ylim(limits[varname])

        ax.legend()
        ax.grid()

        if save:
            save_figure(fig, varname, "good_data", df_var, folder=save)
            # close saved figures and free space
            for fig in figures:
                plt.close(fig)
                gc.collect()  # collect with garbage collector

        del df_var
        gc.collect()  # collect with garbage collector


def dataframe_qc(df, config, varname, aggregate_qc=True, qc_suffix="_QC", show=False, save=True, limits={}, tests=[]):
    """
    Applies Quality Control to the dataframe according to the configuration.
    :param df: input dataframe
    :param config: configuration file for qartod QC tests
    :param varname: column name to apply QC
    :param aggregate_qc: if True only a column with the aggreggated qc result, otherwise generate a column per test
    :param progress: Progress() object from rich library to generate nice bars
    :return: dataframe with an additional qc colums
    """
    qc_config = {"qartod": config["qartod"]}  # keep only the qartod args

    # Run QC
    qc = QcConfig(qc_config)

    var_df = df[varname]
    ones = np.ones(len(var_df))

    qc_results = qc.run(
        inp=var_df.values,
        tinp=var_df.index.values,
        zinp=ones
    )

    # Converting ndarray to mask array
    results = []
    for name, values in qc_results["qartod"].items():
        results.append(values)

    # Store aggregate results (final qc flags) in the same structure
    qc_results["qartod"]["aggregate"] = np.ma.maximum.reduce(results, axis=0)

    if show or save:
        show_qc_results(qc_results, varname, df, save=save, limits=limits, tests=tests)

    if aggregate_qc:
        df[varname + qc_suffix] = qc_results["qartod"]["aggregate"].astype(np.int8)
    else:
        for test in qc_results["qartod"].keys():
            df[test] = qc_results["qartod"][test].astype(np.int8)

    return df


def apply_qc(df, qc_config: dict, show=False, save="", var_list=[], limits={}, tests=[]):
    """
    Apply quality control to a dataframe
    :return: dataframe with QC flags
    """
    qc_applied = {}
    for varname in df.columns:
        qc_applied[varname] = False

    qc_elements = {}  # list of variables and their assigned QC config
    for varname in df.columns:
        if varname in qc_config.keys():
            # Direct assignation varname TEMP qc config TEMP:
            qc_elements[varname] = qc_config[varname]
        else:
            # Indirect assignation by prefix CSPD_19m -> uses qc config CSPD
            for obs_prop_name in qc_config.keys():
                if varname.startswith(obs_prop_name):
                    qc_elements[varname] = qc_config[obs_prop_name]

    for variable_name, variable_config in qc_elements.items():
        show_var = False
        save_var = False
        if variable_name in var_list:
            if show:
                show_var = True
            if save:
                save_var = save

        df = dataframe_qc(df, variable_config, varname=variable_name, show=show_var, save=save_var, limits=limits,
                          tests=tests)
        qc_applied[variable_name] = True

    for variable, applied in qc_applied.items():
        if not qc_applied[variable]:
            df[variable + "_QC"] = 2

    if show:
        plt.show()
    gc.collect()
    return df


def dataset_qc(mc: MetadataCollector, sensor: str | dict, df: pd.DataFrame, show=False, save="", varlist=[], tests=[],
               limits: dict = {}, paralell=False):
    init = time.time()
    assert_type(mc, MetadataCollector)
    assert_types(sensor, [str, dict])
    assert_type(df, pd.DataFrame)

    if isinstance(sensor, dict):
        sensor_id = sensor["#id"]
    else:
        sensor_id = sensor
        sensor = mc.get_document("sensors", sensor_id)
    qc_config = {}
    for var in sensor["variables"]:
        varname = var["@variables"]

        if "@qualityControl" in var.keys():
            # get QualityControl configuration for variable
            qc_id = var["@qualityControl"]
            c = mc.get_document("qualityControl", qc_id)
            c = mc.strip_metadata_fields(c)
            qc_config[varname] = c
        else:
            rich.print(f"[yellow]No QC found for '{varname}'!")
            pass

    if "all" in varlist or not varlist:  # if empty or explicitly all
        varlist = list(df.columns)

    rich.print(f"[magenta]show={show} save={save}")

    if paralell:
        rich.print("[cyan]Applying Quality Control (multiprocessing)...")
        dataframes = slice_dataframes(df, frequency="M")  # generate a dataframe for each month
        argument_list = []
        for df in dataframes:
            argument_list.append((df, qc_config, show, save, varlist, limits, tests))

        t = time.time()
        qc_dataframes = multiprocess(argument_list, apply_qc, max_workers=20)
        rich.print("Multiprocess QC %.02f s" % (time.time() - t))
        t = time.time()
        df = merge_dataframes(qc_dataframes, sort=True)
        rich.print("Merge dataframes %.02f s" % (time.time() - t))
        gc.collect()
    else:
        rich.print("[cyan]Applying Quality Control (single process)...")
        apply_qc(df, qc_config, show, save, varlist, limits, tests)

    rich.print("[cyan]QC took %.02f seconds" % (time.time() - init))
    return df
