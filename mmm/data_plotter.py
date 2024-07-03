#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 9/5/24
"""
import os.path
from argparse import ArgumentParser
from matplotlib import pyplot as plt
import pandas as pd
from pandas.api.types import is_numeric_dtype
import rich
import yaml
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.shapereader as shpreader
from glob import glob



try:
    from mmm import init_metadata_collector
except ModuleNotFoundError:
    from metadata_collector import init_metadata_collector


qc_flags = {
    "good": 1,
    "not_applied": 2,
    "suspicious": 3,
    "bad": 4,
    "missing": 9
}

__qc_sizes = {
    "good": 0.5,
    "not_applied": 1,
    "suspicious": 4,
    "bad": 4,
    "missing": 4
}

__qc_colors = {
    "good": "green",
    "not_applied": "royalblue",
    "suspicious": "orange",
    "bad": "red",
    "missing": "black"
}


def open_csv_data(fileneme):
    df = pd.read_csv(fileneme)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df.sort_index(ascending=True)
    return df


def open_netcdf_data(filename):
    import mooda as md
    wf = md.read_nc(filename)
    df = wf.data
    df = df.reset_index()
    if is_numeric_dtype(df["TIME"]):
        rich.print("Converting non-numeric type to epoch time")
        df['TIME'] = pd.to_datetime(df['TIME'], unit='s')

    df = df.rename(columns={"TIME": "timestamp"})
    df = df.set_index("timestamp")
    df = df.sort_index(ascending=True)

    longitude = 0.0
    latitude = 0.0
    if "LATITUDE" in df.columns:
        latitude = df["LATITUDE"].values[0]
    if "LONGITUDE" in df.columns:
        longitude = df["LONGITUDE"].values[0]

    for col in list(df.columns):
        if col.startswith("LATITUDE") or col.startswith("LONGITUDE") or col.startswith("DEPTH") or \
                not is_numeric_dtype(df[col]):
            del df[col]
    print(df)
    return df, latitude, longitude


def plot_variable(df, varname, ax, variable_name):
    df_var = df.copy(deep=True)

    for flag in qc_flags.keys():
        var_qc = varname + "_QC"
        rich.print(f"Plotting {varname} qc={flag}")
        df_flag = df_var[df_var[var_qc] == qc_flags[flag]]
        flag_count = len(df_flag.index.values)
        if flag_count <= 0:
            rich.print(f"skipping {varname} qc={flag}, only got {flag_count} points")
            continue

        percent = 100 * flag_count / len(df_var.index.values)
        label = flag + f" {percent:.01f} %%"
        ax.scatter(x=df_flag.index.values, y=df_flag[varname].values, color=__qc_colors[flag], marker='o',
                   s=__qc_sizes[flag], label=label)
    ax.set_title(variable_name.replace("_", " "))
    ax.tick_params(axis='x', labelrotation=45)  # Rotate x-axis labels for better readability
    ax.legend(loc='upper right', bbox_to_anchor=(1, 1))


def plot_timeseries(mc, dataset: dict, file: str):
    extension = args.file.split(".")[-1].lower()
    if extension == "csv":
        df = open_csv_data(args.file)
        latitude = None
        longitude = None
    elif extension == "nc":
        df, latitude, longitude = open_netcdf_data(args.file)
    else:
        rich.print(f"Format {extension} not understood!")
    cols = list(df.columns)
    rich.print(cols)
    # Keep variables until _
    variables = [c.split("_")[0] for c in cols]
    variables = np.unique(variables)
    variables = [c for c in variables if is_numeric_dtype(df[c])]

    variable_names = {}
    for var in variables:
        variable_names[var] = mc.get_document("variables", var)["standard_name"]

    # Number of plots
    num_plots = len(variables) + 1  # Change this to the number of plots you want

    # Calculate the number of rows and columns based on the number of plots
    num_cols = int(np.ceil(np.sqrt(num_plots)))
    num_rows = int(np.ceil(num_plots / num_cols))

    # Create a figure and axes
    fig, axs = plt.subplots(num_rows, num_cols, figsize=(12, 8))
    # Flatten the axes if the number of rows or columns is 1

    axs = axs.flatten()
    i = 0
    rich.print(axs)
    for var in variables:
        ax = axs[i]
        name = variable_names[var]
        plot_variable(df, var, ax, name)
        i += 1

    for j in range(i, len(axs)):
        axs[i].remove()

    plt.show()


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("dataset_id", type=str, help="dataset ID", default="")
    argparser.add_argument("file", type=str, help="Dataset File", default="")
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False,
                           default="secrets.yaml")
    args = argparser.parse_args()

    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    rich.print("init MetadataCollector")
    mc = init_metadata_collector(secrets)

    if not os.path.exists(args.file):
        rich.print(f"[red]Could not find file {args.file}")
        exit()

    dataset = mc.get_document("datasets", args.dataset_id)

    if dataset["dataType"] == "timeseries":
        # Create plot for timeseries
        plot_timeseries(mc, dataset, args.file)




