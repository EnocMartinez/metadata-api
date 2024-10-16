#!/usr/bin/env python3
"""
This file contains several functions to deal with DataFrames

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 1/12/23
"""

import pandas as pd
import rich
from rich.progress import Progress
from mmm.common import qc_flags
import numpy as np
import time
import gc
from mmm.parallelism import multiprocess


def open_csv(csv_file, time_format="%Y-%m-%d %H:%M:%S", time_range=[], format=False) -> pd.DataFrame:
    """
    Opens a CSV datasets and arranges it to be processed and inserted
    :param csv_file: CSV file to process
    :param time_format: format of the timestamp
    :param time_range: list of two timestamps used to slice the input dataset
    :return: dataframe with the dataset
    """
    df = pd.read_csv(csv_file)
    if "timestamp" not in df.columns:
        df = df.rename(columns={df.columns[0]: "timestamp"})  # rename first column to timestamp

    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format=time_format)
    except ValueError:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%dT%H:%M:%Sz")
    df = df.set_index("timestamp")

    if format:
        df = df.sort_index(ascending=True)
        #df = df.dropna()
        try:
            df = df["2000-01-01":]
        except Exception as e:
            pass

    if type(time_range) is str and time_range:
        time_range = time_range.split("/")

    if time_range:
        df = df[time_range[0]:time_range[1]]

    for var in df.columns:
        if var.endswith("_QC"):
            df[var] = df[var].replace(np.nan, -1)
            df[var] = df[var].astype(np.int8)  # set to integer
            df[var] = df[var].replace(-1, np.nan)

    return df


def get_dataframe_precision(df, check_values=1000, min_precision=-1):
    """
    Retunrs a dict with the precision for each column in the dataframe
    :param df: input dataframes
    :param check_values: instead of checking the precision of all values, the first N values will be checked
    :param min_precision: If precision is less than min_precision, use this value instead
    """
    column_precision = {}
    length = min(check_values, len(df.index.values))
    for c in df.columns:
        if "_QC" in c:
            continue  # avoid QC columns
        precisions = np.zeros(length, dtype=int)
        for i in range(0, length):
            value = df[c].values[i]
            try:
                _, float_precision = str(value).split(".")
            except ValueError:
                float_precision = ""  # if error precision is 0 (point not found)
            precisions[i] = len(float_precision)  # store the precision
        column_precision[c] = max(precisions.max(), min_precision)
    return column_precision


def delete_vars_from_df(df, vars: list):
    # ignore (delete) variables in list
    for var in vars:
        # use it as a column name
        found = False
        if var in df.columns:
            found = True
            rich.print(f"[yellow]Variable {var} won't be resampled")
            del df[var]

        if not found:  # maybe it's a prefix, delete everything starting like var
            for v in [col for col in df.columns if col.startswith(var)]:
                rich.print(f"[yellow]Ignoring variable {v}")
                del df[v]  # delete it
    return df


def lin_to_log(df: pd.DataFrame, vars: list):
    """
    Converts some columns of a dataframe from linear to logarithmic
    :param df: dataframe
    :param vars: list of variables to convert
    :returns: dataframe with convert columns
    """
    for var in df.columns:
        if var in vars:
            df[var] = 10 * np.log10(df[var])
    return df


def log_to_lin(df: pd.DataFrame, vars: list):
    """
    Converts some columns of a dataframe from logarithmic to linear
    :param df: dataframe
    :param vars: list of variables to convert
    :returns: dataframe with convert columns
    """

    for var in df.columns:
        if var in vars:
            df[var] = 10 ** (df[var] / 10)
    return df


def resample_dataframe(df, average_period="30min", std_column=True, log_vars=[], ignore=[]):
    """
    Resamples incoming dataframe, performing the arithmetic mean. If QC control is applied select only values with
    "good data" flag (1). If no values with good data found in a data segment, try to average suspicious data. Data
    segments with only one value will be ignored (no std can be computed).
    :param df: input dataframe
    :param average_period: average period (e.g. 1h, 15 min, etc.)
    :param std_column: if True creates a columns with the standard deviation for each variable
    :return: averaged dataframe
    """
    df = df.copy()
    df = delete_vars_from_df(df, ignore)

    # Get the current precision column by column
    precisions = get_dataframe_precision(df)
    if log_vars:
        print("converting the following variables from log to lin:", log_vars)
        df = log_to_lin(df, log_vars)

    resampled_dataframes = []
    # print("Splitting input dataframe to a dataframe for each variable")
    for var in df.columns:
        if var.endswith("_QC"):  # ignore qc variables
            continue

        var_qc = var + "_QC"
        df_var = df[var].to_frame()
        df_var[var_qc] = df[var_qc]

        # Check if QC has been applied to this variable
        if max(df[var_qc].values) == min(df[var_qc].values) == 2:
            rich.print(f"[yellow]QC not applied to {var}")
            rdf = df_var.resample(average_period).mean().dropna(how="any")
            rdf[var_qc] = rdf[var_qc].fillna(0).astype(np.int8)

        else:
            var_qc = var + "_QC"
            # Generate a dataframe for good, suspicious and bad data
            df_good = df_var[df_var[var_qc] == qc_flags["good"]]
            df_na = df_var[df_var[var_qc] == qc_flags["not_applied"]]
            df_suspicious = df_var[df_var[var_qc] == qc_flags["suspicious"]]

            # Resample all dataframes independently to avoid averaging good data with bad data and drop all n/a records
            rdf = df_good.resample(average_period).mean().dropna(how="any")
            rdf_na = df_na.resample(average_period).mean().dropna(how="any")
            #  rdf["good_count"] = df_good[var_qc].resample(average_period).count().dropna(how="any")
            rdf[var_qc] = rdf[var_qc].astype(np.int8)

            rdf_suspicious = df_suspicious.resample(average_period).mean().dropna(how="any")
            rdf_suspicious[var_qc] = rdf_suspicious[var_qc].astype(np.int8)

            if std_column:
                rdf_std = df_good.resample(average_period).std().dropna(how="any")
                rdf_suspicious_std = df_suspicious.resample(average_period).std().dropna(how="any")
                rdf_na_std = rdf_na.resample(average_period).std().dropna(how="any")

            # Join good and suspicious data
            rdf = rdf.join(rdf_na, how="outer", rsuffix="_na")
            rdf = rdf.join(rdf_suspicious, how="outer", rsuffix="_suspicious")

            # Fill n/a in QC variable with 0s and convert it to int8 (after .mean() it was float)
            rdf[var_qc] = rdf[var_qc].fillna(0).astype(np.int8)

            # Select all lines where there wasn't any good data (missing good data -> qc = 0)
            i = 0
            for index, row in rdf.loc[rdf[var_qc] == 0].iterrows():
                i += 1
                if not np.isnan(row[var + "_na"]):
                    # modify values at resampled dataframe (rdf)
                    rdf.at[index, var] = row[var + "_na"]
                    rdf.at[index, var_qc] = qc_flags["not_applied"]

                if not np.isnan(row[var + "_suspicious"]):
                    # modify values at resampled dataframe (rdf)
                    rdf.at[index, var] = row[var + "_suspicious"]
                    rdf.at[index, var_qc] = qc_flags["suspicious"]

            # Calculate standard deviations
            if std_column:
                var_std = var + "_STD"
                rdf[var_std] = 0

                # assign good data stdev where qc = 1
                rdf.loc[rdf[var_qc] == qc_flags["good"], var_std] = rdf_std[var]
                # assign data with QC not applied
                if not rdf.loc[rdf[var_qc] == qc_flags["not_applied"]].empty:
                    rdf.loc[rdf[var_qc] == qc_flags["not_applied"], var_std] = rdf_na_std[var]
                # assign suspicious stdev where qc = 3
                if not rdf.loc[rdf[var_qc] == qc_flags["suspicious"]].empty:
                    rdf.loc[rdf[var_qc] == qc_flags["suspicious"], var_std] = rdf_suspicious_std[var]

                del rdf_std
                del rdf_suspicious_std
                del rdf_na_std
                gc.collect()

            # delete suspicious and bad columns
            del rdf[var + "_na"]
            del rdf[var + "_suspicious"]
            del rdf[var + "_QC_na"]
            del rdf[var + "_QC_suspicious"]

            # delete all unused dataframes
            del df_var
            del rdf_na
            del rdf_suspicious
            del df_good
            del df_suspicious

            gc.collect()  # try to free some memory with garbage collector
        rdf = rdf.dropna(how="any")  # make sure that each data point has an associated qc and stdev

        # append dataframe
        resampled_dataframes.append(rdf)

    # Join all dataframes
    df_out = resampled_dataframes[0]
    for i in range(1, len(resampled_dataframes)):
        merge_df = resampled_dataframes[i]
        df_out = df_out.join(merge_df, how="outer")

    df_out.dropna(how="all", inplace=True)

    if log_vars:  # Convert back to linear
        print("converting back to log", log_vars)
        df_out = lin_to_log(df_out, log_vars)

    # Apply the precision
    for colname, precision in precisions.items():
        df_out[colname] = df_out[colname].round(decimals=precision)

    # Set the precisions for the standard deviations (precision + 2)
    for colname, precision in precisions.items():
        std_var = colname + "_STD"
        if std_var in df_out.keys():
            df_out[std_var] = df_out[std_var].round(decimals=(precision+2))

        if colname in log_vars:  # If the variable is logarithmic it makes no sense calculating the standard deviation
            del df_out[std_var]

    return df_out


def resample_polar_dataframe(df, magnitude_label, angle_label, units="degrees", average_period="30min", log_vars=[],
                             ignore=[]):
    """
    Resamples a polar dataset. First converts from polar to cartesian, then the dataset is resampled using the
    resample_dataset function, then the resampled dataset is converted back to polar coordinates.
    :param df: input dataframe
    :param magnitude_label: magnitude column label
    :param angle_label: angle column label
    :param units: angle units 'degrees' or 'radians'
    :param average_period:
    :return: average period (e.g. 1h, 15 min, etc.)
    """
    df = df.copy()
    df = delete_vars_from_df(df, ignore)

    # column_order = df.columns  # keep the column order

    columns = [col for col in df.columns if not col.endswith("_QC")]  # get columns names (no qc vars)
    column_order = []
    for col in columns:
        column_order += [col, col + "_QC", col + "_STD"]

    precisions = get_dataframe_precision(df)

    # replace 0s with a tiny number to avoid 0 angle when module is 0
    almost_zero = 10 **(-int((precisions[magnitude_label] + 2)))  # a number 100 smaller than the variable precision
    df[magnitude_label] = df[magnitude_label].replace(0, almost_zero)

    # Calculate sin and cos of the angle to allow resampling
    expanded_df = expand_angle_mean(df, magnitude_label, angle_label, units=units)
    # Convert from polar to cartesian
    cartesian_df = polar_to_cartesian(expanded_df, magnitude_label, angle_label, units=units, x_label="x", y_label="y",
                                      delete=False)
    # Resample polar dataframe
    resampled_cartesian_df = resample_dataframe(cartesian_df, average_period=average_period, std_column=True,
                                                log_vars=log_vars, ignore=ignore)
    # re-calculate the angle (use previously calculated sin / cos values)
    resampled_cartesian_df = calculate_angle_mean(resampled_cartesian_df, angle_label, units=units)
    # convert to polar
    resampled_df = cartesian_to_polar(resampled_cartesian_df, "x", "y", units=units, magnitude_label=magnitude_label,
                                      angle_label=angle_label)

    resampled_df = resampled_df.reindex(columns=column_order)  # reindex columns

    # Convert all QC flags to int8
    for c in resampled_df.columns:
        if c.endswith("_QC"):
            resampled_df[c] = resampled_df[c].replace(np.nan, -1)
            resampled_df[c] = resampled_df[c].astype(np.int8)
            resampled_df[c] = resampled_df[c].replace(-1, np.nan)

    # apply precision
    for c, precision in precisions.items():
        resampled_df[c] = resampled_df[c].round(decimals=precision)

    # delete STD for angle column
    if angle_label + "_STD" in resampled_df.columns:
        del resampled_df[angle_label + "_STD"]

    return resampled_df


def polar_to_cartesian(df, magnitude_label, angle_label, units="degrees", x_label="X", y_label="y", delete=True):
    """
    Converts a dataframe from polar (magnitude, angle) to cartesian (X, Y)
    :param df: input dataframe
    :param magnitude_label: magnitude's name in dataframe
    :param angle_label: angle's name in dataframe
    :param units: angle units, it must be "degrees" or "radians"
    :param x_label: label for the cartesian x values (defaults to X)
    :param y_label: label for the cartesian y values ( defaults to Y)
    :param delete: if True (default) deletes old polar columns
    :return: dataframe expanded with north and east vectors
    """
    magnitudes = df[magnitude_label].values
    angles = df[angle_label].values
    if units == "degrees":
        angles = np.deg2rad(angles)

    x = abs(magnitudes) * np.sin(angles)
    y = abs(magnitudes) * np.cos(angles)
    df[x_label] = x
    df[y_label] = y

    # Delete old polar columns
    if delete:
        del df[magnitude_label]
        del df[angle_label]

    if magnitude_label + "_QC" in df.columns:
        # If quality control has been applied to the dataframe, add also qc to the cartesian dataframe
        # Get greater QC flag, so both x and y have the most restrictive quality flag in magnitudes / angles
        df[x_label + "_QC"] = np.maximum(df[magnitude_label + "_QC"].values, df[angle_label + "_QC"].values)
        df[y_label + "_QC"] = df[x_label + "_QC"].values
        # Delete old qc flags
        if delete:
            del df[magnitude_label + "_QC"]
            del df[angle_label + "_QC"]

    return df


def cartesian_to_polar(df, x_label, y_label, units="degrees", magnitude_label="magnitude", angle_label="angle",
                       delete=True):
    """
    Converts a dataframe from cartesian (X, Y) to polar (magnitud, angle)
    :param df: input dataframe
    :param x_label: X column name (input)
    :param y_label: Y column name (input)
    :param magnitude_label: magnitude column name (output)
    :param angle_label: angle column name (output)
    :param units: angle units, it must be "degrees" or "radians"
    :param delete: if True (default) deletes old cartesian columns
    :return: dataframe expanded with north and east vectors
    """
    x = df[x_label].values
    y = df[y_label].values
    magnitudes = np.sqrt((np.power(x, 2) + np.power(y, 2)))  # magnitude is the module of the x,y vector
    angles = np.arctan2(x, y)
    angles[angles < 0] += 2 * np.pi  # change the range from -pi:pi to 0:2pi

    if units == "degrees":
        angles = np.rad2deg(angles)

    df[magnitude_label] = magnitudes
    df[angle_label] = angles

    if delete:
        del df[x_label]
        del df[y_label]
    if x_label + "_QC" in df.columns:
        # If quality control has been applied to the dataframe, add also qc to the cartesian dataframe
        # Get greater QC flag, so both x and y have the most restrictive quality flag in magnitudes / angles
        df[magnitude_label + "_QC"] = np.maximum(df[y_label + "_QC"].values, df[x_label + "_QC"].values)
        df[angle_label + "_QC"] = df[x_label + "_QC"].values
        # Delete old qc flags
        del df[x_label + "_QC"]
        del df[y_label + "_QC"]
    return df


def average_angles(magnitude, angle):
    """
    Averages
    :param wind:
    :param angle:
    :return:
    """
    u = []
    v = []
    for i in range(len(magnitude)):
        u.append(np.sin(angle[i] * np.pi / 180.) * magnitude[i])
        v.append(np.cos(angle[i] * np.pi / 180.) * magnitude[i])
    u_mean = np.nanmean(u)
    v_mean = np.nanmean(v)
    angle_mean = np.arctan2(u_mean, v_mean)
    angle_mean_deg = angle_mean * 180 / np.pi  # angle final en degreee
    return angle_mean_deg


def expand_angle_mean(df, magnitude,  angle, units="degrees"):
    """
    Expands a dataframe with <angle>_sin and <angle>_cos. This
    :param df: input dataframe
    :param magnitude: magnitude columns name
    :param angle: angle column name
    :param units: "degrees" or "radians"
    :return: expanded dataframe
    """
    if units in ["degrees", "deg"]:
        angle_rad = np.deg2rad(df[angle].values)
    elif units in ["radians" or "rad"]:
        angle_rad = df[angle].values
    else:
        raise ValueError("Unkonwn units %s" % units)

    df[angle + "_sin"] = np.sin(angle_rad)*df[magnitude].values
    df[angle + "_cos"] = np.cos(angle_rad)*df[magnitude].values

    if magnitude + "_QC" in df.columns:
        # genearte a qc column with the MAX from magnitude qc and angle qc
        df[angle + "_sin" + "_QC"] = np.maximum(df[angle + "_QC"].values, df[magnitude + "_QC"].values)
        df[angle + "_cos" + "_QC"] = df[angle + "_sin" + "_QC"].values

    return df


def calculate_angle_mean(df, angle, units="degrees"):
    """
    This function calculates the mean of an angle (expand_angle_mean should be called first).
    :param df: input df (expanded with expand angle mean)
    :param angle: angle column name
    :param units: "degrees" or "radians"
    :return:
    """
    sin = df[angle + "_sin"].values
    cos = df[angle + "_cos"].values
    df[angle] = np.arctan2(sin, cos)
    if units in ["degrees", "deg"]:
        df[angle] = np.rad2deg(df[angle].values)

    del df[angle + "_sin"]
    del df[angle + "_cos"]
    return df


def purge_dataframe(df, deployment_history: list):
    """
    Takes the deployment history of a sensor and drops all observations outside the deployment periods
    :param df: dataframe
    :param deployment_history: list of dicts with the info of each deployment
    :return: purged dataset
    """

    print("Erasing data acquired outside deployment periods")
    __deployed_status = ["now", ""]  # keywords to detect that a instrument is already deployed (empty end time or "")
    purge_start = ["1980-01-01T00:00:00"]  # purge all measurements before a deployment
    purge_end = []

    for deployment in deployment_history:
        deployment_start = deployment["timePeriod"][0]
        deployment_end = deployment["timePeriod"][1]

        # erase timezone (if any)
        deployment_start = deployment_start.replace("Z", "").replace("z", "")
        deployment_end = deployment_end.replace("Z", "").replace("z", "")

        purge_start.append(deployment_end)
        purge_end.append(deployment_start)

    # if the sensor us currenty deployed
    if purge_start[-1] in __deployed_status:
        # ignore the last timeperiod
        purge_start = purge_start[:-1]
    else:
        # if it is not deployed, purge until now
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")  # string with current time
        purge_end.append(now)

    with Progress() as progress:
        task = progress.add_task("[cyan]Purging dataframes...", total=len(purge_start))
        for i in range(0, len(purge_start)):
            start = purge_start[i]
            end = purge_end[i]
            start_datetime = pd.to_datetime(start)
            end_datetime = pd.to_datetime(end)
            if end_datetime < df.index.values[0] or start_datetime > df.index.values[-1]:
                rich.print("    [blue]Ignoring interval %s and %s..." % (start, end))
                pass  # period outside dataset, skip it
            else:
                rich.print("    [cyan]Drop data between %s and %s..." % (start, end))
                df = df.drop(df[start:end].index)
            progress.update(task, advance=1)
    return df


def drop_duplicated_indexes(df, store_dup="", keep="first"):
    """
    Drops duplicated data points. If an index
    :param df:
    :param store_dup: if set, the duplicated indexes will be stored in this file
    :param keep:  when a duplicated is found keep "first" or "last" as good. If False all occurences will be dropped
    :return: df without duplicates
    """
    dup_idx = df[df.index.duplicated(keep=keep)]
    total_idx = len(df.index.values)
    dupdf = df[df.index.duplicated()]
    print(dupdf)
    if len(dup_idx.index) > 0:
        print("Found %d duplicated entries (%.04f %%)" % (len(dup_idx.index), 100 * len(dup_idx) / total_idx))
        if store_dup:
            rich.print("[cyan]Storing a copy of duplicated indexes at %s" % store_dup)
            dup_idx.to_csv(store_dup)
        print("Dropping duplicate entries...")
        df = df.drop(dup_idx.index)
    return df


def slice_dataframes(df, max_rows=-1, frequency=""):
    """
    Slices input dataframe into multiple dataframe, making sure than every dataframe has at most "max_rows"
    :param df: input dataframe
    :param max_rows: max rows en every dataframe
    :param frequency: M for month, W for week, etc.
    :return: list with dataframes
    """
    if max_rows < 0 and not frequency:
        raise ValueError("Specify max rows or a frequency")

    if max_rows > 0:
        if len(df.index.values) > max_rows:
            length = len(df.index.values)
            i = 0
            dataframes = []
            with Progress() as progress:
                task = progress.add_task("slicing dataframes...", total=length)
                while i + max_rows < length:
                    init = i
                    end = i + max_rows
                    newdf = df.iloc[init:end]
                    dataframes.append(newdf)
                    i += max_rows
                    progress.update(task, advance=max_rows)
                newdf = df.iloc[i:]
                dataframes.append(newdf)
                progress.update(task, advance=(length-i))
        else:
            dataframes = [df]
    else:  # split by frequency
        dataframes = [g for n, g in df.groupby(pd.Grouper(freq=frequency))]

    # ensure that we do not have empty dataframes
    dataframes = [df for df in dataframes if not df.empty]
    rich.print("[green]sliced!")
    return dataframes


def slice_dataframe_by_columns(df, columns: list):
    """
    Slices input dataframe into multiple dataframes, by columns
    :param df: input dataframe
    :param columns: List of columns to keep
    :return: dataframe only with selected columns
    """

    df = df.copy()

    for c in df.columns:
        if c not in columns:
            del df[c]
    gc.collect()

    return df


def merge_dataframes_by_columns(dataframes: list, timestamp="timestamp"):
    """
    Merge a list of dataframes to a single dataframe by merging on timestamp
    :param dataframes:
    :param timestamp:
    :return:
    """
    if len(dataframes) < 1:
        raise ValueError(f"Got {len(dataframes)} dataframes in list!")
    df = dataframes[0]
    for i in range(1, len(dataframes)):
        temp_df = dataframes[i].copy()
        df = df.merge(temp_df, on=timestamp, how="outer")
        del temp_df
        dataframes[i] = None
        gc.collect()
    return df


def merge_dataframes(df_list, sort=False):
    """
    Appends together several dataframes into a big dataframe
    :param df_list: list of dataframes to be appended together
    :param sort: If True, the resulting dataframe will be sorted based on its index
    :return:
    """

    df = pd.concat(df_list)
    if sort:
        df = df.sort_index(ascending=True)
    return df


class DataframeLocator:
    """
    Dummy class used to
    """
    def __init__(self):
        pass


def slice_and_process(df, handler, args, frequency="M", max_workers=20, text="progress..."):
    """

    :return:
    """
    dataframes = slice_dataframes(df, frequency=frequency)
    arguments = ()
    for i in range(len(dataframes)):  # for each dataframe
        current_slice = ()
        for j in range(0, len(args)):
            if type(args[j]) == DataframeLocator:
                current_slice += (dataframes[i],)
            else:
                current_slice += (args[j],)
        arguments += (current_slice,)

    processed_df = multiprocess(arguments, handler, max_workers=max_workers, text=text)
    return merge_dataframes(processed_df)

def ceil_month(t):
    """"
    Ceil a Timestamp to month (not implemented in pandas)
    """
    init_month = t.month
    t.ceil("1D")
    while t.month == init_month:
        t += pd.Timedelta("1D")
    t = t.floor("1D")
    return t


def ceil_year(t):
    """"
    Ceil a Timestamp to year (not implemented in pandas)
    """
    init_year = t.year
    t.ceil("1D")
    while t.year == init_year:
        t += pd.Timedelta("1D")
    t = t.floor("1D")
    return t


def ceil_timestamp(t: pd.Timestamp, period: str) -> pd.Timestamp:
    """
    Ceils a timestamp, handling day, month and year periods
    :param period: period to split, can be 'daily', 'monthly' or 'yearly'
    :param period: period to split, can be 'daily', 'monthly' or 'yearly'
    """

    if period == "daily":
        t = t + pd.Timedelta("1D")
        t = t.floor("1D")
        return t
    elif period == "monthly":
        return ceil_month(t)
    elif period == "yearly":
        return ceil_year(t)
    else:
        ValueError(f"Unexpected period '{period}'")


def calculate_time_intervals(start_time: pd.Timestamp, end_time: pd.Timestamp, period: str) -> [(pd.Timestamp, pd.Timestamp),]:
    """
    Splits a time range into smaller intervals according to period. It will always fit to the beginning of the next day,
    month, day. As example period 2023-06-01T03:12:00Z/'2023-06-03T12:00:00Z separated daily will be:
        [(2023-06-01T03:12:00Z, 2023-06-02T00:00:00Z),
        (2023-06-02T00:00:00Z, 2023-06-03T00:00:00Z),
        (2023-06-03T00:00:00Z, 2023-06-03T12:00:00Z)]

    :param start_time: start of the interval
    :param end_time: end of the interval
    :param period: period to split, can be 'daily', 'monthly' or 'yearly'
    :returns: list of tuples with (time_start, time_end)
    """
    assert type(start_time) is pd.Timestamp, f"expected pd.Timestamp, got {type(start_time)}"
    assert type(end_time) is pd.Timestamp, f"expected pd.Timestamp, got {type(end_time)}"
    assert type(period) is str, f"expected str, got {type(end_time)}"

    intervals = []
    partial_time_start = ceil_timestamp(start_time, period)
    if partial_time_start != start_time:
        print(f"adding {start_time} to {partial_time_start}")
        intervals.append([start_time, partial_time_start])

    while partial_time_start < end_time:
        partial_end_time = ceil_timestamp(partial_time_start, period)
        partial_end_time = min(partial_end_time, end_time)
        intervals.append((partial_time_start, partial_end_time))
        partial_time_start = partial_end_time

    return intervals