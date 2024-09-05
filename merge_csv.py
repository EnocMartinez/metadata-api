#!/usr/bin/env python3
"""
Merges all CSV files in a folder into a single big CSV file

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 23/3/21
"""

from argparse import ArgumentParser
import rich
from shutil import copy2

from mmm.common import file_list
from mmm.data_manipulation import open_csv, merge_dataframes


def merge_csv(input_files, output):
    """
    Merges multiple CSV files into a single large CSV. Does not check the files, just header
    :param input_files: list of csv files
    :param output: output filename
    :return: nothing
    """
    rich.print("Merging files:", input_files)
    rich.print(f"Into {output}")
    input_files = sorted(input_files)
    # Make sure all headers are equal
    with open(input_files[0]) as f:
        header = f.readline()

    for file in input_files[1:]:
        with open(file) as f:
            header2 = f.readline()
        if header2 != header:
            rich.print(f"[red]Headers do not match! (files f{input_files[0]} and {file})")
            raise ValueError("Headers do not match")

    copy2(input_files[0], output)
    with open(output, "a") as fout:
        for file in input_files[1:]:
            with open(file) as fin:
                lines = fin.readlines()[1:]
            fout.writelines(lines)
    rich.print("[green]Done!")


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("input", help="Folder containing of files to be merged")
    argparser.add_argument("output", help="Output file", type=str)
    args = argparser.parse_args()
    files = file_list(args.input)
    files = sorted(files)
    try:
        merge_csv(files, args.output)
    except ValueError:
        rich.print("couldn't do quick merge, loading them into dataframes...")

        dataframes = [open_csv(file, time_format="%Y-%m-%dT%H:%M:%Sz") for file in files]
        df = merge_dataframes(dataframes, sort=True)
        df = df.sort_index()
        rich.print(df)
        df.to_csv(args.output)









