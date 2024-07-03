#!/usr/bin/env python3
from mmm import MetadataCollector
from mmm.common import file_list
import rich
import json
import os

files = file_list("MongoData/metadata")
rich.print(files)

for file in files:
    if os.path.basename(file).startswith("_"):
        continue  # skip internal files
    #rich.print(f"file {file}")
    try:
        with open(file) as f:
            doc = json.load(f)
    except json.decoder.JSONDecodeError:
        rich.print(f"[red]file {file} not properly encoded!!")
        continue
    if doc["@version"] != 1:
        rich.print(f"[red]{file} Needs to be updated; v={doc['@version']}")

        doc["@version"] = 1
        with open(file, "w") as f:
            rich.print(f"Fixing version for {file}")
            json.dump(doc, f, indent=2)
    # Now let's write a copy into history
    basename = os.path.basename(file)
    filename = basename.split("."[0])
    histfile = os.path.join("MongoData/metadata_hist")


