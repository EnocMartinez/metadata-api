#!/usr/bin/env python3
"""
This file contains high-level functions that connects MongoDB metadata with other services such as CKAN, ERDDAP, etc.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 27/10/23
"""
import os

from mmm import MetadataCollector, CkanClient
import rich

from mmm.data_maniuplation import open_csv, drop_duplicated_indexes
from mmm.sensorthings.api import Sensor, Thing, ObservedProperty, FeatureOfInterest, Location, Datastream
from mmm.sensorthings.db import SensorThingsDbConnector
import pandas as pd


def load_fields_from_dict(doc: dict, fields: list) -> dict:
    """
    Takes a document from MongoDB and returns all fields in list. If a field in the list is not there, ignore it:

        doc = {"a": 1, "b": 1  "c": 1} and fields = ["a", "b", "d"]
            return {"a": 1, "b": 1 }
    :param doc:
    :param fields:
    :return:
    """
    assert(type(doc) == dict)
    assert (type(fields) == list)
    results = {}
    for field in fields:
        if field in doc.keys():
            results[field] = doc[field]
    return results


def propagate_mongodb_to_ckan(mc: MetadataCollector, ckan: CkanClient, collections: list = []):
    """
    Propagates metadata in MongoDB to CKAN

    :param mc: MetadataCollector
    :param ckan: CkanClient object
    :param collections: list of collections to propagaate
    :return:

    Projects are registered as groups
    Institutions are registered as institutions
    Datasets are registered as packages
    :return:
    """
    assert (type(mc) is MetadataCollector)
    assert (type(ckan) is CkanClient)
    assert (type(collections) is list)

    rich.print("Propagating data from MongoDB to CKAN")
    rich.print(f"Using the following collections: {collections}")

    # Institutions
    if "organizations" in collections:
        ckan_organizations = ckan.get_organization_list()
        rich.print(ckan_organizations)

        for doc in mc.get_documents("organizations"):
            rich.print(doc)
            name = doc["#id"]
            if "public" in doc.keys() and doc["public"]:
                organization_id = doc["#id"].lower()
                title = doc["fullName"]
                extras = load_fields_from_dict(doc, ["ROR", "EDMO"])
                ckan.organization_create(organization_id, name, title, extras=extras)
            else:
                rich.print(f"ignoring private organization {name}...")

    # CKAN Projects
    if "projects" in collections:
        ckan_groups = ckan.get_group_list()
        rich.print(ckan_groups)

        for doc in mc.get_documents("projects"):
            if doc["type"] == "contract":
                rich.print("ignore contract projects")
                continue

            rich.print(f"propagating {doc['#id']}")
            project_id = doc["#id"].lower()
            acronym = doc["acronym"]
            name = acronym.lower()
            title = doc["title"]
            extras = {
                "grant_id": doc["funding"]["grantId"],
                "funding_call": doc["funding"]["call"],
            }
            if "dateStart" in doc.keys() and doc["dateStart"]:
                extras["start_date"] = doc["dateStart"]
            if "dateEnd" in doc.keys() and doc["dateEnd"]:
                extras["end_date"] = doc["dateEnd"]

            logo = ""
            if "logo" in doc.keys():
                logo = doc["logo"]

            ckan.group_create(project_id, name, acronym, description=title, extras=extras, image_url=logo)

    if "datasets" in collections:
        for doc in mc.get_documents("datasets"):
            name = doc["#id"]
            dataset_id = name.lower()
            title = doc["title"]
            description = doc["summary"]

            station = mc.get_station(doc["@stations"])

            sensors = [s for s in doc["@sensors"]]
            sensors = ", ".join(sensors)

            extras = {
                "station": station["#id"],
                "latitude": station["deployment"]["coordinates"]["latitude"],
                "longitude": station["deployment"]["coordinates"]["longitude"],
                "depth": station["deployment"]["coordinates"]["depth"],
                "sensors": sensors
            }
            owner = ""

            # process contacts
            for contact in station["contacts"]:
                role = contact["role"]
                if "@people" in contact.keys():
                    person = mc.get_people(contact["@people"])
                    name = person["name"]
                    extras[role] = name
                elif "@organizations" in contact.keys():
                    org = mc.get_organization(contact["@organizations"])
                    extras[role] = org["fullName"]
                    if role == "owner":
                        owner = contact["@organizations"].lower()

            ckan.package_register(doc["#id"], title, description, dataset_id, extras=extras, owner_org=owner.lower())


def get_properties(doc: dict, properties: list) -> dict:
    """
    Create a new dict with some of the properties from doc
    :param doc: original doc
    :param properties: params to copy (keys)
    :return: dict with copies
    """
    assert (type(doc) is dict)
    assert (type(properties) is list)
    data = {}
    for p in properties:
        data[p] = doc[p]
    return data


def propagate_mongodb_to_sensorthings(mc: MetadataCollector, collections: str, url, update=True, authentication=""):
    """
    Propagates info at MetadataCollctor the SensorThings API
    """

    assert (type(mc) is MetadataCollector)
    assert (type(collections) is list)
    # Stations as thing
    if "all" in collections:
        collections = mc.collection_names

    sensor_ids = {}  # key: mongodb #id, value: sensorthings ID
    things_ids = {}
    location_ids = {}
    obs_props_ids = {}
    if "sensors" in collections:
        sensors = mc.get_documents("sensors")
        for doc in sensors:
            sensor_id = doc["#id"]
            rich.print(f"Processing sensor {sensor_id}")

            name = doc["#id"]
            description = doc["description"]

            keys = ["longName", "serialNumber", "instrumentType", "manufacturer", "model"]
            properties = get_properties(doc, keys)
            s = Sensor(name, description, metadata="", properties=properties)
            s.register(url, update=update)
            sensor_ids[sensor_id] = s.id

    if "variables" in collections:
        variables = mc.get_documents("variables")
        for doc in variables:
            name = doc["#id"]
            description = doc["description"]
            definition = doc["definition"]
            prop = {
                "standard_name": doc["standard_name"]
            }
            o = ObservedProperty(name, description, definition, properties=prop)
            o.register(url, update=update)
            obs_props_ids[name] = o.id

    if "stations" in collections:
        stations = mc.get_documents("stations")
        for doc in stations:
            name = doc["#id"]
            prop = load_fields_from_dict(doc, ["platformType", "manufacturer", "contacts", "emsoFacility", "deployment"])

            # Register Location
            loc_name = f"Location of station {name}"
            loc_description = f"Location of station {name}"
            lat = prop["deployment"]["coordinates"]["latitude"]
            lon = prop["deployment"]["coordinates"]["longitude"]
            depth = prop["deployment"]["coordinates"]["depth"]
            location = Location(loc_name, loc_description, lat, lon, depth)
            location.register(url)

            # Register FeatureOfInterest
            foi_name = name
            foi_description = f"FeatureOfInterest generated from station {name}"
            feature = {
                "type": "Point",
                "coordinates": [lat, lon]
            }
            properties = {
                "depth": depth,
                "depth_units": "meters"
            }
            foi = FeatureOfInterest(foi_name, foi_description, feature, properties=properties)
            foi.register(url)

            # Register Thing
            description = doc["longName"]
            t = Thing(name, description, properties=prop, locations=[{"@iot.id": location.id}])
            t.register(url, update=update)
            things_ids[name] = t.id
        
    for sensor in sensors:
        rich.print(f"Creating Datastreams for sensor {sensor['#id']}")
        sensor_name = sensor["#id"]

        # Create full_data datastreams!
        for var in sensor["variables"]:
            varname = var["@variables"]
            units = var["@units"]
            station = sensor["deployment"]["@stations"]
            sensor_id = sensor_ids[sensor_name]
            thing_id = things_ids[station]
            obs_prop_id = obs_props_ids[varname]
            if var["dataType"] == "timeseries":  # creating timeseries data
                ds_name = f"{station}:{sensor_name}:{varname}:full_data"
                units_doc = mc.get_document("units", units)
                ds_units = load_fields_from_dict(units_doc, ["name", "symbol", "definition"])
                properties = {
                    "dataType": "timeseries",
                    "fullData": True
                }
                if "@qualityControl" in var.keys():
                    qc_doc = mc.get_document("qualityControl", var["@qualityControl"])
                    properties["qualityControl"] = qc_doc["qartod"]

                ds = Datastream(ds_name, ds_name, ds_units, thing_id, obs_prop_id, sensor_id, properties=properties)
                ds.register(url, update=update)

            elif var["dataType"] == "profile":  # creating profile data
                ds_name = f"{station}:{sensor_name}:{varname}:full_data"
                units_doc = mc.get_document("units", units)
                ds_units = load_fields_from_dict(units_doc, ["name", "symbol", "definition"])
                properties = {
                    "dataType": "profile",
                    "fullData": True
                }
                if "@qualityControl" in var.keys():
                    qc_doc = mc.get_document("qualityControl", var["@qualityControl"])
                    properties["qualityControl"] = qc_doc["qartod"]

                ds = Datastream(ds_name, ds_name, ds_units, thing_id, obs_prop_id, sensor_id, properties=properties)
                ds.register(url, update=update)

        # Creating average data
        for process in sensor["processes"]:
            if process["type"] == "average":
                period = process["parameters"]["period"]
                for var in sensor["variables"]:
                    varname = var["@variables"]
                    if "ignore" in process["parameters"].keys() and varname in process["parameters"]["ignore"]:
                        rich.print(f"[yellow]Average ignores {varname}...")
                        continue
                    units = var["@units"]
                    data_type = var["dataType"]
                    obs_prop_id = obs_props_ids[varname]
                    if var["dataType"] == "timeseries" or var["dataType"] == "profile":  # creating raw_data timeseries
                        ds_name = f"{station}:{sensor_name}:{varname}:{period}_average"
                        ds_full_data_name = f"{station}:{sensor_name}:{varname}:{period}full_data"
                        units_doc = mc.get_document("units", units)
                        ds_units = load_fields_from_dict(units_doc, ["name", "symbol", "definition"])
                        properties = {
                            "fullSensorData": False,
                            "dataType": data_type,
                            "averagePeriod": period,
                            "averageFrom": ds_full_data_name
                        }

                        ds = Datastream(ds_name, ds_name, ds_units, thing_id, obs_prop_id, sensor_id,
                                        properties=properties)
                        ds.register(url, update=update)


def bulk_load_data(filename: str, psql_conf: dict, mc: MetadataCollector, url: str, sensor_name: str, data_type, average="") -> bool:
    """
    This function performs a bulk load of the data contained in the input file
    """
    if filename.endswith(".csv"):
        try:
            df = open_csv(filename, time_format="%Y-%m-%dT%H:%M:%Sz")
        except ValueError:
            df = open_csv(filename)
    else:
        rich.print(f"[red]extension {filename.split('.')[-1]} not recognized")
        raise ValueError("Invalid extension")

    db = SensorThingsDbConnector(psql_conf)

    # Get the datastream names
    sensor_id = db.value_from_query(f'select "ID" from "SENSORS" where "NAME" = \'{sensor_name}\';')
    datastreams = db.dict_from_query(f'select "NAME", "ID" from "DATASTREAMS" where "SENSOR_ID" = \'{sensor_id}\';')
    # Harcoded solution: name is expected to be station:sensor:variable:processing_type
    datastream_dict = {}
    if data_type == "timeseries":
        rich.print(f"[purple]====> timeseries {sensor_name} <=======")

        # Keep elements with full data
        df = drop_duplicated_indexes(df)
        datastreams = {key: value for key, value in datastreams.items() if key.endswith("full_data")}
        datastreams = {key.split(":")[2]: value for key, value in datastreams.items()}
        rich.print("[green]start data bulk load")
        db.inject_to_timeseries(df, datastreams)

    elif data_type == "profile" and average:
        rich.print(f"[purple]====> profile with average {sensor_name} <=======")

        station_name = list(datastreams.keys())[0].split(":")[0]
        datastreams = {key: value for key, value in datastreams.items() if key.endswith(f"{average}_average")}
        datastreams = {key.split(":")[2]: value for key, value in datastreams.items()}

        rich.print(f"station name: {station_name}")
        foi_id = db.value_from_query(f'select "ID" from "FEATURES" where "NAME" = \'{station_name}\';')
        rich.print(datastreams)
        db.inject_to_observations(df, datastreams, url, foi_id, average, profile=True)

    elif data_type == "profile":
        rich.print(f"[purple]====> profile  {sensor_name} <=======")
        datastreams = {key: value for key, value in datastreams.items() if key.endswith("full_data")}
        datastreams = {key.split(":")[2]: value for key, value in datastreams.items()}
        rich.print("[green]start data bulk load")
        db.inject_to_profiles(df, datastreams)

    else:
        rich.print(f"[purple]====> average {sensor_name}<=======")
        rich.print(f"looking for elements with '{average}' average period")
        rich.print(f"looking for {average} averaged elements")
        datastreams = {key: value for key, value in datastreams.items() if key.endswith(f"{average}_average")}

        # The first part of a datastream name is the station name
        station_name = list(datastreams.keys())[0].split(":")[0]
        datastreams = {key.split(":")[2]: value for key, value in datastreams.items()}
        rich.print("[green]start data bulk load")

        rich.print(f"station name: {station_name}")
        foi_id = db.value_from_query(f'select "ID" from "FEATURES" where "NAME" = \'{station_name}\';')
        db.inject_to_observations(df, datastreams, url, foi_id, average)
