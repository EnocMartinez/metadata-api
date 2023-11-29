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
from mmm.sensorthings.api import Sensor, Thing, ObservedProperty, FeatureOfInterest


def get_optional_fields(doc: dict, fields: list) -> dict:
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
                extras = get_optional_fields(doc, ["ROR", "EDMO"])
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


def propagate_mongodb_to_sensorthings(mc: MetadataCollector, collections: str, url, authentication=""):
    """
    Propagates info at MetadataCollctor the SensorThings API
    """

    assert (type(mc) is MetadataCollector)
    assert (type(collections) is list)
    # Stations as thing
    if "all" in collections:
        collections = mc.collection_names

    # ==== Sensor Mapping ==== #
    #
    # | MMAPI          | SensorThings                         |
    # |----------------|--------------------------------------|
    # | #id            | name                                 |
    # | description    | description                          |
    # | shortName      | properties:shortName
    # | longName      | properties:longName
    # | serialNumber      | properties:serialNumber
    # | instrumentType | properties:instrumentType            |
    # | model          | properties:model                     |
    # | varibales      | NOT mapped (included as datastreams) |
    # | contacts       | NOT mapped                           |

    sensors = []
    if "sensors" in collections:
        for doc in mc.get_documents("sensors"):
            sensor_id = doc["#id"]
            rich.print(doc)
            rich.print(f"Processinc sensor {sensor_id}")

            name = doc["#id"]
            description = doc["description"]

            keys = ["description", "shortName", "longName", "serialNumber", "instrumentType", "manufacturer", "model"]
            properties = get_properties(doc, keys)
            s = Sensor(name, description, metadata="", properties=properties)
            sensors.append(s)
            s.register(url)




