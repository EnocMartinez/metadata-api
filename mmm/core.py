#!/usr/bin/env python3
"""
This file contains high-level functions that connects MongoDB metadata with other services such as CKAN, ERDDAP, etc.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 27/10/23
"""

from mmm import MetadataCollector, CkanClient
import rich


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
            organization_id = doc["#id"].lower()
            name = doc["#id"]
            title = doc["fullName"]
            extras = get_optional_fields(doc, ["ROR", "EDMO"])
            ckan.organization_create(organization_id, name, title, extras=extras)

    # CKAN Projects
    if "projects" in collections:
        ckan_groups = ckan.get_group_list()
        rich.print(ckan_groups)

        for doc in mc.get_documents("projects"):
            rich.print(f"propagating {doc['#id']}")
            name = doc["#id"]
            name = doc["#id"]
            ckan.group_create()


