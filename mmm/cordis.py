#!/usr/bin/env python3
"""
Gets data from cordis (https://cordis.europa.eu) and returns project info in JSON format

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 3/10/23
"""
import os.path
from argparse import ArgumentParser
import lxml.etree as etree
import yaml
import rich
from xmlutils import get_elements, get_element, get_element_text
import requests
import json


def get_cost_from_organization(organization: etree.ElementTree()):
    possible_terms = ["totalCost", "ecContribution", "netEcContribution"]
    return float(__attribute_from_names(organization, possible_terms))


def __attribute_from_names(organization: etree.ElementTree, terms: list):
    """
    Checks for possible attributes in an etree element. Returns the value of the first element in the list found
    :param organization:  ElementTree containing a organization
    :param target:
    :return:
    """
    result = ""
    for term in terms:
        try:
           return organization.attrib[term]
        except KeyError:
            continue
    if not result:
        raise LookupError(f"Could not extract value, none of the following terms where found: {terms}")


def get_cordis_metadata(project_id: int, folder=".cordis"):
    """
    Returns project metadata based on data downloaded from CORDIS.
    :param project_id: cordis id
    :param institution: short name of the institution to look for more details, e.g. "UPC"
    :param clear: Clear all previously downloaded files
    :return: json structure with project data
    """

    os.makedirs(folder, exist_ok=True)
    if args.clear:
        rich.print("Cleaning downloaded files...")
        files = [os.path.join(folder, f) for f in os.listdir(folder)]
        for f in files:
            rich.print(f"deleting {f}")
            os.remove(f)

    tmp_file = os.path.join(folder, f"{project_id}.xml")
    if os.path.exists(tmp_file):
        rich.print("reading file locally")
    else:
        rich.print("file not found locally, downloading from cordis...", end="")
        url = f"https://cordis.europa.eu/project/id/{project_id}/en?format=xml"
        r = requests.get(url)
        with open(tmp_file, "w") as f:
            f.write(r.text)
        rich.print("[green]done")


    with open(tmp_file) as f:
        text = f.read()

    text = text.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
    tree = etree.ElementTree(etree.fromstring(text))

    elements = { # key(cordis): value(output)
        "id": "project_id",
        "acronym": "acronym",
        "title": "title",
        "totalCost": "totalBudget",
        "startDate": "dateStart",
        "endDate": "dateEnd",
        #"objective": "summary"
    }
    data = {
        "#id": "",
        "type": "european"
    }
    pref = "{http://cordis.europa.eu}"
    for cordis_key, output_key in elements.items():
        data[output_key] = get_elements(tree, f"{pref}{cordis_key}")[0].text  # get always the first element

    data["#id"] = data["acronym"]  # use acronym as ID

    data["totalBudget"] = float(data["totalBudget"])
    data["funding"] = {
        "grantId": data.pop("project_id"),
        "@organizations": "ec"  # add European Comission as funder
    }

    # Getting call info, first try with relatedMasterCall
    try:
        call = get_element(tree, f"{pref}call", attr="type", attr_value="relatedMasterCall")
        data["funding"]["call"] = get_element_text(call, f"{pref}title")
    except LookupError:
        # then, go for subcall
        call = get_element(tree, f"{pref}call", attr="type", attr_value="relatedSubCall")
        data["funding"]["call"] = get_element_text(call, f"{pref}title")

    organizations = get_elements(tree, f"{pref}organization")
    rich.print(f"Looking for details of organization '{args.institution}'")

    for o in organizations:
        contribution = o.attrib["type"]
        try:
            short_name = get_element_text(o, f"{pref}shortName")
            if short_name == args.institution:
                data["ourBudget"] = get_cost_from_organization(o)
                data["funding"]["partnershipType"] = o.attrib["type"]
            elif contribution == "coordinator":
                data["funding"]["coordinator"] = short_name
        except LookupError as e:
            continue

    if "ourBudget" not in data.keys():
        raise LookupError(f"Institution {args.institution} not found in partners list!")

    return data


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("project_id", type=str, help="Project ID to fetch in CORDIS", default="")
    argparser.add_argument("-i", "--institution", type=str, help="Institution of interest short name, e.g. UPC", default="UPC")
    argparser.add_argument("--clear", action="store_true", help="clears all prevoius downloads", default=False)
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False,
                           default="secrets-local.yaml")
    argparser.add_argument("--force", action="store_true", help="skips insert question", default=False)

    args = argparser.parse_args()
    data = get_cordis_metadata(args.project_id, args.institution)
    rich.print(json.dumps(data, indent=2))

    if not args.force:
        rich.print("[cyan]Store this information into database? (yes/on)")
        response = input()
        if response != "yes":
            rich.print("[red]Aborting")
            exit()
    else:
        rich.print("[purple]Ingesting into database (forced with cli arguments)")

    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
        staconf = secrets["sensorthings"]

    try:
        from .metadata_collector import MetadataCollector
        mc = MetadataCollector(secrets["mongodb"]["connection"], secrets["mongodb"]["database"], ensure_ids=False)

    except ImportError:
        from metadata_collector import MetadataCollector
        mc = MetadataCollector(secrets["mongodb"]["connection"], secrets["mongodb"]["database"], ensure_ids=False)

    mc.insert_document("projects", data)
    # resp = requests.post("http://localhost:5000/v1.0/projects", data=json.dumps(data),
    #                      headers={"Content-Type": "applications/json"})
    # rich.print(f"code: {resp.status_code}")
    # rich.print(f"text: {resp.text}")
