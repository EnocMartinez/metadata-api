#!/usr/bin/env python3
"""
CKAN API client to publish/update datasets and dataset metadata

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 23/3/21
"""

from argparse import ArgumentParser
from mmm.common import normalize_string
import requests
import json
import rich
from mmm import MetadataCollector

def process_extras(extras: dict) -> list:
    """
    Converts from {"myparam": "myvalue"} to CKAN extras like [{"key": "myparam", "value": "myvalue"}]
    :param extras: key-value dict. Values should be strings ints or flotas
    :return: list of CKAN extras
    """
    assert(type(extras) == dict)
    processed = []
    for key, value in extras.items():
        processed.append({"key": key, "value": value})
    return processed


class CkanClient:
    def __init__(self, mc: MetadataCollector, url, api_key):
        """
        Creates a CKAN client, able to publish datasets
        """
        rich.print(f"[purple]=== Initializing CkanClient ===")
        rich.print(f"    url: {url}")
        rich.print(f"")
        self.mc = mc
        self.url = url
        if self.url[-1] != "/":
            self.url += "/"
        self.url = self.url + "api/3/action/"
        self.api_key = api_key

    def create_package_from_sensor(self):
        """
        Generates a dataset for input sensor
        :return:
        """
        print("Generating a new CKAN dataset (package)")

        dataset_id = self.sensor_conf["resource_id"].replace(" ", "_").lower()
        self.dataset_id = dataset_id

        #  Check if the dataset already exists
        dataset = self.check_if_package_exists(dataset_id)
        if dataset:
            rich.print("[yellow]Dataset %s already registered!" % dataset_id)
            self.dataset = dataset
            self.dataset_url = self.url + "dataset/" + dataset["id"]
            # return dataset

        else:

            author = self.mc.get_people_from_role(self.sensor_id, "DataManager")
            owner = self.mc.get_organization_from_role(self.sensor_id, "Owner")

            self.dataset = self.package_create(dataset_id,
                                                 self.sensor_conf["label"],
                                                 private=False,
                                                 description=self.sensor_conf["description"],
                                                 id=dataset_id,
                                                 author=author["givenName"] + " " + author["familyName"],
                                                 author_email=author["email"],
                                                 owner_org=owner["@id"])

            self.dataset_url = self.url + "dataset/" + self.dataset["id"]

        if self.generate_dois:
            self.dataset_doi = self.add_doi_to_ckan_element(self.dataset_url, self.dataset, "package")


    def upload_data_link(self, dataset_id, name, description, link):
        return self.resource_create(dataset_id, name.lower() + "_csv_data", name=name, description=description,
                                      resource_url=link, format="csv")

    def get_organization_list(self):
        """
        Get the organizations in the CKAN
        :return: list of dict's with organizations
        """
        url = self.url + "organization_list"
        return self.ckan_get(url)

    def get_license_list(self):
        url = self.url + "license_list"
        return self.ckan_get(url)

    def get_dataset_list(self):
        url = self.url + "current_package_list_with_resources"
        resp = self.ckan_get(url)
        return resp

    def package_create(self, name, title, description="", id="", private=False, author="", author_email="",
                         license_id="cc-by", groups=[], owner_org=""):
        """
        Generates a CKAN dataset (package), more info:
        https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.package_create

        :param name:
        :param title:
        :param private:
        :param author:
        :param author_email:
        :param license_id:
        :param groups:
        :param owner_org:
        :return: CKAN's response as JSON dict
        """
        url = self.url + "package_create"
        id = normalize_string(id)
        data = {
            "name": name,
            "title": title,
            "notes": description,
            "owner_org": owner_org,
            "id": id,
            "private": private,
            "author": author,
            "author_email": author_email,
            "groups": groups,
            "license_id": license_id,
            # "extras": [{"extra 1": "patata"}, {"extra 2": "amb suc"}]
        }
        return self.ckan_post(url, data)

    def package_patch(self, patch_data: dict, id: str):
        """
        Edit attributes within the patch_data dict. The rest remains unchanged
        :param patch_data: dict with data to pach
        :param id: package id
        :return: dataset dict
        """
        url = self.url + f"package_patch"
        patch_data["id"] = id
        return self.ckan_post(url, patch_data)


    def resource_create(self, package_id, id, description="", name="", upload_file=None, resource_url="", format=""):
        """
        Adds a resource to a dataset
        :param datadict: Dictionary with metadata
        :param dataset_id: ID of the dataset
        :param resource: resource file
        """
        # package_id (string) – id of package that the resource should be added to.
        # url (string) – url of resource
        # description (string) – (optional)
        # format (string) – (optional)
        # hash (string) – (optional)
        # name (string) – (optional)
        # resource_type (string) – (optional)
        # mimetype (string) – (optional)
        # mimetype_inner (string) – (optional)
        # cache_url (string) – (optional)
        # size (int) – (optional)
        # created (iso date string) – (optional)
        # last_modified (iso date string) – (optional)
        # cache_last_updated (iso date string) – (optional)
        # upload (FieldStorage (optional) needs multipart/form-data) – (optional)

        resource = self.check_if_resource_exists(id)
        if resource:
            rich.print("[yellow]Resource %s already registered!" % id)
            return resource

        datadict = {
            "id": id,
            "package_id": package_id,
            "description": description,
            "name": name,
        }

        if format:
            datadict["format"] = format
        elif upload_file:
            datadict["format"] = upload_file.split(".")[-1]

        if resource_url:
            datadict["url"] = resource_url

        url = self.url + "resource_create"
        return self.ckan_post(url, datadict, file=upload_file)

    def check_if_package_exists(self, id):
        """
        Checkds if a pacakge exists
        :param id: package id
        :return:
        """
        return self.check_if_exists("package_show", id)

    def check_if_resource_exists(self, id):
        """
        Checkds if a pacakge exists
        :param id: package id
        :return:
        """
        return self.check_if_exists("resource_show", id)

    def check_if_exists(self, endpoint, id):
        """
        Tries to get an entity to check if it exists or not
        :param endpoint: entity enpoint, e.g. "package_show" for dataset, "resource_show" for resource...
        :param id: id of the entity
        :return: None if it does not exit, otherwise returns the entity
        """
        url = self.url + endpoint
        data = {"id": id}
        try:
            entity = self.ckan_get(url, data=data)
        except ValueError:
            return None

        return entity  # only if it exists!

    def get_resource(self, resource_id):
        url = self.url + "resource_show"
        data = {
            "id": resource_id
        }
        return self.ckan_get(url, data=data)

    # ---------------- ORGANIZATIONS ---------------- #
    def get_organizations_list(self):
        """
        Gets all organizations
        :return:
        """
        url = self.url + "organization_list"
        return self.ckan_get(url)

    def organization_create(self, group_id:str, name: str, title:str, description="", image_url="", extras=[]):
        """
        Creates a group (project) in CKAN
        +info: https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.group_create
        :param group_id: group's id
        :param name: group's name
        :param title: group's title
        :param description: grop's description (optional)
        :param image_url: group's image (optinal)
        :param extras: additional key-value pairs
        :return:
        """
        group_data = {
            "name": name,
            "id": group_id,
            "title": title,
            "description": description,
            "image_url": image_url,
            "extras": process_extras(extras)
        }

        url = self.url + "organization_create"
        return self.ckan_post(url, group_data)


    # ---------------- GROUPS ---------------- #
    def get_group_list(self):
        """
        Gets CKAN's groups
        """
        url = self.url + "group_list"
        return self.ckan_get(url)

    def group_create(self, group_id:str, name: str, title:str, description="", image_url="", extras=[]):
        """
        Creates a group (project) in CKAN
        +info: https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.group_create
        :param group_id: group's id
        :param name: group's name
        :param title: group's title
        :param description: grop's description (optional)
        :param image_url: group's image (optinal)
        :param extras: additional key-value pairs
        :return:
        """
        group_data = {
            "name": name,
            "id": group_id,
            "title": title,
            "description": description,
            "image_url": image_url,
            "extras": process_extras(extras)
        }

        url = self.url + "group_create"
        return self.ckan_post(url, group_data)

    # ---------------- GENERIC METHODS ---------------- #
    def ckan_get(self, url, data={}):
        headers = {"Authorization": self.api_key, 'Content-Type': "application/x-www-form-urlencoded"}
        resp = requests.get(url, headers=headers, params=data)
        if resp.status_code > 300:
            raise ValueError(f"CKAN HTTP Error code {resp.status_code}, text: {resp.text}")
        return json.loads(resp.text)["result"]

    def ckan_post(self, url, data, file=None):
        headers = {"Authorization": self.api_key}
        resource = []
        rich.print(data)
        if file:
            resource = [("upload", open(file))]
        else:
            data = json.dumps(data, indent=2)
            headers['Content-Type'] = "application/json"

        resp = requests.post(url, data=data, headers=headers, files=resource)
        if resp.status_code > 300:
            rich.print(data)
            raise ValueError(f"CKAN HTTP Error code {resp.status_code}, text: {resp.text}")
        return json.loads(resp.text)["result"]

    def ckan_patch(self, url, data):
        rich.print("url", url)
        rich.print("data", data)

        headers = {"Authorization": self.api_key}
        data = json.dumps(data)
        headers['Content-Type'] = "application/x-www-form-urlencoded"
        headers['Content-Type'] = "application/json"
        resp = requests.patch(url, data=data, headers=headers)
        if resp.status_code > 300:
            raise ValueError(f"CKAN HTTP Error code {resp.status_code}, text: {resp.text}")
        response = json.loads(resp.text)["result"]
        return response

    def resource_patch(self, patch_data: dict, id: str):
        """
        Edit attributes within the patch_data dict. The rest remains unchanged
        :param patch_data: dict with data to pach
        :param id: package id
        :return: dataset dict
        """
        url = self.url + f"resource_patch"
        patch_data["id"] = id
        return self.ckan_post(url, patch_data)