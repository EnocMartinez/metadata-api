#!/usr/bin/env python3
"""
This script provides the flask methods for the Marine Metadata API.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 24/1/23
"""

from argparse import ArgumentParser
import yaml
from flask import Flask, request, Response
from flask_cors import CORS
from mmm.metadata_collector import MetadataCollector
from mmm.common import setup_log
from mmm.schemas import mmm_schemas
import json
import os

app = Flask(__name__)
CORS(app)


def api_error(message, code=400):
    log.error(message)
    json_error = {"error": True, "code": code, "message": message}
    return Response(json.dumps(json_error), status=code, mimetype="application/json")


@app.route('/v1.0/<path:collection>', methods=['GET'])
def get_collection(collection: str):
    try:
        documents = mc.get_documents(collection)
    except LookupError:
        return api_error(f"Collection not '{collection}', valid collection names {mc.collection_names}")
    return Response(json.dumps(documents), status=200, mimetype="application/json")


@app.route('/v1.0/<path:collection>', methods=['POST'])
def post_to_collection(collection: str):
    document = json.loads(request.data)
    log.debug(f"Checking if collection {collection} exists...")
    if collection not in mc.collection_names:
        return api_error(f"Collection not '{collection}', valid collection names {mc.collection_names}")

    if "#id" not in document.keys():
        return api_error(f"Field #id not found in document")

    document_id = document["#id"]
    identifiers = mc.get_identifiers(collection)
    if document_id in identifiers:
        return api_error(f"Document with #id={document_id} already exists in collection '{collection}'")

    log.info(f"Adding document {document_id} to collection '{collection}'")
    try:
        inserted_document = mc.insert_document(collection, document)
    except Exception as e:
        return api_error(f"Unexpected error while inserting document: {e}", code=500)

    return Response(json.dumps(inserted_document), status=200, mimetype="application/json")


@app.route('/v1.0/<path:collection>/<path:document_id>', methods=['PUT'])
def put_to_collection(collection: str, document_id: str):
    document = json.loads(request.data)
    log.debug(f"Checking if collection {collection} exists...")
    if collection not in mc.collection_names:
        return api_error(f"Collection not '{collection}', valid collection names {mc.collection_names}")

    if "#id" not in document.keys():
        return api_error(f"Field #id not found in document")

    identifiers = mc.get_identifiers(collection)
    if document_id not in identifiers:
        return api_error(f"Document with #id={document_id} does not exist in collection '{collection}', use PUT instead")

    log.info(f"Adding document {document_id} to collection '{collection}'")
    try:
        inserted_document = mc.replace_document(collection, document_id, document)

    except AssertionError:
        return api_error(f"No changes detected")
    except Exception as e:
        return api_error(f"Unexpected error while replacing document: {e}", code=500)

    return Response(json.dumps(inserted_document), status=200, mimetype="application/json")


@app.route('/v1.0/<path:collection>/<path:identifier>', methods=['GET'])
def get_by_id(collection: str, identifier: str):
    if collection not in mc.collection_names:
        error_msg = f"Collection not '{collection}', valid collection names {mc.collection_names}"
        json_error = {"error": True, "code": 400,  "message": error_msg}
        return Response(json.dumps(json_error), status=400, mimetype="application/json")
    try:
        document = mc.get_document(collection, identifier)
    except LookupError:
        error_msg = f"Document with #id={identifier} does not exist in collection '{collection}', use PUT instead"
        json_error = {"error": True, "code": 400,  "message": error_msg}
        return Response(json.dumps(json_error), status=404, mimetype="application/json")

    return Response(json.dumps(document), status=200, mimetype="application/json")


@app.route('/v1.0/schema/<path:collection>', methods=['GET'])
def get_schema(collection: str):
    if collection not in mc.collection_names:
        error_msg = f"Collection not '{collection}', valid collection names {mc.collection_names}"
        json_error = {"error": True, "code": 400,  "message": error_msg}
        return Response(json.dumps(json_error), status=400, mimetype="application/json")

    schema = mmm_schemas[collection]
    return Response(json.dumps(schema), status=200, mimetype="application/json")


if __name__ == "__main__":
    log = setup_log("Metadata API")
    __required_environment_variables = ["mongodb_connection"]
    for key in __required_environment_variables:
        if key not in os.environ.keys():
            raise EnvironmentError(f"Environment variable '{key}' not set!")

    __mongodb_database = "metadata"
    mc = MetadataCollector(os.environ["mongodb_connection"], __mongodb_database)
    log.info("starting Metadata API")
    app.run(host="0.0.0.0", debug=True)

