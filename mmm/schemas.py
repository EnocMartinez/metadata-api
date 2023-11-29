#!/usr/bin/env python3

# Generic metadata for ALL documents
mmm_metadata = {
    "$id": "mmm:document_metadata",
    "type": "object",
    "properties": {
        "#id": {"type": "string"},
        "#version": {"type": "integer"},
        "#creationDate": {"type": "string"},
        "#modificationDate": {"type": "string"},
        "#author": {"type": "string"},
    },
    "required": ["#id", "#version", "#creationDate", "#modificationDate", "#author"]
}

# --------- Generic ----------- #

__string_list__ = {
    "type": "array",
    "minItems": 1,
    "items": {"type": "string"}
}

__label_definition = {
    "type": "object",
    "properties": {
        "definition": {"type": "string"},
        "label": {"type": "string"}
    },
    "required": ["definition", "label"]
}

__coordinates__ = {
    "type": "object",
    "properties": {
        "latitude": {"type": "number"},
        "longitude": {"type": "number"},
        "depth": {"type": "number"}
    },
    "required": ["latitude", "longitude", "depth"]
}

__data_types__ = {
    "type": "string",
    "enum": [
        # time-series
        "timeseries",  # fix-point timeseries
        "profile",  # depth-dependant timeseries. Depth is usually stored as integer to simplify indexing
        # others
        "files",  # arbitrary file-based data, such as audio, pictures or video.
        "detections"  # event detections based from other data. An example is fish detections from a picture
    ]
}

__data_sources__ = {
    "type": "string",
    "enum": [
        "sensorthings-db",              # regular SensorThings database
        "sensorthings-tsdb",            # SensorThings raw-data hypertable (TimescaleDB)
        "sensorthings-tsdb-profile",    # SensorThings profiles-data hypertable (TimescaleDB)
        "filesystem"  # files in a directory tree in the filesystem, like year/month/day/myfile.txt
    ]
}

# ----------- Conventions ------------ #
__doi_roles__ = [  # Roles for dataset attribution from MetadataKernel
    "ContactPerson",
    # Person with knowledge of how to access, troubleshoot, or otherwise field issues related to the resource
    "DataCollector",  # Person/institution responsible for finding or gathering/collecting data
    "DataCurator",  # Person tasked with reviewing, enhancing, cleaning, or standardizing metadata
    "DataManager",  # Person responsible for maintaining the finished resource
    "Distributor",
    # Institution tasked with responsibility to generate/disseminate copies of  the resource in either electronic or print form
    "Editor",  # A person who oversees the details related to the publication format of the resource
    "ProjectLeader",  # Person officially designated as head of project team or sub project team instrumental
    "HostingInstitution",
    # Typically, the organisation allowing the resource to be available on the internet through the provision of its hardware/software/operating support     "Researcher", # A person involved in analysing data or the results of an experiment or formal study
    "ProjectLeader",
    # Person officially designated as head of project team or sub- project team instrumental in the work necessary to development of the resource
    "RelatedPerson",
    # A person without a specifically defined role in the development of the resource, but who is someone the author wishes to recognize
    "Researcher",  # A person involved in analysing data or the results of an experiment or formal study.
    "ResearchGroup",
    # Typically refers to a group of individuals with a lab, department, or division that has a specifically defined focus of activity.
    "RightsHolder",
    # Person or institution owning or managing property rights, including intellectual property rights over the resource
    "Other",  # Any person or institution making a significant contribution to the development and/or maintenance of the
    # resource, but whose contribution is not adequately described by any of the other values
]

__operation_roles__ = [
    # People onboard
    "diver",  # Person who participates in a diving operation, may also participate in operations onboard
    "crewMember",  # person who participates in operations in a boat but does not dive
    "captain",  # boat driver

    # land-base roles
    "operator",  # person who operates a machine or process
    "other"  # all others
]

__device_roles__ = [
    "owner",  # institution owning a station/sensor
    "operator",  # person responsible to operate certain station/sensor
    "principalInvestigator",  # PI of a station/sensor
    "dataManager"
]


def __contacts_with_roles__(roles: list):
    # Generate an array of either people or organization with a certain role list, defined as argument
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "@people": {"type": "string"},
                "@organizations": {"type": "string"},
                "role": {
                    "type": "string",
                    "enum": roles
                }
            },
            "oneOf": [
                {"required": ["@people"], "not": {"required": ["@organizations"]}},
                {"required": ["@organizations"], "not": {"required": ["@people"]}},
            ],
            "required": ["role"]
        }
    }


def __people_with_roles__(roles: list):
    # Generate an array of either people or organization with a certain role list, defined as argument
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "@people": {"type": "string"},
                "roles": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "string",
                        "enum": roles
                    }
                },
            },
            "required": ["@people", "roles"]
        }
    }


__activity_type__ = [
    "deployment",  # Deployment of a instrument, platform or resource
    "recovery",  # recovery a previously deployed asset
    "cleaning",  # cleaning of a sensor, probably removing biofouling
    "maintenance",  # operation to ensure the proper functionality of an asset, such as replacing broken parts
    "test",  # activity to test the proper functionality of a sensor/platform/resource
    "other"
]

__operation_type__ = [
    "intervention",  # Surface or underwater intervention into sensor/station/resource
    "cruise",  # several activities carried out during a cruise
    "test",  # test the functionality of a device
    "other"  # any other option
]

__project_types__ = [
    "european",  # Project funded by the European Comission
    "national",  # Project funded by the national science ministry (or similar)
    "contract"  # Project with a company, EU or national project regulations do not apply
]

__partnership_types__ = ["participant", "thirdParty", "other"]

# -----------------------------#

__people = {
    "$id": "mmm:people",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "givenName": {"type": "string"},
        "familyName": {"type": "string"},
        "orcid": {"type": "string"},
        "email": {"type": "string"},
        "@organizations": {"type": "string"}
    },
    "required": ["name", "givenName", "familyName", "email", "@organizations"]
}

__organizations = {
    "$id": "mmm:organizations",
    "type": "object",
    "properties": {
        "fullName": {"type": "string"},
        "acronym": {"type": "string"},
        "ROR": {"type": "string"},
        "EDMO": {"type": "string"},
    },
    "required": ["fullName", "acronym"]
}

# ------------------------------------------------- #
# -------------------- Sensors -------------------- #
# ------------------------------------------------- #
# Sensors define every sensor, which variables is
# it measuring and also the processing, which include
# pre-processing (AI-based inference) or post-processing
# (averaged data)


__inference_parameters = {
    "type": "object",
    "properties": {
        "model": {"type": "string"},  # url where the model is published
        "info": {"type": "string"}  # free-text comment
    },
    "required": ["model"]
}


__average_parameters = {
    "type": "object",
    "properties": {
        "period": {"type": "string"},  # period of the average, such as
        "ignore": __string_list__    # list of variables to ignore in the average (e.g. battery level)
    },
    "required": ["period"]
}

__process_options = {
    "type": "object",
    "properties": {
        "type": {
            "type": "string",
            "enum": ["average", "inference"]
        },
        "parameters": {"type": "object"}
    },
    "required": ["type", "parameters"],
    "if": {
        "properties": {"type": {"const": "average"}}
    },
    "then": {
        "parameters": __average_parameters
    },
    "else": {
        "parameters": __inference_parameters
    }
}


__sensors = {
    "$id": "mmm:sensors",
    "type": "object",
    "properties": {
        "description": {"type": "string"},
        "shortName": {"type": "string"},
        "longName": {"type": "string"},
        "serialNumber": {"type": "string"},
        "instrumentType": __label_definition,
        "model": __label_definition,
        "manufacturer": __label_definition,
        "contacts": __contacts_with_roles__(__device_roles__),
        "variables": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "@variables": {"type": "string"},
                    "@units": {"type": "string"},
                    "@qualityControl": {"type": "string"},
                    "dataType": __data_types__,
                    "technical": {"type": "boolean"}  # define as technical variable, not of interest for datasets
                },
                "required": ["@variables", "@units", "dataType"]
            },
        },
        "processes": {  # pre-processing or post-processing options
            "type": "array",
            "minItems": 1,
            "items": __process_options
        }
    },
    "required": ["description", "shortName", "longName", "serialNumber", "instrumentType", "model", "manufacturer"]
}

__stations = {
    "$id": "mmm:stations",
    "type": "object",
    "properties": {
        "shortName": {"type": "string"},
        "longName": {"type": "string"},
        "platformType": __label_definition,
        "manufacturer": __label_definition,
        "contacts": __contacts_with_roles__(__device_roles__),
        "emsoFacility": {"type": "string"},
        "deployment": {
            "type": "object",
            "properties": {
                "date": {"type": "string"},
                "coordinates": __coordinates__
            },
            "required": ["date", "coordinates"]
        },
    },
    "required": ["shortName", "longName", "platformType", "deployment", "contacts"]
}

__datasets = {
    "$id": "mmm:datasets",
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "summary": {"type": "string"},
        "@stations": {"type": "string"},  # only ONE station
        "@sensors": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "string"
            }
        },
        "@variables": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "string",
            }
        },
        "dataType": __data_types__,  # may be redundant, but helps parsing info
        "dataSource": __data_sources__,
        "dataSourceOptions": {"type": "object"},
        "export": {
            "type": "object",
            "properties": {
                "namePattern": {"type": "string"},
                "period": {"type": "string"},
                "host": {"type": "string"},
                "path": {"type": "string"}
            },
            "required": ["namePattern", "period", "path", "host"]
        },
        "contacts": __contacts_with_roles__(__doi_roles__)
    },
    "required": ["title", "summary", "@stations", "@sensors", "dataType", "dataSource", "contacts", "dataSourceOptions", "export"]
}

__activities = {
    "$id": "mmm:activities",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "time": {"type": "string"},
        "type": {
            "type": "string",
            "enum": __activity_type__
        },
        "appliedTo": {
            "type": "object",
            "properties": {
                "@sensors": __string_list__,
                "@stations": __string_list__,
                "@resources": __string_list__
            },
            "oneOf": [
                {"required": ["@sensors"], "not": {"required": ["@stations", "@resources"]}},
                {"required": ["@stations"], "not": {"required": ["@sensors", "@resources"]}},
                {"required": ["@resources"], "not": {"required": ["@sensors", "@stations"]}}
            ]
        },
        "where": {
            "type": "object",
            "properties": {
                "@stations": {"type": "string"}
            },
            "required": ["@stations"]
        },
    },
    "required": ["name", "appliedTo", "time"]
}

__operations = {
    "$id": "mmm:operations",
    "type": "object",
    "properties": {
        "description": {"type": "string"},
        "timeRange": {"type": "string"},
        "type": {
            "type": "string",
            "enum": __operation_type__
        },
        "participants": __people_with_roles__(__operation_roles__),
        "@activities": __string_list__,
        "@projects": __string_list__,
        "@resources": __string_list__,
        "additionalCosts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "@people": {"type": "string"},
                    "@organizations": {"type": "string"},
                    "comment": {"type": "string"},
                    "cost": {"type": "number"}  # in euros
                },
                "oneOf": [
                    {"required": ["@organizations"], "not": {"required": ["@people"]}},
                    {"required": ["@people"], "not": {"required": ["@organizations"]}}
                ],
                "required": ["comment"]
            }
        }
    },
    "required": ["description", "timeRange", "type", "participants", "@activities"],
}

__variables = {
    "$id": "mmm:variable",
    "type": "object",
    "properties": {
        "standard_name": {"type": "string"},
        "description": {"type": "string"},
        "definition": {"type": "string"}
    },
    "required": ["standard_name", "description", "definition"]
}

__units = {
    "$id": "mmm:units",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "symbol": {"type": "string"},
        "definition": {"type": "string"}
    },
    "required": ["name", "symbol", "definition"]
}

__projects = {
    "$id": "mmm:projects",
    "type": "object",
    "properties": {
        "acronym": {"type": "string"},
        "title": {"type": "string"},
        "totalBudget": {"type": "number"},
        "ourBudget": {"type": "number"},
        "type": {"type": "string", "enum": __project_types__},
        "active": {"type": "boolean"},
        # Link to founding entity
        "funding": {
            "type": "object",
            "properties": {
                "@organizations": {"type": "string"},
                "grantId": {"type": "string"},
                "call": {"type": "string"},
                "coordinator": {"type": "string"},
                "partnershipType": {"type": "string", "enum": __partnership_types__},
            },
            "required": ["@organizations", "grantId"]
        },
    },
    "required": ["acronym", "title", "totalBudget", "ourBudget", "type", "funding"]
}

mmm_schemas = {
    "people": __people,
    "organizations": __organizations,
    "sensors": __sensors,
    "datasets": __datasets,
    "stations": __stations,
    "variables": __variables,
    # "qualityControl": {},
    "units": __units,
    "operations": __operations,
    "activities": __activities,
    "projects": __projects,
    # "resources": {}
}
