# Metadata API #

This project contains the Metadata API, a RESTful API designed to administrate metadata regarding scientific data and metadata such as observation stations, datasets, operations... This API is heavily influenced by the SensorThings API, but has some major differences:  

- It adds additional classes such as people, operations, activities, deployments, projects and more.
- It does not manage data, just the metadata
- Instead of using a relational database it relies on a MongoDB to store JSON documents
- Every document is versioned and stored, so the whole lifecycle of a document can be retrieved.


## Metadata API to SensorThings mappings ##

### Conventions

* MongoDB `#id` fields corresponds to SensorThings `name` field
* SensorThings entities `name` field must be unique for every collection (database unique restriction added)
* For every station, a Thing and a FeatureOfInterest are created, sharing the same name

### Sensors

| MMAPI          | SensorThings                         |
|----------------|--------------------------------------|
| #id            | name                                 |
| descrpition    | descritpion                          |
| instrumentType | properties:instrumentType            |
| model          | properties:model                     |
| variables      | NOT mapped (included as datastreams) |
| contacts       | NOT mapped                           |

## Sensors and Datasets ##
Sensors produce data. Datasets compile data from sensors, easy enough. 

Data coming from sensors is archived depending on the Sensor's data type. Available data types are:
* **fixed-point**: fixed-point timeseries. Stored in raw_data hypertable
* **profile**: depth-dependant timeseries. Stored in profile_data hypertable
* **trajectory**: fixed-point timeseries. Stored in raw_data hypertable. Lat/lon/depth treated as regular variables
* **files**: File-based sensor. Files can be any multimedia type, such as audio, video, pictures, etc. Each file has an entry in the "OSERVATIONS" table 

The main difference between point and trajectory is when datasets are generated in a later stage.

Every sensor may have a set of "processes" associated. Currently, the associated processes are:
* **Averaging**: average raw data over a certain period of time. This data will be stored in "OBSERVATIONS".
* **Inference**: Run an inference to the observation (e.g. run an object detection AI job). The data will be stored as parameters in "OBSERVATIONS". The individual detections may also be stored as separated timeseries in "OBSERVATIONS".

## Datasets ##

Within MMAPI different types of datasets can be declared:

* **timeseries**: Regular timeseries dataset (fix-point, profile or trajectory) 
* **files**: Any kind of data produced periodically as files. Can be pictures, video, audio, etc.

### Contact info ###

* **author**: Enoc Martínez  
* **version**: 0.0.1  
* **organization**: Universitat Politècnica de Catalunya (UPC)  
* **contact**: enoc.martinez@upc.edu  


