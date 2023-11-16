# Metadata API #

This project contains the Metadata API, a RESTful API designed to administrate metadata regarding scientific data and metadata such as observation stations, datasets, operations... This API is heavily influenced by the SensorThings API, but has some major differences:  
- 
- It adds additional classes such as people, operations, activites, deployments, projects and more.
- It does not manage data, just the metadata
- Instead of using a relational database it relies on a MongoDB to store JSON documents
- Every document is versioned and stored, so the whole lifecycle of a document can be retrieved.


### Contact info ###

* **author**: Enoc Martínez  
* **version**: 0.0.1  
* **organization**: Universitat Politècnica de Catalunya (UPC)  
* **contact**: enoc.martinez@upc.edu  