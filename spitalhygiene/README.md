# Data Science für Spitalhygiene

This file contains additional information on the procedures involved in the VRE project, including
1. Loading of data from the Data_Science_Atelier to CSV
2. Import of CSV data into the VRE model
3. Creation of the model using the sklearn module
4. Export of the model data into Neo4J (mostly for visualization purposes)
5. Visualization based on data in the Neo4J database

Note: `~` in this script refers to the `spitalhygiene` folder (i.e. the folder containing this README) in the `spitalhygiene/INS-348-setup` branch of the insel mono-repo on Bitbucket.

### 1. Loading data from the Data_Science_Atelier to CSV

This step is controlled by the `~/resources/update_db.sh` (bash version) or `~/resources/update_db.py` (python version). Both scripts are dependent on JDK, and make use of 
Theus' `spitalhygiene-2.0-SNAPSHOT-jar-with-dependencies.jar` file. This file is **not** part of this branch, and must be made available separately. 

Both file versions (bash and python) essentially perform an easy task: they connect to the Data_Science_Atelier, execute a series of SQL queries using Theus' JAR file, 
and save the retrieved data in CSV format to a specified folder. All executed queries are found in the `~/sql` folder. Created CSV files are named identically to the SQL query scripts.

### 2. Import of CSV data into VRE model

The CSV import and model creation is controlled by the `~/vre/src/main/python/vre/feature_extractor.py` script. Data are loaded in multiple steps: 

1. An HDFS_data_loader() instance is created, responsible for the extraction of data from the loaded CSV files
2. Patient data are extracted via a call to the HDFS_data_loader.patient_data() function
3. Features and labels are processed using an instance of the features_extractor() class
4. Features and labels are prepared via a call to features_extractor().prepare_features_and_labels()
5. Relevant model data are exported to CSV via features_extractor().export_csv()

### 3. Creation of the model using sklearn module

Model creation is also controlled by the `~/vre/src/main/python/vre/feature_extractor.py` script. Features are extracted and the model is fitted using an instance of the
`sklearn.feature_extraction.DictVectorizer()` instance, followed by a call to the `DictVectorizer.fit_transform()` method.

### 4. Model data export into Neo4J

Data from the model are exported to various csv files representing the different relationships between features. This is accomplished using the `Neo4JExporter()` object. Different
features such as patients, rooms, devices, etc. are then written to CSV via calls to the `Neo4JExporter.write_[...]` method. These files are then read directly into the Neo4J database
in the `~/resources/update_db.sh` bash script via:

`neo4j-import --into path/to/graph.db --nodes path/to/patient.csv --nodes path/to/drugs.csv (... including all relevant CSV files as arguments)`

Note that This part has not yet been implemented in the python version `~/resources/update_db.py`.

### 5. Data visualization from the Neo4J database

This part is currently not implemented.

