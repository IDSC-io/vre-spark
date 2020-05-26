configuration = dict()

configuration["PATHS"] = {
    # Path to the "vre_input" folder
    "input_dir": "./data/raw",

    # Path to the "vre_output" folder
    "output_dir": "./data/processed",

    # Path to directory containing test csv files
    "test_data_dir": "./data/raw/test_data",

    # Path to directory containing complete csv data files for model
    "model_data_dir": "./data/raw/model_data",

    # absolute file path for the exported feature vector CSV file
    "csv_export_path": "./data/processed/feature_vector/feature_vector.csv",

    # path to directory in which edge_list.csv and node_list.csv for import into Gephi will be saved
    "gephi_export_dir": "./data/processed/gephi",

    # directory containing all log files
    "log_dir": "./log",

    # directory into which all Neo4J data will be exported
    "neo4j_dir": "./src/data/processed/neo4j",

    # directory containing the odbc connection files (see README for structure)
    "odbc_file_path": "./configuration/server_connection_test.txt"
}

configuration["DELIMITERS"] = {
    # delimiter used for CSV files (default is ,)
    "csv_sep": ","
}

configuration["PARAMETERS"] = {
    # Indicator for data to use, one of 'test' (will load test patient data only) or 'full' (data for all patients)
    "dataset": "full",

    # Number of patients and cases to be loaded, either None (load all data) or any positive integer
    "load_limit": None
}
