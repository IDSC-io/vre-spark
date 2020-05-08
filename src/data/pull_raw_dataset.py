# -*- coding: utf-8 -*-
"""This script controls the data update for all "raw" data in the VRE Model. It will execute all SQL queries
(i.e. all files with a '.sql' extension') found in ``SQL_DIR``, and save extracted data to CSV files in ``CSV_DIR``.
CSV and SQL files will be named identically, e.g.:

``LA_ISH_NBEW.sql`` :math:`\\longrightarrow` ``LA_ISH_NBEW.csv``

The Atelier_DataScience is queried directly via the `pyodbc` module, and requires an additional connection file
containing details on the ODBC connection to the Atelier (see VRE Model Overview for more information).
"""

import configparser
import csv
import datetime
import os
import pyodbc


def write_sql_query_results_to_csv(path_to_sql, path_to_csv, csv_sep, connection_file, trusted_connection=True, force_overwrite=False):
    """Executes an SQL query and writes the results to path_to_csv.

    Args:
        path_to_sql (str):          Path to .sql file containing the query to be executed
        path_to_csv (str):          Path to .csv file destination, to which data will be written to in "csv_sep"
                                    delimited fashion
        csv_sep (str):              Delimiter used in the csv file
        connection_file (str):      path to file containing information used for server connection and authentication,
                                    as well as database selection (read and passed to ``pyodbc.connect()`` )
                                    This information is read from an external file so as to avoid hard-coding usernames
                                    and passwords
        trusted_connection (bool):  additional argument passed to pyodbc.connect(), converted to "yes" if ``True`` and
                                    "no" otherwise (defaults to ``True``)
    """
    if os.path.exists(path_to_csv):
        if force_overwrite:
            print(f"Overwriting {path_to_csv} as force_overwrite is enabled.")
        else:
            print(f"{path_to_csv} exists. force_overwrite is disabled, not overwriting.")
            return None

    connection_string = ';'.join([line.replace('\n', '') for line in open(connection_file, 'r')])

    conn = pyodbc.connect(connection_string, trusted_connection='yes' if trusted_connection else 'no')
    cursor = conn.cursor()

    # Read the SQL file
    query = ' '.join([line.replace('\n', '') for line in open(path_to_sql, 'r')])

    # execute query
    try:
        cursor.execute(query)
    except pyodbc.ProgrammingError as e:
        print(e)
        return e

    # Then write results to SQL
    # --> register special dialect to control csv delimiter and proper newline formatting
    csv.register_dialect('sql_special', delimiter=csv_sep, lineterminator='\n')
    with open(path_to_csv, 'w') as writefile:
        csv_writer = csv.writer(writefile, dialect='sql_special')
        csv_writer.writerow([i[0] for i in cursor.description])  # write headers
        csv_writer.writerows(cursor)

    # close connection
    conn.close()


def pull_raw_dataset():

    # extract correct filepath
    this_filepath = os.path.dirname(os.path.realpath(__file__))
    # contains the directory in which this script is located, irrespective of the current working directory

    # load config file:
    config_reader = configparser.ConfigParser()
    config_reader.read(os.path.join(this_filepath, '../../configuration/basic_config.ini'))

    SQL_DIR = os.path.join(this_filepath, "./sql/test_dataset") if config_reader['PARAMETERS']['dataset'] == 'test' \
        else os.path.join(this_filepath, "./sql/full_dataset")  # absolute or relative path to directory containing SQL files

    CSV_DIR = config_reader['PATHS']['test_data_dir'] if config_reader['PARAMETERS']['dataset'] == 'test' \
        else config_reader['PATHS']['model_data_dir']  # absolute or relative path to directory where data is stored

    CSV_DELIM = config_reader['DELIMITERS']['csv_sep']  # delimiter for CSV files written from SQL results

    print(f"data_basis set to: {config_reader['PARAMETERS']['dataset']}\n")

    # execute all queries in SQL_DIR
    print('Loading data from SQL server:\n')

    sql_files = [each_file for each_file in os.listdir(SQL_DIR) if each_file.endswith('.sql')]
    # --> Use this line instead for loading only specific files:
    # sql_files = [each_file for each_file in os.listdir(SQL_DIR) if each_file in ['OE_PFLEGE_MAP.sql']]

    exceptions = []

    for each_file in sql_files:
        print('--> Loading file: ' + each_file + '... ', end='', flush=True)
        start_dt = datetime.datetime.now()

        # execute query and write results
        potential_exception = write_sql_query_results_to_csv(path_to_sql=os.path.join(SQL_DIR, each_file),
                                                             path_to_csv=os.path.join(CSV_DIR, each_file.replace('.sql', '.csv')),
                                                             csv_sep=CSV_DELIM,
                                                             connection_file=config_reader['PATHS']['odbc_file_path'],
                                                             trusted_connection=False)
        if potential_exception is not None:
            exceptions.append(potential_exception)
        else:
            print(f'\tDone!\t Query execution time: {str(datetime.datetime.now()-start_dt).split(".")[0]}')
        # --> print timedelta without fractional seconds (original string would be printed as 0:00:13.4567)

    if len(exceptions) != 0:
        raise Exception("\n Not all files loaded successfully, check above.")

    print('\nAll files loaded successfully!')


if __name__ == '__main__':
    pull_raw_dataset()
