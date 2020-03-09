# Directory Overview

Folder `vre`:   

Contains all functions relevant for data processing, model creation, feature vector export, and testing.

-----

Folder `tests`:

Contains test code for each class in the model. Tests are intended to be run with the `pytest` module as follows:

- Make sure `test_data_dir` in `BasicConfig.ini` points to a directory containing all test CSV files. This is a small subset of the complete data extract of the Atelier_DataScience
and may **not** be in the repo (i.e. must be made available separately).
- Navigate to the `tests` directory
- Start the tests via `python -m pytest` or `python -m pytest > output.log`

-----

Folder `sql`:

Contains 3 folders:
 - `tests` &rightarrow; Contains SQL queries yielding the test dataset
 - `vre` &rightarrow; Contains SQL queries yielding the complete model dataset
 - `quality` &rightarrow; Contains SQL queries yielding data used for quality assessment

-----

Folder `resources`:

Contains various output files from the model and the `update_db.*` scripts, which control the querying and export of data from Atelier_DataScience to CSV.

-----

Folder `docs`:

Contains the Sphinx documentation for the entire VRE model code.

