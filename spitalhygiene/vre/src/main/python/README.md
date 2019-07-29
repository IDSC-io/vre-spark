# Directory Overview

Folder `vre`   

Contains all functions relevant for data processing, model creation and feature vector export.

-----

Folder `tests`:

Contains test code for each class in the model. Tests are intended to be run with the `pytest` module as follows:

- Make sure `test_data_dir` in `BasicConfig.ini` points to a directory containing all test CSV files. This is a small subset of the complete data extract of the Atelier_DataScience
and may **not** be in the repo (i.e. must be made available separately).
- Navigate to the `tests` directory
- Start the tests via `python -m pytest` or `python -m pytest > output.log`

