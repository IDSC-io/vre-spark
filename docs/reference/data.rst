************************
``src/data`` folder
************************

This folder contains important functions for loading data from SQL into CSV, thereby preparing the "raw" data used for
building the actual network models.

Most importantly, this folder also contains the file ``Update_Model.sh``, which is a bash script controlling `all steps`
in the VRE calculation. Since all VRE data are recalculated once per day, this includes (in order):

1) Backing up data from the previous calculation cycle in the HDFS file system (from step 4 in the previous run)
2) Reloading data from SQL into CSV
3) Running the VRE analysis (all steps of the analysis are controlled with ``feature_extractor.py``)
4) Adding new data (SQL files, CSV files, other data) to the HDFS file system


File: pull_raw_dataset.py
===============================

.. automodule:: src.data.pull_raw_dataset
   :members:

   
File: preprocesor.py
===============================

.. automodule:: src.data.preprocessor
   :members:
