Officially published code repository of research paper [A. Atkinson and B. Ellenberger et al. - Extending outbreak investigation with machine learning and graph theory: Benefits of new tools with application to a nosocomial outbreak of a multidrug-resistant organism](https://pubmed.ncbi.nlm.nih.gov/36111457/).

vre-spark
==============================

VRE Spark Project - Identifying and monitoring transmission pathways of VRE in a hospital network.

## History & Problem

In 2018, the Insel Gruppe AG experienced the most extensive documented outbreak of an MDRO (Multi-Drug Resistant Organism). It is a sequence type 796 of vancomycin-resistant Enterococcus faecium (VRE), which has a high transfer rate. About 350 patients were affected by this VRE outbreak. In response, the Insel Gruppe AG invested significant financial and human resources to curb the outbreak. Within this framework, a collaboration between the IDSC and the Institute for Infectious Diseases (IFIK) was organized with the idea of better understanding the routes of diffusion and transmission of VRE. For this, data from various source systems in the IDSC were combined into a complete data set and integrated into a complex network model. At the same time, the longer-term goal is to combine the new findings into a "blueprint" that could be transferred to other hospitals.

**The client** Insel Gruppe AG, Spitalhygiene

**The problem** VRE Bacterium spreads among patients by means of unknown transmission vectors. It is hypothesized that transmission vectors are most probably the entities (employees, rooms, devices) interacting with the patients. The aim of this project is to identify these transmission vectors and find probable entities that transfer VRE from one patient to the other. Additional goals are predicting the probability of VRE contamination of entities and subsequent ordering of measures to reduce contamination.

**The current state** A task force consisting of people from Spitalhygiene, IDSC and IFIK was set up to find means to model the transmissions of VRE. The following milestones have been reached in prior work:

* **Data inspection** of different Insel hospital for suitability for VRE analysis
  The ISH_NDIA and ISH_NDRG tables were chosen as well as several from ipdos and IFIK (?)
  Deliverables: Documentation of included data sources, tables etc.
* **Research cluster installation of current model** to enable recurring execution of the model
* **Automated pipeline** for processing of data and calculating model-relevant statistics
  The model is run once per defined time window and provides an update of metrics for inspection
  Deliverables: Code and instructions for automated pipeline
* **SQL Query Data Export** in order to export a dataset to build up the network model
  Deliverables: Scripts and queries for data export
* **Transformation to Neo4J-compatible Format (Deprecated)** to allow for large-scale network analysis
  Deliverables: Prepared Neo4J format transformer
* **Visualization of Network in Neo4J (Deprecated)** in order to give an overview of the interaction graph
  Deliverables: Neo4J Visualization
* **Export of a feature vector per network node** to run extended statistical analysis
  Deliverables: Code and documentation of how to export feature vectors
* **Assessment of data quality of the CDWH data used for VRE** entailing missing values etc.
  Deliverables: Data Quality Plots and report
* **Definition and implementation of initial algorithms/metrics of network analysis** to explore their ability to predict VRE positive patients
  Deliverables: Interaction Network plot, Hotspot list (built from centralities)
* **Exploration of different network metrics (centralities)**
  Deliverables: Network Centralities comparison (Patient Degree Ratio, Betweenness centrality, Eigenvector centrality)
* **Data Cleanup: Resolving identification issues of certain entities**
* **Presentation of Results to Spitalhygiene**
  Deliverables: Results Presentation
* **Annotation of Screening Context**
  Deliverables: Annotations file/documentation
* **Documentation of SQL-Extract, model and visualizations**
  Deliverables: Sphinx documentation of all models, data extraction and visualizations
* **Migration to new server environment**
* **Additional visualizations for VRE Task Force**
  Deliverables: Screening-Results and Hotspots over Time, Eigenvector Centrality and Comparisons
* **Preprocessing of Isolation-Codes**
* **Wave-ware Room annotation**

**Requested product/process/services** Using data about interactions between employees, rooms, devices and patients, the requested model should show the most common factors that lead to the transmission of VRE from one patient to the other. Furthermore, it should allow the Spitalhygiene to detect entities in the network that need a special focus in terms of decontamination.

**Preliminary ideas for solution / improvement** A preliminary analysis of the network using network centrality metrics reveals entities that are of higher importance to the network of interactions and potentially could carry the VRE bacterium across interaction communities. These entities are hereafter called 'hotspots'.

## Milestones

#### Milestone "Publication of poster"

* Definition of Done: Poster published
* Estimated Effort: 30 man days
* Deadline: End of March, 2019
* Deliverables:
  * Poster showing initial findings

#### Milestone "Implementation of network metrics"

* Definition of Done: network metrics are discussed and implemented, its results are understood
* Estimated Effort: 60 man days
* Deadline: End of August, 2021
* Deliverables:
  * Implementation of network metrics application based on network graph
  * Validation of centralities: `models/validate_centralities.py`

#### Milestone "Reporting of findings to Management"

* Definition of Done: Findings are analysed and interpreted, and furthermore distilled into a management report
* Estimated Effort: 30 man days
* Deadline: End of October, 2021
* Deliverables:
  * Management report with findings

#### Milestone "Dashboarding of findings"

* Definition of Done: Dashboarding application is containerized and deployed in productive environment
* Estimated Effort: **ETL**: 14 man days, **DS**: 40 man days, **SW-DEV**: 30 man days
* Deadline: End of December, 2021
* Deliverables:
  * Visualization of networks and metrics
  * Initial dashboarding application with regular data updates

## Challenges

### Political

* Many people are involved such that political issues arise naturally. Discussions regarding hygiene is a hot topic, especially if people are accused of not working according to all hygienic standards. Instead, the topic should be addressed as positions of higher criticality to VRE transmissions and as such requiring a higher responsibility to decontamination.

### Technical

* The IDSC Hadoop Cluster Platform is not completely ready to send applications into production. There are some major blockers ahead before we can continuously improve models and code and subsequently deploy new services.

## Funding

* [2018.10 - 2019.12] VRE-Taskforce, IDSC, Inselhospital

## Involved Persons (Active contributors are in bold)

* DS-Team
  * [2019.08 - current] **Benjamin Ellenberger**, Data Scientist (Lead); Stefan Zahnd, Data Scientist (Buddy, until 2020.03)
  * [2018.10 - 2019.08] Stefan Zahnd, Data Scientist (Lead)
  * [2018.10 - 2019.05] Adriano Pagano (adriano@sqooba.io), Theus Hossmann (theus@sqooba.io), Sqooba
* PL MED
  * [2019.01 - current] **Olga Endrich**, Medicine Product Line Manager (olga.endrich@insel.ch)
  * **Alexander Benedikt Leichtle**, Research Product Line Manager (alexander.leichtle@insel.ch)
  * [2018.10 - 2019.01] Nasanin Sedille-Mostafaie, former Medicine Product Line Manager (nazanin.sedille-mostafaie@insel.ch)
* Spitalhygiene
  * **Andrew Atkinson** Statistician of Spitalhygiene (andrew.atkinson@insel.ch)
  * **Jonas Marschall** Head of Spitalhygiene (jonas.marschall@insel.ch)
  * **Luisa Paola Salazar Vizcaya** Scientist (luisapaola.salazarvizcaya@insel.ch)
  * **Tanja Kaspar** Lead of Infection prevention(tanja.kaspar@insel.ch)
  * **Vanja Piezzi** Assistant Doctor (vanja.piezzi@insel.ch)
  * **Nasstasja Wassilew** Senior physician (nasstasja.wassilew@insel.ch)
  * **Omar Al-Khalil** Assistant Doctor (omar.al-khalil@insel.ch)
  * **Chris Debatin**, IFIK (c.debatin@outlook.de)
  * Yvonne Schmiedel (until 2019) (yvonne.schmiedel@insel.ch)

## Infrastructure requirements

* SQL Server (MSSQL) is used to extract the data from the source systems, transform them to be more widely usable, then load them into appropriate database tables (ETL approach of data processing).

* Python with Data Processing (pyodbc, numpy, pandas, scipy) Graph Theory (networkx), ML (scikit-learn, pytorch) and interactive Visualization (matplotlib, seaborn, bokeh, Holoviz, Holoviews) capabilities allows to load the data from the SQL server, transform it into task-specific features such as the graph representation of patients, employees, rooms and devices. Furthermore, we run graph theoretical metrics or feed the graph together with other patient features to a machine learning model to produce results such as the hotspot list or predicted patient VRE status. Finally, we produce an interactive transmission graph visualization to inspect the complex entity interactions or visualize the results.

* Docker is an operationalization platform for applications, allowing each one to run in its own operating environment independent from the Docker’s base system. This allows each application to be in its most native operating environment and removes the difficult management of common but conflicting dependencies of all applications. Thus, it allows to compose different technology stacks to work together.

* Airflow scheduler helps us to run the model at specific intervals and produce results that are partly fed back into the SQL server environment for further processing and reporting.

* SQL Server Reporting Service (SSRS) is used to produce the tabular results that can be accessed by permitted users.

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
