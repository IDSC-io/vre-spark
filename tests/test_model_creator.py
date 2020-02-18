import sys
sys.path.append('../vre')

from feature_extractor import feature_extractor

import pandas as pd


def test_prepare_features_and_labels(patient_data):
    mc = feature_extractor()
    features, labels, dates, v = mc.prepare_features_and_labels(
        patient_data["patients"]
    )

    assert len(features) == len(labels) == len(dates) == 2
    assert labels[1] == 2


def test_export_data(patient_data, tmpdir_factory):
    mc = feature_extractor()
    features, labels, dates, v = mc.prepare_features_and_labels( patient_data["patients"] )

    fn = tmpdir_factory.mktemp("data").join("risk_factors.csv")

    mc.export_csv(features, labels, dates, v, str(fn))

    result_dataframe = pd.read_csv(str(fn))

    assert not result_dataframe.empty
    assert result_dataframe["age"].sum() == 144
    assert result_dataframe["device=ECC"].sum() == 1
    assert result_dataframe["label"].sum() == 3


def test_export_gephi(patient_data, tmpdir_factory):
    mc = feature_extractor()
    features, labels, dates, v = mc.prepare_features_and_labels( patient_data["patients"] )

    print(features)
    print(v.feature_names_)

    file_dir = tmpdir_factory.mktemp("data") # creates a temporary directory only for this test session

    mc.export_gephi(features, labels, dates, v, file_dir)

    result_nodes = pd.read_csv(file_dir.join("node_list.csv"))
    result_edges = pd.read_csv(file_dir.join("edge_list.csv"))

    assert result_nodes.shape == (2, 4)
    assert result_edges.shape == (2, 5)

    assert result_nodes["Category"][0] == 1
    assert result_nodes["Category"][1] == 2
