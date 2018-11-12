from feature_extractor import features_extractor

import pandas as pd


def test_prepare_features_and_labels(patient_data):
    mc = features_extractor()
    features, labels, dates, v = mc.prepare_features_and_labels(
        patient_data["patients"]
    )

    assert len(features) == len(labels) == len(dates) == 2
    assert labels[1] == 2


def test_export_data(patient_data, tmpdir_factory):
    mc = features_extractor()
    features, labels, dates, v = mc.prepare_features_and_labels(
        patient_data["patients"]
    )

    fn = tmpdir_factory.mktemp("data").join("risk_factors.csv")

    mc.export_csv(features, labels, dates, v, str(fn))

    result_dataframe = pd.read_csv(str(fn))

    assert not result_dataframe.empty
    assert result_dataframe["age"].sum() == 144
    assert result_dataframe["device=ECC"].sum() == 1
    assert result_dataframe["label"].sum() == 3


def test_export_gephi(patient_data, tmpdir_factory):
    mc = features_extractor()
    features, labels, dates, v = mc.prepare_features_and_labels(
        patient_data["patients"]
    )

    print(features)
    print(v.feature_names_)

    fn_nodes = tmpdir_factory.mktemp("data").join("node_list.csv")
    fn_edges = tmpdir_factory.mktemp("data").join("edge_list.csv")

    mc.export_gephi(features, labels, dates, v, str(fn_edges), str(fn_nodes))

    result_nodes = pd.read_csv(str(fn_nodes))
    result_edges = pd.read_csv(str(fn_edges))

    assert result_nodes.shape == (2, 4)
    assert result_edges.shape == (2, 5)

    assert result_nodes["Category"][0] == 1
    assert result_nodes["Category"][1] == 2
