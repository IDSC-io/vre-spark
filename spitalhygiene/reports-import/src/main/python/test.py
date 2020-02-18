#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
import filter_reports
import os
from os.path import join
import tempfile

def read_file(path):
    with open(path, "r") as f:
        return f.read().strip()

class Test(TestCase):


    def test_processing(self):
        with tempfile.TemporaryDirectory() as dirpath:
            filter_reports.main("test-files", dirpath, "test.csv")
            assert os.path.exists(join(dirpath, "abcde/1234/file.txt"))
            assert read_file(join(dirpath, "abcde/1234/file.txt")) == "abcde-1234"
            assert os.path.exists(join(dirpath, "abcde/9999/file.txt"))
            assert read_file(join(dirpath, "abcde/9999/file.txt")) == "abcde-9999"
            assert os.path.exists(join(dirpath, "fghi/2345/file.txt"))
            assert read_file(join(dirpath, "fghi/2345/file.txt")) == "fghi-2345"
            assert os.path.exists(join(dirpath, "fghi/5678/file.txt"))
            assert read_file(join(dirpath, "fghi/5678/file.txt")) == "fghi-5678"




