#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module to filter out patient reports based on CSV file containing PIDs and FIDs

Usage: ./filter_reports.py source_folder destination_folder csv_to_include
        where:
            - source_folder: dump from hdfs containing the report files
            - destination_folder: where to copy the files of interest
            - csv_to_include: csv file containing the list of PIDs and FIDs to retain
"""

import errno
import os
import shutil
import sys
from collections import defaultdict

def main(rootdir, outdir, pid_fid_file):
    print("Src: {src}, Dst: {dst}, Filter csv file: {csv}".format(src=rootdir, dst=outdir, csv=pid_fid_file))
    print("Start filtering.....")

    patient_case_dict = defaultdict(list)

    def mkdirs_if_not_exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise


    with open(pid_fid_file, "r") as f:
        f.readline()  # skip header
        for line in f:
            values = line.split(",")
            if values[2] != "":
                patient_case_dict[values[1]].append(values[2])
            else:
                patient_case_dict[values[1]]

    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if os.path.splitext(file)[-1] != ".txt":
                continue
            pid = subdir.split("/")[-2]
            fid = subdir.split("/")[-1]
            if pid in patient_case_dict:
                if not patient_case_dict[pid] or fid in patient_case_dict[pid]:
                    mkdirs_if_not_exists(os.path.join(outdir, pid, fid))
                    shutil.copy2(os.path.join(subdir, file), os.path.join(outdir, pid, fid, file))

    print("Done!")

if __name__ == "__main__":
    rootdir = sys.argv[1]
    outdir = sys.argv[2]
    pid_fid_file = sys.argv[3]
    main(rootdir, outdir, pid_fid_file)