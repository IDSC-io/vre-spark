import csv
from spitalhygiene.HDFS_data_loader import HDFS_data_loader

class Neo4JExporter:
    """
    TODO: Refactor
    TODO: Test
    """

    def write_patient(patients):
        with open("patient.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [
                    "PATNR:ID",
                    "type:LABEL",
                    "risk:LABEL",
                    "risk_datetime:datetime{timezone:Europe/Bern}",
                ]
            )
            for key, p in patients.items():
                l = "False"
                d = None
                if p.has_risk():
                    l = "True"
                    d = p.get_risk_date()
                csvwriter.writerow(
                    (
                        p.patient_id,
                        "Patient",
                        l,
                        d.strftime("%Y-%m-%dT%H:%M") if d is not None else None,
                    )
                )


    def write_patient_patient(ps):
        with open("patient_patient.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    "from:datetime{timezone:Europe/Bern}",
                    "to:datetime{timezone:Europe/Bern}",
                    "room",
                    ":TYPE",
                ]
            )
            for p in ps:
                csvwriter.writerow(p)


    def write_room(rooms):
        with open("zimmer.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(["Name:ID", "type:LABEL"])
            for k, r in rooms.items():
                csvwriter.writerow([r.name, "Room"])


    def write_patient_room(rooms):
        with open("patient_zimmer.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    "from:datetime{timezone:Europe/Bern}",
                    "to:datetime{timezone:Europe/Bern}",
                    ":TYPE",
                ]
            )
            for k, r in rooms.items():
                for m in r.moves:
                    if m.bwi_dt is not None and m.bwe_dt is not None and m.case is not None:
                        csvwriter.writerow(
                            [
                                m.case.patient.patient_id,
                                r.name,
                                m.bwi_dt.strftime("%Y-%m-%dT%H:%M"),
                                m.bwe_dt.strftime("%Y-%m-%dT%H:%M"),
                                "in",
                            ]
                        )


    def write_bed(rooms):
        with open("bett.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(["Name:ID", "type:LABEL"])
            for k, r in rooms.items():
                for b in r.beds:
                    csvwriter.writerow([b, "Bed"])


    def write_room_bed(rooms):
        with open("zimmer_bett.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow([":START_ID", ":END_ID", ":TYPE"])
            for k, r in rooms.items():
                for b in r.beds:
                    csvwriter.writerow([r.name, b, "in"])


    def write_device(devices):
        with open("geraet.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow([":ID", "type:LABEL", "name"])
            for k, g in devices.items():
                csvwriter.writerow([g.geraet_id, "Device", g.geraet_name])


    def write_patient_device(patients):
        with open("patient_geraed.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [":START_ID", ":END_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"]
            )
            for k, p in patients.items():
                for ki, c in p.cases.items():
                    for t in c.appointments:
                        if t.termin_datum.year > 2000:
                            for d in t.devices:
                                csvwriter.writerow(
                                    [
                                        p.patient_id,
                                        d.geraet_id,
                                        "used",
                                        t.termin_datum.strftime("%Y-%m-%dT%H:%M"),
                                    ]
                                )


    def write_drug(drugs):
        with open("drugs.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow([":ID", "type:LABEL", "name"])
            for k, d in drugs.items():
                csvwriter.writerow([k, "Drug", d])


    def write_patient_medication(patients):
        with open("patient_medication.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    ":TYPE",
                    "from:datetime{timezone:Europe/Bern}",
                    "to:datetime{timezone:Europe/Bern}",
                ]
            )
            for k, p in patients.items():
                exposure = p.get_antibiotic_exposure("nevermind")
                if exposure is None:
                    continue
                for k, d in exposure.items():
                    csvwriter.writerow(
                        [
                            p.patient_id,
                            d[0],
                            "administered",
                            d[1].strftime("%Y-%m-%dT%H:%M"),
                            d[2].strftime("%Y-%m-%dT%H:%M"),
                        ]
                    )


    def write_employees(employees):
        with open("employees.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow([":ID", "type:LABEL", "name"])
            for k, e in employees.items():
                csvwriter.writerow([k, "Employee", k])


    def write_patient_employee(patients):
        with open("patient_employee.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [":START_ID", ":END_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"]
            )
            for k, p in patients.items():
                for ki, c in p.cases.items():
                    for t in c.appointments:
                        if t.termin_datum.year > 2000:
                            for e in t.employees:
                                if e.mitarbeiter_id != "-1":
                                    csvwriter.writerow(
                                        [
                                            p.patient_id,
                                            e.mitarbeiter_id,
                                            "appointment_with",
                                            t.termin_datum.strftime("%Y-%m-%dT%H:%M"),
                                        ]
                                    )


    def write_referrer(partners):
        with open("referrers.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [
                    ":ID",
                    "type:LABEL",
                    "name1",
                    "name2",
                    "name3",
                    "land",
                    "plz",
                    "ort",
                    "ort2",
                ]
            )
            for partner in partners.values():
                csvwriter.writerow(
                    [
                        partner.gp_art,
                        "Referrer",
                        partner.name1,
                        partner.name2,
                        partner.name3,
                        partner.land,
                        partner.pstlz,
                        partner.ort,
                        partner.ort2,
                    ]
                )


    def write_referrer_patient(patients):
        with open("referrer_patient.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [":END_ID", ":START_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"]
            )
            for patient in patients.values():
                for case in patient.cases.values():
                    if case.moves_start is not None:
                        for referrer in case.referrers:
                            csvwriter.writerow(
                                [
                                    patient.patient_id,
                                    referrer.gp_art,
                                    "referring",
                                    case.moves_start.strftime("%Y-%m-%dT%H:%M"),
                                ]
                            )

    def write_chop_code(chops):
        with open("chops.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [
                    ":ID",
                    "type:LABEL",
                    "level1",
                    "level2",
                    "level3",
                    "level4",
                    "level5",
                    "level6",
                    "catalog",
                ]
            )
            for chop in chops.values():
                csvwriter.writerow(
                    [
                        chop.chop_code,
                        "CHOP",
                        chop.chop_level1,
                        chop.chop_level2,
                        chop.chop_level3,
                        chop.chop_level4,
                        chop.chop_level5,
                        chop.chop_level6,
                        chop.chop_sap_katalog_id
                    ]
                )

    def write_chop_patient(patients):
        with open("chop_patient.csv", "w") as csvfile:
            csvwriter = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csvwriter.writerow(
                [":START_ID", ":END_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"]
            )
            for patient in patients.values():
                for case in patient.cases.values():
                    for surgery in case.surgeries:
                        csvwriter.writerow(
                            [
                                patient.patient_id,
                                surgery.chop.chop_code,
                                "surgery",
                                surgery.bgd_op.strftime("%Y-%m-%dT%H:%M"),
                            ]
                        )

if __name__ == '__main__':

    loader = HDFS_data_loader()
    patient_data = loader.patient_data()

    exporter = Neo4JExporter()

    #contact_patients = get_contact_patients(patients)

    # Export to csv files for Neo4J
    exporter.write_patient(patient_data["patients"])
    #exporter.write_patient_patient(contact_patients)
    exporter.write_room(patient_data["rooms"])
    exporter.write_patient_room(patient_data["rooms"])
    exporter.write_bed(patient_data["rooms"])
    exporter.write_room_bed(patient_data["rooms"])
    exporter.write_device(patient_data["devices"])
    exporter.write_patient_device(patient_data["patients"])
    exporter.write_drug(patient_data["drugs"])
    exporter.write_patient_medication(patient_data["patients"])
    exporter.write_employees(patient_data["employees"])
    exporter.write_patient_employee(patient_data["patients"])
    exporter.write_referrer(patient_data["partners"])
    exporter.write_referrer_patient(patient_data["patients"])
    exporter.write_chop_code(patient_data["chops"])
    exporter.write_chop_patient(patient_data["patients"])