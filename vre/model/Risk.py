import datetime
import logging


class Risk:
    """Models a ``Risk`` (i.e. Screening) object.
    """
    def __init__(self, auftrag_nr, erfassung, entnahme, vorname, nachname, gbdat, pat_nr, pruefziffer, PID,
                 auftraggeber, kostenstelle, material_type, transport, resultat, analyse_methode, screening_context):
        """Initiates a Risk (i.e. Screening) object.
        """
        self.auftrag_nbr = auftrag_nr
        self.erfassung = datetime.datetime.strptime(erfassung, '%Y-%m-%d').date() if erfassung != '' else None
        self.entnahme = datetime.datetime.strptime(entnahme, '%Y-%m-%d').date() if entnahme != '' else None
        self.vorname = vorname
        self.nachname = nachname
        self.geburtsdatum = gbdat
        self.patient_nbr = pat_nr
        self.pruefziffer = pruefziffer
        self.pid = PID
        self.auftraggeber = auftraggeber
        self.kostenstelle = kostenstelle
        self.material_type = material_type
        self.transport = transport
        self.result = resultat
        self.analysis_method = analyse_methode
        self.screening_context = screening_context

    def NOLONGERUSED_add_risk_to_patient(lines, patients):
        """Reads the risk csv, create Risk objects from the rows and adds these to the respective patients.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_LA_ISH_NRSF_NORM
        ["PATIENTID",   "RSFNR",    "KZTXT",                            "ERDAT",        "ERTIM"]
        ["00004887743", "000042",   "Screening auf VRE. Spezielle L",   "2018-08-23",   "10:25:30"]
        ["00004963016", "000042",   "Screening auf VRE. Spezielle L",   "2018-05-09",   "15:48:48"]

        :param patients: Dictionary mapping patient ids to Patient() --> {'00008301433' : Patient(), ... }
        """
        logging.debug("add_risk_to_patient")
        nr_not_found = 0
        nr_ok = 0
        for line in lines:
            risk = Risk(*line)
            if patients.get(risk.patient_id, None) is not None:
                patients[risk.patient_id].add_risk(risk)
            else:
                nr_not_found += 1
                continue
            nr_ok += 1
        logging.info(f"{nr_ok} risks added, {nr_not_found} patients not found for risk")

    def NOLONGERUSED_add_deleted_risk_to_patient(lines, patients):
        '''
        Read the deleted risk csv and creates Risk objects with code '000142' for deleted VRE screening from the rows.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:

        >> TABLE NAME: deleted_screenings
        ["VNAME",       "NNAME",    "PATIENTID",    "GBDAT",        "ScreeningDate"]
        ["Margarete",   "Bucher",   "00014616742",  "1963-11-11",   "2018-03-12"]
        ["Edouard",     "Kurth",    "00014533820",  "1954-02-15",   "2018-02-16"]

        Discard entries which are missing the date of screening.

        :param patients: Dictionary mapping patient ids to Patient() --> {'00008301433' : Patient(), ... }
        '''
        logging.debug("add_deleted_risk_to_patient")
        nr_not_found = 0
        nr_malformed = 0
        nr_ok = 0
        for line in lines:
            if len(line[4]) > 0 and line[4] != "NULL":
                deleted_risk = Risk(*[line[2], '000142', 'Deleted VRE Screening', line[4], '00:00:00'])
            else:
                nr_malformed += 1
                continue
            if patients.get(deleted_risk.patient_id, None) is not None:
                patients[deleted_risk.patient_id].add_risk(deleted_risk)
            else:
                nr_not_found += 1
                continue
            nr_ok += 1
        logging.info(f"{nr_ok} risks added, {nr_not_found} patients not found for deleted risk, {nr_malformed} malformed.")

    @staticmethod
    def generate_screening_overview_map(lines):
        """Generates the ward screening overview dictionary.

        This function will generate a dictionary containing an overview of which screenings were active at any
        particular day. This information is extracted from an iterator object (``lines``), which provides data from
        the Atelier_DataScience in the table ``dbo.WARD_SCREENINGS``. The table is formatted as follows:

        ============= ======= ==============
        screening_day org_pf  screening_type
        ============= ======= ==============
        2018-01-01    O MITTE W
        2018-01-01    O SUED  E
        ============= ======= ==============

        Args:
            lines (iterator() object):  csv iterator from which data will be read

        Returns:
            dict:
                Dictionary mapping ``datetime.date()`` objects to tuples of length 2 of the form
                ``(ward_name, screening_type)``

                :math:`\\longrightarrow` ``{'2018-10-22' : ('O SUED', 'W'), '2018-09-15' : ('IB BLAU', 'E'), ...}``
        """
        screening_dict = {}
        for each_line in lines:
            extracted_date = datetime.datetime.strptime(each_line[0], '%Y-%m-%d').date()
            if extracted_date not in screening_dict:
                screening_dict[extracted_date] = [(each_line[1], each_line[2])]
            else:
                screening_dict[extracted_date].append( (each_line[1], each_line[2]) )

        return screening_dict

    @staticmethod
    def generate_oe_pflege_map(lines):
        """Generates the oe_pflege_map dictionary.

        This function will generate a dictionary mapping "inofficial" ward names to the names of official *Pflegerische
        OEs*. This information is extracted from an iterator object (lines), which provides data from the
        Atelier_DataScience in the table ``dbo.OE_PFLEGE_MAP``. The table is formatted as follows:

        ========= =============
        oe_pflege oe_pflege_map
        ========= =============
        E 108     E120-21
        BEWA      C WEST
        ========= =============

        Args:
            lines (iterator() object):  csv iterator from which data will be read

        Returns:
            dict:
                Dictionary mapping inofficial ward names to official names of *pflegerische OEs*

                :math:`\\longrightarrow` ``{'BEWA' : 'C WEST', 'E 121' : 'E 120-21', ...} ``
        """
        oe_pflege_dict = {}
        for each_line in lines:
            oe_pflege_dict[each_line[0]] = each_line[1]

        return oe_pflege_dict

    @staticmethod
    def add_annotated_screening_data_to_patients(lines, patient_dict):
        """Annotates and adds screening data to all patients in the model.

        This function is the core piece for adding VRE screening data to the model. It will read all screenings exported
        from the Atelier_DataScience ``V_VRE_SCREENING_DATA`` view, which is structured as follows:

        ========== ========== ========== =============== ============= ============ ========== =========== =========== ============ ============ ============ ========= ======== =============== =================
        auftrag_nr erfassung  entnahme   vorname         nachname      geburtsdatum patient_nr pruefziffer patient_id  auftraggeber kostenstelle material_typ transport resultat analyse_methode screening_context
        ========== ========== ========== =============== ============= ============ ========== =========== =========== ============ ============ ============ ========= ======== =============== =================
        1234567    2018-33-33 2018-33-33 Mister          X             2999-33-33   0000000000 8           00000000000 S099         sepi         are          tmpo      nn       PCR             NULL
        1234567    2018-33-33 2018-33-33 Mister          X             2999-33-33   0000000000 8           00000000000 sepi         sepi         are          tmpo      nn       PCR             NULL
        1234567    2018-33-33 2018-33-33 Misses          Y             2999-33-33   0000000000 9           00000000000 sepi         sepi         are          tmpo      nn       PCR             Klinisch
        ========== ========== ========== =============== ============= ============ ========== =========== =========== ============ ============ ============ ========= ======== =============== =================

        A Risk() object will be created from each line received. These risk objects have the following arguments:

        - auftrag_nr
        - erfassung
        - entnahme
        - patient_nr
        - pruefziffer
        - patient_id
        - auftraggeber
        - kostenstelle
        - material_typ
        - transport
        - resultat
        - analyse_methode
        - screening_context
        - ward_name
        - room_name
        - pflege_oe_name

        The ``screening_context`` attribute will either be set to *Klinisch* or ``NULL``. If set to *Klinisch*, this
        indicates that the screening was performed as a non-ordinary VRE screening such as testing a urine sample for
        the presence of VRE bacteria. These screenings will be added to the VRE model without further processing. The
        same is true for VRE screening entries set to *Ausland*, which refer to patients having been transpored from
        abroad to Switzerland and being screened here upon hospital entry.

        For most screenings however, the ``screening_context`` will be set to NULL. In this case, the context must be
        **extracted** from the data available in the model. This process is tedious and involves the following steps:

        1) Extract Move() (in german: *Aufenthalt*) objects for screening which were available to the patient at the
            time of ``erfassung``. This **must** be a single move, as a patient cannot be stationed in two different
            wards simultaneously.
        2) Extract the Ward() for the Move() extracted in step 1, and add the ward name to the ``self.ward_name``
            attribute.
        3) Extract the exact Room() name where the patient was located from the Move(), and add it to the
            ``self.room_name`` attribute. This room name is sometimes indicated for Moves(), but will not be available
            for the vast majority of data. In that case, room_name will be set to ``None`` instead.
        4) Map the extracted Ward() name to an official *Pflegerische OE* using the ``oe_pflege_map`` dictionary. This
            dictionary maps "inofficial" ward names to an official name of a *pflegerische OE*. This step is very
            important, as the official name of the *pflegerische OE* allows the creation of a link to Waveware and
            thereby the correct floor, building, and room (in some situations) in which the screening has taken place.
            The officially extracted pflegerische OE will be appended to the ``self.pflege_oe_name`` attribute.
        5) Check which type of screening was active in the *pflegerische OE* extracted in step 4. This information is
            found in the ``ward_screening_overview`` dictionary, which indicates the various types of screenings that
            were active at specific dates in different pflegerische OEs. Note that in this step, if no screening
            context can be found for a particular VRE screen at its date of ``erfassung``, the dates in
            ``ward_screening_overview`` will be matched with a tolerance of **+/- 1 day** due to the fact that VRE
            screenings are occasionally performed one day sooner or later than planned.

        Args:
            lines (iterator):       iterator object of the to-be-read file `not` containing the header line
            patient_dict (dict):    Dictionary mapping patient ids to Patient() --> {'00008301433' : Patient(), ... }
        """
        load_count = 0
        for each_line in lines:
            this_risk = Risk(*each_line)

            # Check if this patient's PID exists in patient_dict
            if this_risk.pid in patient_dict:
                load_count += 1

                potential_moves = patient_dict[this_risk.pid].get_moves_at_date(this_risk.erfassung)
                print(potential_moves)
                # if load_count > 5:
                #     break



        #
        #
        #
        # move_wards = []
        # screening_wards = []
        # logging.debug("adding_all_screenings_to_patients")
        # nr_pat_not_found = 0
        # nr_ok = 0
        # for line in lines:
        #     this_risk = Risk(*line)
        #     if patient_dict.get(this_risk.pid) is None:  # Check whether or not PID exists
        #         nr_pat_not_found += 1
        #         continue
        #     potential_moves = patient_dict.get(this_risk.pid).get_location_info(focus_date=this_risk.erfassung)
        #     if len(potential_moves) > 0:  # indicates at least one potential match
        #         move_wards.append('+'.join([each_move.org_fa for each_move in potential_moves]))
        #         screening_wards.append(this_risk.options)
        #         nr_ok += 1
        # # print results to file
        # with open('match_results.txt', 'w') as writefile:
        #     for i in range(len(move_wards)):
        #         writefile.write(f"{move_wards[i]}; {screening_wards[i]}\n")
        #
        #
        #
        #
        #
        #     #     patient_dict[this_risk.patient_id].add_risk(this_risk)
        #     #     nr_ok += 1
        #     # else:
        #     #     nr_pat_not_found += 1
        #     #     continue
        # logging.info(f"{nr_ok} screenings added, {nr_pat_not_found} patients from VRE screening data not found.")






