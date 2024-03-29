import datetime

import logging

from tqdm import tqdm
import pandas as pd


class RiskScreening:
    """Models a ``RiskScreening`` (i.e. Screening) object.
    """

    def __init__(self, order_id, recording_date, measurement_date, first_name, last_name, birth_date,
                 patient_id, result):
        """Initiates a Risk (i.e. Screening) object.
        """
        # TODO: [BE] The relevant date is recording date or measurement date? I believe it would be measurement date but they are mostly the same date.
        self.order_id = order_id
        self.recording_date = recording_date.date()
        self.patient_id = patient_id.zfill(11) if not pd.isna(patient_id) else ""  # extend the patient id to length 11 to get a standardized representation
        self.result = result

        # not used
        self.measurement_date = measurement_date
        self.last_name = last_name
        self.first_name = first_name
        self.birth_date = birth_date

    def is_positive(self):
        return self.result != "nn"

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
                screening_dict[extracted_date].append((each_line[1], each_line[2]))

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
    def add_annotated_screening_data_to_patients(csv_path, encoding, patient_dict, from_range, to_range, is_verbose=True):
        """Annotates and adds screening data to all patients in the model.

        This function is the core piece for adding VRE screening data to the model. It will read all screenings exported
        from the Atelier_DataScience ``V_VRE_SCREENING_DATA`` view, which is structured as follows:

        ========== ========== ========== =============== ============= ============ =========== ============ ============ ============ ========= ======== =============== =================
        auftrag_nr erfassung  entnahme   vorname         nachname      geburtsdatum patient_id  auftraggeber kostenstelle material_typ transport resultat analyse_methode screening_context
        ========== ========== ========== =============== ============= ============ =========== ============ ============ ============ ========= ======== =============== =================
        1234567    2018-33-33 2018-33-33 Mister          X             2999-33-33   00000000000 S099         sepi         are          tmpo      nn       PCR             NULL
        1234567    2018-33-33 2018-33-33 Mister          X             2999-33-33   00000000000 sepi         sepi         are          tmpo      nn       PCR             NULL
        1234567    2018-33-33 2018-33-33 Misses          Y             2999-33-33   00000000000 sepi         sepi         are          tmpo      nn       PCR             Klinisch
        ========== ========== ========== =============== ============= ============ =========== ============ ============ ============ ========= ======== =============== =================

        A Risk() object will be created from each line received. These risk objects have the following arguments:

        - auftrag_nr
        - erfassung
        - entnahme
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
        same is true for VRE screening entries set to *Ausland*, which refer to patients having been transported from
        abroad to Switzerland and being screened here upon hospital entry.

        For most screenings however, the ``screening_context`` will be set to NULL. In this case, the context must be
        **extracted** from the data available in the model. This process is tedious and involves the following steps:

        1) Extract Stay() (in german: *Aufenthalt*) objects for screening which were available to the patient at the
            time of ``erfassung``. This **must** be a single stay, as a patient cannot be stationed in two different
            wards simultaneously.
        2) Extract the Ward() for the Stay() extracted in step 1, and add the ward name to the ``self.ward_name``
            attribute.
        3) Extract the exact Room() name where the patient was located from the Stay(), and add it to the
            ``self.room_name`` attribute. This room name is sometimes indicated for Stays(), but will not be available
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
        risk_screening_df = pd.read_csv(csv_path, encoding=encoding, parse_dates=["Record Date"], dtype=str)

        # in principle they are all int, history makes them a varchar/string
        # risk_df["Patient ID"] = risk_df["Patient ID"].astype(int)

        if from_range is not None:
            risk_screening_df = risk_screening_df.loc[risk_screening_df['Record Date'] > from_range]

        if to_range is not None:
            risk_screening_df = risk_screening_df.loc[risk_screening_df['Record Date'] <= to_range]

        risk_screening_objects = list(map(lambda row: RiskScreening(*row), tqdm(risk_screening_df.values.tolist(), disable=not is_verbose)))
        stay_wards = []
        screening_wards = []
        logging.debug("adding_all_screenings_to_patients")
        nr_pat_not_found = 0
        nr_pat_no_relevant_stays_found = 0
        nr_ok = 0
        nr_screenings_positive = 0
        for risk_screening in tqdm(risk_screening_objects, disable=not is_verbose):
            if risk_screening.patient_id not in patient_dict.keys():  # Check whether or not PID exists
                nr_pat_not_found += 1
                continue

            # check if patient had an official stay at the hospital during the risk
            #potential_stays = patient_dict.get(risk.patient_id).get_stays_at_date(risk.recording_date)
            #if True: # len(potential_stays) > 0:  # indicates at least one potential match
            patient_dict[risk_screening.patient_id].add_risk_screening(risk_screening)
            nr_ok += 1

            if risk_screening.is_positive():
                nr_screenings_positive += 1

                # stay_wards.append('+'.join([each_stay.department for each_stay in potential_stays]))
                # screening_wards.append(this_risk.options)
                # print results to file
                # with open('match_results.txt', 'w') as writefile:
                #     for i in range(len(stay_wards)):
                #         writefile.write(f"{stay_wards[i]}; {screening_wards[i]}\n")
            #else:
            #    nr_pat_no_relevant_stays_found += 1
            #    continue

        logging.info(f"{nr_ok} screenings added, {nr_screenings_positive} positive screenings, {nr_pat_not_found} patients from screening data not found, {nr_pat_no_relevant_stays_found} patients with no relevant stays found.")
    # TODO: Leads to stackoverflow
    # TODO [BE]: How to solve printing objects reasonably such that they are printed extensively when on recursion level 0, else in reduced form?
    # def __repr__(self):
    #     return str(dict((key, value) for key, value in self.__dict__.items()
    #             if not callable(value) and not key.startswith('__')))
    #
    # def __str__(self):
    #     return self.__repr__()