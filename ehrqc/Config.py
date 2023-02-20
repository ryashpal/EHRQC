# database connection details
db_details = {
    "sql_host_name": 'localhost',
    "sql_port_number": 5434,
    "sql_user_name": 'postgres',
    "sql_password": 'mysecretpassword',
    "sql_db_name": 'mimic4',
}

# new schema to host the migrated tables

lookup_schema_name = 'vocabulary_test_20220207'

source_schema_name = 'omop_migration_source_20220207'

etl_schema_name = 'omop_migration_etl_20220207'

cdm_schema_name = 'omop_test_20220207'

# Vocabulary files path

vocabulary = {
    'concept': '/superbugai-data/vocabulary_download_v5/CONCEPT.csv',
    'vocabulary': '/superbugai-data/vocabulary_download_v5/VOCABULARY.csv',
    'domain': '/superbugai-data/vocabulary_download_v5/DOMAIN.csv',
    'concept_class': '/superbugai-data/vocabulary_download_v5/CONCEPT_CLASS.csv',
    'concept_relationship': '/superbugai-data/vocabulary_download_v5/CONCEPT_RELATIONSHIP.csv',
    'relationship': '/superbugai-data/vocabulary_download_v5/RELATIONSHIP.csv',
    'concept_synonym': '/superbugai-data/vocabulary_download_v5/CONCEPT_SYNONYM.csv',
    'concept_ancestor': '/superbugai-data/vocabulary_download_v5/CONCEPT_ANCESTOR.csv',
    'tmp_custom_mapping': '/superbugai-data/vocabulary_download_v5/tmp_custom_mapping_tmp.csv',
}

# CSV file column mapping

patients = {
    'file_name': '/superbugai-data/mimiciv/test_data/patients.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'gender': 'gender',
        'anchor_age': 'anchor_age',
        'anchor_year': 'anchor_year',
        'anchor_year_group': 'anchor_year_group',
        'dod': 'dod'
    },
}

admissions = {
    'file_name': '/superbugai-data/mimiciv/test_data/admissions.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'admittime': 'admittime',
        'dischtime': 'dischtime',
        'deathtime': 'deathtime',
        'admission_type': 'admission_type',
        'admission_location': 'admission_location',
        'discharge_location': 'discharge_location',
        'insurance': 'insurance',
        'language': 'language',
        'marital_status': 'marital_status',
        'ethnicity': 'ethnicity',
        'edregtime': 'edregtime',
        'edouttime': 'edouttime',
        'hospital_expire_flag': 'hospital_expire_flag',
    },
}

transfers = {
    'file_name': '/superbugai-data/mimiciv/test_data/transfers.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'transfer_id': 'transfer_id',
        'eventtype': 'eventtype',
        'careunit': 'careunit',
        'intime': 'intime',
        'outtime': 'outtime',
    },
}

diagnoses = {
    'file_name': '/superbugai-data/mimiciv/test_data/diagnoses_icd.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'seq_num': 'seq_num',
        'icd_code': 'icd_code',
        'icd_version': 'icd_version',
    },
}

services = {
    'file_name': '/superbugai-data/mimiciv/test_data/services.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'transfertime': 'transfertime',
        'prev_service': 'prev_service',
        'curr_service': 'curr_service',
    },
}

labevents = {
    'file_name': '/superbugai-data/mimiciv/test_data/labevents.csv',
    'column_mapping': {
        'labevent_id': 'labevent_id',
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'specimen_id': 'specimen_id',
        'itemid': 'itemid',
        'charttime': 'charttime',
        'storetime': 'storetime',
        'value': 'value',
        'valuenum': 'valuenum',
        'valueuom': 'valueuom',
        'ref_range_lower': 'ref_range_lower',
        'ref_range_upper': 'ref_range_upper',
        'flag': 'flag',
        'priority': 'priority',
        'comments': 'comments',
    },
}

labitems_lookup = {
    'file_name': '/superbugai-data/mimiciv/test_data/d_labitems.csv',
    'column_mapping': {
        'itemid': 'itemid',
        'label': 'label',
        'fluid': 'fluid',
        'category': 'category',
        'loinc_code': 'loinc_code',
    },
}

procedures = {
    'file_name': '/superbugai-data/mimiciv/test_data/procedures_icd.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'seq_num': 'seq_num',
        'chartdate': 'chartdate',
        'icd_code': 'icd_code',
        'icd_version': 'icd_version',
    },
}

hcpcsevents = {
    'file_name': '/superbugai-data/mimiciv/test_data/hcpcsevents.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'chartdate': 'chartdate',
        'hcpcs_cd': 'hcpcs_cd',
        'seq_num': 'seq_num',
        'short_description': 'short_description',
    },
}

drugcodes = {
    'file_name': '/superbugai-data/mimiciv/test_data/drgcodes.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'drg_type': 'drg_type',
        'drg_code': 'drg_code',
        'description': 'description',
        'drg_severity': 'drg_severity',
        'drg_mortality': 'drg_mortality',
    },
}

prescriptions = {
    'file_name': '/superbugai-data/mimiciv/test_data/prescriptions.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'pharmacy_id': 'pharmacy_id',
        'starttime': 'starttime',
        'stoptime': 'stoptime',
        'drug_type': 'drug_type',
        'drug': 'drug',
        'gsn': 'gsn',
        'ndc': 'ndc',
        'prod_strength': 'prod_strength',
        'form_rx': 'form_rx',
        'dose_val_rx': 'dose_val_rx',
        'dose_unit_rx': 'dose_unit_rx',
        'form_val_disp': 'form_val_disp',
        'form_unit_disp': 'form_unit_disp',
        'doses_per_24_hrs': 'doses_per_24_hrs',
        'route': 'route',
    },
}

microbiologyevents = {
    'file_name': '/superbugai-data/mimiciv/test_data/microbiologyevents.csv',
    'column_mapping': {
        'microevent_id': 'microevent_id',
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'micro_specimen_id': 'micro_specimen_id',
        'chartdate': 'chartdate',
        'charttime': 'charttime',
        'spec_itemid': 'spec_itemid',
        'spec_type_desc': 'spec_type_desc',
        'test_seq': 'test_seq',
        'storedate': 'storedate',
        'storetime': 'storetime',
        'test_itemid': 'test_itemid',
        'test_name': 'test_name',
        'org_itemid': 'org_itemid',
        'org_name': 'org_name',
        'isolate_num': 'isolate_num',
        'quantity': 'quantity',
        'ab_itemid': 'ab_itemid',
        'ab_name': 'ab_name',
        'dilution_text': 'dilution_text',
        'dilution_comparison': 'dilution_comparison',
        'dilution_value': 'dilution_value',
        'interpretation': 'interpretation',
        'comments': 'comments',
    },
}

pharmacy = {
    'file_name': '/superbugai-data/mimiciv/test_data/pharmacy.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'pharmacy_id': 'pharmacy_id',
        'poe_id': 'poe_id',
        'starttime': 'starttime',
        'stoptime': 'stoptime',
        'medication': 'medication',
        'proc_type': 'proc_type',
        'status': 'status',
        'entertime': 'entertime',
        'verifiedtime': 'verifiedtime',
        'route': 'route',
        'frequency': 'frequency',
        'disp_sched': 'disp_sched',
        'infusion_type': 'infusion_type',
        'sliding_scale': 'sliding_scale',
        'lockout_interval': 'lockout_interval',
        'basal_rate': 'basal_rate',
        'one_hr_max': 'one_hr_max',
        'doses_per_24_hrs': 'doses_per_24_hrs',
        'duration': 'duration',
        'duration_interval': 'duration_interval',
        'expiration_value': 'expiration_value',
        'expiration_unit': 'expiration_unit',
        'expirationdate': 'expirationdate',
        'dispensation': 'dispensation',
        'fill_quantity': 'fill_quantity',
    },
}

procedureevents = {
    'file_name': '/superbugai-data/mimiciv/test_data/procedureevents.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'stay_id': 'stay_id',
        'starttime': 'starttime',
        'endtime': 'endtime',
        'storetime': 'storetime',
        'itemid': 'itemid',
        'value': 'value',
        'valueuom': 'valueuom',
        'location': 'location',
        'locationcategory': 'locationcategory',
        'orderid': 'orderid',
        'linkorderid': 'linkorderid',
        'ordercategoryname': 'ordercategoryname',
        'secondaryordercategoryname': 'secondaryordercategoryname',
        'ordercategorydescription': 'ordercategorydescription',
        'patientweight': 'patientweight',
        'totalamount': 'totalamount',
        'totalamountuom': 'totalamountuom',
        'isopenbag': 'isopenbag',
        'continueinnextdept': 'continueinnextdept',
        'cancelreason': 'cancelreason',
        'statusdescription': 'statusdescription',
        'comments_date': 'comments_date',
        'originalamount': 'originalamount',
        'originalrate': 'originalrate',
    },
}

items_lookup = {
    'file_name': '/superbugai-data/mimiciv/test_data/d_items.csv',
    'column_mapping': {
        'itemid': 'itemid',
        'label': 'label',
        'abbreviation': 'abbreviation',
        'linksto': 'linksto',
        'category': 'category',
        'unitname': 'unitname',
        'param_type': 'param_type',
        'lownormalvalue': 'lownormalvalue',
        'highnormalvalue': 'highnormalvalue',
    },
}

datetimeevents = {
    'file_name': '/superbugai-data/mimiciv/test_data/datetimeevents.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'stay_id': 'stay_id',
        'charttime': 'charttime',
        'storetime': 'storetime',
        'itemid': 'itemid',
        'value': 'value',
        'valueuom': 'valueuom',
        'warning': 'warning',
    },
}

chartevents = {
    'file_name': '/superbugai-data/mimiciv/test_data/chartevents.csv',
    'column_mapping': {
        'subject_id': 'subject_id',
        'hadm_id': 'hadm_id',
        'stay_id': 'stay_id',
        'charttime': 'charttime',
        'storetime': 'storetime',
        'itemid': 'itemid',
        'value': 'value',
        'valuenum': 'valuenum',
        'valueuom': 'valueuom',
        'warning': 'warning',
    },
}

customMapping = {
    'admission_type': {
        'source_attributes': {
            'field_name': 'admission_type',
            'table_name': 'src_admissions',
            'vocabulary_id': 'mimiciv_vis_admission_type',
            'where_condition': '',
        },
        'standard_attributes': {
            'domain_id': 'Visit',
            'vocabulary_id': 'CMS Place of Service',
            'concept_class_id': 'Visit',
            'key_phrase': '',
        },
    },
    # 'admission_location': {
    #     'source_attributes': {
    #         'field_name': 'admission_location',
    #         'table_name': 'src_admissions',
    #         'vocabulary_id': 'mimiciv_vis_admission_location',
    #         'where_condition': '',
    #     },
    #     'standard_attributes': {
    #         'domain_id': 'Observation',
    #         'vocabulary_id': 'SNOMED',
    #         'concept_class_id': 'Location',
    #         'key_phrase': '',
    #     },
    # },
    'discharge_location': {
        'source_attributes': {
            'field_name': 'discharge_location',
            'table_name': 'src_admissions',
            'vocabulary_id': 'mimiciv_vis_discharge_location',
            'where_condition': '',
        },
        'standard_attributes': {
            'domain_id': 'Observation',
            'vocabulary_id': 'SNOMED',
            'concept_class_id': 'Location',
            'key_phrase': '',
        },
    },
    'ethnicity': {
        'source_attributes': {
            'field_name': 'ethnicity',
            'table_name': 'src_admissions',
            'vocabulary_id': 'mimiciv_per_ethnicity',
            'where_condition': '',
        },
        'standard_attributes': {
            'domain_id': 'Race',
            'vocabulary_id': 'Race',
            'concept_class_id': 'Race',
            'key_phrase': '',
        },
    },
    'microtest': {
        'source_attributes': {
            'field_name': 'test_name',
            'table_name': 'src_microbiologyevents',
            'vocabulary_id': 'mimiciv_micro_microtest',
            'where_condition': '',
        },
        'standard_attributes': {
            'domain_id': 'Measurement',
            'vocabulary_id': 'SNOMED',
            'concept_class_id': 'Procedure',
            'key_phrase': '',
        },
    },
    'organism': {
        'source_attributes': {
            'field_name': 'org_name',
            'table_name': 'src_microbiologyevents',
            'vocabulary_id': 'mimiciv_micro_organism',
            'where_condition': '',
        },
        'standard_attributes': {
            'domain_id': 'Observation',
            'vocabulary_id': 'SNOMED',
            'concept_class_id': 'Organism',
            'key_phrase': '',
        },
    },
}
