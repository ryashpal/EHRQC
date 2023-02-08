# **EHRQC**

EHR-QC is a complete end-to-end pipeline to standardise and preprocess Electronic Health Records (EHR) for downstream integrative machine learning applications. This utility has two distinct modules;

1. Standardisation
2. Pre-processing

Both the modules can be run as a single end-to-end pipeline or individual components can be run in a standalone manner.

This utility is primarily focussed to provide a domain specific toolset for performing commmon standardisation and pre-processing tasks while handling the healthcare data. A command line interface is designed to provide an abstraction over the internal implementation details while at the same time being easy to use for anyone with basic Linux skills.

## Installation

### Clone the repository from GitHub.

```shell
git clone git@github.com:ryashpal/EHRQC.git
```

### Create a python virtual environment and activate it

```shell
python -m venv .venv
source .venv/bin/activate
```

### Install the required dependencies

```shell
pip install -r requirements.txt
```

## Configuration

The database connection details needs to be updated in the configuration file before using the utility;

```
# database connection details
db_details = {
    "sql_host_name": 'localhost',
    "sql_port_number": 5434,
    "sql_user_name": 'postgres',
    "sql_password": 'mysecretpassword',
    "sql_db_name": 'mimic4',
}
```

Additionally, the following configurations are required for migration to OMOP-CDM;


1. New schema names

*Schema names to create and host the migrated tables*

```bash
lookup_schema_name: New schema name to host the vocabulary tables

etl_schema_name: New schema name to host the temporary migration tables

cdm_schema_name: New schema name to host the CDM tables
```


2. Vocabulary files path

*File paths of the Athena vocabulary files and the custom mapping*

```bash
vocabulary = {
    'concept': '/path/to/CONCEPT.csv',
    'vocabulary': '/path/to/VOCABULARY.csv',
    'domain': '/path/to/DOMAIN.csv',
    'concept_class': '/path/to/CONCEPT_CLASS.csv',
    'concept_relationship': '/path/to/CONCEPT_RELATIONSHIP.csv',
    'relationship': '/path/to/RELATIONSHIP.csv',
    'concept_synonym': '/path/to/CONCEPT_SYNONYM.csv',
    'concept_ancestor': '/path/to/CONCEPT_ANCESTOR.csv',
    'tmp_custom_mapping': '/path/to/tmp_custom_mapping.csv',
}
```


3. CSV file column mapping

*CSV file paths containing EHR data and the column mappings*

```bash
patients = {

    file_name: Path for the csv file
    
    column_mapping: {
    
        -- column name in the file: standard column name,
        
    },
    
}
```

For example, the mapping information for entity `Patients` will be as shown below;

```bash
patients = {
    'file_name': '/path/to/patients.csv',
    'column_mapping': {
        'subject_id': "<Subject ID column name in the csv file>",
        'gender': "<Gender column name in the csv file>",
        'anchor_age': "<Age column name in the csv file>",
        'anchor_year': "<Year column name in the csv file>",
        'dod': "<DOD column name in the csv file>"
    },
}
```

# **Add all the other entitiy columns here**

## Workflow

![image](https://user-images.githubusercontent.com/56529301/215373211-c7a311f8-e8ed-4740-a565-1bedfe512ec8.png)


# Standardisation

## OMOP-CDM migration

### To obtain help menu

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -h
```

or

 ```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run --help
```

This will display the following help menu;

```
usage: Run.py [-h] [-l] [-f] [-s] [-m] [-c] [-e] [-u]

Migrate EHR to OMOP-CDM

optional arguments:
  -h, --help            show this help message and exit
  -l, --create_lookup   Create lookup by importing Athena vocabulary and custom mapping
  -f, --import_file     Import EHR from a csv files
  -s, --stage           Stage the data on the ETL schema
  -m, --generate_mapping
                        Generate custom mapping of concepts from the data
  -c, --import_custom_mapping
                        Import custom mapping file
  -e, --perform_etl     Perform migration Extract-Transform-Load (ETL) operations
  -u, --unload          Unload data to CDM schema
```

### Create lookup by importing Athena vocabulary and custom mapping

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -l
```

This function will import Athena vocabulary files from the path specified in the configuration files (`vocabulary` attribute) in to the lookup schema specified by `lookup_schema_name`.

### Import EHR from a csv files

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -f
```

This function will import EHR data from csv files from the path specified in the configuration files (under every entity) in to the source schema specified by `source_schema_name`.

### Stage the data on the ETL schema

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -s
```

This function will stage the data from source schema into the etl schema specified by `etl_schema_name` attrbibute in the configuration file.

### Generate custom mapping of concepts from the data

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -m
```

This function will automatically generate mappings for the specified vocabularies in the configuration file under `customMapping` attribute and update the vocabulary in the database with mapped automatically concepts.

### Import custom mapping file

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -c
```

This function will import manually generated custom mappings from the csv file specified under `vocabulary>tmp_custom_mapping` attribute in the configuration file and update the vocabulary in the database with manually mapped concepts.

### Perform migration Extract-Transform-Load (ETL) operations

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -e
```

This function will perform the Extract-Transform-Load (ETL) operations necessary to format the source data as per the OMOP-CDM schema and stores the final tables in etl schema.

### Unload data to CDM schema

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.Run -u
```

This function will unload the final tables from the lookup and etl shema to the destination schema called cdm schema.

## Concept Mapping

### To obtain help menu

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.ConceptMapper -h
```

or

 ```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.ConceptMapper --help
```

This will display the following help menu;

```
usage: ConceptMapper.py [-h] [--vocab_path VOCAB_PATH] [--cdb_path CDB_PATH] [--mc_status_path MC_STATUS_PATH] [--model_pack_path MODEL_PACK_PATH]
                        domain_id vocabulary_id concept_class_id concepts_path concept_name_row mapped_concepts_save_path

Perform concept mapping

positional arguments:
  domain_id             Domain ID of the standard vocabulary to be mapped
  vocabulary_id         Vocabulary ID of the standard vocabulary to be mapped
  concept_class_id      Concept class ID of the standard vocabulary to be mapped
  concepts_path         Path for the concepts csv file
  concept_name_row      Name of the concept name row in the concepts csv file
  mapped_concepts_save_path
                        Path for saving the mapped concepts csv file

optional arguments:
  -h, --help            show this help message and exit
  --vocab_path VOCAB_PATH
                        Path for the Medcat vocab file
  --cdb_path CDB_PATH   Path for the Medcat cdb file
  --mc_status_path MC_STATUS_PATH
                        Path for the Medcat mc_status folder
  --model_pack_path MODEL_PACK_PATH
                        Path for the Medcat model_pack_path zip file
```

### Generate custom mappings for review

```shell
.venv/bin/python -m ehrqc.standardise.migrate_omop.ConceptMapper ....
```

This function will create mappings for the requested concepts and save it as a csv file.


## Pre-processing

## Extract


> :warning: **`Note 1: The extract module is required if the EHR data exists in a relational database`**

> :warning: **`Note 2: If the EHR data is in the flat file (.csv), please proceed to next sections`**

### To obtain help menu

```shell
.venv/bin/python -m ehrqc.extract.Extract -h
```

This will display the following help menu;

```
usage: Extract.py [-h] save_path source_db data_type schema_name

EHRQC

positional arguments:
  save_path    Path of the file to store the outputs
  source_db    Source name [mimic, omop]
  data_type    Data type name [demographics, vitals, lab_measurements]
  schema_name  Source schema name

optional arguments:
  -h, --help   show this help message and exit
```

### To extract Demographic data from OMOP schema

```shell
.venv/bin/python -m ehrqc.extract.Extract temp/omop_demograpics.csv omop demographics omop_cdm
```
This will extract the demographics data from omop schema and store it in the `save_path`.

### To extract Vitals data from OMOP schema

```shell
.venv/bin/python -m ehrqc.extract.Extract temp/omop_vitals.csv omop vitals omop_cdm
```
This will extract the vitals data from omop schema and store it in the `save_path`.

### To extract Lab Measurements data from OMOP schema

```shell
.venv/bin/python -m ehrqc.extract.Extract temp/omop_lab_measurements.csv omop lab_measurements omop_cdm
```
This will extract the lab measurements data from omop schema and store it in the `save_path`.

### To extract Demographic data from MIMIC schema

```shell
.venv/bin/python -m ehrqc.extract.Extract temp/mimic_demographics.csv mimic demographics mimiciv
```
This will extract the demographics data from mimic schema and store it in the `save_path`.

### To extract Vitals data from MIMIC schema

```shell
.venv/bin/python -m ehrqc.extract.Extract temp/mimic_vitals.csv mimic vitals mimiciv
```
This will extract the vitals data from mimic schema and store it in the `save_path`.

### To extract Lab Measurements data from MIMIC schema

```shell
.venv/bin/python -m ehrqc.extract.Extract temp/mimic_lab_measurements.csv mimic lab_measurements mimiciv
```
This will extract the lab measurements data from mimic schema and store it in the `save_path`.

## Plot

### To obtain help menu

```shell
usage: Plot.py [-h] [-c COLUMN_MAPPING] plot_type source_path save_path

EHRQC

positional arguments:
  plot_type             Type of plot to generate [demographics_explore, vitals_explore, lab_measurements_explore, vitals_outliers,
                        lab_measurements_outliers]
  source_path           Source data path
  save_path             Path of the file to store the output

optional arguments:
  -h, --help            show this help message and exit
  -c COLUMN_MAPPING, --column_mapping COLUMN_MAPPING
```

The column mapping has to be in json format as shown below;

```
'{"expected_column_name": "custom_column_name"}'
```

For instance, if the "Age" attribute in demographics csv file is under the column name "Number of Years" instead of default "age" column name, then the following mapping can be applied;

```
'{"age": "Number of Years"}'
```

Similarly, more than one columns can be mapped in this manner;

```
'{"age": "Number of Years", "gender": "Sex"}'
```

### To plot exploration graphs for demographics

```shell
.venv/bin/python -m ehrqc.qc.Plot demographics_explore temp/mimic_demographics.csv temp/mimic_demographics_explore.html -c {}
```

This will generate QC plots from the demograhic data obtained from the `source_path` and save it in the `save_path`. If the source csv file is not in a standard format, then a `column_mapping` needs to be provided.

This utility expects the file to contain the information under the following columns;

| Expected Column Name | Column Details |
|-----------|-------------|
| age | Age of the person |
| weight | Weight of the person |
| height | Height of the person |
| gender | Gender of the person |
| ethnicity | Ethnicity of the person |

#### Example Output

[Demographics Plots](https://ryashpal.github.io/EHRQC/demographics.html)

### To plot exploration graphs for vitals

```shell
.venv/bin/python -m ehrqc.qc.Plot vitals_explore temp/mimic_vitals.csv temp/mimic_vitals_explore.html
```

This will generate QC plots from the vitals data obtained from the `source_path` and save it in the `save_path`. If the source csv file is not in a standard format, then a `column_mapping` needs to be provided.

This utility expects the file to contain the information under the following columns;

| Expected Column Name | Column Details |
|-----------|-------------|
| heartrate | Heart Rate |
| sysbp | Systolic Blood Pressure |
| diabp | Diastolic Blood Pressure |
| meanbp | Mean Blood Pressure |
| resprate | Respiratory Rate |
| tempc | Temperature |
| spo2 | Oxygen Saturation |
| gcseye | Glasgow Coma Scale - Eye Response |
| gcsverbal | Glasgow Coma Scale - Verbal Response |
| gcsmotor | Glasgow Coma Scale - Motor Response |

#### Example Output

[Vitals Plots](https://ryashpal.github.io/EHRQC/vitals.html)

### To plot exploration graphs for lab measurements

```shell
.venv/bin/python -m ehrqc.qc.Plot lab_measurements_explore temp/mimic_lab_measurements.csv temp/mimic_lab_measurements_explore.html
```

This will generate QC plots from the lab measurements data obtained from the `source_path` and save it in the `save_path`. If the source csv file is not in a standard format, then a `column_mapping` needs to be provided.

This utility expects the file to contain the information under the following columns;

| Expected Column Name | Column Details |
|-----------|-------------|
| glucose | Glucose |
| hemoglobin | Hemoglobin |
| anion_gap | Anion Gap |
| bicarbonate | Bicarbonate |
| calcium_total | Calcium Total |
| chloride | Chloride |
| creatinine | Creatinine |
| magnesium | Magnesium |
| phosphate | Phosphate |
| potassium | Potassium |
| sodium | Sodium |
| urea_nitrogen | Urea Nitrogen |
| hematocrit | Hematocrit |
| mch | Mean Cell Hemoglobin |
| mchc | Mean Corpuscular Hemoglobin Concentration |
| mcv | Mean Corpuscular Volume |
| platelet_count | Platelet Count |
| rdw | Red cell Distribution Width |
| red_blood_cells | Red Blood Cells |
| white_blood_cells | White Blood Cells |

#### Example Output

[Lab measurements Plots](https://ryashpal.github.io/EHRQC/lab_measurements.html)

### To plot outliers graphs for vitals

```shell
.venv/bin/python -m ehrqc.qc.Plot vitals_outliers temp/mimic_vitals_imputed.csv temp/mimic_vitals_outliers.html
```

This will generate QC plots from the vitals data obtained from the `source_path` and save it in the `save_path`. If the source csv file is not in a standard format, then a `column_mapping` needs to be provided.

This utility expects the file to contain the information under the following columns;


| Expected Column Name | Column Details |
|-----------|-------------|
| heartrate | Heart Rate |
| sysbp | Systolic Blood Pressure |
| diabp | Diastolic Blood Pressure |
| meanbp | Mean Blood Pressure |
| resprate | Respiratory Rate |
| tempc | Temperature |
| spo2 | Oxygen Saturation |
| gcseye | Glasgow Coma Scale - Eye Response |
| gcsverbal | Glasgow Coma Scale - Verbal Response |
| gcsmotor | Glasgow Coma Scale - Motor Response |


#### Example Output

[Vitals outliers Plots](https://ryashpal.github.io/EHRQC/vitals_outliers.html)

### To plot outliers graphs for lab measurements

```shell
.venv/bin/python -m ehrqc.qc.Plot lab_measurements_outliers temp/mimic_lab_measurements_imputed.csv temp/mimic_lab_measurements_outliers.html
```

This will generate QC plots from the lab measurements data obtained from the `source_path` and save it in the `save_path`. If the source csv file is not in a standard format, then a `column_mapping` needs to be provided.

This utility expects the file to contain the information under the following columns;

| Expected Column Name | Column Details |
|-----------|-------------|
| glucose | Glucose |
| hemoglobin | Hemoglobin |
| anion_gap | Anion Gap |
| bicarbonate | Bicarbonate |
| calcium_total | Calcium Total |
| chloride | Chloride |
| creatinine | Creatinine |
| magnesium | Magnesium |
| phosphate | Phosphate |
| potassium | Potassium |
| sodium | Sodium |
| urea_nitrogen | Urea Nitrogen |
| hematocrit | Hematocrit |
| mch | Mean Cell Hemoglobin |
| mchc | Mean Corpuscular Hemoglobin Concentration |
| mcv | Mean Corpuscular Volume |
| platelet_count | Platelet Count |
| rdw | Red cell Distribution Width |
| red_blood_cells | Red Blood Cells |
| white_blood_cells | White Blood Cells |

#### Example Output

[Lab measurements outliers Plots](https://ryashpal.github.io/EHRQC/lab_measurements_outliers.html)

## Impute

### To obtain help menu

```shell
.venv/bin/python -m ehrqc.qc.Impute -h
```

This will display the following help menu;

```
usage: Impute.py [-h] [-sp SAVE_PATH] [-a ALGORITHM] action source_path

EHRQC

positional arguments:
  action                Action to perform [compare, impute]
  source_path           Source data path

optional arguments:
  -h, --help            show this help message and exit
  -sp SAVE_PATH, --save_path SAVE_PATH
                        Path of the file to store the outputs (required only for action=impute)
  -a ALGORITHM, --algorithm ALGORITHM
                        Missing data imputation algorithm [mean, median, knn, miss_forest, expectation_maximization, multiple_imputation]
```

### To compare different imputation algorithms

```shell
.venv/bin/python -m ehrqc.qc.Impute 'compare' temp/mimic_vitals.csv
```

This will create a random missingness in the data and compare 6 different missing data algorithms [mean, median, knn, miss forest, expectation maximisation, multiple imputation] and report their reconstriction r-squared scores.

### To impute data

```shell
.venv/bin/python -m ehrqc.qc.Impute impute 'temp/mimic_vitals.csv' -sp='temp/mimic_vitals_imputed.csv' -a=mean
```

This will impute missing values in the data obtained from the `source_path` using the specified algorithm and save it in the `save_path`. This function support the following algorithms

- mean
- median
- knn
- miss forest
- expectation maximisation
- multiple imputation


## Pre-processing Pipeline

### To obtain help menu

```shell
.venv/bin/python -m ehrqc.qc.Pipeline -h
```

or

 ```shell
.venv/bin/python -m ehrqc.qc.Pipeline --help
```

This will display the following help menu;

```
usage: Pipeline.py [-h] [-d] [-i] save_path source_db data_type schema_name

EHRQC

positional arguments:
  save_path             Path of the folder to store the outputs
  source_db             Source name [mimic, omop]
  data_type             Data type name [demographics, vitals, lab_measurements]
  schema_name           Source schema name

optional arguments:
  -h, --help            show this help message and exit
  -d, --draw_graphs     Draw graphs to visualise EHR data quality
  -i, --impute_missing  Impute missing values by automatically selecting the best imputation strategy for this data
```

### To extract Demographic data from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop demographics omop_cdm
```
This will create a csv file containing the raw data with the name `omop_demographics_raw_data.csv` in the `save_path`.

### To extract Vitals data from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop vitals omop_cdm
```
This will create a csv file containing the raw data with the name `omop_vitals_raw_data.csv` in the `save_path`.

### To extract Lab Measurements data from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop lab_measurements omop_cdm
```
This will create a csv file containing the raw data with the name `omop_lab_measurements_raw_data.csv` in the `save_path`.

### To extract Demographic data from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic demographics mimiciv
```
This will create a csv file containing the raw data with the name `mimic_demographics_raw_data.csv` in the `save_path`.

### To extract Vitals data from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic vitals mimiciv
```
This will create a csv file containing the raw data with the name `mimic_vitals_raw_data.csv` in the `save_path`.

### To extract Lab Measurements data from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic lab_measurements mimiciv
```
This will create a csv file containing the raw data with the name `mimic_lab_measurements_raw_data.csv`, and a  in the `save_path`.

### Demographics Graphs from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic demographics mimiciv -d
```
This will create a csv file containing the raw data with the name `mimic_demographics_raw_data.csv`, and a html file containing the generated graphs with the name `mimic_demographics_plots.html` in the `save_path`.

### Demographics Graphs from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop demographics omop_cdm -d
```
This will create a csv file containing the raw data with the name `omop_demographics_raw_data.csv`, and a html file containing the generated graphs with the name `omop_demographics_plots.html` in the `save_path`.

### Vitals Graphs from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic vitals mimiciv -d
```
This will create a csv file containing the raw data with the name `mimic_vitals_raw_data.csv`, and a html file containing the generated graphs with the name `mimic_vitals_plots.html` in the `save_path`.

### Vitals Graphs from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop vitals omop_cdm -d
```
This will create a csv file containing the raw data with the name `omop_vitals_raw_data.csv`, and a html file containing the generated graphs with the name `omop_vitals_plots.html` in the `save_path`.

### Lab Measurements Graphs from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic lab_measurements mimiciv -d
```
This will create a csv file containing the raw data with the name `mimic_lab_measurements_raw_data.csv`, and a html file containing the generated graphs with the name `mimic_lab_measurements_plots.html` in the `save_path`.

### Lab Measurements Graphs from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop lab_measurements omop_cdm -d
```
This will create a csv file containing the raw data with the name `omop_lab_measurements_raw_data.csv`, and a html file containing the generated graphs with the name `omop_lab_measurements_plots.html` in the `save_path`.

### Missing Data Imputation for Vitals from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic vitals mimiciv -d -i
```
This will create a csv file containing the raw data with the name `mimic_vitals_raw_data.csv`, a csv file containing the imputed data with the name `mimic_vitals_imputed_data.csv`, and a html file containing the generated graphs with the name `mimic_vitals_plots.html` in the `save_path`.

### Missing Data Imputation for Vitals from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop vitals omop_cdm -d -i
```
This will create a csv file containing the raw data with the name `omop_vitals_raw_data.csv`, a csv file containing the imputed data with the name `omop_vitals_imputed_data.csv`, and a html file containing the generated graphs with the name `omop_vitals_plots.html` in the `save_path`.

### Missing Data Imputation for Lab Measurements from MIMIC schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp mimic lab_measurements mimiciv -d -i
```
This will create a csv file containing the raw data with the name `mimic_lab_measurements_raw_data.csv`, a csv file containing the imputed data with the name `mimic_lab_measurements_imputed_data.csv`, and a html file containing the generated graphs with the name `mimic_lab_measurements_plots.html` in the `save_path`.

### Missing Data Imputation for Lab Measurements from OMOP schema

```shell
.venv/bin/python -m ehrqc.qc.Pipeline temp omop lab_measurements omop_cdm -d -i
```
This will create a csv file containing the raw data with the name `omop_lab_measurements_raw_data.csv`, a csv file containing the imputed data with the name `omop_lab_measurements_imputed_data.csv`, and a html file containing the generated graphs with the name `omop_lab_measurements_plots.html` in the `save_path`.


## Acknowledgements

<img src="https://user-images.githubusercontent.com/56529301/155898403-c453ab3f-df17-45c8-ac0a-b314461f5e8f.png" 
alt="the-alfred-hospital-logo" width="100"/>
<img src="https://user-images.githubusercontent.com/56529301/155898442-ba8dcbb1-14dd-4c8b-96e6-e02c6a632c0e.png" alt="the-alfred-hospital-logo" width="150"/>
<img src="https://user-images.githubusercontent.com/56529301/155898475-a5244ab5-e16e-4e5d-b562-6a89a7c2b7b7.png" alt="Superbug_AI_Branding_FINAL" width="150"/>
