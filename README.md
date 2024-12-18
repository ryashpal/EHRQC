# **EHRQC**

## Introduction

EHR-QC is a complete end-to-end pipeline to standardise and preprocess Electronic Health Records (EHR) for downstream integrative machine learning applications. This utility has two distinct modules;

1. Standardisation
2. Pre-processing

Both the modules can be run as a single end-to-end pipeline or individual components can be run in a standalone manner.

This utility is primarily focussed to provide a domain specific toolset for performing commmon standardisation and pre-processing tasks while handling the healthcare data. A command line interface is designed to provide an abstraction over the internal implementation details while at the same time being easy to use for anyone with basic Linux skills.

## Workflow

![ehr-qc](https://user-images.githubusercontent.com/56529301/232653693-0a3a778e-3b74-4b28-b352-f22bcecd2111.PNG)

## Quick Start Guide

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

## Documentation

For the most up-to-date documentation about installation, configuration, running, and use cases please refer to the EHR-QC [Documantation](https://ehr-qc-tutorials.readthedocs.io/en/latest/index.html) page.


## Cite Us

If you use this library for your research, please cite our paper:

Yashpal Ramakrishnaiah, Nenad Macesic, Geoffrey I. Webb, Anton Y. Peleg, Sonika Tyagi,

EHR-QC: A streamlined pipeline for automated electronic health records standardisation and preprocessing to predict clinical outcomes, Journal of Biomedical Informatics,

Volume 147, 2023, 104509, ISSN 1532-0464,

https://doi.org/10.1016/j.jbi.2023.104509.

(https://www.sciencedirect.com/science/article/pii/S1532046423002307)

BibTeX:

```
@article{RAMAKRISHNAIAH2023104509,
title = {EHR-QC: A streamlined pipeline for automated electronic health records standardisation and preprocessing to predict clinical outcomes},
journal = {Journal of Biomedical Informatics},
volume = {147},
pages = {104509},
year = {2023},
issn = {1532-0464},
doi = {https://doi.org/10.1016/j.jbi.2023.104509},
url = {https://www.sciencedirect.com/science/article/pii/S1532046423002307},
author = {Yashpal Ramakrishnaiah and Nenad Macesic and Geoffrey I. Webb and Anton Y. Peleg and Sonika Tyagi},
keywords = {Digital health, Electronic health records, EHR, Clinical outcome prediction, Machine learning},
}
```

## Acknowledgements

Our special thanks to;

* [`OHDSI`](https://www.ohdsi.org/)
* [`MIMIC IV`](https://physionet.org/content/mimiciv)
* [`Hugging face`](https://huggingface.co/)
* [`Medcat`](https://github.com/CogStack/MedCAT)
* [`Item Response Theory (IRT) - outlierensembles`](https://cran.r-project.org/web/packages/outlierensembles/index.html)

<img src="https://user-images.githubusercontent.com/56529301/155898403-c453ab3f-df17-45c8-ac0a-b314461f5e8f.png" 
alt="the-alfred-hospital-logo" width="100"/>
<img src="https://user-images.githubusercontent.com/56529301/155898442-ba8dcbb1-14dd-4c8b-96e6-e02c6a632c0e.png" alt="the-alfred-hospital-logo" width="150"/>
<img src="https://user-images.githubusercontent.com/56529301/155898475-a5244ab5-e16e-4e5d-b562-6a89a7c2b7b7.png" alt="Superbug_AI_Branding_FINAL" width="150"/>
