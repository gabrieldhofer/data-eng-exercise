data-engineering-assessment
===========================

Description
-----------

Given the CMS provider data metastore, write a script that downloads all data sets related to the theme "Hospitals".

The column names in the csv headers are currently in mixed case with spaces and special characters. Convert all column names to snake_case. (Example: "Patients' rating of the faciilty linear mean score" becomes "patients_rating_of_the_facility_linear_mean_score").

The csv files should be downloaded and processed in parallel, and the job should be designed to run every day, but only download files that have been modified since the previous run (need to track runs/metadata).

https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

Usage:
------

This program was developed in Google Colab.

Install dependencies:

    $ pip install -r ./requirements.txt

    $ make clean

    $ make init

Then, run the "make" command to run main.py.

    $ make


