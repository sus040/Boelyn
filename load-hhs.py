import psycopg
import argparse
import os
import pandas as pd
import numpy as np
from credentials import DBNAME, USER, PASSWORD  # check credentials_template.py
from helpers import hospital_insert, beds_insert, \
    report_insert_results, filter_duplication_errors

parser = argparse.ArgumentParser()
parser.add_argument("filename", action="store")
parser.add_argument("-d", "--debug", action="store_true")
args = parser.parse_args()


if not os.path.exists("error"):
    os.makedirs("error")

###########################
# Data Reading & Cleaning #
###########################
try:
    batch = pd.read_csv(args.filename)
except FileNotFoundError:
    print(f"File not found: {args.filename}")
    raise FileNotFoundError
print("Successfully read:", len(batch), "rows from file.")

# Data Cleaning
replacements = {'-999999.0': None, '-9999': None, -999999.0: None, -9999: None,
                'NA': None, np.nan: None}
batch.replace(to_replace=replacements, inplace=True)
batch['collection_week'] = pd.to_datetime(batch['collection_week'])

# Duplicate check
duplicate_rows = batch[batch.duplicated(
    subset=['hospital_pk', 'collection_week'], keep=False)]
batch = batch.drop_duplicates(subset=['hospital_pk', 'collection_week'])
duplicate_rows.to_csv("error/hhs_duplicated.csv", index=False)
print(str(len(duplicate_rows)),
      "duplicated rows output to error/hhs_duplicated.csv",
      sep=" ")

# Enforcing non-nullity
check_ridx = (batch["hospital_pk"].isnull()) | \
    (batch["collection_week"].isnull())
nulls = batch[check_ridx]
nulls.to_csv("error/hhs_nulls.csv", index=False)
print(str(len(nulls)),
      "rows with nulls in key columns output to error/hhs_nulls.csv",
      sep=" ")
batch = batch[~check_ridx]

# Enforcing non-negativity
check_ridx = (batch["all_adult_hospital_beds_7_day_avg"] < 0) | \
    (batch["all_pediatric_inpatient_beds_7_day_avg"] < 0) | \
    (batch["all_adult_hospital_inpatient_bed_occupied"
           "_7_day_coverage"] < 0) | \
    (batch["all_pediatric_inpatient_bed_occupied_7_day_avg"] < 0) | \
    (batch["total_icu_beds_7_day_avg"] < 0) | \
    (batch["icu_beds_used_7_day_avg"] < 0) | \
    (batch["inpatient_beds_used_covid_7_day_avg"] < 0) | \
    (batch["staffed_icu_adult_patients_confirmed_covid_7_day_avg"] < 0)
negatives = batch[check_ridx]
negatives.to_csv("error/hhs_negatives.csv", index=False)
print(str(len(negatives)),
      "rows with negative beds values output to error/hhs_negatives.csv",
      sep=" ")
batch = batch[~check_ridx]

#######################
# Database Connection #
#######################
try:
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname=DBNAME, user=USER, password=PASSWORD
    )
except Exception as e:
    print(f"Database error (try checking credentials.py): {e}")


cur = conn.cursor()

#############
# Insertion #
#############

# Insert hospital data, write errors to the errors folder
results = hospital_insert(conn, cur, batch)
results = filter_duplication_errors(results, "error/hospital_dup_cases.csv",
                                    "error/hospital_dup_msgs.csv")
report_insert_results(results, "hospital",
                      "error/hospital_errors.csv", "error/hospital_msgs.csv")
conn.commit()

# Insert beds data, write errors to the errors folder
results = beds_insert(conn, cur, batch)
# results = filter_duplication_errors(results, "error/beds_dup_cases.csv",
#                                     "error/beds_dup_msgs.csv")
report_insert_results(results, "beds",
                      "error/beds_errors.csv", "error/beds_msgs.csv")
conn.commit()

conn.close()
