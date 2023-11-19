import psycopg
import argparse
import pandas as pd
import datetime
from credentials import DBNAME, USER, PASSWORD  # check credentials_template.py
from hhs_helpers import geocode, hospital_insert

parser = argparse.ArgumentParser()
parser.add_argument("filename", action="store")
parser.add_argument("-d", "--debug", action="store_true")
args = parser.parse_args()


###########################
# Data Reading & Cleaning #
###########################


batch = pd.read_csv(args.filename)
print("Successfully read:", len(batch), "rows from file.")

# Data Cleaning
batch.replace(to_replace={'-999999.0': None, 'NA': None})
batch.collection_week = batch.collection_week.apply(
    lambda x: datetime.datetime.strptime(x, '%Y-%m-%d')
)

# This is the weekly beds information for each hospital, needs to be inserted
# into the `beds` table.

conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com",
    dbname=DBNAME, user=USER, password=PASSWORD
)


cur = conn.cursor()
successes, fails = 0, 0

literal = (
    "INSERT INTO hospital (hospital_pk, hospital_name, address, city, zip, "
    "fips_code, state, latitude, longitude)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

#@TODO replace with hospital_insert
for idx, row in batch.iterrows():
    latitude, longitude = geocode(row['geocoded_hospital_address'])
    try:
        cur.execute(literal,
                    (row['hospital_pk'], row['hospital_name'], row['address'],
                     row['city'], row['zip'], row['fips_code'],
                     row['state'], latitude, longitude))
        successes += 1
    except Exception:  # should make this specific
        fails += 1

print("Successfully added:", str(successes), "rows to the hospitals table."
      "\n" + str(fails) + "rows rejected", sep=" ")

successes, fails = 0, 0

literal = (
    "INSERT INTO beds (hospital_pk, collection_week, "
    "all_adult_hospital_beds_7_day_avg, "
    "all_pediatric_inpatient_beds_7_day_avg, "
    "all_adult_hospital_inpatient_bed_"
    "occupied_7_day_coverage, "
    "all_pediatric_inpatient_bed_occupied_7_day_avg, "
    "total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, "
    "inpatient_beds_used_covid_7_day_avg, "
    "staffed_icu_adult_patients_confirmed_covid_7_day_avg)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

for idx, row in batch.iterrows():
    try:
        cur.execute(literal,
                    (row['hospital_pk'], row['collection_week'],
                     row['all_adult_hospital_beds_7_day_avg'],
                     row['all_pediatric_inpatient_beds_7_day_avg'],
                     row['all_adult_hospital_inpatient_bed_occupied_7_'
                         'day_coverage'],
                     row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                     row['total_icu_beds_7_day_avg'],
                     row['icu_beds_used_7_day_avg'],
                     row['inpatient_beds_used_covid_7_day_avg'],
                     row[
                        'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
                     ]))
        successes += 1
    except Exception:  # should make this specific
        fails += 1

print("Successfully added:", str(successes), "rows to the beds table."
      "\n" + str(fails) + "rows rejected", sep=" ")
successes, fails = 0, 0

conn.commit()
conn.close()
