import psycopg
import pandas as pd
import sys
from credentials import DBNAME, USER, PASSWORD  # check credentials_template.py

filename = sys.argv[1]
batch = pd.read_csv(filename)

# Data Cleaning
batch.replace(to_replace={'-999999.0': pd.NA, 'NA': pd.NA})

# This is the weekly beds information for each hospital, needs to be inserted
# into the `beds` table.

conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com",
    dbname=DBNAME, user=USER, password=PASSWORD
)

cur = conn.cursor()

for idx, row in batch.iterrows():
    print(row)
    cur.execute("INSERT INTO beds (hospital_pk, collection_week, "
                "all_adult_hospital_beds_7_day_avg, "
                "all_pediatric_inpatient_beds_7_day_avg, "
                "all_adult_hospital_inpatient_bed_occupied_7_day_coverage, "
                "all_pediatric_inpatient_bed_occupied_7_day_avg, "
                "total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, "
                "inpatient_beds_used_covid_7_day_avg, "
                "staffed_icu_adult_patients_confirmed_covid_7_day_avg)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (str(row['hospital_pk']), str(row['collection_week']),
                 str(row['all_adult_hospital_beds_7_day_avg']),
                 str(row['all_pediatric_inpatient_beds_7_day_avg']),
                 str(row['all_adult_hospital_inpatient_bed_occupied_7_'
                         'day_coverage']),
                 str(row['all_pediatric_inpatient_bed_occupied_7_day_avg']),
                 str(row['total_icu_beds_7_day_avg']),
                 str(row['icu_beds_used_7_day_avg']),
                 str(row['inpatient_beds_used_covid_7_day_avg']),
                 str(row['staffed_icu_adult_patients_confirmed'
                         '_covid_7_day_avg'])))

conn.commit()
conn.close()
