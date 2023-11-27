import argparse
import psycopg
import pandas as pd
import numpy as np
from datetime import datetime
from credentials import DBNAME, USER, PASSWORD
from hhs_helpers import geocode

def read_and_clean_data(filename):
    try:
        batch = pd.read_csv(filename)
    except FileNotFoundError:
        print(f"File not found: {filename}")
        raise
    batch.replace(to_replace={-999999.0: None, 'NA': None, np.nan: None}, inplace=True)
    batch['collection_week'] = pd.to_datetime(batch['collection_week'])
    return batch

def check_for_duplicates(batch):
    duplicate_rows = batch[batch.duplicated(
        subset=['hospital_pk', 'collection_week'], keep=False)]
    batch_cleaned = batch.drop_duplicates(
        subset=['hospital_pk', 'collection_week'])
    return duplicate_rows, batch_cleaned

def fetch_existing_hospital_pks(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT hospital_pk FROM hospital")
        existing_hospital_pks = set(row[0] for row in cur.fetchall())
    return existing_hospital_pks

def process_hospital_data(cur, batch):
    successes, fails = 0, 0
    geocoded_cache = {}
    for i, row in batch.iterrows():
        if row['geocoded_hospital_address'] not in geocoded_cache:
            geocoded_cache[row['geocoded_hospital_address']] = geocode(row['geocoded_hospital_address'])
        latitude, longitude = geocoded_cache[row['geocoded_hospital_address']]

        try:
            cur.execute(
                "INSERT INTO hospital (hospital_pk, hospital_name, address, city, zip, fips_code, state, latitude, longitude) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (hospital_pk) DO UPDATE SET "
                "address = EXCLUDED.address, city = EXCLUDED.city, zip = EXCLUDED.zip, fips_code = EXCLUDED.fips_code, state = EXCLUDED.state, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude",
                (row['hospital_pk'], row['hospital_name'], row['address'], row['city'], row['zip'], row['fips_code'], row['state'], latitude, longitude))
            successes += 1
        except Exception as e:
            print(f"Error in hospital row {i}: {e}")
            fails += 1
    return successes, fails

def process_beds_data(cur, batch):
    successes, fails = 0, 0
    for i, row in batch.iterrows():
        try:
            cur.execute(
                "INSERT INTO beds (hospital_pk, collection_week, all_adult_hospital_beds_7_day_avg, all_pediatric_inpatient_beds_7_day_avg, all_adult_hospital_inpatient_bed_occupied_7_day_coverage, all_pediatric_inpatient_bed_occupied_7_day_avg, total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, inpatient_beds_used_covid_7_day_avg, staffed_icu_adult_patients_confirmed_covid_7_day_avg) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (row['hospital_pk'], row['collection_week'], row['all_adult_hospital_beds_7_day_avg'], row['all_pediatric_inpatient_beds_7_day_avg'], row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage'], row['all_pediatric_inpatient_bed_occupied_7_day_avg'], row['total_icu_beds_7_day_avg'], row['icu_beds_used_7_day_avg'], row['inpatient_beds_used_covid_7_day_avg'], row['staffed_icu_adult_patients_confirmed_covid_7_day_avg']))
            successes += 1
        except Exception as e:
            print(f"Error in beds row {i}: {e}")
            fails += 1
    return successes, fails

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", action="store")
    args = parser.parse_args()

    batch = read_and_clean_data(args.filename)
    print("Successfully read:", len(batch), "rows from file.")

    duplicate_rows, batch = check_for_duplicates(batch)
    duplicate_rows.to_excel("discarded_data.xlsx", index=False)

    try:
        with psycopg.connect(host="pinniped.postgres.database.azure.com", dbname=DBNAME, user=USER, password=PASSWORD) as conn:
            with conn.cursor() as cur:
                hospital_successes, hospital_fails = process_hospital_data(cur, batch)
                print(f"Hospital table processed. Successes: {hospital_successes}, Failures: {hospital_fails}")

                beds_successes, beds_fails = process_beds_data(cur, batch)
                print(f"Beds table processed. Successes: {beds_successes}, Failures: {beds_fails}")

                conn.commit()
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main()
