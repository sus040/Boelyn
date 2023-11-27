import psycopg
import argparse
import os
import pandas as pd
from credentials import DBNAME, USER, PASSWORD  # check credentials_template.py
from helpers import hospital_insert, beds_insert

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
batch.replace(to_replace={'-999999.0': None, '-9999': None,
                          -999999.0: None, -9999: None,
                          'NA': None})
batch['collection_week'] = pd.to_datetime(batch['collection_week'])

# Duplicate check
duplicate_rows = batch[batch.duplicated(
    subset=['hospital_pk', 'collection_week'], keep=False)]
batch = batch.drop_duplicates(subset=['hospital_pk', 'collection_week'])
duplicate_rows.to_csv("error/hhs_duplicated.csv", index=False)
print(str(len(duplicate_rows)) +
      " duplicated rows output to error/hhs_duplicated.csv")

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
error_cases = pd.DataFrame(hospital_insert(cur, batch))
error_cases.to_csv("error/hospital_errors.csv")
print("Wrote " + len(error_cases) + "rejected rows to " +
      "error/hospital_errors.csv")

# Insert beds data, write errors to the errors folder
error_cases = pd.DataFrame(beds_insert(cur, batch))
error_cases.to_csv("error/beds_errors.csv")
print("Wrote " + len(error_cases) + "rejected rows to " +
      "error/beds_errors.csv")

conn.commit()
conn.close()
