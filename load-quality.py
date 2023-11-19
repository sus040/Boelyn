import argparse
import pandas as pd
from datetime import datetime
import psycopg
from credentials import DBNAME, USER, PASSWORD  # check credentials_template.py

# Define command-line arguments
parser = argparse.ArgumentParser(
    description='Load hospital quality data into the database.')
parser.add_argument(
    'date', type=str, help='Date of the quality data (YYYY-MM-DD)')
parser.add_argument('csv_file', type=str, help='CSV file name')
args = parser.parse_args()

# Read data from CSV file into a pandas dataframe
try:
    data = pd.read_csv(args.csv_file)
    print("Data read from CSV successfully:")
    print(data.head())  # Print the first few rows of the dataframe
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

# Data Cleaning
data['Hospital overall rating'].replace('Not Available', None, inplace=True)

# Define a function to insert data into the postgreSQL table


def insert_quality_data(quality_data, date):
    try:
        print("Connecting to the database...")
        with psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname=DBNAME, user=USER, password=PASSWORD
        ) as conn:

            with conn.cursor() as cursor:
                print("Connected. Checking if 'quality' table exists.")
                cursor.execute("SELECT to_regclass('quality');")
                result = cursor.fetchone()
                if result and result[0]:
                    print("Table 'quality' exists. Proceeding.")
                else:
                    print("Table 'quality' does not exist. Exiting.")
                    return

                skipped_rows = 0
                for _, row in quality_data.iterrows():
                    cursor.execute("SELECT COUNT(*) FROM hospital WHERE "
                                   "hospital_pk = %s", (row['Facility ID'],))
                    if cursor.fetchone()[0] == 0:
                        skipped_rows += 1
                        print(f"Facility ID {row['Facility ID']} not found in "
                              "hospital table. Skipping.")
                        continue
                    cursor.execute(
                        "INSERT INTO quality (Facility_ID, "
                        "hospital_type, "
                        "hospital_ownership, emergency_services, "
                        "quality_rating, rating_date)"
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (row['Facility ID'],
                         row['Hospital Type'],
                         row['Hospital Ownership'],
                         row['Emergency Services'],
                         row['Hospital overall rating'],
                         datetime.strptime(date, '%Y-%m-%d')))

                conn.commit()
                print("Data successfully inserted into the 'quality' table.")
                print(f"Number of skipped rows: {skipped_rows}")

    except Exception as e:
        print(f"Error inserting data into the database: {e}")


# Call the function to insert data into the postgreSQL table
insert_quality_data(data, args.date)
