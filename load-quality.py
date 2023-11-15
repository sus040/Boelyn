import argparse
import pandas as pd
import psycopg

parser = argparse.ArgumentParser(
    description='Load hospital quality data into the database.')
parser.add_argument(
    'date', type=str, help='Date of the quality data (YYYY-MM-DD)')
parser.add_argument('csv_file', type=str, help='CSV file name')
args = parser.parse_args()


try:
    data = pd.read_csv(args.csv_file)
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()


try:
    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname="jamiekim",
        user="jamiekim",
        password="aBG1x?8P96"
    )
    cursor = conn.cursor()
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()


def insert_quality_data(quality_data, date):
    for index, row in quality_data.iterrows():
        cursor.execute("INSERT INTO quality (Facility_ID, hospital_type,\
                       hospital_ownership, emergency_services, quality_rating,\
                       rating_date) VALUES (%s, %s, %s, %s, %s, %s)",
                       (row['facility_id'], row['hospital_type'],
                        row['hospital_ownership'], row['emergency_services'],
                        row['hospital_overall_rating'], date))


insert_quality_data(data, args.date)


conn.commit()
cursor.close()
conn.close()
