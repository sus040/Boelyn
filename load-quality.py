import pandas as pd  # Import pandas library for data manipulation
import numpy as np  # Import numpy library for numerical operations
from datetime import datetime  # Import datetime for handling dates
import psycopg  # Import psycopg for postgreSQL database connection
from credentials import DBNAME, USER, PASSWORD  # Import database credentials
import argparse  # Import argparse for parsing command-line arguments
import time  # Import time for tracking execution time

# Set up command-line argument parsing
parser = argparse.ArgumentParser(
    description='Load hospital quality data into the database.'
)
parser.add_argument('date', help='Date of the quality data (YYYY-MM-DD)')
parser.add_argument('csv_file', help='CSV file name')
args = parser.parse_args()  # Parse the arguments provided by the user


def connect_to_database():
    """Establishes and returns a database connection."""
    try:
        # Connect to the postgreSQL database
        conn = psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname=DBNAME, user=USER, password=PASSWORD
        )
        print("Connected to the database.")
        return conn  # Return the connection object
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit()  # Exit the script in case of connection failure


def read_and_clean_data(csv_file):
    """Reads and cleans data from a CSV file."""
    try:
        data = pd.read_csv(csv_file)  # Read data from the CSV file
        print("Data read from CSV successfully:")
        print(data.head())  # Display the first few rows of the dataframe
        # Replace 'Not Available' with None (null) and NaN with None
        data['Hospital overall rating'].replace(
            'Not Available', None, inplace=True)
        data.replace(np.nan, None, inplace=True)
        return data  # Return the cleaned data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        exit()  # Exit the script in case of file read failure


def insert_quality_data(conn, quality_data, date):
    """Inserts data into the postgreSQL table."""
    start_time = time.time()  # Record the start time of the operation
    # Convert Facility ID to string
    quality_data['Facility ID'] = quality_data['Facility ID'].astype(str)

    with conn.cursor() as cursor:  # Open a cursor
        # Check if the 'quality' table exists
        cursor.execute("SELECT to_regclass('quality');")
        if cursor.fetchone():
            print("Table 'quality' exists. Proceeding.")
        else:
            print("Table 'quality' does not exist. Exiting.")
            return

        # Fetch existing hospital primary keys
        cursor.execute("SELECT hospital_pk FROM hospital")
        existing_hospital_pks = set(row[0] for row in cursor.fetchall())

        insert_data = []  # List to store data for bulk insertion
        skipped_rows_data = []  # List to store skipped rows
        for _, row in quality_data.iterrows():  # Iterate over each row
            if row['Facility ID'] not in existing_hospital_pks:
                # Add to skipped rows if not found
                skipped_rows_data.append(row)
                continue  # Skip to the next row

            # Prepare the row for insertion
            insert_data.append((
                row['Facility ID'],
                row['Hospital Type'],
                row['Hospital Ownership'],
                row['Emergency Services'],
                row['Hospital overall rating'],
                datetime.strptime(date, '%Y-%m-%d')
            ))

        # Execute bulk insert into the 'quality' table
        cursor.executemany(
            "INSERT INTO quality (Facility_ID, "
            "hospital_type, hospital_ownership, "
            "emergency_services, quality_rating, rating_date) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            insert_data
        )
        conn.commit()  # Commit the transaction
        print("Data successfully inserted into the 'quality' table.")

        # Display and save the number of skipped rows
        num_skipped_rows = len(skipped_rows_data)
        print(f"Number of skipped rows: {num_skipped_rows}")
        if skipped_rows_data:
            skipped_df = pd.DataFrame(skipped_rows_data)
            skipped_df.to_csv(f"skipped_rows_{date}.csv", index=False)
            print(f"Skipped rows saved to skipped_rows_{date}.csv")

    end_time = time.time()  # Record the end time of the operation
    elapsed_time = end_time - start_time  # Calculate the total elapsed time
    minutes = int(elapsed_time // 60)  # Convert time to minutes
    seconds = int(elapsed_time % 60)  # Convert time to seconds
    print(f"Operation completed in {minutes} minutes and {seconds} seconds.")


# Main execution process
data = read_and_clean_data(args.csv_file)  # Read and clean the CSV data
conn = connect_to_database()  # Establish a database connection
insert_quality_data(conn, data, args.date)  # Insert data into the database
conn.close()  # Close the database connection
