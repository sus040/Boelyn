# hhs-helpers.py
import pandas as pd
HOSPITAL_LITERAL = (
    "INSERT INTO hospital (hospital_pk, hospital_name, address, city, zip, "
    "fips_code, state, latitude, longitude)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

BEDS_LITERAL = (
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


def geocode(num):
    """Handles geocode string unpacking for latitude/longitude"""
    if isinstance(num, str):
        coords = num.replace('POINT (', '').replace(')', '').split()
        longitude = int(float(coords[0]))
        latitude = int(float(coords[1]))
        return latitude, longitude
    else:
        return None, None


def report_insert_results(results, tbl_name, err_target, msg_target):
    """Helper function to report results from an insert function"""
    successes, fails, error_cases, error_msgs = results
    print("Successfully added:", str(successes), "rows to the table", tbl_name,
          "\n", str(fails), "rows rejected", sep=" ")
    error_cases = pd.DataFrame(error_cases)
    error_cases.to_csv(err_target)
    print("Wrote", str(len(error_cases)), "rejected rows to", err_target,
          sep=" ")
    error_msgs = pd.DataFrame(error_msgs)
    error_msgs.to_csv(msg_target)
    print("Wrote", str(len(error_msgs)), "error messages to", msg_target,
          sep=" ")


def hospital_insert(conn, cur, data):
    """Handles insertion into the hospital table
    Inputs
    ------
    cur - psycopg cursor object
    data - pandas dataframe batch of hospital records"""
    error_cases, error_msgs = [], []
    successes, fails = 0, 0
    for _, row in data.iterrows():
        latitude, longitude = geocode(row['geocoded_hospital_address'])
        try:
            cur.execute(HOSPITAL_LITERAL,
                        (row['hospital_pk'], row['hospital_name'],
                         row['address'], row['city'], row['zip'],
                         row['fips_code'], row['state'], latitude, longitude))
            successes += 1
            conn.commit()
        except Exception as e:
            fails += 1
            error_cases.append(row)
            error_msgs.append(str(e))
            conn.rollback()
    return (successes, fails, error_cases, error_msgs)


def fetch_existing_hospital_pks(conn):
    """Helper function to select hospital primary keys"""
    with conn.cursor() as cur:
        cur.execute("SELECT hospital_pk FROM hospital")
        existing_hospital_pks = set(row[0] for row in cur.fetchall())
    return existing_hospital_pks


def beds_insert(conn, cur, data):
    """Handles insertion into the beds table
    Inputs
    ------
    cur - psycopg cursor objects with connection initialized
    data - batch of beds data as a pandas dataframe
    err_target - filepath to write error cases to
    msg_target - filepath to write error messages to"""
    error_cases, error_msgs = [], []
    successes, fails = 0, 0
    for _, row in data.iterrows():
        try:
            cur.execute(BEDS_LITERAL,
                        (row['hospital_pk'], row['collection_week'],
                         row['all_adult_hospital_beds_7_day_avg'],
                         row['all_pediatric_inpatient_beds_7_day_avg'],
                         row['all_adult_hospital_inpatient_bed_occupied_7_'
                             'day_coverage'],
                         row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                         row['total_icu_beds_7_day_avg'],
                         row['icu_beds_used_7_day_avg'],
                         row['inpatient_beds_used_covid_7_day_avg'],
                         row['staffed_icu_adult_patients_confirmed_'
                             'covid_7_day_avg']))
            successes += 1
            conn.commit()
        except Exception as e:
            fails += 1
            error_cases.append(row)
            error_msgs.append(str(e))
            conn.rollback()
    return (successes, fails, error_cases, error_msgs)
