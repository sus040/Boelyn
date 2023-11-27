# hhs-helpers.py
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


def hospital_insert(cur, data):
    """Handles insertion into the hospital table
    Inputs
    ------
    cur - psycopg cursor object
    data - pandas dataframe batch of hospital records"""
    error_cases = []
    successes, fails = 0, 0
    for idx, row in data.iterrows():
        latitude, longitude = geocode(row['geocoded_hospital_address'])
        try:
            cur.execute(HOSPITAL_LITERAL,
                        (row['hospital_pk'], row['hospital_name'],
                         row['address'], row['city'], row['zip'],
                         row['fips_code'], row['state'], latitude, longitude))
            successes += 1
        except Exception:  # should make this specific
            fails += 1
            error_cases.append(row)

    print("Successfully added:", str(successes), "rows to the hospitals table."
          "\n" + str(fails) + "rows rejected", sep=" ")

    return error_cases


def fetch_existing_hospital_pks(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT hospital_pk FROM hospital")
        existing_hospital_pks = set(row[0] for row in cur.fetchall())
    return existing_hospital_pks


def beds_insert(cur, data):
    """Handles insertion into the beds table
    Inputs
    ------
    cur - psycopg cursor objects with connection initialized
    data - batch of beds data as a pandas dataframe"""
    error_cases = []
    successes, fails = 0, 0
    for idx, row in data.iterrows():
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
        except Exception:  # should make this specific
            fails += 1
            error_cases.append(row)

    print("Successfully added:", str(successes), "rows to the beds table."
          "\n" + str(fails) + "rows rejected", sep=" ")

    return error_cases
