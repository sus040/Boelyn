# hhs-helpers.py
LITERAL = (
    "INSERT INTO hospital (hospital_pk, hospital_name, address, city, zip, "
    "fips_code, state, latitude, longitude)"
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
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
    successes, fails = 0, 0
    for idx, row in data.iterrows():
        latitude, longitude = geocode(row['geocoded_hospital_address'])
        try:
            cur.execute(LITERAL,
                        (row['hospital_pk'], row['hospital_name'],
                         row['address'], row['city'], row['zip'],
                         row['fips_code'], row['state'], latitude, longitude))
            successes += 1
        except Exception:  # should make this specific
            fails += 1

    print("Successfully added:", str(successes), "rows to the hospitals table."
          "\n" + str(fails) + "rows rejected", sep=" ")
