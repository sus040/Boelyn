DROP TABLE IF EXISTS hospital, beds, quality;

CREATE TABLE hospital (
    hospital_pk VARCHAR(255) NOT NULL PRIMARY KEY,
    hospital_name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    zip VARCHAR(10) NOT NULL,
    fips_code VARCHAR(20) NOT NULL,
    state CHAR(2) NOT NULL,
    latitude DECIMAL(6,3),
    longitude DECIMAL(6,3)
);

CREATE TABLE beds (
    record_id SERIAL PRIMARY KEY,
    hospital_pk VARCHAR(255) NOT NULL,
    collection_week DATE NOT NULL,
    all_adult_hospital_beds_7_day_avg INT,
    all_pediatric_inpatient_beds_7_day_avg INT,
    all_adult_hospital_inpatient_bed_occupied_7_day_coverage INT,
    all_pediatric_inpatient_bed_occupied_7_day_avg INT,
    total_icu_beds_7_day_avg INT,
    icu_beds_used_7_day_avg INT,
    inpatient_beds_used_covid_7_day_avg INT,
    staffed_icu_adult_patients_confirmed_covid_7_day_avg INT,
    FOREIGN KEY (hospital_pk) REFERENCES hospital(hospital_pk)
);

CREATE TABLE quality (
    quality_id SERIAL PRIMARY KEY,
    Facility_ID VARCHAR(255) NOT NULL REFERENCES hospital(hospital_pk),
    hospital_type VARCHAR(255),
    hospital_ownership VARCHAR(255),
    emergency_services BOOLEAN NOT NULL,
    quality_rating INT,
    rating_date DATE NOT NULL
);