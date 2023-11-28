DROP TABLE IF EXISTS hospital, beds, quality;

CREATE TABLE hospital (
    hospital_pk VARCHAR(255) PRIMARY KEY,
    hospital_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    zip VARCHAR(10),
    fips_code VARCHAR(20),
    state CHAR(2),
    latitude DECIMAL(6,3),
    longitude DECIMAL(6,3)
);

CREATE TABLE beds (
    record_id SERIAL PRIMARY KEY,
    hospital_pk VARCHAR(255) NOT NULL,
    collection_week DATE NOT NULL,
    all_adult_hospital_beds_7_day_avg NUMERIC 
    CHECK (all_adult_hospital_beds_7_day_avg >= 0),
    all_pediatric_inpatient_beds_7_day_avg NUMERIC
    CHECK (all_pediatric_inpatient_beds_7_day_avg >= 0),
    all_adult_hospital_inpatient_bed_occupied_7_day_coverage NUMERIC
    CHECK (all_adult_hospital_inpatient_bed_occupied_7_day_coverage >= 0),
    all_pediatric_inpatient_bed_occupied_7_day_avg NUMERIC
    CHECK (all_pediatric_inpatient_bed_occupied_7_day_avg >= 0),
    total_icu_beds_7_day_avg NUMERIC
    CHECK (total_icu_beds_7_day_avg >= 0),
    icu_beds_used_7_day_avg NUMERIC
    CHECK (icu_beds_used_7_day_avg >= 0),
    inpatient_beds_used_covid_7_day_avg NUMERIC
    CHECK (inpatient_beds_used_covid_7_day_avg >= 0),
    staffed_icu_adult_patients_confirmed_covid_7_day_avg NUMERIC
    CHECK (staffed_icu_adult_patients_confirmed_covid_7_day_avg >= 0),
    FOREIGN KEY (hospital_pk) REFERENCES hospital(hospital_pk)
);

CREATE TABLE quality (
    -- should we make Facility_ID the PK and also the foreign key?
    quality_id SERIAL PRIMARY KEY,
    Facility_ID VARCHAR(255) NOT NULL REFERENCES hospital(hospital_pk),
    hospital_type VARCHAR(255),
    hospital_ownership VARCHAR(255),
    emergency_services BOOLEAN,
    quality_rating INT CHECK (quality_rating > 0 AND quality_rating < 6),
    rating_date DATE
);