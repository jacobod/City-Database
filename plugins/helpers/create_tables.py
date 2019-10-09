intl_airports_sql = """
CREATE TABLE IF NOT EXISTS intl_airports (
    airport_id      VARCHAR(100),
    type            VARCHAR(100),
    name            VARCHAR(200),
    elevation_ft    DOUBLE PRECISION,
    continent       VARCHAR(100),
    iso_country     VARCHAR(200),
    iso_region      VARCHAR(200),
    municipality    VARCHAR(200),
    lat             DOUBLE PRECISION,
    long            DOUBLE PRECISION,
    City_ID         VARCHAR(MAX),
);
"""

us_tbls_sql = """
CREATE TABLE IF NOT EXISTS us_airports (
    airport_id      VARCHAR(100),
    type            VARCHAR(100),
    name            VARCHAR(200),
    elevation_ft    DOUBLE PRECISION,
    municipality    VARCHAR(200),
    lat             DOUBLE PRECISION,
    long            DOUBLE PRECISION,
    state           VARCHAR(200),
    City_ID         VARCHAR(MAX)
);
"""

demo_sql = """
CREATE TABLE IF NOT EXISTS demographics (
    City                                         VARCHAR(100),
    State                                        VARCHAR(100),
    Median_Age                                   DOUBLE PRECISION,
    Total_Male_Population                        DOUBLE PRECISION,
    Total_Female_Population                      DOUBLE PRECISION,
    Total_Population                             BIGINT
    Total_Number_of_Veterans                     DOUBLE PRECISION,
    Total_Foreign_born                           DOUBLE PRECISION,
    Average_Household_Size                       DOUBLE PRECISION,
    State_Code                                   VARCHAR(100),
    Total_American_Indian_and_Alaska_Native      DOUBLE PRECISION,
    Total_Asian                                  DOUBLE PRECISION,
    Total_Black_or_African_American              DOUBLE PRECISION,
    Total_Hispanic_or_Latino                     DOUBLE PRECISION,
    Total_White                                  DOUBLE PRECISION,
    Percent_American_Indian_and_Alaska_Native    DOUBLE PRECISION,
    Percent_Asian                                DOUBLE PRECISION,
    Percent_Black_or_African_American            DOUBLE PRECISION,
    Percent_Hispanic_or_Latino                   DOUBLE PRECISION,
    Percent_White                                DOUBLE PRECISION,
    Percent_Male_Population                      DOUBLE PRECISION,
    Percent_Female_Population                    DOUBLE PRECISION,
    Percent_Number_of_Veterans                   DOUBLE PRECISION,
    Percent_Foreign_born                         DOUBLE PRECISION,
    City_ID                                      VARCHAR(MAX),
);
"""

temps_sql = """
CREATE TABLE IF NOT EXISTS city_temps (
    dt                               TIMESTAMP,
    AverageTemperature               DOUBLE PRECISION,
    AverageTemperatureUncertainty    DOUBLE PRECISION,
    City                             VARCHAR(100),
    Country                          VARCHAR(100),
    Latitude                         DOUBLE PRECISION,
    Longitude                        DOUBLE PRECISION,
    City_ID                          VARCHAR(MAX),
);
"""

imm_sql = """
CREATE TABLE IF NOT EXISTS immigration (
    cicid                    DOUBLE PRECISION, 
    arrival_year             BIGINT, 
    arrival_month            BIGINT,
    citizen_country_id       BIGINT, 
    residence_country_id     BIGINT, 
    port_id                  VARCHAR(MAX), 
    arrival_mode             BIGINT,
    state_visited            VARCHAR(200),
    age                      BIGINT,
    visa_purpose             DOUBLE PRECISION, 
    count                    BIGINT, 
    entdepa                  VARCHAR(200), 
    matflag                  VARCHAR(200),
    biryear                  BIGINT,
    gender                   VARCHAR(200), 
    airline                  VARCHAR(MAX), 
    admnum                   DOUBLE PRECISION, 
    fltno                    VARCHAR(MAX), 
    visatype                 VARCHAR(MAX),
    port_city                VARCHAR(MAX),
    port_state               VARCHAR(MAX),
    residence_country        VARCHAR(MAX), 
    citizen_country          VARCHAR(MAX), 
    arrival_date             TIMESTAMP, 
    departure_date           TIMESTAMP, 
    date_addto               TIMESTAMP, 
    date_adfile              TIMESTAMP,
);
"""

cities_sql = """
CREATE TABLE IF NOT EXISTS cities (
    City       VARCHAR(MAX),
    State      VARCHAR(MAX),
    Country    VARCHAR(MAX),
    City_ID    VARCHAR(MAX),
    Port_ID    VARCHAR(MAX),
);
"""

imm_locs_sql = """
CREATE TABLE IF NOT EXISTS immigration_ports (
    LOCATION_ID              VARCHAR(MAX),
    city                     VARCHAR(MAX),
    state                    VARCHAR(MAX)
);
"""

imm_countries_sql = """
CREATE TABLE IF NOT EXISTS immigration_countries (
    COUNTRY_ID               BIGINT,
    i94cntyl                 VARCHAR(MAX)
);
"""

create_queries = [intl_airports_sql, us_tbls_sql, demo_sql, 
                temps_sql, imm_sql, cities_sql, imm_locs_sql,
                 imm_countries_sql]