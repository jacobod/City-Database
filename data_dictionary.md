# City DB Data Dictionary

### intl_airports:

    airport_id      VARCHAR(100),       Unique Airport ID
    type            VARCHAR(100),       Airport Type
    name            VARCHAR(200),       Airport Name
    elevation_ft    DOUBLE PRECISION,   elevation (in ft)
    continent       VARCHAR(100),       
    iso_country     VARCHAR(200),
    iso_region      VARCHAR(200),
    municipality    VARCHAR(200),       City name airport is located in
    lat             DOUBLE PRECISION,   
    long            DOUBLE PRECISION,
    City_ID         VARCHAR(MAX),       City_ID in cities table

### us_airports:

    airport_id      VARCHAR(100),       Unique Airport ID
    type            VARCHAR(100),       Airport Type
    name            VARCHAR(200),       Airport Name
    elevation_ft    DOUBLE PRECISION,   elevation (in ft)
    municipality    VARCHAR(200),       City name airport is located in
    lat             DOUBLE PRECISION,   
    long            DOUBLE PRECISION,
    state           VARCHAR(200),       2-char state
    City_ID         VARCHAR(MAX)        City_ID in cities table


### demographics:

    City                                         VARCHAR(100),      City name
    State                                        VARCHAR(100),      Full state name
    Median_Age                                   DOUBLE PRECISION,  
    Total_Male_Population                        DOUBLE PRECISION,
    Total_Female_Population                      DOUBLE PRECISION,
    Total_Population                             BIGINT
    Total_Number_of_Veterans                     DOUBLE PRECISION,
    Total_Foreign_born                           DOUBLE PRECISION,
    Average_Household_Size                       DOUBLE PRECISION,
    State_Code                                   VARCHAR(100),      2-char state abbrev.
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
    City_ID                                      VARCHAR(MAX),      City_ID in cities table


### city_temps:

    dt                               TIMESTAMP,         Date of recording
    AverageTemperature               DOUBLE PRECISION,  land temperature in celsius
    AverageTemperatureUncertainty    DOUBLE PRECISION,  the 95% confidence interval around the average 
    City                             VARCHAR(100),
    Country                          VARCHAR(100),
    Latitude                         DOUBLE PRECISION,
    Longitude                        DOUBLE PRECISION,
    City_ID                          VARCHAR(MAX),      City_ID in cities table


### immigration:

    cicid                    DOUBLE PRECISION, Unique row ID
    arrival_year             BIGINT, 
    arrival_month            BIGINT,
    citizen_country_id       BIGINT, 
    residence_country_id     BIGINT, 
    port_id                  VARCHAR(MAX),      City entered country in (see immigration_ports)
    arrival_mode             BIGINT,            How visitor arrived (Air, Sea, etc)
    state_visited            VARCHAR(200),
    age                      BIGINT,
    visa_purpose             DOUBLE PRECISION,  Why visitor arrived (Business, Pleasure, Student, etc)
    count                    BIGINT, 
    entdepa                  VARCHAR(200), 
    matflag                  VARCHAR(200),
    biryear                  BIGINT,            4 digit year of birth
    gender                   VARCHAR(200), 
    airline                  VARCHAR(MAX),      Airline used to arrive in U.S.
    admnum                   DOUBLE PRECISION, 
    fltno                    VARCHAR(MAX),      Flight number of Airline
    visatype                 VARCHAR(MAX),      Class of admission legally admitting visitor to temporarily stay in U.S.
    port_city                VARCHAR(MAX),
    port_state               VARCHAR(MAX),
    residence_country        VARCHAR(MAX), 
    citizen_country          VARCHAR(MAX), 
    arrival_date             TIMESTAMP,         Arrival Date in the USA
    departure_date           TIMESTAMP,         Departure Date from the USA
    date_addto               TIMESTAMP, 
    date_adfile              TIMESTAMP,


### cities:

    City       VARCHAR(MAX),
    State      VARCHAR(MAX),
    Country    VARCHAR(MAX),
    City_ID    VARCHAR(MAX),
    Port_ID    VARCHAR(MAX),    The Port_ID in immigration table



### immigration_ports:

    LOCATION_ID              VARCHAR(MAX),
    city                     VARCHAR(MAX),
    state                    VARCHAR(MAX)


### immigration_countries:

    COUNTRY_ID               BIGINT,
    i94cntyl                 VARCHAR(MAX)
