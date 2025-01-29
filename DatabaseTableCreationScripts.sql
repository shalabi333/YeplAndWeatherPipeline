--################################################### STAGING ###################################################--
USE SCHEMA YELP_WEATHER_INSIGHTS.STAGING;

    CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.YELP_TIP (
    user_id TEXT,
    business_id TEXT,
    text TEXT,
    timestamp DATETIME,
    compliment_count INT
);
---------
COPY INTO YELP_WEATHER_INSIGHTS.STAGING.YELP_TIP
FROM (SELECT 
    parse_json($1):user_id::STRING as user_id,
    parse_json($1):business_id::STRING as business_id,
    parse_json($1):text::STRING as text,
    to_timestamp_ntz(parse_json($1):date::STRING) as timestamp,
    parse_json($1):compliment_count::INTEGER as compliment_count
FROM 
    @YELP_WEATHER_INSIGHTS.STAGING.JSONSTAGE
    (FILE_FORMAT => 'JSONFORMAT', PATTERN => '.*yelp_academic_dataset_tip\.json'))
ON_ERROR = 'continue';



CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.YELP_BUSINESS (
    business_id TEXT,
    name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code INT,
    latitude FLOAT,
    longitude FLOAT,
    stars NUMERIC(3,2),
    review_count INT,
    is_open INT,
    attributes_NoiseLevel TEXT,
    attributes_BikeParking BOOLEAN,
    attributes_RestaurantsAttire TEXT,
    attributes_BusinessAcceptsCreditCards BOOLEAN,
    attributes_BusinessParking TEXT,
    attributes_RestaurantsReservations TEXT,
    attributes_GoodForKids BOOLEAN,
    attributes_RestaurantsTakeOut BOOLEAN,
    attributes_Caters BOOLEAN,
    attributes_WiFi TEXT,
    attributes_RestaurantsDelivery BOOLEAN,
    attributes_HasTV BOOLEAN,
    attributes_RestaurantsPriceRange2 INT,
    attributes_Alcohol TEXT,
    attributes_Music TEXT,
    attributes_BusinessAcceptsBitcoin BOOLEAN,
    attributes_GoodForDancing BOOLEAN,
    attributes_DogsAllowed BOOLEAN,
    attributes_BestNights TEXT,
    attributes_RestaurantsGoodForGroups BOOLEAN,
    attributes_OutdoorSeating BOOLEAN,
    attributes_HappyHour BOOLEAN,
    attributes_RestaurantsTableService BOOLEAN,
    attributes_GoodForMeal TEXT,
    attributes_WheelchairAccessible BOOLEAN,
    attributes_Ambience TEXT,
    attributes_CoatCheck BOOLEAN,
    attributes_DriveThru BOOLEAN,
    attributes_Smoking TEXT,
    attributes_BYOB BOOLEAN,
    attributes_Corkage BOOLEAN,
    categories TEXT,
    hours_Monday TEXT,
    hours_Tuesday TEXT,
    hours_Wednesday TEXT,
    hours_Thursday TEXT,
    hours_Friday TEXT,
    hours_Saturday TEXT,
    hours_Sunday TEXT
    );

---------

COPY INTO YELP_BUSINESS (business_id, name, address, city, state, postal_code, latitude, longitude, stars,
                        review_count, is_open, attributes_NoiseLevel, attributes_BikeParking, attributes_RestaurantsAttire,
                        attributes_BusinessAcceptsCreditCards, attributes_BusinessParking, attributes_RestaurantsReservations,
                        attributes_GoodForKids, attributes_RestaurantsTakeOut, attributes_Caters, attributes_WiFi,
                        attributes_RestaurantsDelivery, attributes_HasTV, attributes_RestaurantsPriceRange2, attributes_Alcohol,
                        attributes_Music, attributes_BusinessAcceptsBitcoin, attributes_GoodForDancing, attributes_DogsAllowed,
                        attributes_BestNights, attributes_RestaurantsGoodForGroups, attributes_OutdoorSeating,
                        attributes_HappyHour, attributes_RestaurantsTableService, attributes_GoodForMeal,
                        attributes_WheelchairAccessible, attributes_Ambience, attributes_CoatCheck, attributes_DriveThru,
                        attributes_Smoking, attributes_BYOB, attributes_Corkage, categories,
                        hours_Monday, hours_Tuesday, hours_Wednesday, hours_Thursday, hours_Friday, hours_Saturday, hours_Sunday)
    FROM (SELECT parse_json($1):business_id,
                 parse_json($1):name,
                 parse_json($1):address,
                 parse_json($1):city,
                 parse_json($1):state,
                 parse_json($1):postal_code,
                 parse_json($1):latitude,
                 parse_json($1):longitude,
                 parse_json($1):stars,
                 parse_json($1):review_count,
                 parse_json($1):is_open,
                 parse_json($1):attributes.NoiseLevel,
                 parse_json($1):attributes.BikeParking,
                 parse_json($1):attributes.RestaurantsAttire,
                 parse_json($1):attributes.BusinessAcceptsCreditCards,
                 parse_json($1):attributes.BusinessParking,
                 parse_json($1):attributes.RestaurantsReservations,
                 parse_json($1):attributes.GoodForKids,
                 parse_json($1):attributes.RestaurantsTakeOut,
                 parse_json($1):attributes.Caters,
                 parse_json($1):attributes.WiFi,
                 parse_json($1):attributes.RestaurantsDelivery,
                 parse_json($1):attributes.HasTV,
                 parse_json($1):attributes.RestaurantsPriceRange2,
                 parse_json($1):attributes.Alcohol,
                 parse_json($1):attributes.Music,
                 parse_json($1):attributes.BusinessAcceptsBitcoin,
                 parse_json($1):attributes.GoodForDancing,
                 parse_json($1):attributes.DogsAllowed,
                 parse_json($1):attributes.BestNights,
                 parse_json($1):attributes.RestaurantsGoodForGroups,
                 parse_json($1):attributes.OutdoorSeating,
                 parse_json($1):attributes.HappyHour,
                 parse_json($1):attributes.RestaurantsTableService,
                 parse_json($1):attributes.GoodForMeal,
                 parse_json($1):attributes.WheelchairAccessible,
                 parse_json($1):attributes.Ambience,
                 parse_json($1):attributes.CoatCheck,
                 parse_json($1):attributes.DriveThru,
                 parse_json($1):attributes.Smoking,
                 parse_json($1):attributes.BYOB,
                 parse_json($1):attributes.Corkage,
                 parse_json($1):categories,
                 parse_json($1):hours.Monday,
                 parse_json($1):hours.Tuesday,
                 parse_json($1):hours.Wednesday,
                 parse_json($1):hours.Thursday,
                 parse_json($1):hours.Friday,
                 parse_json($1):hours.Saturday,
                 parse_json($1):hours.Sunday 
       FROM @YELP_WEATHER_INSIGHTS.STAGING.JSONSTAGE
    (FILE_FORMAT => 'JSONFORMAT', PATTERN => '.*yelp_academic_dataset_business\.json'))
    ON_ERROR = 'continue';
	


    
    CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.YELP_CHECKIN (
    business_id TEXT,
    date TEXT
    );
---------
    COPY INTO yelp_checkin(business_id, date)
    FROM (SELECT parse_json($1):business_id,
                 parse_json($1):date
          FROM @YELP_WEATHER_INSIGHTS.STAGING.JSONSTAGE
    (FILE_FORMAT => 'JSONFORMAT', PATTERN => '.*yelp_academic_dataset_checkin\.json'))
    ON_ERROR = 'continue';




    CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.YELP_REVIEW (
    review_id TEXT,
    user_id TEXT,
    business_id TEXT,
    stars NUMERIC(3,2),
    useful INT,
    funny INT,
    cool INT,
    text TEXT,
    timestamp DATETIME
    );
---------
    
    COPY INTO yelp_review(review_id, user_id, business_id, stars, useful, funny, cool, text, timestamp)
    FROM (SELECT parse_json($1):review_id,
                 parse_json($1):user_id,
                 parse_json($1):business_id,
                 parse_json($1):stars,
                 parse_json($1):useful,
                 parse_json($1):funny,
                 parse_json($1):cool,
                 parse_json($1):text,
                 to_timestamp_ntz(parse_json($1):date)
         FROM @YELP_WEATHER_INSIGHTS.STAGING.JSONSTAGE
    (FILE_FORMAT => 'JSONFORMAT', PATTERN => '.*yelp_academic_dataset_review\.json'))
    ON_ERROR = 'continue';
  

    CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.YELP_USER (
    user_id TEXT,
    name TEXT,
    review_count INT,
    yelping_since DATETIME,
    useful INT,
    funny INT,
    cool INT,
    elite TEXT,
    friends TEXT,
    fans INT,
    average_stars NUMERIC(3,2),
    compliment_hot INT,
    compliment_more INT,
    compliment_profile INT,
    compliment_cute INT,
    compliment_list INT,
    compliment_note INT,
    compliment_plain INT,
    compliment_cool INT,
    compliment_funny INT,
    compliment_writer INT,
    compliment_photos INT
    );

---------
    
COPY INTO YELP_USER(user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends, fans, average_stars,
                    compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note,
                    compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos)
    FROM (SELECT parse_json($1):user_id,
                 parse_json($1):name,
                 parse_json($1):review_count,
                 to_timestamp_ntz(parse_json($1):yelping_since),
                 parse_json($1):useful,
                 parse_json($1):funny,
                 parse_json($1):cool,
                 parse_json($1):elite,
                 parse_json($1):friends,
                 parse_json($1):fans,
                 parse_json($1):average_stars,
                 parse_json($1):compliment_hot,
                 parse_json($1):compliment_more,
                 parse_json($1):compliment_profile,
                 parse_json($1):compliment_cute,
                 parse_json($1):compliment_list,
                 parse_json($1):compliment_note,
                 parse_json($1):compliment_plain,
                 parse_json($1):compliment_cool,
                 parse_json($1):compliment_funny,
                 parse_json($1):compliment_writer,
                 parse_json($1):compliment_photos
         FROM @YELP_WEATHER_INSIGHTS.STAGING.JSONSTAGE
    (FILE_FORMAT => 'JSONFORMAT', PATTERN => '.*yelp_academic_dataset_user\.json'))
    ON_ERROR = 'continue';




    CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.YELP_COVID (
    business_id TEXT,
    highlights TEXT,
    delivery_or_takeout TEXT,
    grubhub_enabled TEXT,
    call_to_action_enabled TEXT,
    request_a_quote_enabled TEXT,
    covid_banner TEXT,
    temporary_closed_until TEXT,
    virtual_services_offered TEXT
    );
---------
COPY INTO YELP_COVID(business_id, highlights, delivery_or_takeout, grubhub_enabled, call_to_action_enabled,
                     request_a_quote_enabled, covid_banner, temporary_closed_until, virtual_services_offered)
    FROM (SELECT parse_json($1):business_id,
                 parse_json($1):highlights,
                 parse_json($1):delivery_or_takeout,
                 parse_json($1):Grubhub_enabled,
                 parse_json($1):Call_To_Action_enabled,
                 parse_json($1):Request_a_Quote_Enabled,
                 parse_json($1):Covid_Banner,
                 parse_json($1):Temporary_Closed_Until,
                 parse_json($1):Virtual_Services_Offered
         FROM @YELP_WEATHER_INSIGHTS.STAGING.JSONSTAGE
    (FILE_FORMAT => 'JSONFORMAT', PATTERN => '.*yelp_academic_dataset_covid_features\.json'))
    ON_ERROR = 'continue';
 

  

CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.WEATHER_TEMPERATURE(
    date INT,
    min FLOAT,
    max FLOAT,
    normal_min FLOAT,
    normal_max FLOAT
);
--------------

COPY INTO YELP_WEATHER_INSIGHTS.STAGING.WEATHER_TEMPERATURE
    FROM @YELP_WEATHER_INSIGHTS.STAGING.stage_gcp
    FILE_FORMAT = (TYPE = csv field_delimiter=',' skip_header = 1)
    FILES = ('usw00023169-temperature-degreef.csv')

CREATE OR REPLACE TABLE YELP_WEATHER_INSIGHTS.STAGING.WEATHER_PRECIPITATION (
	date TEXT,
	precipitation TEXT,
	precipitation_normal FLOAT
	
);
-- COPY COMMAND WITH PATTERN FOR FILE NAMES
COPY INTO YELP_WEATHER_INSIGHTS.STAGING.WEATHER_PRECIPITATION
    FROM @YELP_WEATHER_INSIGHTS.STAGING.stage_gcp
    FILE_FORMAT = (TYPE = csv field_delimiter=',' skip_header = 1)
    FILES = ('usw00023169-las-vegas-mccarran-intl-ap-precipitation-inch.csv')





--################################################### ODS ###################################################--


USE SCHEMA YELP_WEATHER_INSIGHTS.ODS;

/* Table location */
CREATE OR REPLACE TABLE location (
    location_id     INT      PRIMARY KEY  IDENTITY,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     INT,
    latitude        FLOAT,
    longitude       FLOAT
);

/*Create Business table */
CREATE OR REPLACE TABLE business (
    business_id         TEXT   PRIMARY KEY,
    name                TEXT,
    location_id         INT,
    stars               NUMERIC(3,2),
    review_count        INT,
    is_open             BOOLEAN,
	categories 			TEXT,
    CONSTRAINT FK_BUSINESS_LOCATION_ID FOREIGN KEY(location_id)    REFERENCES  location(location_id)
);


/*Create Business Attributes table*/
CREATE OR REPLACE TABLE business_attributes (
    business_id         		TEXT     PRIMARY KEY,
    NoiseLevel 					TEXT,
    BikeParking 				BOOLEAN,
    RestaurantsAttire 			TEXT,
    BusinessAcceptsCreditCards 	BOOLEAN,
    BusinessParking 			TEXT,
    RestaurantsReservations 	TEXT,
    GoodForKids 				BOOLEAN,
    RestaurantsTakeOut 			BOOLEAN,
    Caters 						BOOLEAN,
    WiFi 						TEXT,
    RestaurantsDelivery 		BOOLEAN,
    HasTV 						BOOLEAN,
    RestaurantsPriceRange2 		INT,
    Alcohol 					TEXT,
    Music 						TEXT,
    BusinessAcceptsBitcoin 		BOOLEAN,
    GoodForDancing 				BOOLEAN,
    DogsAllowed 				BOOLEAN,
    BestNights 					TEXT,
    RestaurantsGoodForGroups 	BOOLEAN,
    OutdoorSeating 				BOOLEAN,
    HappyHour 					BOOLEAN,
    RestaurantsTableService 	BOOLEAN,
    GoodForMeal 				TEXT,
    WheelchairAccessible 		BOOLEAN,
    Ambience 					TEXT,
    CoatCheck 					BOOLEAN,
    DriveThru 					BOOLEAN,
    Smoking 					TEXT,
    BYOB 						BOOLEAN,
    Corkage 					BOOLEAN,
	CONSTRAINT FK_BUSINESS_ATTRIBUTE_BUSINESS_ID FOREIGN KEY(business_id)    REFERENCES  business(business_id)
);

/* Create Business Hours table */
CREATE OR REPLACE TABLE business_hours (
    business_id       TEXT   PRIMARY KEY,
    monday            TEXT,
	tuesday           TEXT,
	wednesday         TEXT,
	thursday          TEXT,
	friday            TEXT,
	saturday          TEXT,
	sunday            TEXT,
	CONSTRAINT FK_BUSINESS_HOURS_BUSINESS_ID FOREIGN KEY(business_id)    REFERENCES  business(business_id)
);


/* Create Date Time table */
CREATE OR REPLACE TABLE date_time (
    timestamp           DATETIME    PRIMARY KEY,
    date                DATE,
    day                 INT,
    month               INT,
    year                INT,
	week                INT,
	quarter             INT
);

/* Create User table */
CREATE OR REPLACE TABLE user (
    user_id             TEXT      PRIMARY KEY,
    name                TEXT,
    review_count        INT,
    yelping_since       DATETIME,
    useful              INT,
    funny               INT,
    cool                INT,
    elite               TEXT,
    friends             TEXT,
    fans                INT,
    average_stars       NUMERIC(3,2),
    compliment_hot      INT,
    compliment_more     INT,
    compliment_profile  INT,
    compliment_cute     INT,
    compliment_list     INT,
    compliment_note     INT,
    compliment_plain    INT,
    compliment_cool     INT,
    compliment_funny    INT,
    compliment_writer   INT,
    compliment_photos   INT,
    CONSTRAINT FK_USER_DATE_TIME_ID FOREIGN KEY(yelping_since)      REFERENCES  date_time(timestamp)
);

/* Create Tip table 
*/
CREATE OR REPLACE TABLE tip (
    tip_id              INT  PRIMARY KEY   IDENTITY,
    user_id             TEXT,
    business_id         TEXT,
    text                TEXT,
    timestamp           DATETIME,
    compliment_count    INT,
	CONSTRAINT FK_TIP_BUSINESS_ID  FOREIGN KEY(business_id)    REFERENCES  business(business_id),
    CONSTRAINT FK_TIP_USER_ID      FOREIGN KEY(user_id)        REFERENCES  user(user_id),
    CONSTRAINT FK_TIP_DATE_TIME_ID FOREIGN KEY(timestamp)      REFERENCES  date_time(timestamp)
);

/* Create Review table */
CREATE OR REPLACE TABLE review (
    review_id           TEXT   PRIMARY KEY,
    user_id             TEXT,
    business_id         TEXT,
    stars               NUMERIC(3,2),
    useful              BOOLEAN,
    funny               BOOLEAN,
    cool                BOOLEAN,
    text                TEXT,
    timestamp           DATETIME,
	CONSTRAINT FK_REVIEW_BUSINESS_ID FOREIGN KEY(business_id)    REFERENCES  business(business_id),
    CONSTRAINT FK_REVIEW_USER_ID FOREIGN KEY(user_id)        REFERENCES  user(user_id),
    CONSTRAINT FK_REVIEW_DATE_TIME_ID FOREIGN KEY(timestamp)      REFERENCES  date_time(timestamp)
);

/* Create Chekin table*/
CREATE OR REPLACE TABLE checkin (
    checkin_id          INT     PRIMARY KEY  IDENTITY,
    business_id         TEXT,
    timestamp           DATETIME,
    CONSTRAINT FK_CHECKIN_BUSINESS_ID FOREIGN KEY(business_id)    REFERENCES  business(business_id)
);

/* Create Covid table */
CREATE OR REPLACE TABLE covid (
    covid_id                    INT     PRIMARY KEY  IDENTITY,
    business_id                 TEXT,
    highlights                  TEXT,
    delivery_or_takeout         TEXT,
    grubhub_enabled             TEXT,
    call_to_action_enabled      TEXT,
    request_a_quote_enabled     TEXT,
    covid_banner                TEXT,
    temporary_closed_until      TEXT,
    virtual_services_offered    TEXT,
    CONSTRAINT FK_COVID_BUSINESS_ID   FOREIGN KEY(business_id)    REFERENCES  business(business_id)
);

/* Create Temperature table */
CREATE OR REPLACE TABLE temperature (
    temperature_id              INT     PRIMARY KEY  IDENTITY,
    date                        DATE,
    temp_min                    FLOAT,
    temp_max                    FLOAT,
    temp_normal_min             FLOAT,
    temp_normal_max             FLOAT
);

/* Create Precipitation table */
CREATE OR REPLACE TABLE precipitation (
    precipitation_id            INT     PRIMARY KEY  IDENTITY,
    date                        DATE,
    precipitation               FLOAT,
    precipitation_normal        FLOAT
);


--################################################### DWH ###################################################--
USE SCHEMA YELP_WEATHER_INSIGHTS.DWH;

/* Create Dimension table business */
CREATE OR REPLACE TABLE dimBusiness (
    business_id                       TEXT   PRIMARY KEY,
    name                              TEXT,
    stars                             NUMERIC(3,2),
    review_count                      INT,
    is_open                           BOOLEAN,
	categories 			              TEXT,
	location_address                  TEXT,
    location_city                     TEXT,
    location_state                    TEXT,
    location_postal_code              INT,
    location_latitude                 FLOAT,
    location_longitude                FLOAT, 
	checkin_date                      DATETIME,
	covid_highlights                  TEXT,
    covid_delivery_or_takeout         TEXT,
    covid_grubhub_enabled             TEXT,
    covid_call_to_action_enabled      TEXT,
    covid_request_a_quote_enabled     TEXT,
    covid_covid_banner                TEXT,
    covid_temporary_closed_until      TEXT,
    covid_virtual_services_offered    TEXT
);


/* Create Dimension table Date */
CREATE OR REPLACE TABLE dimDate (
    timestamp           DATETIME    PRIMARY KEY,
    date                DATE,
    day                 INT,
    month               INT,
    year                INT,
	week                INT,
	quarter             INT
);

/* Create Dimension table User */
CREATE OR REPLACE TABLE dimUser (
    user_id             TEXT      PRIMARY KEY,
    name                TEXT,
    review_count        INT,
    yelping_since       DATETIME,
    useful              INT,
    funny               INT,
    cool                INT,
    elite               TEXT,
    friends             TEXT,
    fans                INT,
    average_stars       NUMERIC(3,2),
    compliment_hot      INT,
    compliment_more     INT,
    compliment_profile  INT,
    compliment_cute     INT,
    compliment_list     INT,
    compliment_note     INT,
    compliment_plain    INT,
    compliment_cool     INT,
    compliment_funny    INT,
    compliment_writer   INT,
    compliment_photos   INT
);


/* Create Dimension table Weather (Consolidating Temperature and Precipitation)*/
CREATE OR REPLACE TABLE dimWeather (
    date                        DATE    PRIMARY KEY,
    temp_min                    FLOAT,
    temp_max                    FLOAT,
    temp_normal_min             FLOAT,
    temp_normal_max             FLOAT,
    precipitation               FLOAT,
    precipitation_normal        FLOAT
);


/* Create Fact table Review */
CREATE OR REPLACE TABLE factReview (
    review_id           TEXT   PRIMARY KEY,
    user_id             TEXT,
    business_id         TEXT,
    stars               NUMERIC(3,2),
    useful              BOOLEAN,
    funny               BOOLEAN,
    cool                BOOLEAN,
    text                TEXT,
    timestamp           DATETIME,
	date                DATE,
	CONSTRAINT FK_FACT_REVIEW_DIMBUSINESS_ID              FOREIGN KEY(business_id)    REFERENCES  dimBusiness(business_id),
    CONSTRAINT FK_FACT_REVIEW_DIMUSER_ID                  FOREIGN KEY(user_id)        REFERENCES  dimUser(user_id),
    CONSTRAINT FK_FACT_REVIEW_TIMESTAMP_DIMDATE_TIMESTAMP FOREIGN KEY(timestamp)      REFERENCES  dimDate(timestamp),
	CONSTRAINT FK_FACT_REVIEW_DATE_DIMWEATHER_DATE        FOREIGN KEY(date)           REFERENCES  dimWeather(date)
);


