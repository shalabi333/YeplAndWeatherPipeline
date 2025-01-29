USE DATABASE "YELP_WEATHER_INSIGHTS";
USE SCHEMA "YELP_WEATHER_INSIGHTS"."DWH";

--################################################### ODS_TO_DWH ###################################################--


-- DimensionTableDate
INSERT INTO dimDate ( timestamp,date,day,month,year,week,quarter)
SELECT
	time.timestamp,
    time.date,     
    time.day,      
    time.month,    
    time.year,     
	time.week,     
	time.quarter
FROM YELP_WEATHER_INSIGHTS.ODS.DATE_TIME AS time;



-- DimensionTableWeather 
INSERT INTO dimWeather ( date,temp_min,temp_max,temp_normal_min,temp_normal_max, precipitation, precipitation_normal )
SELECT temp.date,
       temp.temp_min,
       temp.temp_max,
       temp.temp_normal_min,
       temp.temp_normal_max,
       pre.precipitation,
       pre.precipitation_normal
FROM YELP_WEATHER_INSIGHTS.ODS.PRECIPITATION as pre
INNER JOIN YELP_WEATHER_INSIGHTS.ODS.TEMPERATURE AS temp
ON pre.date = temp.date;


--DimensionTableUser 
INSERT INTO dimUser ( user_id,name,review_count,yelping_since,useful,funny,cool,elite,
    friends,fans,average_stars,compliment_hot,compliment_more,compliment_profile,compliment_cute,compliment_list,
    compliment_note,compliment_plain,compliment_cool,compliment_funny,compliment_writer,compliment_photos)
SELECT
	user.user_id,
    user.name,
    user.review_count,
    user.yelping_since,
    user.useful,
    user.funny,
    user.cool,
    user.elite,
    user.friends,
    user.fans,
    user.average_stars,
    user.compliment_hot,
    user.compliment_more,
    user.compliment_profile ,
    user.compliment_cute,
    user.compliment_list,
    user.compliment_note,
    user.compliment_plain,
    user.compliment_cool,
    user.compliment_funny,
    user.compliment_writer,
    user.compliment_photos
FROM YELP_WEATHER_INSIGHTS.ODS.USER as user;


-- DimensionTableBusiness 
INSERT INTO dimBusiness ( business_id,name,stars,review_count,is_open,categories,location_address,location_city,
                        location_state,location_postal_code,location_latitude,location_longitude,checkin_date,
                        covid_highlights,covid_delivery_or_takeout,covid_grubhub_enabled,covid_call_to_action_enabled,
                        covid_request_a_quote_enabled,covid_covid_banner,covid_temporary_closed_until,covid_virtual_services_offered )
SELECT business.business_id,
       business.name,
       business.stars,
       business.review_count,
       business.is_open,
       business.categories,
       location.address,
       location.city,
       location.state,
       location.postal_code,
       location.latitude,
       location.longitude,
       checkin.timestamp,
       covid.highlights,
       covid.delivery_or_takeout,
       covid.grubhub_enabled,
       covid.call_to_action_enabled,
       covid.request_a_quote_enabled,
       covid.covid_banner,
       covid.temporary_closed_until,
       covid.virtual_services_offered
FROM YELP_WEATHER_INSIGHTS.ODS.BUSINESS        AS business
LEFT JOIN YELP_WEATHER_INSIGHTS.ODS.LOCATION   AS location ON business.location_id=location.location_id
LEFT JOIN YELP_WEATHER_INSIGHTS.ODS.CHECKIN    AS checkin ON business.business_id=checkin.business_id
LEFT JOIN YELP_WEATHER_INSIGHTS.ODS.COVID      AS covid ON business.business_id=covid.business_id;



--FactTableReview 
INSERT INTO factReview (review_id, user_id, business_id, stars, useful, funny, cool, text, timestamp, date)
SELECT  review.review_id,
        review.user_id,
        review.business_id,
        review.stars,
        review.useful,
        review.funny,
        review.cool,
        review.text,
        review.timestamp,
        time.date
FROM YELP_WEATHER_INSIGHTS.ODS.REVIEW AS review
LEFT JOIN YELP_WEATHER_INSIGHTS.ODS.DATE_TIME AS time ON review.timestamp=time.timestamp;