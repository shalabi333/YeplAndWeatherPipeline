USE DATABASE "YELP_WEATHER_INSIGHTS";
USE SCHEMA "YELP_WEATHER_INSIGHTS"."ODS";

--################################################### STAGING_TO_ODS ###################################################--






-- Location
INSERT INTO location (address, city, state, postal_code, latitude, longitude)
SELECT business.address, business.city, business.state, business.postal_code, business.latitude, business.longitude
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_BUSINESS AS business
QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY business.state, business.postal_code, business.city, business.address 
              ORDER BY business.state, business.postal_code, business.city, business.address) = 1;





             
-- Business
INSERT INTO business (business_id, name, location_id, stars, review_count, is_open, categories)
SELECT  business.business_id,
        business.name,
        location.location_id,
        business.stars,
        business.review_count,
        business.is_open,
		business.categories
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_BUSINESS AS business
LEFT JOIN location AS location
ON business.address = location.address AND
business.city = location.city AND
business.state = location.state AND
business.postal_code = location.postal_code
WHERE business.business_id NOT IN (SELECT business_id FROM business);






-- BusinessHours
INSERT INTO business_hours (business_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
SELECT  business.business_id,
		business.hours_Monday,
		business.hours_Tuesday,
		business.hours_Wednesday,
		business.hours_Thursday,
		business.hours_Friday,
		business.hours_Saturday,
		business.hours_Sunday
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_BUSINESS AS business;






-- BusinessAttributes
INSERT INTO business_attributes (
		business_id,NoiseLevel,BikeParking,RestaurantsAttire,BusinessAcceptsCreditCards,BusinessParking,RestaurantsReservations,GoodForKids,RestaurantsTakeOut,Caters, 						
		WiFi,RestaurantsDelivery,HasTV,RestaurantsPriceRange2,Alcohol,Music,BusinessAcceptsBitcoin,GoodForDancing,DogsAllowed,BestNights,
		RestaurantsGoodForGroups,OutdoorSeating,HappyHour,RestaurantsTableService,GoodForMeal,WheelchairAccessible,Ambience,CoatCheck,DriveThru,Smoking,BYOB,Corkage )
SELECT 	business.business_id,
		business.attributes_NoiseLevel,
		business.attributes_BikeParking,
		business.attributes_RestaurantsAttire,
		business.attributes_BusinessAcceptsCreditCards,
		business.attributes_BusinessParking,
		business.attributes_RestaurantsReservations,
		business.attributes_GoodForKids,
		business.attributes_RestaurantsTakeOut,
		business.attributes_Caters,
		business.attributes_WiFi,
		business.attributes_RestaurantsDelivery,
		business.attributes_HasTV,
		business.attributes_RestaurantsPriceRange2,
		business.attributes_Alcohol,
		business.attributes_Music,
		business.attributes_BusinessAcceptsBitcoin,
		business.attributes_GoodForDancing,
		business.attributes_DogsAllowed,
		business.attributes_BestNights,
		business.attributes_RestaurantsGoodForGroups,
		business.attributes_OutdoorSeating,
		business.attributes_HappyHour,
		business.attributes_RestaurantsTableService,
		business.attributes_GoodForMeal,
		business.attributes_WheelchairAccessible,
		business.attributes_Ambience,
		business.attributes_CoatCheck,
		business.attributes_DriveThru,
		business.attributes_Smoking,
		business.attributes_BYOB,
		business.attributes_Corkage
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_BUSINESS AS business;






-- TimestampsUser
INSERT INTO date_time (timestamp, date, day, month, year, week, quarter)
SELECT user.yelping_since,
       DATE(user.yelping_since),
       DAY(user.yelping_since),
       MONTH(user.yelping_since),
       YEAR(user.yelping_since),
	   WEEK(user.yelping_since),
	   QUARTER(user.yelping_since)
FROM yelp_weather_insights.staging.yelp_user AS user
WHERE user.yelping_since NOT IN (SELECT timestamp FROM date_time);






-- User
INSERT INTO user (user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends,
                  fans, average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute,
                  compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny,
                  compliment_writer, compliment_photos)
       
SELECT user.user_id, user.name, user.review_count, user.yelping_since, user.useful, user.funny, user.cool, user.elite, user.friends,
       user.fans, user.average_stars, user.compliment_hot, user.compliment_more, user.compliment_profile, user.compliment_cute,
       user.compliment_list, user.compliment_note, user.compliment_plain, user.compliment_cool, user.compliment_funny,
       user.compliment_writer, user.compliment_photos
FROM yelp_weather_insights.staging.yelp_user AS user
WHERE user.user_id NOT IN (SELECT user_id FROM user);






-- TimestampsTip
INSERT INTO date_time (timestamp, date, day, month, year, week, quarter)
SELECT tip.timestamp,
       DATE(tip.timestamp),
       DAY(tip.timestamp),
       MONTH(tip.timestamp),
       YEAR(tip.timestamp),
	   WEEK(tip.timestamp),
	   QUARTER(tip.timestamp)
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_TIP AS tip
WHERE tip.timestamp NOT IN (SELECT timestamp FROM date_time);






--Tips
INSERT INTO tip (user_id, business_id, text, timestamp, compliment_count)
SELECT tip.user_id, tip.business_id, tip.text, tip.timestamp, tip.compliment_count
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_TIP AS tip;






--Checkin
INSERT INTO checkin (business_id, timestamp)
SELECT business_id, 
       CAST(F.value AS DATETIME)AS DATE
FROM   YELP_WEATHER_INSIGHTS.STAGING.YELP_CHECKIN  ,
       LATERAL Flatten(input => split(DATE,',')) AS F;






--TimestampsCheckin 
INSERT INTO date_time (timestamp, date, day, month, year, week, quarter)
SELECT checkin.timestamp,
       DATE(checkin.timestamp),
       DAY(checkin.timestamp),
       MONTH(checkin.timestamp),
       YEAR(checkin.timestamp),
	   WEEK(checkin.timestamp),
	   QUARTER(checkin.timestamp)
FROM YELP_WEATHER_INSIGHTS.ODS.CHECKIN AS checkin
WHERE checkin.timestamp NOT IN (SELECT timestamp FROM date_time);






-- Covid 
INSERT INTO covid (business_id, highlights, delivery_or_takeout, grubhub_enabled, call_to_action_enabled, 
				   request_a_quote_enabled, covid_banner, temporary_closed_until, virtual_services_offered)
SELECT covid.business_id, highlights, covid.delivery_or_takeout, covid.grubhub_enabled, covid.call_to_action_enabled, 
	   covid.request_a_quote_enabled, covid.covid_banner, covid.temporary_closed_until, covid.virtual_services_offered
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_COVID AS covid;






-- TimestampsReview 
INSERT INTO date_time (timestamp, date, day, month, year, week, quarter)
SELECT review.timestamp,
       DATE(review.timestamp),
       DAY(review.timestamp),
       MONTH(review.timestamp),
       YEAR(review.timestamp),
	   WEEK(review.timestamp),
	   QUARTER(review.timestamp)
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_REVIEW AS review
WHERE review.timestamp NOT IN (SELECT timestamp FROM date_time);






-- Review 
INSERT INTO review (review_id, user_id, business_id, stars, useful, funny, cool, text, timestamp)
SELECT  review.review_id, review.user_id, review.business_id, review.stars, 
		review.useful, review.funny, review.cool, review.text, review.timestamp
FROM YELP_WEATHER_INSIGHTS.STAGING.YELP_REVIEW  AS review
WHERE review.review_id NOT IN (SELECT review_id FROM review);






-- Temperature
INSERT INTO temperature (date, temp_min, temp_max, temp_normal_min, temp_normal_max)
SELECT TO_DATE(CAST(temp.date AS STRING), 'YYYYMMDD'), temp.min, temp.max, temp.normal_min, temp.normal_max
FROM YELP_WEATHER_INSIGHTS.STAGING.WEATHER_TEMPERATURE AS temp;






-- Precipitation

INSERT INTO precipitation (date, precipitation, precipitation_normal)
SELECT TO_DATE(CAST(prec.date AS STRING), 'YYYYMMDD'), TRY_CAST(prec.precipitation AS FLOAT), prec.precipitation_normal
FROM YELP_WEATHER_INSIGHTS.STAGING.WEATHER_PRECIPITATION AS prec;



























