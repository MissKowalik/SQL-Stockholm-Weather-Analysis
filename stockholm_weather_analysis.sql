-- 1. Which was the hottest summer month and what was the temperature each year?
-- (For each year - what was the hottest month and the respective temperature?)
SELECT
    year
    ,month
    ,temperature
FROM
   (SELECT
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,avg(DAILY_AVERAGE_TEMPERATURE) AS temperature
        ,RANK() OVER(PARTITION BY year ORDER BY temperature DESC) AS temp_rank
    FROM WEATHER_DATA.GOLD.DAILY_TEMPERATURE
    WHERE month IN (6,7,8)
    GROUP BY year, month)
WHERE temp_rank = 1;



-- 2. Which was the hottest day in the summer months and what was the temperature each year?
SELECT
    year
    ,month
    ,day
    ,temperature
FROM    
    (SELECT 
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,EXTRACT(day FROM RECORD_DATE) AS day
        ,avg(HOURLY_TEMPERATURE) AS temperature
        ,RANK() OVER(PARTITION BY year ORDER BY temperature DESC) AS temp_rank
    FROM WEATHER_DATA.SILVER.HOURLY_TEMPERATURE
    WHERE month IN (6,7,8)
    GROUP BY year, month, day)
WHERE temp_rank = 1;


-- 3. Which was the coldest winter month and what was the temperature each year? 

SELECT 
    year
    ,month
    ,temperature
FROM
    (SELECT
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,ROUND(avg(DAILY_AVERAGE_TEMPERATURE),1) AS temperature
        ,RANK() OVER(PARTITION BY year ORDER BY temperature) AS rank_temp
    FROM WEATHER_DATA.GOLD.DAILY_TEMPERATURE
    WHERE month IN (9,10,11,12,1,2)
    GROUP BY year, month)
WHERE rank_temp = 1;



-- 4. Which was the coldest day in the winter months and what was the temperature each year?
SELECT 
    year
    ,month
    ,day
    ,temperature
FROM
    (SELECT
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,EXTRACT(day FROM RECORD_DATE) AS day
        ,ROUND(avg(HOURLY_TEMPERATURE),1) AS temperature
        ,RANK() OVER(PARTITION BY year ORDER BY temperature) AS rank_temp
    FROM WEATHER_DATA.SILVER.HOURLY_TEMPERATURE
    WHERE month IN (9,10,11,12,1,2)
    GROUP BY year, month, day)
WHERE rank_temp = 1
ORDER BY year;



-- 5. Which was the windiest month each year and what was the average windspeed?
SELECT
    year
    ,month
    ,windspeed
FROM
    (SELECT
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,avg(AVERAGE_WIND_SPEED) AS windspeed
        ,RANK() OVER(PARTITION BY year ORDER BY windspeed DESC) AS wind_rank
    FROM WEATHER_DATA.GOLD.DAILY_WIND_SPEED
    GROUP BY year, month)
WHERE wind_rank = 1;



-- 6. Which was the windiest day and what was its average wind speed, calculate for each year?
SELECT
    year
    ,month
    ,day
    ,windspeed
FROM
    (SELECT
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,EXTRACT(day FROM RECORD_DATE) AS day
        ,avg(hourly_wind_speed) AS windspeed
        ,RANK() OVER(PARTITION BY year ORDER BY windspeed DESC) AS wind_rank
    FROM WEATHER_DATA.SILVER.HOURLY_WIND_SPEED
    GROUP BY year, month, day)
WHERE wind_rank = 1;


-- 7. Which month had the most rain fall each year?
SELECT
    year
    ,month
    ,rainfall
FROM
    (SELECT
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,sum(daily_rain_fall) AS rainfall
        ,RANK() OVER(PARTITION BY year ORDER BY rainfall DESC) AS rain_rank
    FROM WEATHER_DATA.GOLD.DAILY_RAIN_FALL
    GROUP BY year, month)
WHERE rain_rank = 1;



-- 8. Which month had the most snowfall each year?
SELECT
    year
    ,month
    ,snowfall
FROM
    (SELECT
        EXTRACT(year FROM RECORD_DATE) AS year
        ,EXTRACT(month FROM RECORD_DATE) AS month
        ,sum(daily_snow_fall) AS snowfall
        ,RANK() OVER(PARTITION BY year ORDER BY snowfall DESC) AS snow_rank
    FROM WEATHER_DATA.GOLD.DAILY_SNOW_FALL
    GROUP BY year, month)
WHERE snow_rank = 1;



-- 9. Which date marked the start of spring each year?

-- Spring will arrive after 7 consecutive days with temperature above 0 
-- 15 February is set as the earliest allowed date 
-- The latest date for spring arrival is 31 July

SELECT
    date AS spring_start
FROM 
    (SELECT
        date
        ,RANK() OVER(PARTITION BY EXTRACT(year FROM date) ORDER BY date) AS rank
    FROM
        (SELECT 
            RECORD_DATE AS date
            ,avg(hourly_temperature) AS temperature
            ,LAG(temperature, 6) OVER (ORDER BY Record_Date) AS TempMinus6Days
            ,LAG(temperature, 5) OVER (ORDER BY Record_Date) AS TempMinus5Days
            ,LAG(temperature, 4) OVER (ORDER BY Record_Date) AS TempMinus4Days
            ,LAG(temperature, 3) OVER (ORDER BY Record_Date) AS TempMinus3Days
            ,LAG(temperature, 2) OVER (ORDER BY Record_Date) AS TempMinus2Days
            ,LAG(temperature, 1) OVER (ORDER BY Record_Date) AS TempMinus1Days
        FROM WEATHER_DATA.SILVER.HOURLY_TEMPERATURE
        GROUP BY date
        ORDER BY date)
    WHERE temperature >= 0
    AND TempMinus6Days >= 0
    AND TempMinus5Days >= 0
    AND TempMinus4Days >= 0
    AND TempMinus3Days >= 0
    AND TempMinus2Days >= 0
    AND TempMinus1Days >= 0
    AND RIGHT(date, 5) BETWEEN '02-15' AND '07-31')
WHERE rank = 1
ORDER BY date;


-- 9. cleaned up query

SELECT
    date AS spring_start
FROM 
    (SELECT
        date
        ,RANK() OVER(PARTITION BY EXTRACT(year FROM date) ORDER BY date) AS rank
    FROM
        (SELECT 
            RECORD_DATE AS date
            ,avg(hourly_temperature) AS temperature
            ,CASE WHEN temperature >0 THEN 1 ELSE 0 END AS temp_above_zero
            ,SUM(temp_above_zero) OVER(ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_total
        FROM WEATHER_DATA.SILVER.HOURLY_TEMPERATURE
        GROUP BY date
        ORDER BY date)
    WHERE running_total = 7
    AND RIGHT(date, 5) BETWEEN '02-15' AND '07-31')
WHERE rank = 2
ORDER BY date;



-- 10. Which year saw the most weather anomalies? 

-- Anomalies:
-- Total daily rain fall exceeds 10mm
-- Total daily snow fall exceeds 30cm of snow
-- Average daily temperature was equal to more than 28 C 
-- Hourly wind speed exceeds 60 km/h for any given hour of the day


WITH rainfall AS 
(
    SELECT
        EXTRACT(year FROM date) AS year
        ,COUNT(ten_mm_rain) AS rain_anomalies
    FROM
        (SELECT
            RECORD_DATE AS date
            ,CASE WHEN sum(hourly_rain_fall) > 10 THEN 1 ELSE 0 END AS ten_mm_rain
        FROM WEATHER_DATA.SILVER.HOURLY_RAIN_FALL
        GROUP BY date)
    WHERE ten_mm_rain = 1
    GROUP BY year
),

    snowfall AS 
(
    SELECT
        EXTRACT(year FROM date) AS year
        ,COUNT(thirty_cm_snow) AS snow_anomalies
    FROM
        (SELECT
            RECORD_DATE AS date
            ,CASE WHEN sum(hourly_snow_fall) >30 THEN 1 ELSE 0 END AS thirty_cm_snow
        FROM WEATHER_DATA.SILVER.HOURLY_SNOW_FALL
        GROUP BY date)
    WHERE thirty_cm_snow = 1
    GROUP BY year
),

    temperature AS
(
    SELECT
        EXTRACT(year FROM date) AS year
        ,COUNT(hot_days) AS temperature_anomalies
    FROM
        (SELECT
            RECORD_DATE AS date
            ,CASE WHEN avg(hourly_temperature) >28 THEN 1 ELSE 0 END AS hot_days
        FROM WEATHER_DATA.SILVER.HOURLY_TEMPERATURE
        GROUP BY date)
    WHERE hot_days = 1
    GROUP BY year
),

    wind AS
(
    SELECT
        EXTRACT(year FROM date) AS year
        ,COUNT(sum_windy_hours) AS wind_anomalies
    FROM
        (SELECT
            RECORD_DATE AS date
            ,RECORD_HOUR AS hour
            ,avg(hourly_wind_speed)
            ,CASE WHEN avg(hourly_wind_speed) >60 THEN 1 ELSE 0 END AS windy_hours
            ,SUM(windy_hours) OVER(PARTITION BY date) AS sum_windy_hours
        FROM WEATHER_DATA.SILVER.HOURLY_WIND_SPEED
        GROUP BY date, hour) 
    WHERE sum_windy_hours = 24
    GROUP BY year
)

SELECT
    r.year
    ,(COALESCE(r.rain_anomalies, 0) + COALESCE(s.snow_anomalies, 0) + COALESCE(t.temperature_anomalies, 0) + COALESCE(w.wind_anomalies, 0)) AS total_weather_anomalies
FROM rainfall AS r
LEFT JOIN snowfall AS s
ON r.year = s.year
LEFT JOIN temperature AS t
ON r.year = t.year
LEFT JOIN wind AS w
ON r.year = w.year
ORDER BY total_weather_anomalies DESC
LIMIT 1;