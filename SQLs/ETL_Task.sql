ALTER TASK HEALTH_TABLES_PROCESS RESUME;

CREATE OR REPLACE TASK HEALTH_TABLES_PROCESS
  WAREHOUSE = HEALTH_WH
  SCHEDULE = '3 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('HEALTH_STREAM')

AS
--RESET STREAM TO NOT AUTO RUN TASK
CREATE OR REPLACE STREAM HEALTH_STREAM ON TABLE "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT";

-- CREAT TIME DIM TABLE
-- Used to join to POPULATION table to get name, date for rest of data to join to
CREATE OR REPLACE TABLE HK.DATE_DIM (
   DATE             DATE        NOT NULL
  ,YEAR             SMALLINT    NOT NULL
  ,MONTH            SMALLINT    NOT NULL
  ,MONTH_NAME       CHAR(3)     NOT NULL
  ,DAY_OF_MON       SMALLINT    NOT NULL
  ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
  ,WEEK_OF_YEAR     SMALLINT    NOT NULL
  ,DAY_OF_YEAR      SMALLINT    NOT NULL
)
AS
  WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT => 1000))  -- Number of days after reference date in previous line
  )
  SELECT MY_DATE
        ,YEAR(MY_DATE)
        ,MONTH(MY_DATE)
        ,MONTHNAME(MY_DATE)
        ,DAY(MY_DATE)
        ,DAYOFWEEK(MY_DATE) + 1
        ,WEEKOFYEAR(MY_DATE)
        ,DAYOFYEAR(MY_DATE)
    FROM CTE_MY_DATE
;


-- CREATE POPULATION DATSET
-- Parses out ID and other aspects of the data set
-- Tries to eliminate dups - no promises
CREATE OR REPLACE TABLE HK.POPULATION AS(
  select 
    RECORD:identifier::STRING as ID,
    RECORD:age::INT as AGE,
    RECORD:bloodstype::STRING as BLOODTYPE,
    RECORD:gender::STRING as GENDER,
    RECORD:loaddate::TIMESTAMP AS LOADTIME
  FROM "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT" HKI
  INNER JOIN 
    (select 
        RECORD:identifier::STRING  AS ID, 
        MAX(RECORD:loaddate)::TIMESTAMP AS LOADTIME
    from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
    group by 1) U ON ID = U.ID AND RECORD:loaddate = U.LOADTIME 
  group by 1, 2, 3, 4, 5
);


-- DATED_POP
-- Creates a table of all names and date ranges
-- all other data points are joined to this data to get daily numbers
CREATE OR REPLACE TABLE HK.DATED_POP AS (
  select  ID, DATE
  FROM "HEALTHKIT"."HK"."POPULATION",
       "HEALTHKIT"."HK"."DATE_DIM"
  group by 1, 2
  order by id, date
);

---------------------------------------------------------------------------------------------------------------------------------------


create or replace table HK.ACTIVE_ENERGY_BURNED as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:ActiveEnergyBurned))

  order by 1, 2
);

create or replace table HK.APPLESTANDTIME as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:AppleStandTime))
  
  order by 1, 2
);

create or replace table HK.BASALENERGYBURNED as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:BasalEnergyBurned))

  order by 1, 2
);


create or replace table HK.FLIGHTSCLIMBED as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:FlightsClimbed))

  order by 1, 2
);


create or replace table HK.STEPCOUNT as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:StepCount))

  order by 1, 2
);


create or replace table HK.HEARTRATE as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:HeartRate))

  order by 1, 2
);


create or replace table HK.HEARTRATESDNN as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:HeartRateVariabilitySDNN))

  order by 1, 2
);


create or replace table HK.SUGAR as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietarySugar))

  
);


create or replace table HK.SODIUM as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietarySodium))

  
);


create or replace table HK.PROTEIN as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryProtein))

  
);


create or replace table HK.FATTOTAL as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatTotal))

  
);


create or replace table HK.FATSAT as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatSaturated))

  
);


create or replace table HK.FATPOLY as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatPolyunsaturated))

  
);


create or replace table HK.FATMONO as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatMonounsaturated))

  
);


create or replace table HK.DIETARYENERGY as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryEnergyConsumed))

  
);


create or replace table HK.CHOLESTEROL as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryCholesterol))

  
);


create or replace table HK.CARBS as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,
    
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKExternalUUID::STRING as ExternalUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodImageName::STRING as FoodImageName,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodMeal::STRING as FoodMeal,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodType::STRING as FoodType,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodTypeUUID::STRING as FoodTypeUUID,
        
    PARSE_JSON(REPLACE('{"'||replace(replace(ARRAY_TO_STRING(split(right(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|'), 
        len(ARRAY_TO_STRING(split(split(split(value, '{')[1],'}')[0]::STRING,'    '),'|')) -1),'|'),'|'),' ',''),':','":"')||'"}','|','","')):HKFoodUSDANumber::STRING as FoodUSDANumber
        
  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryCarbohydrates))

  
);


create or replace table HK.WEIGHT as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    (replace(split(value, ' ')[15],'(','') || ' ' || split(value, ' ')[16] || ' ' || split(value, ' ')[17])::TIMESTAMP AS DATE,
    split(value, ' ')[0]::DOUBLE as VALUE,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    split(value, ' ')[6]::STRING  AS DEVICE,
    split(value, ' ')[14]::STRING as ExternalUUID

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:BodyMass))
  
  order by 1, 2
);


create or replace table HK.WALKRUNDISTANCE as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:DistanceWalkingRunning))

  order by 1, 2
);


create or replace table HK.ENVIRONMENTAUDIO as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:EnvironmentalAudioExposure))

  order by 1, 2
);


create or replace table HK.HEAPHONEAUDIO as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:HeadphoneAudioExposure))

  order by 1, 2
);


create or replace table HK.RESTINGHEARTRATE as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:RestingHeartRate))

  order by 1, 2
);


create or replace table HK.WALKINGHEARTAVG as (
  select DISTINCT
    RECORD:identifier::STRING as ID,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-7] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-6] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-5],'(', '')::TIMESTAMP as STARTTIME,

    replace(split(value, ' ')[ARRAY_SIZE(split(value, ' '))-3] ||' '||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-2] ||
    split(value, ' ')[ARRAY_SIZE(split(value, ' '))-1],')', '')::TIMESTAMP as ENDTIME,

    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "HEALTHKIT"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:WalkingHeartRateAverage))

  order by 1, 2
);

create or replace table HK.POP_AGG2 AS (
    select DISTINCT
    DP.ID
    ,DP.DATE
    ,AEB.VALUE as ACTIVE_ENERGY_BURNED
    ,AST.VALUE AS APPLE_STAND_TIME
    ,BEB.VALUE AS BASAL_ENERGY_BURNED
    ,CARBS.VALUE AS CARBS
    ,CSTRL.VALUE AS CHOLESTEROL
    ,DE.VALUE as DIETARY_ENERGY
    ,FM.VALUE AS FATMONO
    ,FP.VALUE AS FATPOLY
    ,FS.VALUE AS FATSAT
    ,FT.VALUE AS FATTOTAL
    ,FC.VALUE AS FLIGHTSCLIMBED
    ,PRO.VALUE AS PROTEIN
    ,SOD.VALUE AS SODIUM
    ,STEPS.VALUE AS STEPS
    ,SUG.VALUE AS SUGAR
    ,WRD.VALUE AS WALK_RUN
    
    from "HEALTHKIT"."HK"."DATED_POP" DP 
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."ACTIVE_ENERGY_BURNED" group by 1,2,4) AEB ON DP.ID = AEB.ID AND DP.DATE = AEB.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."APPLESTANDTIME" group by 1,2,4) AST ON DP.ID = AST.ID AND DP.DATE = AST.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."BASALENERGYBURNED" group by 1,2,4) BEB ON DP.ID = BEB.ID AND DP.DATE = BEB.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."CARBS" group by 1,2,4) CARBS ON DP.ID = CARBS.ID AND DP.DATE = CARBS.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."CHOLESTEROL" group by 1,2,4) CSTRL ON DP.ID = CSTRL.ID AND DP.DATE = CSTRL.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."DIETARYENERGY" group by 1,2,4) DE ON DP.ID = DE.ID AND DP.DATE = DE.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."FATMONO" group by 1,2,4) FM ON DP.ID = FM.ID AND DP.DATE = FM.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."FATPOLY" group by 1,2,4) FP ON DP.ID = FP.ID AND DP.DATE = FP.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."FATSAT" group by 1,2,4) FS ON DP.ID = FS.ID AND DP.DATE = FS.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."FATTOTAL" group by 1,2,4) FT ON DP.ID = FT.ID AND DP.DATE = FT.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."FLIGHTSCLIMBED" group by 1,2,4) FC ON DP.ID = FC.ID AND DP.DATE = FC.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."PROTEIN" group by 1,2,4) PRO ON DP.ID = PRO.ID AND DP.DATE = PRO.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."SODIUM" group by 1,2,4) SOD ON DP.ID = SOD.ID AND DP.DATE = SOD.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."STEPCOUNT" group by 1,2,4) STEPS ON DP.ID = STEPS.ID AND DP.DATE = STEPS.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."SUGAR" group by 1,2,4) SUG ON DP.ID = SUG.ID AND DP.DATE = SUG.DATE
        LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "HEALTHKIT"."HK"."WALKRUNDISTANCE" group by 1,2,4) WRD ON DP.ID = WRD.ID AND DP.DATE = WRD.DATE

    order by 1 asc, 2 asc
);

ALTER TABLE "HEALTHKIT"."HK"."POP_AGG" SWAP WITH "HEALTHKIT"."HK"."POP_AGG2";
DROP TABLE "HEALTHKIT"."HK"."POP_AGG2";

GRANT SELECT ON ALL TABLES IN SCHEMA "HEALTHKIT"."HK" TO ROLE HEALTHKITSERVICE;


; -- END TASK




