  
CREATE or replace PROCEDURE SNOWHEALTH.HK.HEALTH_TABLES_SPROC()
  RETURNS BOOLEAN
  LANGUAGE javascript
  EXECUTE AS OWNER
 AS
  $$
  
  /*
  var load = snowflake.execute( { sqlText:
  `copy into SNOWHEALTH.PUBLIC.HEALTHKIT_IMPORT
    from @SNOWHEALTH.PUBLIC.SNOWHEALTHS3
    FILE_FORMAT = (TYPE = JSON);` })
  */

  var load = snowflake.execute( { sqlText: `
      create or replace TABLE POP_AGG (
        ID VARCHAR(16777216),
        DATE DATE,
        AGE NUMBER(38,0),
        BLOODTYPE VARCHAR(16777216),
        GENDER VARCHAR(16777216),
        ACTIVE_ENERGY_BURNED FLOAT,
        APPLE_STAND_TIME FLOAT,
        BASAL_ENERGY_BURNED FLOAT,
        CARBS FLOAT,
        CHOLESTEROL FLOAT,
        DIETARY_ENERGY FLOAT,
        FATMONO FLOAT,
        FATPOLY FLOAT,
        FATSAT FLOAT,
        FATTOTAL FLOAT,
        FLIGHTSCLIMBED FLOAT,
        PROTEIN FLOAT,
        SODIUM FLOAT,
        STEPS FLOAT,
        SUGAR FLOAT,
        WALK_RUN FLOAT,
        HEART_RATE_ARR ARRAY,
        REST_HEART_RATE_ARR ARRAY,
        SDNN_HEART_RATE_ARR ARRAY,
        WALK_HEART_RATE_ARR ARRAY,
        STAND_TIME_ARR ARRAY,
        ENVIRONMENT_AUDIO_ARR ARRAY,
        HEADPHONE_AUDIO_ARR ARRAY,
        FLIGHTS_ARR ARRAY,
        WALK_RUN_ARR ARRAY,
        STEPS_ARR ARRAY,
        ACTIVE_ENERGY_ARR ARRAY,
        BASAL_ARR ARRAY,
        RUN_DATE TIMESTAMP_LTZ(9)
      );`})



  
  var rs = snowflake.execute( { sqlText: 
      `CREATE OR REPLACE TABLE HK.DATE_DIM (
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
            FROM TABLE(GENERATOR(ROWCOUNT => 366))  -- Number of days after reference date in previous line
        )
        SELECT MY_DATE
              ,YEAR(MY_DATE)
              ,MONTH(MY_DATE)
              ,MONTHNAME(MY_DATE)
              ,DAY(MY_DATE)
              ,DAYOFWEEK(MY_DATE) + 1
              ,WEEKOFYEAR(MY_DATE)
              ,DAYOFYEAR(MY_DATE)
          FROM CTE_MY_DATE`
       } );
       
rs = snowflake.execute( { sqlText: 
      `CREATE OR REPLACE TABLE HK.HOUR_DIM (
         DATE             DATETIME    NOT NULL
        ,YEAR             SMALLINT    NOT NULL
        ,MONTH            SMALLINT    NOT NULL
        ,MONTH_NAME       CHAR(3)     NOT NULL
        ,DAY_OF_MON       SMALLINT    NOT NULL
        ,HOUR             SMALLINT    NOT NULL
        ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
        ,WEEK_OF_YEAR     SMALLINT    NOT NULL
        ,DAY_OF_YEAR      SMALLINT    NOT NULL
      )
      AS
        WITH CTE_MY_HOURS AS (
          SELECT DATEADD(HOUR, SEQ4(), '2020-01-01 00') AS MY_DATE
            FROM TABLE(GENERATOR(ROWCOUNT => 366 * 24))  -- Number of days after reference date in previous line
        )
        SELECT MY_DATE
              ,YEAR(MY_DATE)
              ,MONTH(MY_DATE)
              ,MONTHNAME(MY_DATE)
              ,DAY(MY_DATE)
              ,EXTRACT(HOUR FROM MY_DATE)
              ,DAYOFWEEK(MY_DATE) + 1
              ,WEEKOFYEAR(MY_DATE)
              ,DAYOFYEAR(MY_DATE)
          FROM CTE_MY_HOURS;`
       } );


       rs = snowflake.execute( { sqlText: 
        `CREATE OR REPLACE TABLE HK.POPULATION AS(
          select 
            RECORD:identifier::STRING as ID,
            RECORD:age::INT as AGE,
            RECORD:bloodstype::STRING as BLOODTYPE,
            RECORD:gender::STRING as GENDER,
            RECORD:loaddate::TIMESTAMP AS LOADTIME
          FROM "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT" HKI
          INNER JOIN 
            (select 
                RECORD:identifier::STRING  AS ID, 
                MAX(RECORD:loaddate)::TIMESTAMP AS LOADTIME
            from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
            group by 1) U ON ID = U.ID AND RECORD:loaddate = U.LOADTIME 
          group by 1, 2, 3, 4, 5
        );`} );
      

      rs = snowflake.execute( { sqlText: 
      `CREATE OR REPLACE TABLE HK.DATED_POP AS (
          select  ID, DATE
          FROM "SNOWHEALTH"."HK"."POPULATION",
               "SNOWHEALTH"."HK"."DATE_DIM"
          group by 1, 2
          order by id, date
        );`} );
      
      rs = snowflake.execute( { sqlText: 
      `CREATE OR REPLACE TABLE HK.HOURS_POP AS (
          select  ID, DATE
          FROM "SNOWHEALTH"."HK"."POPULATION",
              "SNOWHEALTH"."HK"."HOUR_DIM"
          group by 1, 2
          order by id, date
        );`} );
      
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.ACTIVE_ENERGY_BURNED as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:ActiveEnergyBurned))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.APPLESTANDTIME as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:AppleStandTime))
  
  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.BASALENERGYBURNED as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:BasalEnergyBurned))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.FLIGHTSCLIMBED as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:FlightsClimbed))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.STEPCOUNT as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:StepCount))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.HEARTRATE as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:HeartRate))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.HEARTRATESDNN as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:HeartRateVariabilitySDNN))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.SUGAR as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietarySugar))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.SODIUM as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietarySodium))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.PROTEIN as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryProtein))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.FATTOTAL as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatTotal))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.FATSAT as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatSaturated))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.FATPOLY as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatPolyunsaturated))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.FATMONO as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryFatMonounsaturated))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.DIETARYENERGY as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryEnergyConsumed))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.CHOLESTEROL as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryCholesterol))

  
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.CARBS as (
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
        
  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:DietaryCarbohydrates))

  
);
`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.WEIGHT as (
  select DISTINCT
    RECORD:identifier::STRING as ID,
    (replace(split(value, ' ')[15],'(','') || ' ' || split(value, ' ')[16] || ' ' || split(value, ' ')[17])::TIMESTAMP AS DATE,
    split(value, ' ')[0]::DOUBLE as VALUE,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    split(value, ' ')[3] || ' ' || split(value, ' ')[4] AS APP,
    split(value, ' ')[6]::STRING  AS DEVICE,
    split(value, ' ')[14]::STRING as ExternalUUID

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT"
  ,table(flatten(input => RECORD:BodyMass))
  
  order by 1, 2
);
`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.WALKRUNDISTANCE as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:DistanceWalkingRunning))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.ENVIRONMENTAUDIO as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:EnvironmentalAudioExposure))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.HEAPHONEAUDIO as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:HeadphoneAudioExposure))

  order by 1, 2
);
`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.RESTINGHEARTRATE as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:RestingHeartRate))

  order by 1, 2
);`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `create or replace table HK.WALKINGHEARTAVG as (
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

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:WalkingHeartRateAverage))

  order by 1, 2
);`} );
//--------------------------------------------------------
     rs = snowflake.execute( { sqlText: 
      `create or replace table HK.POP_AGG2 AS (
        select DISTINCT
          DP.ID
          ,DP.DATE
          ,POP.AGE AS AGE
          ,POP.BLOODTYPE AS BLOODTYPE
          ,POP.GENDER AS GENDER
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
          
          ,HRTRT.DATA AS HEART_RATE_ARR
          ,RHRTRT.DATA AS REST_HEART_RATE_ARR
          ,SDNNHRTRT.DATA AS SDNN_HEART_RATE_ARR
          ,WHRTRT.DATA AS WALK_HEART_RATE_ARR
          ,TAST.DATA AS STAND_TIME_ARR
          ,ENAUDIO.DATA AS ENVIRONMENT_AUDIO_ARR
          ,HPAUDIO.DATA AS HEADPHONE_AUDIO_ARR
          ,FCDETAIL.DATA AS FLIGHTS_ARR
          ,WRDIST.DATA AS WALK_RUN_ARR
          ,STEPSDET.DATA AS STEPS_ARR
          ,AEBDET.DATA AS ACTIVE_ENERGY_ARR
          ,BASALDET.DATA AS BASAL_ARR
          
          ,current_timestamp as RUN_DATE
        
          from "SNOWHEALTH"."HK"."DATED_POP" DP 
              LEFT JOIN "SNOWHEALTH"."HK"."POPULATION" POP ON DP.ID = POP.ID
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."ACTIVE_ENERGY_BURNED" group by 1,2,4) AEB ON DP.ID = AEB.ID AND DP.DATE = AEB.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."APPLESTANDTIME" group by 1,2,4) AST ON DP.ID = AST.ID AND DP.DATE = AST.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."BASALENERGYBURNED" group by 1,2,4) BEB ON DP.ID = BEB.ID AND DP.DATE = BEB.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."CARBS" group by 1,2,4) CARBS ON DP.ID = CARBS.ID AND DP.DATE = CARBS.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."CHOLESTEROL" group by 1,2,4) CSTRL ON DP.ID = CSTRL.ID AND DP.DATE = CSTRL.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."DIETARYENERGY" group by 1,2,4) DE ON DP.ID = DE.ID AND DP.DATE = DE.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."FATMONO" group by 1,2,4) FM ON DP.ID = FM.ID AND DP.DATE = FM.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."FATPOLY" group by 1,2,4) FP ON DP.ID = FP.ID AND DP.DATE = FP.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."FATSAT" group by 1,2,4) FS ON DP.ID = FS.ID AND DP.DATE = FS.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."FATTOTAL" group by 1,2,4) FT ON DP.ID = FT.ID AND DP.DATE = FT.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."FLIGHTSCLIMBED" group by 1,2,4) FC ON DP.ID = FC.ID AND DP.DATE = FC.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."PROTEIN" group by 1,2,4) PRO ON DP.ID = PRO.ID AND DP.DATE = PRO.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."SODIUM" group by 1,2,4) SOD ON DP.ID = SOD.ID AND DP.DATE = SOD.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."STEPCOUNT" group by 1,2,4) STEPS ON DP.ID = STEPS.ID AND DP.DATE = STEPS.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."SUGAR" group by 1,2,4) SUG ON DP.ID = SUG.ID AND DP.DATE = SUG.DATE
              LEFT JOIN (SELECT ID, DATE_TRUNC('DAY', STARTTIME) AS DATE, sum(VALUE) as VALUE, UNIT FROM "SNOWHEALTH"."HK"."WALKRUNDISTANCE" group by 1,2,4) WRD ON DP.ID = WRD.ID AND DP.DATE = WRD.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::STRING||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct B.ID AS ID, B.DATE AS STARTTIME, IFF((VALUE is not null), value::STRING,'') as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."HEARTRATE" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                          ) group by ID, date_trunc('day',STARTTIME)) HRTRT ON HRTRT.ID=DP.ID AND HRTRT.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."RESTINGHEARTRATE")
                          group by ID, date_trunc('day',STARTTIME) ) RHRTRT ON RHRTRT.ID=DP.ID AND RHRTRT.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."HEARTRATESDNN")
                          group by ID, date_trunc('day',STARTTIME) ) SDNNHRTRT ON SDNNHRTRT.ID=DP.ID AND SDNNHRTRT.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."WALKINGHEARTAVG")
                          group by ID, date_trunc('day',STARTTIME) ) WHRTRT ON WHRTRT.ID=DP.ID AND WHRTRT.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."APPLESTANDTIME" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME)) TAST ON TAST.ID=DP.ID AND TAST.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."ENVIRONMENTAUDIO")
                          group by ID, date_trunc('day',STARTTIME) ) ENAUDIO ON ENAUDIO.ID=DP.ID AND ENAUDIO.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."HEAPHONEAUDIO")
                          group by ID, date_trunc('day',STARTTIME) ) HPAUDIO ON HPAUDIO.ID=DP.ID AND HPAUDIO.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."FLIGHTSCLIMBED" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) FCDETAIL ON FCDETAIL.ID=DP.ID AND FCDETAIL.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."WALKRUNDISTANCE" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) WRDIST ON WRDIST.ID=DP.ID AND WRDIST.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."STEPCOUNT" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME)) STEPSDET ON STEPSDET.ID=DP.ID AND STEPSDET.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."ACTIVE_ENERGY_BURNED" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) AEBDET ON AEBDET.ID=DP.ID AND AEBDET.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select distinct B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."BASALENERGYBURNED" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) BASALDET ON BASALDET.ID=DP.ID AND BASALDET.DATE = DP.DATE
                          
        order by 1 asc, 2 asc
      );`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: `ALTER TABLE "SNOWHEALTH"."HK"."POP_AGG" SWAP WITH "SNOWHEALTH"."HK"."POP_AGG2";`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `DROP TABLE "SNOWHEALTH"."HK"."POP_AGG2";`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: 
      `GRANT SELECT ON ALL TABLES IN SCHEMA "SNOWHEALTH"."HK" TO ROLE HEALTHKITSERVICE;`} );
//--------------------------------------------------------
      rs = snowflake.execute( { sqlText: `CREATE OR REPLACE STREAM SNOWHEALTH.HK.HEALTH_STREAM ON TABLE "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT";`} );

//--------------------- ENABLE SHARE OF DATA TO EXCHANGE (share previously created) -----------------------------------
/*
      rs = snowflake.execute( { sqlText: `GRANT USAGE ON DATABASE "SNOWHEALTH" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT USAGE ON SCHEMA "SNOWHEALTH"."PUBLIC" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."ACTIVE_ENERGY_BURNED" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."WEIGHT" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."WALKRUNDISTANCE" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."WALKINGHEARTAVG" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."SUGAR" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."STEPCOUNT" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."SODIUM" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."RESTINGHEARTRATE" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."PROTEIN" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."POP_AGG" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."POPULATION" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."HOUR_DIM" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."HOURS_POP" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."HEARTRATESDNN" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."HEARTRATE" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."HEAPHONEAUDIO" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."FLIGHTSCLIMBED" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."FATTOTAL" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."FATSAT" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."FATPOLY" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."FATMONO" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."ENVIRONMENTAUDIO" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."DIETARYENERGY" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."DATE_DIM" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."DATED_POP" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."CHOLESTEROL" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."CARBS" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."BASALENERGYBURNED" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."APPLESTANDTIME" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
*/
      return 'Done';
  $$;