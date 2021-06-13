call SNOWHEALTH.HK.HEALTH_TABLES_SPROC();

CREATE OR REPLACE PROCEDURE SNOWHEALTH.HK.HEALTH_TABLES_SPROC()
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$   
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
          SELECT DATEADD(DAY, SEQ4(), '2019-01-01') AS MY_DATE
            FROM TABLE(GENERATOR(ROWCOUNT => 366 * 5))  -- Count starts from Jan 1 2019 + 5 years by day
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
          SELECT DATEADD(HOUR, SEQ4(), '2019-01-01 00') AS MY_DATE
            FROM TABLE(GENERATOR(ROWCOUNT => (366*5) * 24))  -- Count starts from Jan 1 2019 + 5 years by day by hour
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
          ,WEEK_HIST.DATA AS WEEK_CALS_ARR


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
                          from (select ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."RESTINGHEARTRATE")
                          group by ID, date_trunc('day',STARTTIME) ) RHRTRT ON RHRTRT.ID=DP.ID AND RHRTRT.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."HEARTRATESDNN")
                          group by ID, date_trunc('day',STARTTIME) ) SDNNHRTRT ON SDNNHRTRT.ID=DP.ID AND SDNNHRTRT.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."WALKINGHEARTAVG")
                          group by ID, date_trunc('day',STARTTIME) ) WHRTRT ON WHRTRT.ID=DP.ID AND WHRTRT.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."APPLESTANDTIME" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME)) TAST ON TAST.ID=DP.ID AND TAST.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."ENVIRONMENTAUDIO")
                          group by ID, date_trunc('day',STARTTIME) ) ENAUDIO ON ENAUDIO.ID=DP.ID AND ENAUDIO.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select ID, STARTTIME, VALUE from "SNOWHEALTH"."HK"."HEAPHONEAUDIO")
                          group by ID, date_trunc('day',STARTTIME) ) HPAUDIO ON HPAUDIO.ID=DP.ID AND HPAUDIO.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."FLIGHTSCLIMBED" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) FCDETAIL ON FCDETAIL.ID=DP.ID AND FCDETAIL.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."WALKRUNDISTANCE" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) WRDIST ON WRDIST.ID=DP.ID AND WRDIST.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."STEPCOUNT" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME)) STEPSDET ON STEPSDET.ID=DP.ID AND STEPSDET.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."ACTIVE_ENERGY_BURNED" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) AEBDET ON AEBDET.ID=DP.ID AND AEBDET.DATE = DP.DATE
              LEFT JOIN (select ID, date_trunc('day',STARTTIME) AS DATE, ARRAY_AGG(PARSE_JSON('{"DATE":"'||STARTTIME::TIMESTAMP ||'", "val":"'|| VALUE::DOUBLE||'"}')) within group (order by starttime::TIMESTAMP ASC) AS DATA
                          from (select B.ID AS ID, B.DATE AS STARTTIME, NVL(SUM(VALUE),0) as VALUE
                                FROM "SNOWHEALTH"."HK"."HOURS_POP" B LEFT OUTER JOIN  "SNOWHEALTH"."HK"."BASALENERGYBURNED" A ON A.ID = B.ID AND DATE_TRUNC('HOUR',A.STARTTIME) = B.DATE
                                GROUP BY 1, 2 
                          ) group by ID, date_trunc('day',STARTTIME) ) BASALDET ON BASALDET.ID=DP.ID AND BASALDET.DATE = DP.DATE
            
            -- ADD WEEKLY HISTORY
            LEFT JOIN(
                select DP.ID AS ID, DP.DATE AS T,
                    ARRAY_AGG(PARSE_JSON('{"DATE":"'||TIME::TIMESTAMP ||'", "ACTIVE":"'|| CONSUMED::DOUBLE||'", "BASAL":"'|| ACTIVE_ENERGY::DOUBLE||'", "CONSUMED":"'|| BASAL_ENERGY::DOUBLE||'"}')) within group (order by TIME::TIMESTAMP ASC) AS DATA
                from SNOWHEALTH.HK.DATED_POP DP
                join SNOWHEALTH.DAILY_AGG.DAILY_ENERGY_CALS DEC on DP.ID=DEC.ID
                where TIME BETWEEN (DP.date-9) AND (DP.DATE)
                GROUP BY 1, 2
                order by 1, 2 desc
            ) WEEK_HIST ON WEEK_HIST.ID = DP.ID AND WEEK_HIST.T=DP.DATE 


        order by 1 asc, 2 asc
      );`} );
//-------------------------------------------------------- SWAP OLD TABLE WITH NEW TABLE
      rs = snowflake.execute( { sqlText: `ALTER TABLE "SNOWHEALTH"."HK"."POP_AGG" SWAP WITH "SNOWHEALTH"."HK"."POP_AGG2";`} );
//-------------------------------------------------------- DROP NEW TABLE
      rs = snowflake.execute( { sqlText: `DROP TABLE "SNOWHEALTH"."HK"."POP_AGG2";`} );
//-------------------------------------------------------- GIVE ACCESS TO ROLE
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON ALL TABLES IN SCHEMA "SNOWHEALTH"."HK" TO ROLE HEALTHKITSERVICE;`} );
//-------------------------------------------------------- RESET STREAM
      rs = snowflake.execute( { sqlText: `CREATE OR REPLACE STREAM SNOWHEALTH.HK.HEALTH_STREAM ON TABLE "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT";`} );
//--------------------- ENABLE SHARE OF DATA TO EXCHANGE (share previously created) -----------------------------------
      rs = snowflake.execute( { sqlText: `GRANT USAGE ON DATABASE "SNOWHEALTH" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT USAGE ON SCHEMA "SNOWHEALTH"."PUBLIC" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."ACTIVE_ENERGY_BURNED" TO SHARE "SNOWHEALTH_EXCHANGE";`} );
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

      //ADD DATA TO CITIBIKE SHARE
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."DATED_POP" TO SHARE "CITIBIKE_SFC_SHARE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."DATE_DIM" TO SHARE "CITIBIKE_SFC_SHARE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."HOUR_DIM" TO SHARE "CITIBIKE_SFC_SHARE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."HOURS_POP" TO SHARE "CITIBIKE_SFC_SHARE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."POPULATION" TO SHARE "CITIBIKE_SFC_SHARE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE "SNOWHEALTH"."HK"."POP_AGG" TO SHARE "CITIBIKE_SFC_SHARE";`} );
      rs = snowflake.execute( { sqlText: `GRANT SELECT ON TABLE SNOWHEALTH.HK.TRIGGER_UPDATE TO SHARE "CITIBIKE_SFC_SHARE";`} );
      
      rs = snowflake.execute( { sqlText: `insert into SNOWHEALTH.HK.TRIGGER_UPDATE (SELECT CURRENT_TIMESTAMP());`} );
      rs = snowflake.execute( { sqlText: `alter table SNOWHEALTH.HK.TRIGGER_UPDATE set change_tracking = true;`} );

      return 'Done';
$$;