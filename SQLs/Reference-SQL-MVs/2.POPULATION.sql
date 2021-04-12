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
