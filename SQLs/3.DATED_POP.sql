CREATE OR REPLACE TABLE HK.DATED_POP AS (
  select  ID, DATE
  FROM "HEALTHKIT"."HK"."POPULATION",
       "HEALTHKIT"."PUBLIC"."DATE_DIM"
  group by 1, 2
  order by id, date
);