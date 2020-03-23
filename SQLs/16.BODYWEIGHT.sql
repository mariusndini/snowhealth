
create or replace table HK.WEIGHT as (
  select 
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

