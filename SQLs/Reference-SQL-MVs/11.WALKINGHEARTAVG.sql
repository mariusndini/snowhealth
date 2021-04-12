CREATE OR REPLACE SECURE MATERIALIZED VIEW SNOWHEALTH.HK.WALKINGHEARTAVG 
CLUSTER BY (ID, STARTTIME)
as
  select distinct
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
    null AS APP,
    (split(value, ' ')[6] || ' ' || split(value, ' ')[5])::STRING  AS DEVICE

  from "SNOWHEALTH"."PUBLIC"."HEALTHKIT_IMPORT",
        table(flatten(input => RECORD:WalkingHeartRateAverage))
