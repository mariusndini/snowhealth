CREATE OR REPLACE SECURE MATERIALIZED VIEW SNOWHEALTH.HK.SUGAR 
CLUSTER BY (ID, STARTTIME)
as
 select distinct
    RECORD:identifier::STRING as ID,
    split(value, ' ')[0]::DOUBLE as value,
    split(value, ' ')[1]::STRING AS UNIT,
    split(value, ' ')[2]::STRING AS UUID,
    null AS APP,
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

;