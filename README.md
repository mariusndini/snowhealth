# iOS Health Data Demo

This demp/architecture walks through how to pull data from iOS through and process that data in AWS S3 and there after pull  that data through Snowpipe into Snowflake. Once data is in Snowflake it is possible to distribute data or report on said data.

![img](https://github.com/mariusndini/img/blob/master/SnowHealthArch.png)


## iOS Bridge Application
Our phones collect a lot of health data which has a lot of potential uses. IOS healthkit SDK makes it possible for app developers to tie into the iPhone and offers a centralized data repo for health data. The same SDK makes it possible to pull data from this central repository. The possible data points are documented on apple's healthkit docs. In short, apples are capturing body measurements, vital signs, lab and test results, nutrition, physical activity, etc.  

The bridge application serves the purpose to read this data set and through an HTTP post call sent it AWS. This application is open to all and makes it easy to pull datasets and requires users permission on which data sets to pull. 

The users are asked to provide permission for the bridge application to  pull the following information, if available. 

`
  var json:[String : Any] = [:]
  var targets:[HKQuantityTypeIdentifier] =
  [HKQuantityTypeIdentifier.dietarySugar,
   HKQuantityTypeIdentifier.dietaryEnergyConsumed,
   HKQuantityTypeIdentifier.bodyMassIndex,
   HKQuantityTypeIdentifier.bodyMass,
   HKQuantityTypeIdentifier.activeEnergyBurned,
   HKQuantityTypeIdentifier.stepCount,
   HKQuantityTypeIdentifier.flightsClimbed,
   HKQuantityTypeIdentifier.distanceWalkingRunning,
   HKQuantityTypeIdentifier.dietaryWater,
   HKQuantityTypeIdentifier.dietaryCarbohydrates,
   HKQuantityTypeIdentifier.dietaryProtein,
   HKQuantityTypeIdentifier.dietaryFatTotal,
   HKQuantityTypeIdentifier.dietaryFatSaturated,
   HKQuantityTypeIdentifier.dietaryFatMonounsaturated,
   HKQuantityTypeIdentifier.dietaryFatPolyunsaturated,
   HKQuantityTypeIdentifier.dietaryCholesterol,
   HKQuantityTypeIdentifier.dietaryEnergyConsumed,
   HKQuantityTypeIdentifier.dietarySodium,
   HKQuantityTypeIdentifier.dietarySugar,
   HKQuantityTypeIdentifier.basalEnergyBurned,
   HKQuantityTypeIdentifier.waistCircumference,
   HKQuantityTypeIdentifier.walkingHeartRateAverage,
   HKQuantityTypeIdentifier.restingHeartRate,
   HKQuantityTypeIdentifier.walkingHeartRateAverage,
   HKQuantityTypeIdentifier.environmentalAudioExposure,
   HKQuantityTypeIdentifier.headphoneAudioExposure,
   HKQuantityTypeIdentifier.appleStandTime
  ]
`

The bridge application look like below and is simple in nature since its only goal is to push data from the iPhone to the cloud. 

Essentially the user picks a start date, end date, enters a unique identifier to filter front end apps and data is uploaded one day at a time.

![img](https://github.com/mariusndini/img/blob/master/HealthiOSBridge.png)


### HTTP Post
The iOS bridge application is to send the data to AWS API Gateway through an HTTP post call. This HTTP post call is formatted in JSON and contains all the information that the user gives permission to pull above. 

### AWS Cloud Infrastructure
AWS is the cloud provider of choice to provide the cloud scale. This is of personal preference Azure & GCP can be used all the same. 

### API Gateway
When a user uses the iPhone bridge application, the app will make an HTTP post call. This post call needs to be handled somewhere and API Gateway is the receiver of this information. Once the API gateway receives this information it will need to pass it along to Lambda to do something with the data. 

### Lambda
Lambda offers immediate and scalable infrastructure to do something with data once API Gateway passes the information to a specific Lambda function. This Lambda function is given access to S3 and with that permission it takes the JSON and saves it to S3 bucket for Snowpipe to ingest. 

### S3
Serves as a persistent data store from Lambda for Swipe to ingest. 

### Snowflake
Snowflake is the perfect environment for this data set offering a fully scalable database that is able to effectively the JSON data incoming from the source system (IOS Healthkit). Bringing structure and data availability. 

### Snowpipe
Snowpipe is used to ingest data once it reaches the S3 environment in AWS. Usually the lag time of this data set is two minutes, but depends on when the SNS/SQS messaging alerts Snowpipe of the file PUT drop.

### Snowflake
The data is saved in its original JSON format in Snowflake and through SQL is provided structure to data consumption tools. 


## Data Consumption
Once data is in Snowflake it can be consumed with any of the native connectors that Snowflake offers. 

![img](https://github.com/mariusndini/img/blob/master/HealthiOSBridge_2.png)

## Monday, Dec 14, 2020 Updates
Codebase previously hosted all code with in Snowflake Stored Procedure, this has been replaced by materialized views that the S.Proc uses to generate a denormalized data set to drive the front end.
