import HealthKit

class HealthKitSetupAssistant {
  
  private enum HealthkitSetupError: Error {
    case notAvailableOnDevice
    case dataTypeNotAvailable
  }
  
  public static var healthKitTypesToRead: Set<HKObjectType> = []
  
  class func authorizeHealthKit(completion: @escaping (Bool, Error?) -> Swift.Void) {
    
    //1. Check to see if HealthKit Is Available on this device
    guard HKHealthStore.isHealthDataAvailable() else {
      completion(false, HealthkitSetupError.notAvailableOnDevice)
      return
    }
    
    //2. Prepare the data types that will interact with HealthKit
    guard   let dateOfBirth = HKObjectType.characteristicType(forIdentifier: .dateOfBirth),
            let bloodType = HKObjectType.characteristicType(forIdentifier: .bloodType),
            let biologicalSex = HKObjectType.characteristicType(forIdentifier: .biologicalSex),
            let bodyMassIndex = HKObjectType.quantityType(forIdentifier: .bodyMassIndex),
            let height = HKObjectType.quantityType(forIdentifier: .height),
            let bodyMass = HKObjectType.quantityType(forIdentifier: .bodyMass),
      
            let activeEnergy = HKObjectType.quantityType(forIdentifier: .activeEnergyBurned),
            let steps = HKObjectType.quantityType(forIdentifier: .stepCount),
            let flights = HKObjectType.quantityType(forIdentifier: .flightsClimbed),
            let distanceWalkingRunning = HKObjectType.quantityType(forIdentifier: .distanceWalkingRunning),

            let water = HKObjectType.quantityType(forIdentifier: .dietaryWater),
            let carbs = HKObjectType.quantityType(forIdentifier: .dietaryCarbohydrates),
            let dietaryProtein = HKObjectType.quantityType(forIdentifier: .dietaryProtein),
            let dietaryFatTotal = HKObjectType.quantityType(forIdentifier: .dietaryFatTotal),
            let dietaryFatSaturated = HKObjectType.quantityType(forIdentifier: .dietaryFatSaturated),
            let dietaryFatMonounsaturated = HKObjectType.quantityType(forIdentifier: .dietaryFatMonounsaturated),
            let dietaryFatPolyunsaturated = HKObjectType.quantityType(forIdentifier: .dietaryFatPolyunsaturated),
            let dietaryCholesterol = HKObjectType.quantityType(forIdentifier: .dietaryCholesterol),
            let dietaryEnergyConsumed = HKObjectType.quantityType(forIdentifier: .dietaryEnergyConsumed),
            let dietarySodium = HKObjectType.quantityType(forIdentifier: .dietarySodium),
            let dietarySugar = HKObjectType.quantityType(forIdentifier: .dietarySugar),

      
            let basalenergyburned = HKObjectType.quantityType(forIdentifier: .basalEnergyBurned),
            let waistCircumference = HKObjectType.quantityType(forIdentifier: .waistCircumference),
            let heartRate = HKObjectType.quantityType(forIdentifier: .walkingHeartRateAverage),
            let restingHeartRate = HKObjectType.quantityType(forIdentifier: .restingHeartRate),
            let walkingHeartRateAverage = HKObjectType.quantityType(forIdentifier: .walkingHeartRateAverage),

            let environmentalAudioExposure = HKObjectType.quantityType(forIdentifier: .environmentalAudioExposure),
            let headphoneAudioExposure = HKObjectType.quantityType(forIdentifier: .headphoneAudioExposure),
            let appleStandTime = HKObjectType.quantityType(forIdentifier: .appleStandTime)

      
      else {
        completion(false, HealthkitSetupError.dataTypeNotAvailable)
        return
    }
    
    self.healthKitTypesToRead = [dateOfBirth, bloodType, environmentalAudioExposure,
                                                   biologicalSex, headphoneAudioExposure,
                                                   bodyMassIndex, dietaryEnergyConsumed,
                                                   height, dietaryCholesterol, distanceWalkingRunning,
                                                   bodyMass, dietaryFatSaturated,
                                                   steps, dietaryFatPolyunsaturated,
                                                   flights, dietaryProtein, dietarySugar,
                                                   water, dietaryFatTotal, dietarySodium,
                                                   carbs, dietaryFatMonounsaturated,
                                                   basalenergyburned, waistCircumference,
                                                   activeEnergy, heartRate, appleStandTime,
                                                   restingHeartRate, walkingHeartRateAverage]
    
    //4. Request Authorization
    HKHealthStore().requestAuthorization(toShare: .none, read: healthKitTypesToRead) { (success, error) in
      completion(success, error)
    }
  }
}
  
