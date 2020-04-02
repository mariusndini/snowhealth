import UIKit
import HealthKit

class MasterViewController: UITabBarController {
  //static let healthStore = HKHealthStore()
  var json:[String : Any] = [:]
  //https://developer.apple.com/documentation/healthkit/hkquantitytypeidentifier
  static var targets:[HKQuantityTypeIdentifier] =
  [HKQuantityTypeIdentifier.bodyMassIndex,
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
   HKQuantityTypeIdentifier.heartRate,
   HKQuantityTypeIdentifier.heartRateVariabilitySDNN,
   HKQuantityTypeIdentifier.restingHeartRate,
   HKQuantityTypeIdentifier.walkingHeartRateAverage,
   HKQuantityTypeIdentifier.environmentalAudioExposure,
   HKQuantityTypeIdentifier.headphoneAudioExposure,
   HKQuantityTypeIdentifier.appleStandTime,
   HKQuantityTypeIdentifier.appleExerciseTime
  ]


  override func viewDidLoad() {
    super.viewDidLoad()
    UIApplication.shared.isIdleTimerDisabled = true
    authorizeHealthKit()
  }
  
  override func didReceiveMemoryWarning() {
    super.didReceiveMemoryWarning()
    // Dispose of any resources that can be recreated.
  }
  
  private func authorizeHealthKit() {
    HealthKitSetupAssistant.authorizeHealthKit { (authorized, error) in
      guard authorized else {
        let baseMessage = "HealthKit Authorization Failed"
        
        guard let error = error else { print(baseMessage); return }
        print("\(baseMessage). Reason: \(error.localizedDescription)")
        return
      }
      print("HealthKit Successfully Authorized.")
    }
  }


}



