import UIKit
import HealthKit

class MasterViewController: UITabBarController {
<<<<<<< Updated upstream
  static let healthStore = HKHealthStore()
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

=======
  
>>>>>>> Stashed changes
  override func viewDidLoad() {
    super.viewDidLoad()
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



