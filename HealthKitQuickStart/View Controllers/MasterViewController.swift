import UIKit
import HealthKit

class MasterViewController: UITabBarController {
  static let healthStore = HKHealthStore()

  override func viewDidLoad() {
    super.viewDidLoad()
    authorizeHealthKit()
    setupHealthKid()
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

  
  
  
  
  private func setupHealthKid(){
    do {
       let userAgeSexAndBloodType = try ProfileDataStore.getAgeSexAndBloodType()
       print("age:\(userAgeSexAndBloodType.age)")
      print("gender:\(userAgeSexAndBloodType.biologicalSex.stringRepresentation)")
      print("bloodtype:\(userAgeSexAndBloodType.bloodType.stringRepresentation)")
     } catch let error {
       print ("error \(error)")
     }
    
    //GET STEPS
    guard let stepsTakenType = HKSampleType.quantityType(forIdentifier: .stepCount) else {
      return
    }
    ProfileDataStore.queryQuantitySum(for: stepsTakenType, unit: HKUnit.count()) { (sampleTotal, error) in
      guard let sample = sampleTotal else {
        return
      }
      DispatchQueue.main.async{print("steps: \(Int(sample))")}
    }
    //------------------------------------------------------------
    //get water consumed
    guard let waterConsumedType = HKSampleType.quantityType(forIdentifier: .dietaryWater) else {
      return
    }
    
    ProfileDataStore.queryQuantitySum(for: waterConsumedType, unit: HKUnit.fluidOunceUS()) { (sampleTotal, error) in
       guard let sample = sampleTotal else {
         return
       }
       DispatchQueue.main.async{print("water: \(Int(sample))")}
     }
    
    //------------------------------------------------------------
    //get active cals burned
    guard let activeCalories = HKSampleType.quantityType(forIdentifier: .activeEnergyBurned) else {
      return
    }
    
    ProfileDataStore.queryQuantitySum(for: activeCalories, unit: HKUnit.largeCalorie()) { (sampleTotal, error) in
       guard let sample = sampleTotal else {
         return
       }
       DispatchQueue.main.async{print("active_cals: \(Int(sample))")}
     }
    
    //------------------------------------------------------------
    //get stand time
    guard let standtime = HKSampleType.quantityType(forIdentifier: .appleStandTime) else {
      return
    }
    
    ProfileDataStore.getMostRecentSample(for: standtime) { (sampleTotal, error) in
       guard let sample = sampleTotal else {
         return
       }
       DispatchQueue.main.async{print("standtime: \(sample)")}
     }
    
    //------------------------------------------------------------
    guard let bodyMass = HKSampleType.quantityType(forIdentifier: .bodyMass) else {
      return
    }
    
    ProfileDataStore.getMostRecentSample(for: bodyMass) { (sampleTotal, error) in
       guard let sample = sampleTotal else {
         return
       }
       DispatchQueue.main.async{print("bodymass: \(sample)")}
     }
    
    //------------------------------------------------------------
    guard let flightsclimbed = HKSampleType.quantityType(forIdentifier: .flightsClimbed) else {
      return
    }
    
    ProfileDataStore.getMostRecentSample(for: flightsclimbed) { (sampleTotal, error) in
       guard let sample = sampleTotal else {
         return
       }
       DispatchQueue.main.async{print("flightsclimbed: \(sample)")}
     }
    
    //------------------------------------------------------------
    guard let bmi = HKSampleType.quantityType(forIdentifier: .bodyMassIndex) else {
      return
    }
    
    ProfileDataStore.getMostRecentSample(for: bmi) { (sampleTotal, error) in
       guard let sample = sampleTotal else {
         return
       }
       DispatchQueue.main.async{print("bmi: \(sample)")}
     }
    
    //------------------------------------------------------------
    guard let carbs = HKSampleType.quantityType(forIdentifier: .dietaryCarbohydrates) else {
      return
    }
  
    let calendar = NSCalendar.current
    let twoDaysAgo = calendar.date(byAdding: .day, value: -1, to: NSDate() as Date)
    let currentDate = NSDate()
    let pred = HKQuery.predicateForSamples(withStart: twoDaysAgo, end: currentDate as Date, options: [])

    let query = HKSampleQuery(sampleType: carbs, predicate: pred, limit: 0, sortDescriptors: .none) {
        (sampleQuery, results, error) -> Void in

        if let result = results {
            for item in result {
                if let sample = item as? HKQuantitySample {
                    print(sample)
                }
            }
            
        }
    }
    MasterViewController.self.healthStore.execute(query)
    
    
    
    
    
  }//end set up HK
  

  

}




