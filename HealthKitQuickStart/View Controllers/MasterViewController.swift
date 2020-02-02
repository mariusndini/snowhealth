import UIKit
import HealthKit

class MasterViewController: UITabBarController {
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

  override func viewDidLoad() {
    super.viewDidLoad()
    authorizeHealthKit()
    setupHealthKit()
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
      //print("HealthKit Successfully Authorized.")
    }
  }


  
  private func setupHealthKit(){
    do {
       let userAgeSexAndBloodType = try ProfileDataStore.getAgeSexAndBloodType()
       json["age"] = "\(userAgeSexAndBloodType.age)"
       json["gender"] =  "\(userAgeSexAndBloodType.biologicalSex.stringRepresentation)"
       json["bloodstype"] =  "\(userAgeSexAndBloodType.bloodType.stringRepresentation)"
     } catch let error {
       print ("error \(error)")
     }
    
      //print(HealthKitSetupAssistant.healthKitTypesToRead)
      self.runquery2(count: 0)
      //self.runquery(identifier: HKQuantityTypeIdentifier.dietarySugar)
    
    
  }//end main class view controller
  

  /******  Runs Query for specificed identifier given   ******/
  private func runquery2(count:Int){
    if(count == targets.count){
      do{
        let jsonData = try JSONSerialization.data(withJSONObject: json, options: [])
        let jsonString = String(data: jsonData, encoding: String.Encoding.ascii)!
        print (jsonString.replacingOccurrences(of: "\\", with: "")
                         .replacingOccurrences(of: "\n", with: "")
                         .replacingOccurrences(of: ";", with: "")
                         .replacingOccurrences(of: "=", with: ":")
                         .replacingOccurrences(of: "'", with: "")
                         .replacingOccurrences(of: "", with: "")
                         .replacingOccurrences(of: "HKQuantityTypeIdentifier", with: ""))
      }catch {
          print(error.localizedDescription)
      }
      return
    }
    
    
    let identifier = HKQuantityTypeIdentifier.dietaryCarbohydrates
    guard let id = HKSampleType.quantityType(forIdentifier: identifier) else {
       return
     }
     
     let daysAgo = NSCalendar.current.date(byAdding: .day, value: -1, to: NSDate() as Date)
     let pred = HKQuery.predicateForSamples(withStart: daysAgo, end: NSDate() as Date, options: [])

     let query = HKSampleQuery(sampleType: id, predicate: pred, limit: 0, sortDescriptors: .none) {
         (sampleQuery, results, error) -> Void in

         if let result = results {
            var items:[String] = []
            for item in result {
              if let sample = item as? HKQuantitySample {
                items.append("\(sample)".replacingOccurrences(of: "\"", with: "")
                                        .replacingOccurrences(of: "\n", with: ""))
              }
          }
          self.json["\(self.targets[count].rawValue)"] = items
          
          self.runquery2(count: count + 1)
        }//end if
        
     }
     
     MasterViewController.self.healthStore.execute(query)

   }//end run query
   
  
  
  private func getSum(identifier:HKQuantityTypeIdentifier){
    guard let id = HKSampleType.quantityType(forIdentifier: identifier) else {
      return
    }
    
    ProfileDataStore.queryQuantitySum(for: id, unit: HKUnit.count()) { (sampleTotal, error) in
      guard let sample = sampleTotal else {
        print("error")
        return
      }
      DispatchQueue.main.async{print("\(identifier.rawValue): \(sample)")}
    }

  }//end get sum

  

  

}




