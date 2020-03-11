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
    
    self.runquery2(count: 0, date:"2020/02/02 23:59")
    //self.runquery2(count: 0, date:"2020/02/01 23:59")
    //self.runquery2(count: 0, date:"2020/01/31 23:59")
    //self.runquery2(count: 0, date:"2020/01/30 23:59")
    //self.runquery2(count: 0, date:"2020/01/29 23:59")

    
  }//end main class view controller
  

  /******  Runs Query for specificed identifier given   ******/
  private func runquery2(count:Int, date:String){
    if(count == targets.count){
      do{
        let jsonData = try JSONSerialization.data(withJSONObject: json, options: [])
        let jsonString = String(data: jsonData, encoding: String.Encoding.ascii)!
        let body:String = jsonString.replacingOccurrences(of: "\\", with: "")
                         .replacingOccurrences(of: "\n", with: "")
                         .replacingOccurrences(of: ";", with: "")
                         .replacingOccurrences(of: "=", with: ":")
                         .replacingOccurrences(of: "'", with: "")
                         .replacingOccurrences(of: "", with: "")
                         .replacingOccurrences(of: "HKQuantityTypeIdentifier", with: "")
        //print(body);
        self.sendPost(body: body)
      }catch {
          print(error.localizedDescription)
      }
      return
    }
    
    
    let identifier = HKQuantityTypeIdentifier.dietaryCarbohydrates
    guard let id = HKSampleType.quantityType(forIdentifier: identifier) else {
       return
     }
     
     let formatter = DateFormatter()
     formatter.dateFormat = "yyyy/MM/dd HH:mm"
     let endTime = formatter.date(from: date)!//"2020/02/02 23:59")!

     let daysAgo = NSCalendar.current.date(byAdding: .day, value: -1, to: endTime)
     let pred = HKQuery.predicateForSamples(withStart: daysAgo, end: endTime, options: [])

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
          
          self.runquery2(count: count + 1, date:date)
        }//end if
        
     }
     
     MasterViewController.self.healthStore.execute(query)

   }//end run query
   
  /*
  
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

  */
  
  private func sendPost(body:String){

    var semaphore = DispatchSemaphore (value: 0)

    let parameters = "{\"body\":\(body)}"
    let postData = parameters.data(using: .utf8)

    var request = URLRequest(url: URL(string: "https://vtx9osi61g.execute-api.us-east-1.amazonaws.com/dev/send")!,timeoutInterval: Double.infinity)
    request.addValue("text/plain", forHTTPHeaderField: "Content-Type")

    request.httpMethod = "POST"
    request.httpBody = postData

    let task = URLSession.shared.dataTask(with: request) { data, response, error in
      guard let data = data else {
        print(String(describing: error))
        return
      }
      print(String(data: data, encoding: .utf8)!)
      //print("sent");
      semaphore.signal()
    }

    task.resume()
    semaphore.wait()

    
  }//end end post
 

}


extension Dictionary {
    func percentEncoded() -> Data? {
        return map { key, value in
            let escapedKey = "\(key)".addingPercentEncoding(withAllowedCharacters: .urlQueryValueAllowed) ?? ""
            let escapedValue = "\(value)".addingPercentEncoding(withAllowedCharacters: .urlQueryValueAllowed) ?? ""
            return escapedKey + "=" + escapedValue
        }
        .joined(separator: "&")
        .data(using: .utf8)
    }
}

extension CharacterSet {
    static let urlQueryValueAllowed: CharacterSet = {
        let generalDelimitersToEncode = ":#[]@" // does not include "?" or "/" due to RFC 3986 - Section 3.4
        let subDelimitersToEncode = "!$&'()*+,;="

        var allowed = CharacterSet.urlQueryAllowed
        allowed.remove(charactersIn: "\(generalDelimitersToEncode)\(subDelimitersToEncode)")
        return allowed
    }()
}

