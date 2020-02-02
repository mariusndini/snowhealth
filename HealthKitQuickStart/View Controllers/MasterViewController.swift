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
    
    self.runquery2(count: 0)
    
    
  }//end main class view controller
  

  /******  Runs Query for specificed identifier given   ******/
  private func runquery2(count:Int){
    if(count == targets.count){
      do{
        let jsonData = try JSONSerialization.data(withJSONObject: json, options: [])
        let jsonString = String(data: jsonData, encoding: String.Encoding.ascii)!
        let body = jsonString.replacingOccurrences(of: "\\", with: "")
                         .replacingOccurrences(of: "\n", with: "")
                         .replacingOccurrences(of: ";", with: "")
                         .replacingOccurrences(of: "=", with: ":")
                         .replacingOccurrences(of: "'", with: "")
                         .replacingOccurrences(of: "", with: "")
                         .replacingOccurrences(of: "HKQuantityTypeIdentifier", with: "")
        print(body);
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
     let startTime = formatter.date(from: "2020/02/01 00:01")
     let endTime = formatter.date(from: "2020/02/02 22:31")
    
     let pred = HKQuery.predicateForSamples(withStart: startTime, end: endTime, options: [])

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
    let url = URL(string: "https://webhook.site/534a7afc-1bd2-4b6c-aea9-01d87cd3fe87")!
    var request = URLRequest(url: url)
    request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
    request.httpMethod = "POST"
    let parameters: [String: Any] = [
        "body": body
    ]
    
    request.httpBody = parameters.percentEncoded()
    let task = URLSession.shared.dataTask(with: request) { data, response, error in
        guard let data = data,
            let response = response as? HTTPURLResponse,
            error == nil else {                                              // check for fundamental networking error
            print("error", error ?? "Unknown error")
            return
        }

        guard (200 ... 299) ~= response.statusCode else {                    // check for http errors
            print("statusCode should be 2xx, but is \(response.statusCode)")
            print("response = \(response)")
            return
        }

        let responseString = String(data: data, encoding: .utf8)
        print("responseString = \(responseString)")
    }

    task.resume()
    
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

