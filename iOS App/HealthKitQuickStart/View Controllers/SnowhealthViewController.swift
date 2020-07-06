import Foundation
import UIKit

class SnowhealthViewController: UIViewController {

  @IBOutlet weak var startdate: UIDatePicker!
  @IBOutlet weak var enddate: UIDatePicker!
  @IBOutlet weak var identifier: UITextField!
  @IBOutlet weak var zip: UITextField!
  @IBOutlet weak var status: UILabel!
  
  override func viewDidLoad() {
    super.viewDidLoad()
    //DataSend.retrieveSleepAnalysis()

    if let pin = UserDefaults.standard.string(forKey: "pin") {
          identifier.text = pin

    }else{
        globals.pin = randomString(length: 5).uppercased()
        UserDefaults.standard.set(globals.pin, forKey: "pin")
        identifier.text = globals.pin
    }
    
    if let zip = UserDefaults.standard.string(forKey: "zip") {
      self.zip.text = zip
      
    }else{
        UserDefaults.standard.set("10001", forKey: "zip")
        zip.text = "10001"
    }
    
    if let endD = UserDefaults.standard.string(forKey: "enddate") {
      startdate.setDate(from: endD, format: "dd/MM/yyyy")
    }
    
  }
  
  override func didReceiveMemoryWarning() {
    super.didReceiveMemoryWarning()
    // Dispose of any resources that can be recreated.
  }
  
  
  //close KB on app touch 
  override func touchesBegan(_ touches: Set<UITouch>, with event: UIEvent?) {
    self.view.endEditing(true)
    if(identifier.text == ""){
        identifier.text = randomString(length: 5).uppercased()
    }
    
    if(zip.text == ""){
        zip.text = "10001"
    }
    
    UserDefaults.standard.set(identifier.text!, forKey: "pin")
    UserDefaults.standard.set(zip.text!, forKey: "zip")
    
      
  }
  
  @IBAction func updateIdentifier(_ sender: UITextField) {
    UserDefaults.standard.set(globals.pin, forKey: "pin")
  }
  
  func randomString(length: Int) -> String {
     let letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
     return String((0..<length).map{ _ in letters.randomElement()! })
   }
  
  
  weak var timer: Timer?
  @IBAction func sendbtn(_ sender: Any) {
    print("send btn pushed");
    globals.postcount = 0;
    var start:String = ""
    var end:String = ""
    
    //Get Start date
    var com = Calendar.current.dateComponents([.year, .month, .day], from: startdate.date)
    if let day = com.day, let month = com.month, let year = com.year {
        start = "\(year)/\(month)/\(day) 00.00"
    }
    
    //get end date
    // HAVE TO ADD ONE DAY TO OVERSHOOT DATES TO INCLUDE TODAYS DATE IF NECESSARY
    var dateComponent = DateComponents()
    dateComponent.day = 1
    let futureDate = Calendar.current.date(byAdding: dateComponent, to: enddate.date)
    com = Calendar.current.dateComponents([.year, .month, .day], from: futureDate!)
    
    if let day = com.day, let month = com.month, let year = com.year {
        end = "\(year)/\(month)/\(day) 23.59"
      
        // HAVE TO TAKE OUT THE PREVIOUS DAY THAT WE ADDED ABOVE SO UI DATEPICKER SHOWS RIGHT DATE
        let futurePrevDay = Calendar.current.dateComponents([.year, .month, .day], from: enddate.date)
        if let day = futurePrevDay.day, let month = futurePrevDay.month, let year = futurePrevDay.year {
          UserDefaults.standard.set("\(day)/\(month)/\(year)", forKey: "enddate")
        }
      
      
    }
    
    //get difference in days
    let diffInDays = Calendar.current.dateComponents([.day], from: startdate.date, to: futureDate!).day

    // TImer to update UI
    func startTimer() {
      timer?.invalidate()   // just in case you had existing `Timer`, `invalidate` it before we lose our reference to it
      timer = Timer.scheduledTimer(withTimeInterval: 0.250, repeats: true) { [weak self] _ in
        self!.status.text = "\(globals.postcount) / \(diffInDays! + 1) Loaded";

        if(globals.postcount == diffInDays! + 1){
          self!.status.text = "👍 Please Allow up to 5 Minutes for Your Data to Populate";
        }
      }
      
    }

    func stopTimer() {
        timer?.invalidate()
    }

    startTimer()
      
  
    // Iterate through all the days
    let dayDurationInSeconds: TimeInterval = 60*60*24
    for date in stride(from: startdate.date, to: futureDate!, by: dayDurationInSeconds) {
      var runDate = ""
      com = Calendar.current.dateComponents([.year, .month, .day], from: date)
      if let day = com.day, let month = com.month, let year = com.year {
          runDate = "\(year)/\(month)/\(day)"
      }
      
      let datasend:DataSend = DataSend()
      datasend.runquery(count: 0, date: runDate, identity: "\(identifier.text!)|\(zip.text!)")
      
    }
    

     
  }//end send btn
  

}//end view controller

//Iterate thru dates
extension Date: Strideable {
    public func distance(to other: Date) -> TimeInterval {
        return other.timeIntervalSinceReferenceDate - self.timeIntervalSinceReferenceDate
    }

    public func advanced(by n: TimeInterval) -> Date {
        return self + n
    }
}

extension UIDatePicker {

   func setDate(from string: String, format: String, animated: Bool = true) {
      let formater = DateFormatter()
      formater.dateFormat = format
      let date = formater.date(from: string) ?? Date()
      setDate(date, animated: animated)
   }
}
