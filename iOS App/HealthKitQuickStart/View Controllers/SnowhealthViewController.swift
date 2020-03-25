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
  
    func randomString(length: Int) -> String {
      let letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
      return String((0..<length).map{ _ in letters.randomElement()! })
    }
    
    identifier.text = randomString(length: 5).uppercased()
  
  }
  
  override func didReceiveMemoryWarning() {
    super.didReceiveMemoryWarning()
    // Dispose of any resources that can be recreated.
  }
  
  //close KB on app touch 
  override func touchesBegan(_ touches: Set<UITouch>, with event: UIEvent?) {
      self.view.endEditing(true)
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
    com = Calendar.current.dateComponents([.year, .month, .day], from: enddate.date)
    if let day = com.day, let month = com.month, let year = com.year {
        end = "\(year)/\(month)/\(day) 23.59"
    }
    
    //get difference in days
    let diffInDays = Calendar.current.dateComponents([.day], from: startdate.date, to: enddate.date).day

    // TImer to update UI
    func startTimer() {
      timer?.invalidate()   // just in case you had existing `Timer`, `invalidate` it before we lose our reference to it
      timer = Timer.scheduledTimer(withTimeInterval: 0.250, repeats: true) { [weak self] _ in
        self!.status.text = "\(globals.postcount) / \(diffInDays! + 1) Loaded";

        if(globals.postcount == diffInDays!){
          self!.status.text = "👍 done";
        }
      }
      
    }

    func stopTimer() {
        timer?.invalidate()
    }

    startTimer()
      
  
    // Iterate through all the days
    let dayDurationInSeconds: TimeInterval = 60*60*24
    for date in stride(from: startdate.date, to: enddate.date + 2, by: dayDurationInSeconds) {
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
