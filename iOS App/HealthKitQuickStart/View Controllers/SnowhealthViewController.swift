import Foundation
import UIKit

class SnowhealthViewController: UITableViewController {
  

  
  @IBOutlet weak var startdate: UIDatePicker!
  @IBOutlet weak var enddate: UIDatePicker!
  @IBOutlet weak var identifier: UITextField!
  
  var datasend:DataSend = DataSend()
  
  public func updateStatus(){
    //status.text = "\(globals.postcount) / \(0 + 1) Loaded";
    
  }
  
  @IBAction func sendbtn(_ sender: UIButton) {
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

    // Iterate through all the days
    let dayDurationInSeconds: TimeInterval = 60*60*24
    for date in stride(from: startdate.date, to: enddate.date, by: dayDurationInSeconds) {
      var runDate = ""
      com = Calendar.current.dateComponents([.year, .month, .day], from: date)
      if let day = com.day, let month = com.month, let year = com.year {
          runDate = "\(year)/\(month)/\(day) 23.59"
      }
      
      datasend.runquery(count: 0, date: runDate, identity: identifier.text!)
      
    }
        
  }//end send btn
  

}//end view controller

extension Date: Strideable {
    public func distance(to other: Date) -> TimeInterval {
        return other.timeIntervalSinceReferenceDate - self.timeIntervalSinceReferenceDate
    }

    public func advanced(by n: TimeInterval) -> Date {
        return self + n
    }
}
