

import Foundation
import UIKit
import WebKit

class DashboardViewController: UIViewController {

  @IBOutlet weak var webview: WKWebView!
  
  override func viewDidLoad() {
    super.viewDidLoad()
    let url = URL(string: "https://appslications.s3.amazonaws.com/HealthkitApp/index.html#/pin/\(UserDefaults.standard.string(forKey: "pin")!)")
    let request = URLRequest(url: url!)
    
    webview.load(request)
    
  }

}

