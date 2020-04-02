

import Foundation
import UIKit
import WebKit

class AboutViewController: UIViewController {

  @IBOutlet weak var webview: WKWebView!
  
  override func viewDidLoad() {
    super.viewDidLoad()
    let url = URL(string: "https://appslications.s3.amazonaws.com/HealthkitApp/update/index.html")
    let request = URLRequest(url: url!)
    
    webview.load(request)
    
  }
  



}

