import UIKit
import HealthKit
import WebKit

@UIApplicationMain
class AppDelegate: UIResponder, UIApplicationDelegate {
  var window: UIWindow?
  
  let healthStore = HKHealthStore()
  
  func applicationShouldRequestHealthAuthorization(_ application: UIApplication) {
    healthStore.handleAuthorizationForExtension { success, error in
    }
  }
}
