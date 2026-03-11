import SwiftUI
import FantasmaSDK

@main
struct FantasmaDemoApp: App {
    init() {
        Fantasma.configure(
            serverURL: "http://localhost:8081",
            writeKey: "fg_ing_test"
        )
        Fantasma.track("app_open")
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
