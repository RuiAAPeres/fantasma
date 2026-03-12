import SwiftUI
import FantasmaSDK

@main
struct FantasmaDemoApp: App {
    @State private var didBootstrapSDK = false
    @State private var sdkReady = false
    @State private var startupError: String?

    var body: some Scene {
        WindowGroup {
            ContentView(
                sdkReady: sdkReady,
                startupError: startupError
            )
            .task {
                await bootstrapSDKIfNeeded()
            }
        }
    }

    @MainActor
    private func bootstrapSDKIfNeeded() async {
        guard !didBootstrapSDK else {
            return
        }

        didBootstrapSDK = true

        do {
            try await Fantasma.configure(
                serverURL: URL(string: "http://localhost:8081")!,
                writeKey: "fg_ing_test"
            )
            try await Fantasma.track("app_open")
            sdkReady = true
        } catch {
            startupError = "SDK bootstrap failed: \(error.localizedDescription)"
        }
    }
}
