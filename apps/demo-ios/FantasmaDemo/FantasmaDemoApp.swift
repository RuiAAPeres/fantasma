import SwiftUI
import FantasmaSDK

@main
struct FantasmaDemoApp: App {
    private static let writeKeyEnvironmentName = "FANTASMA_DEMO_WRITE_KEY"

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
            guard
                let writeKey = ProcessInfo.processInfo.environment[Self.writeKeyEnvironmentName]?
                    .trimmingCharacters(in: .whitespacesAndNewlines),
                !writeKey.isEmpty
            else {
                startupError =
                    "Set \(Self.writeKeyEnvironmentName) in the FantasmaDemo scheme using scripts/provision-project.sh output."
                return
            }

            try await Fantasma.configure(
                serverURL: URL(string: "http://localhost:8081")!,
                writeKey: writeKey
            )
            try await Fantasma.track("app_open")
            sdkReady = true
        } catch {
            startupError = "SDK bootstrap failed: \(error.localizedDescription)"
        }
    }
}
