import SwiftUI
import FantasmaSDK

struct ContentView: View {
    let sdkReady: Bool
    let startupError: String?

    @State private var didTrackScreen = false
    @State private var statusText = "Configuring SDK..."

    var body: some View {
        VStack(spacing: 20) {
            Text("Fantasma Demo")
                .font(.largeTitle.bold())

            Text("Tracks explicit events against the local Fantasma ingest service.")
                .font(.body)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)

            Button {
                Task {
                    await trackButtonPress()
                }
            } label: {
                Text("Track button_pressed")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
            .disabled(!sdkReady)

            Text(statusText)
                .font(.footnote)
                .foregroundStyle(.secondary)
                .accessibilityLabel("Latest SDK action")
        }
        .padding(24)
        .task(id: sdkReady) {
            await trackScreenIfNeeded()
        }
        .task(id: startupError) {
            if let startupError {
                statusText = startupError
            }
        }
    }

    private func trackScreenIfNeeded() async {
        guard startupError == nil else {
            return
        }

        guard sdkReady else {
            return
        }

        guard !didTrackScreen else {
            return
        }

        do {
            try await Fantasma.track("screen_view")
            didTrackScreen = true
            statusText = "Queued screen_view for Home"
        } catch {
            statusText = "Failed to queue screen_view: \(error.localizedDescription)"
        }
    }

    private func trackButtonPress() async {
        guard startupError == nil else {
            statusText = startupError ?? statusText
            return
        }

        guard sdkReady else {
            statusText = "SDK not ready yet"
            return
        }

        do {
            try await Fantasma.track("button_pressed")
            statusText = "Queued button_pressed"
        } catch {
            statusText = "Failed to queue button_pressed: \(error.localizedDescription)"
        }
    }
}

#Preview {
    ContentView(
        sdkReady: true,
        startupError: nil
    )
}
