import SwiftUI
import FantasmaSDK

struct ContentView: View {
    @State private var didTrackScreen = false
    @State private var statusText = "Waiting for input"

    var body: some View {
        VStack(spacing: 20) {
            Text("Fantasma Demo")
                .font(.largeTitle.bold())

            Text("Tracks explicit events against the local Fantasma ingest service.")
                .font(.body)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)

            Button(action: trackButtonPress) {
                Text("Track button_pressed")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)

            Text(statusText)
                .font(.footnote)
                .foregroundStyle(.secondary)
                .accessibilityLabel("Latest SDK action")
        }
        .padding(24)
        .task {
            trackScreenIfNeeded()
        }
    }

    private func trackScreenIfNeeded() {
        guard !didTrackScreen else {
            return
        }

        didTrackScreen = true
        Fantasma.track("screen_view", properties: ["screen": "Home"])
        statusText = "Queued screen_view for Home"
    }

    private func trackButtonPress() {
        Fantasma.track("button_pressed")
        statusText = "Queued button_pressed"
    }
}

#Preview {
    ContentView()
}
