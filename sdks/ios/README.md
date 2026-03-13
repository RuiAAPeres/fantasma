# Fantasma iOS SDK

The first iOS SDK implementation lives in `sdks/ios/FantasmaSDK` as a Swift Package.

Public API:

- `configure(serverURL:writeKey:) async throws`
- `track(_:properties:) async throws`
- `flush() async throws`
- `clear() async`

Example usage:

```swift
import Foundation
import FantasmaSDK

let serverURL = URL(string: "http://localhost:8081")!

Task {
    do {
        try await Fantasma.configure(
            serverURL: serverURL,
            writeKey: "<ingest-key-from-provision-project>"
        )
        try await Fantasma.track("app_open")
        try await Fantasma.track("screen_view", properties: ["screen": "Home"])
        try await Fantasma.flush()
        await Fantasma.clear()
    } catch {
        print("Fantasma SDK error: \(error)")
    }
}
```

Pass only explicit string properties to `track(_:properties:)`, with at most 3
keys per event. The SDK auto-populates `platform`, `app_version`, and
`os_version` for each event.

Behavior notes:

- Every tracked event is written to a local SQLite queue before upload.
- `writeKey` must be a project-scoped `ingest` key, not a metrics read key.
- The SDK adds `timestamp`, `install_id`, `platform = "ios"`, `app_version`, and `os_version`.
- Event properties remain explicit string-to-string context passed by the app.
- `clear()` rotates `install_id` and preserves already queued events.
- `track(_:properties:)` throws when the SDK has not been configured.
- `flush()` throws when the SDK has not been configured.
- Upload attempts happen every 10 seconds, when the queue reaches 50 events, when `flush()` is called, and when the app enters background.
