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

Pass only explicit string properties to `track(_:properties:)`, with at most 4
keys per event. Property names must match `^[a-z][a-z0-9_]{0,62}$` and cannot
use reserved keys such as `event`, `install_id`, `metric`, `granularity`,
`start`, `end`, `platform`, `app_version`, or `os_version`. The SDK rejects
invalid property maps before they enter the local queue. The SDK auto-populates
`platform`, `app_version`, and `os_version` for each event.

Behavior notes:

- Every tracked event is written to a local SQLite queue before upload.
- `writeKey` must be a project-scoped `ingest` key, not a metrics read key.
- The SDK adds `timestamp`, `install_id`, `platform = "ios"`, `app_version`, and `os_version`.
- Event properties remain explicit string-to-string context passed by the app, and invalid property maps are rejected locally before persistence.
- `clear()` rotates `install_id` and preserves already queued events.
- `track(_:properties:)` throws when the SDK has not been configured.
- `flush()` throws when the SDK has not been configured.
- Malformed `202 Accepted` responses are treated as invalid responses and preserve queued rows.
- Reconfiguring to a different server URL or write key discards any still-queued rows after the current upload boundary, even across app relaunches, then switches future uploads to the new destination.
- Upload attempts happen every 10 seconds, when the queue reaches 50 events, when `flush()` is called, and when the app enters background.
