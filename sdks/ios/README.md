# Fantasma iOS SDK

The first iOS SDK implementation lives in `sdks/ios/FantasmaSDK` as a Swift Package.

Public API:

- `configure(serverURL:writeKey:)`
- `track(_:properties:)`
- `flush()`
- `clear()`

Example usage:

```swift
import FantasmaSDK

Fantasma.configure(
    serverURL: "http://localhost:8081",
    writeKey: "fg_ing_test"
)

Fantasma.track("app_open")
Fantasma.track("screen_view", properties: ["screen": "Home"])
Fantasma.flush()
Fantasma.clear()
```

Pass only explicit string properties to `track(_:properties:)`, with at most 3
keys per event. The SDK auto-populates `platform`, `app_version`, and
`os_version` for each event.

Behavior notes:

- Every tracked event is written to a local SQLite queue before upload.
- The SDK adds `timestamp`, `install_id`, `platform = "ios"`, `app_version`, and `os_version`.
- Event properties remain explicit string-to-string context passed by the app.
- `clear()` rotates `install_id` and preserves already queued events.
- Upload attempts happen every 10 seconds, when the queue reaches 50 events, when `flush()` is called, and when the app enters background.
