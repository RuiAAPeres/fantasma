# Fantasma iOS SDK

The first iOS SDK implementation lives in `sdks/ios/FantasmaSDK` as a Swift Package.

Public API:

- `configure(serverURL:writeKey:)`
- `track(_:properties:)`
- `identify(_:)`
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
Fantasma.identify("user_123")
Fantasma.flush()
Fantasma.clear()
```

Behavior notes:

- Every tracked event is written to a local SQLite queue before upload.
- The SDK adds `timestamp`, `install_id`, `platform = "ios"`, `app_version`, and the current `session_id`.
- `identify(_:)` only affects future events.
- `clear()` rotates `install_id`, clears `user_id`, rotates `session_id`, and preserves already queued events.
- Upload attempts happen every 10 seconds, when the queue reaches 50 events, when `flush()` is called, and when the app enters background.
