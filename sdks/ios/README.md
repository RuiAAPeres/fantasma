# Fantasma iOS SDK

The iOS SDK sources live in `sdks/ios/FantasmaSDK`, and the repo root publishes
them as the `FantasmaSDK` Swift package.

Public API:

- `configure(serverURL:writeKey:) async throws`
- `track(_:) async throws`
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
        try await Fantasma.track("screen_view")
        try await Fantasma.flush()
        await Fantasma.clear()
    } catch {
        print("Fantasma SDK error: \(error)")
    }
}
```

The SDK auto-populates `platform`, `device`, `app_version`, `os_version`, and
`locale` for each event.

## Install

Add `FantasmaSDK` from the repo root package URL:

```text
https://github.com/RuiAAPeres/fantasma.git
```

Then use:

- Package: `FantasmaSDK`
- Minimum version: your tagged release (for example, `0.4.0`)

```swift
dependencies: [
    .package(url: "https://github.com/RuiAAPeres/fantasma.git", from: "0.4.0"),
]
```

For local development, you can also use an explicit path to this repository until
you have tags available.

```swift
.package(path: "../fantasma"),
```

Behavior notes:

- Every tracked event is written to a local SQLite queue before upload.
- `writeKey` must be a project-scoped `ingest` key, not a metrics read key.
- The SDK adds `timestamp`, `install_id`, `platform`, `device`, `app_version`, `os_version`, and `locale`.
- iPhone emits `platform = "ios"`, `device = "phone"`.
- iPad emits `platform = "ios"`, `device = "tablet"`.
- Native macOS, Mac Catalyst, and iOS-on-Mac desktop-class runs emit `platform = "macos"`, `device = "desktop"`.
- `clear()` rotates `install_id` and preserves already queued events.
- `track(_:)` throws when the SDK has not been configured.
- `flush()` throws when the SDK has not been configured.
- Malformed `202 Accepted` responses are treated as invalid responses and preserve queued rows.
- Reconfiguring to a different server URL or write key discards any still-queued rows after the current upload boundary, even across app relaunches, then switches future uploads to the new destination.
- Upload attempts happen every 30 seconds, when the queue reaches 100 events, when `flush()` is called, and when the app enters background.
