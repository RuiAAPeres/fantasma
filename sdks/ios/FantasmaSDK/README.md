# FantasmaSDK

`FantasmaSDK` is the minimal iOS client for Fantasma's event ingest API.

## Install

Add the local package in Xcode from:

```text
sdks/ios/FantasmaSDK
```

## Usage

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

The SDK adds `platform`, `device`, `app_version`, `os_version`, and `locale`
automatically.

## Behavior

- Tracked events are persisted to SQLite before upload.
- `writeKey` must be a project-scoped `ingest` key.
- Events are uploaded in JSON batches to `POST /v1/events`; the live SDK
  defaults send up to 100 queued events per request against the server-side
  200-event cap.
- Successful `202 Accepted` responses delete uploaded rows from the queue.
- Failed uploads leave rows in SQLite for later replay.
- Malformed `202 Accepted` responses are treated as invalid responses and also leave rows queued.
- `409 project_busy` is treated as a transient upload failure; queued rows stay durable and the SDK will retry on later automatic flushes.
- `409 project_pending_deletion` is treated as a blocked destination; queued rows stay durable, automatic flushes stop retrying that destination, and only reconfiguring to a different server/write-key pair clears the block.
- `track(_:)` throws when the SDK has not been configured.
- `flush()` throws when the SDK has not been configured.
- The SDK also attempts a periodic flush every 30 seconds when configured.
- The SDK auto-populates `platform`, `device`, `app_version`, `os_version`, and `locale` on each event.
- iPhone emits `platform = "ios"`, `device = "phone"`.
- iPad emits `platform = "ios"`, `device = "tablet"`.
- Native macOS, Mac Catalyst, and iOS-on-Mac desktop-class runs emit `platform = "macos"`, `device = "desktop"`.
- The SDK persists one local install identifier, reuses it on every event, and rotates it on `clear()` without mutating already queued rows.
- Reconfiguring to a different server URL or write key discards any still-queued rows after the current upload boundary, even across app relaunches, then switches future uploads to the new destination.
