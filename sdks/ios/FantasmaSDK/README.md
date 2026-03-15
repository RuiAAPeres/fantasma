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
        try await Fantasma.track("screen_view", properties: ["screen": "Home"])
        try await Fantasma.flush()
        await Fantasma.clear()
    } catch {
        print("Fantasma SDK error: \(error)")
    }
}
```

`track(_:properties:)` only takes the explicit string properties you want on
the event, with at most 4 keys per event. Property names must match
`^[a-z][a-z0-9_]{0,62}$` and may not use reserved keys such as `event`,
`install_id`, `metric`, `granularity`, `start`, `end`, `platform`,
`app_version`, or `os_version`. The SDK rejects invalid properties before they
enter the local queue. The SDK adds `platform`, `app_version`, and
`os_version` automatically.

## Behavior

- Tracked events are persisted to SQLite before upload.
- `writeKey` must be a project-scoped `ingest` key.
- Events are uploaded in JSON batches to `POST /v1/events`.
- Successful `202 Accepted` responses delete uploaded rows from the queue.
- Failed uploads leave rows in SQLite for later replay.
- Malformed `202 Accepted` responses are treated as invalid responses and also leave rows queued.
- `track(_:properties:)` throws when the SDK has not been configured.
- `flush()` throws when the SDK has not been configured.
- The SDK auto-populates `platform`, `app_version`, and `os_version` on each event.
- Event properties remain explicit string-to-string context you pass in `track(_:properties:)`, and invalid property maps are rejected locally before persistence.
- The SDK persists one local install identifier, reuses it on every event, and rotates it on `clear()` without mutating already queued rows.
- Reconfiguring to a different server URL or write key discards any still-queued rows after the current upload boundary, even across app relaunches, then switches future uploads to the new destination.
