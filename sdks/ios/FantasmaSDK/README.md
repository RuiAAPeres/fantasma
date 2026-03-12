# FantasmaSDK

`FantasmaSDK` is the minimal iOS client for Fantasma's event ingest API.

## Install

Add the local package in Xcode from:

```text
sdks/ios/FantasmaSDK
```

## Usage

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

`track(_:properties:)` only takes the explicit string properties you want on the event. The SDK adds `platform`, `app_version`, and `os_version` automatically.

## Behavior

- Tracked events are persisted to SQLite before upload.
- Events are uploaded in JSON batches to `POST /v1/events`.
- Successful `202 Accepted` responses delete uploaded rows from the queue.
- Failed uploads leave rows in SQLite for later replay.
- The SDK generates and persists an install identifier locally.
- The SDK auto-populates `platform`, `app_version`, and `os_version` on each event.
- Event properties remain explicit string-to-string context you pass in `track(_:properties:)`.
- `clear()` rotates the local install identifier without mutating already queued rows.
