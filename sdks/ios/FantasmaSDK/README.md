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
Fantasma.identify("user_123")
Fantasma.flush()
Fantasma.clear()
```

## Behavior

- Tracked events are persisted to SQLite before upload.
- Events are uploaded in JSON batches to `POST /v1/events`.
- Successful `202 Accepted` responses delete uploaded rows from the queue.
- Failed uploads leave rows in SQLite for later replay.
