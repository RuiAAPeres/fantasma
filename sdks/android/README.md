# Fantasma Android SDK

`FantasmaSDK` for Android is the Android-first Kotlin client for Fantasma's event
ingest API.

Android parity is at the event-contract and runtime-behavior level, not the
exact public API shape. The iOS SDK exposes a static `Fantasma` facade; Android
uses an instance-based `FantasmaClient` API intentionally because that fits
Android/Kotlin usage better.

## Install

The Android build lives under:

```text
sdks/android
```

The library module is:

```text
:fantasma-sdk
```

## Usage

```kotlin
import android.app.Application
import com.fantasma.sdk.FantasmaClient
import com.fantasma.sdk.FantasmaConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

class DemoApplication : Application() {
    private lateinit var fantasma: FantasmaClient

    override fun onCreate() {
        super.onCreate()
        fantasma = FantasmaClient(
            context = this,
            config = FantasmaConfig(
                serverUrl = "http://10.0.2.2:8081",
                writeKey = "<ingest-key-from-provision-project>",
            ),
        )

        CoroutineScope(Dispatchers.Main).launch {
            fantasma.track("app_open")
        }
    }
}
```

## Public API

- `FantasmaClient(context, FantasmaConfig(serverUrl, writeKey))`
- `track(eventName)`
- `flush()`
- `clear()`
- `close()`

`track()` and `flush()` are suspend functions and throw typed `FantasmaException`
failures. `close()` is idempotent, and any later `track()`, `flush()`, or
`clear()` call on that closed client throws `FantasmaException.ClosedClient`.

## Behavior

- Tracked events are persisted to SQLite before upload.
- `writeKey` must be a project-scoped `ingest` key.
- Events are uploaded in JSON batches to `POST /v1/events`; the Android SDK
  defaults send up to 100 queued events per request against the server-side
  200-event cap.
- Successful `202 Accepted` responses delete uploaded rows from the queue.
- Failed uploads leave rows in SQLite for later replay.
- Malformed `202 Accepted` responses are treated as invalid responses and leave
  rows queued.
- `401`, `422`, and `409 project_pending_deletion` block that destination until
  the app creates a client with a different normalized `(serverUrl, writeKey)`.
- `500`, transport failures, and unrelated `409` responses remain retryable.
- The SDK attempts a periodic flush every 30 seconds, when the queue reaches 100
  events, on explicit `flush()`, and when the app enters background.
- The SDK auto-populates `platform = "android"`, `app_version`,
  `os_version`, and `locale`.
- The SDK persists one app-local install identifier, reuses it across
  destinations, and rotates it on `clear()` without mutating already queued
  rows.
- Clients created with the same normalized destination share one process-local
  runtime.
- Creating a client with a different normalized destination supersedes the
  previously active destination for the process after the current upload
  boundary, matching the Swift SDK's single-destination behavior.
- Older Android client handles should be treated as replaced once a different
  destination is created; future `track()`, `flush()`, and `clear()` calls on a
  superseded handle throw `FantasmaException.ClosedClient`, and the superseded
  runtime retires itself without requiring an explicit `close()` on the old
  handle.
