# Fantasma Flutter SDK

The Flutter SDK lives under `sdks/flutter/fantasma_flutter`.

Public API:

- `FantasmaConfig(serverUrl, writeKey, storageNamespace, {localeProvider})`
- `FantasmaClient(config)`
- `track(eventName)`
- `flush()`
- `clear()`
- `close()`

Example usage:

```dart
import 'dart:ui';

import 'package:fantasma_flutter/fantasma_flutter.dart';
import 'package:flutter/widgets.dart';

final client = FantasmaClient(
  FantasmaConfig(
    serverUrl: 'http://10.0.2.2:8081',
    writeKey: '<ingest-key-from-provision-project>',
    storageNamespace: 'primary',
    localeProvider: () => WidgetsBinding.instance.platformDispatcher.locale,
  ),
);

await client.track('app_open');
await client.flush();
await client.clear();
await client.close();
```

Behavior notes:

- Every tracked event is written to a local SQLite queue before upload.
- `writeKey` must be a project-scoped `ingest` key.
- The SDK adds `timestamp`, `install_id`, `platform`, `app_version`, `os_version`, and `locale`.
- `localeProvider` is resolved when `track()` enqueues the event; queued rows keep that snapshot even if the app locale changes later.
- `storageNamespace` is required and defines the client-local persistence boundary for the queue, install identity, and blocked-destination state.
- Only one live `FantasmaClient` may use a given `storageNamespace` inside a process at a time. Reusing a namespace concurrently is rejected.
- `clear()` rotates `install_id` for future queued events and preserves already queued rows.
- Upload attempts happen every 30 seconds, when the queue reaches 100 events, when `flush()` is called, and when the app pauses or backgrounds.
- Lifecycle-triggered uploads are best-effort flush attempts, not delivery guarantees.
- `401`, `422`, and `409 project_pending_deletion` block that client's normalized destination in local storage until the app creates a client with a different destination or namespace.
- Threshold-triggered and lifecycle-triggered flushes are best-effort background work and do not make `track()` surface upload failures.
