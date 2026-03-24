# Fantasma React Native SDK

The React Native SDK lives under `sdks/react-native/fantasma-react-native`.

It exposes an explicit `FantasmaClient` JS API over the existing iOS and
Android Fantasma SDKs. React Native parity is at the event-contract and
runtime-behavior level, not identical native API shapes underneath.

## Public API

- `new FantasmaClient({ serverUrl, writeKey })`
- `track(eventName)`
- `flush()`
- `clear()`
- `close()`

All methods return promises. Only one live React Native `FantasmaClient` may
exist per process at a time.

## Install

The package name is:

```text
fantasma-react-native
```

The package metadata is publishable, but the repository is still consuming it
from source and has not published it to npm yet.

Current packaging shape:

- bare React Native support through native module files under `ios/` and `android/`
- Expo-managed support through prebuild/dev-client workflow only
- no Expo Go support in v1

## Usage

```ts
import { FantasmaClient } from "fantasma-react-native";

const fantasma = new FantasmaClient({
  serverUrl: "https://api.usefantasma.com",
  writeKey: "<ingest-key-from-provision-project>",
});

await fantasma.track("app_open");
await fantasma.flush();
await fantasma.clear();
await fantasma.close();
```

## Behavior Notes

- The JS client validates config synchronously and reserves the single live-client slot at construction time.
- Native configure/acquire happens on first use.
- If that first native acquire fails, the JS client auto-closes itself, frees the slot, and future calls on that same instance throw `closed_client`.
- `close()` closes the JS client immediately and allows a fresh client to be created right away; the native bridge retires or hands off the underlying destination state safely underneath.
- Destination switching stays supported by closing the current client and constructing a new one with the new destination.
- The native SDKs still own queue durability, built-in metadata population, blocked-destination handling, and identity rotation semantics.

## Error Codes

- `invalid_write_key`
- `unsupported_server_url`
- `invalid_event_name`
- `encoding_failed`
- `invalid_response`
- `upload_failed`
- `storage_failure`
- `closed_client`
- `client_already_active`
