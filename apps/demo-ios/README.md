# demo-ios

Minimal SwiftUI app for validating the Fantasma iOS SDK against a local
Fantasma stack.

## Run

Start the backend:

```bash
docker compose -f infra/docker/compose.yaml up --build
```

Provision a demo project plus scoped keys:

```bash
PROVISIONED="$(./scripts/provision-project.sh \
  --project-name "Fantasma Demo iOS" \
  --ingest-key-name "ios-demo" \
  --read-key-name "demo-read")"
printf '%s\n' "$PROVISIONED"
```

Open the project:

```bash
open apps/demo-ios/FantasmaDemo.xcodeproj
```

Set `FANTASMA_DEMO_WRITE_KEY` in the `FantasmaDemo` scheme environment using
the returned `ingest_key.secret`, then run `FantasmaDemo` in the iOS
Simulator. The app:

- configures `FantasmaSDK` for `http://localhost:8081`
- sends `app_open` on launch
- sends `screen_view` for the home screen
- sends `button_pressed` when the main button is tapped

The resulting metrics are install-based. If the same person runs the app on
two devices or simulators, Fantasma counts those as separate installs.

## API verification

After launching the app and tapping the button at least once, use the
provisioned `read_key.secret` with a UTC window that covers the time you ran
the simulator.

These reads are asynchronous end to end. The demo app does not call `flush()`
during normal interaction, so new events are usually visible only after the
SDK's upload timer fires or the app backgrounds, plus the worker's next poll.
If the first request comes back empty, wait at least 10 to 15 seconds and poll
again, or background the app to trigger an upload sooner.

Confirm that `screen_view` grouped by `screen` includes `Home`:

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/aggregate?event=screen_view&start_date=<UTC-start-date>&end_date=<UTC-end-date>&group_by=screen" \
  -H "X-Fantasma-Key: <read-key-from-provision-project-output>"
```

Confirm that `button_pressed` events were counted:

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/aggregate?event=button_pressed&start_date=<UTC-start-date>&end_date=<UTC-end-date>" \
  -H "X-Fantasma-Key: <read-key-from-provision-project-output>"
```

Confirm that the simulator session appears in the daily session series:

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions/count/daily?start_date=<UTC-start-date>&end_date=<UTC-end-date>" \
  -H "X-Fantasma-Key: <read-key-from-provision-project-output>"
```

## CLI verification

List the project scheme:

```bash
xcodebuild -project apps/demo-ios/FantasmaDemo.xcodeproj -list
```

Build for the simulator:

```bash
xcodebuild \
  -project apps/demo-ios/FantasmaDemo.xcodeproj \
  -scheme FantasmaDemo \
  -destination 'platform=iOS Simulator,name=iPhone 16' \
  CODE_SIGNING_ALLOWED=NO \
  build
```
