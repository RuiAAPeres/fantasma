# demo-ios

Minimal SwiftUI app for validating the Fantasma iOS SDK against a local Fantasma stack.

## Run

Start the backend:

```bash
docker compose -f infra/docker/compose.yaml up --build
```

Open the project:

```bash
open apps/demo-ios/FantasmaDemo.xcodeproj
```

Run `FantasmaDemo` in the iOS Simulator. The app:

- configures `FantasmaSDK` for `http://localhost:8081`
- sends `app_open` on launch
- sends `screen_view` for the home screen
- sends `button_pressed` when the main button is tapped

The resulting metrics are install-based. If the same person runs the app on
two devices or simulators, Fantasma counts those as separate installs.

## API verification

After launching the app and tapping the button at least once, use the metrics
API with a UTC window that covers the time you ran the simulator.

These reads are asynchronous end to end. The demo app does not call `flush()`
during normal interaction, so new events are usually visible only after the
SDK's upload timer fires or the app backgrounds, plus the worker's next poll.
If the first request comes back empty, wait at least 10 to 15 seconds and poll
again, or background the app to trigger an upload sooner.

Confirm that `screen_view` grouped by `screen` includes `Home`:

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=screen_view&start_date=<UTC-start-date>&end_date=<UTC-end-date>&group_by=screen" \
  -H "Authorization: Bearer fg_pat_dev"
```

Confirm that `button_pressed` events were counted:

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=button_pressed&start_date=<UTC-start-date>&end_date=<UTC-end-date>" \
  -H "Authorization: Bearer fg_pat_dev"
```

Confirm that the simulator session appears in the daily session series:

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions/count/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=<UTC-start-date>&end_date=<UTC-end-date>" \
  -H "Authorization: Bearer fg_pat_dev"
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
