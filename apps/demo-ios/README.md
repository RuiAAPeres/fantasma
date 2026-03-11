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
