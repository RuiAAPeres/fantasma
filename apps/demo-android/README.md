# demo-android

Example Android application for validating the Fantasma Android SDK against a
local or self-hosted Fantasma instance.

The Gradle module is included from `sdks/android/settings.gradle.kts` as
`:demo`, and it exercises the real `FantasmaClient` API rather than raw HTTP.
