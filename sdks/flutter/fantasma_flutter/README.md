# Fantasma Flutter SDK

Fantasma Flutter is the first-party Flutter client for Fantasma's install-scoped
event ingest API.

See the repository-level Flutter SDK guide in `../README.md`.

Behavior notes:

- The package currently supports iOS and Android only.
- Tracked events auto-populate `platform`, `device`, `app_version`,
  `os_version`, and `locale`.
- Flutter keeps queue, blocked-destination state, and install identity scoped
  to the explicit `storageNamespace`.
