# fantasma-react-native

React Native bridge package for Fantasma's existing iOS and Android SDKs.

The native iOS and Android layers own queue durability plus built-in metadata,
including `platform`, `device`, `app_version`, `os_version`, and `locale`.
Apple desktop-class runs inherit the native `platform = "macos"`,
`device = "desktop"` mapping through the bundled Swift sources.

See [`../README.md`](../README.md) for the repository-level SDK documentation.
