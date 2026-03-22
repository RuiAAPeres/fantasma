import 'dart:async';
import 'dart:ui';

import 'errors.dart';

typedef LocaleProvider = FutureOr<Locale?> Function();

final class FantasmaConfig {
  FantasmaConfig({
    required String serverUrl,
    required String writeKey,
    required String storageNamespace,
    this.localeProvider,
  })  : normalizedServerUrl = _normalizeServerUrl(serverUrl),
        normalizedWriteKey = _normalizeWriteKey(writeKey),
        storageNamespace = _normalizeStorageNamespace(storageNamespace);

  final String normalizedServerUrl;
  final String normalizedWriteKey;
  final String storageNamespace;
  final LocaleProvider? localeProvider;

  String get destinationSignature =>
      '$normalizedServerUrl|$normalizedWriteKey';

  static String _normalizeServerUrl(String value) {
    final trimmed = value.trim();
    final uri = Uri.tryParse(trimmed);
    if (uri == null) {
      throw const FantasmaConfigException('Invalid server URL.');
    }
    final scheme = uri.scheme.toLowerCase();
    if (scheme != 'http' && scheme != 'https') {
      throw const FantasmaConfigException('Server URL must use http or https.');
    }
    final host = uri.host.toLowerCase();
    if (host.isEmpty) {
      throw const FantasmaConfigException('Server URL must include a host.');
    }

    final normalized = uri.replace(
      scheme: scheme,
      host: host,
      path: _trimTrailingSlash(uri.path),
    );
    return normalized.toString();
  }

  static String _normalizeWriteKey(String value) {
    final trimmed = value.trim();
    if (trimmed.isEmpty) {
      throw const FantasmaConfigException('Write key must not be blank.');
    }
    return trimmed;
  }

  static String _normalizeStorageNamespace(String value) {
    final trimmed = value.trim();
    if (trimmed.isEmpty) {
      throw const FantasmaConfigException(
        'Storage namespace must not be blank.',
      );
    }
    return trimmed;
  }

  static String _trimTrailingSlash(String path) {
    if (path.isEmpty || path == '/') {
      return '';
    }
    var result = path;
    while (result.endsWith('/')) {
      result = result.substring(0, result.length - 1);
    }
    return result;
  }
}
