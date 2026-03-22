import 'dart:io';
import 'dart:ui';

import 'package:device_info_plus/device_info_plus.dart';
import 'package:package_info_plus/package_info_plus.dart';

import 'errors.dart';
import 'models.dart';

abstract interface class FantasmaMetadataProvider {
  Future<MetadataSnapshot> load();
}

final class LiveMetadataProvider implements FantasmaMetadataProvider {
  const LiveMetadataProvider();

  @override
  Future<MetadataSnapshot> load() async {
    final packageInfo = await PackageInfo.fromPlatform();
    final deviceInfo = DeviceInfoPlugin();
    if (Platform.isAndroid) {
      final info = await deviceInfo.androidInfo;
      return MetadataSnapshot(
        platform: 'android',
        appVersion: _appVersion(packageInfo),
        osVersion: info.version.release,
        fallbackLocale: _localeToLanguageTag(PlatformDispatcher.instance.locale),
      );
    }
    if (Platform.isIOS) {
      final info = await deviceInfo.iosInfo;
      return MetadataSnapshot(
        platform: 'ios',
        appVersion: _appVersion(packageInfo),
        osVersion: info.systemVersion,
        fallbackLocale: _localeToLanguageTag(PlatformDispatcher.instance.locale),
      );
    }
    throw const FantasmaPlatformException(
      'Fantasma Flutter supports iOS and Android only.',
    );
  }

  String? _appVersion(PackageInfo info) {
    if (info.version.trim().isNotEmpty) {
      return info.version.trim();
    }
    if (info.buildNumber.trim().isNotEmpty) {
      return info.buildNumber.trim();
    }
    return null;
  }
}

final class StaticMetadataProvider implements FantasmaMetadataProvider {
  StaticMetadataProvider({
    required this.platform,
    required this.appVersion,
    required this.osVersion,
    required Locale fallbackLocale,
  }) : fallbackLocaleTag = _localeToLanguageTag(fallbackLocale);

  final String platform;
  final String? appVersion;
  final String? osVersion;
  final String? fallbackLocaleTag;

  @override
  Future<MetadataSnapshot> load() async {
    return MetadataSnapshot(
      platform: platform,
      appVersion: appVersion,
      osVersion: osVersion,
      fallbackLocale: fallbackLocaleTag,
    );
  }
}

String? localeToLanguageTag(Locale? locale) => _localeToLanguageTag(locale);

String? _localeToLanguageTag(Locale? locale) {
  if (locale == null) {
    return null;
  }
  final tag = locale.toLanguageTag();
  return tag.isEmpty ? null : tag;
}
