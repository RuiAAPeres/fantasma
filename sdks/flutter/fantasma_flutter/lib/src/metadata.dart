import 'dart:io';
import 'dart:ui';

import 'package:device_info_plus/device_info_plus.dart';
import 'package:flutter/foundation.dart';
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
        device: classifyAndroidDeviceFormFactor(info),
        appVersion: _appVersion(packageInfo),
        osVersion: info.version.release,
        fallbackLocale: _localeToLanguageTag(PlatformDispatcher.instance.locale),
      );
    }
    if (Platform.isIOS) {
      final info = await deviceInfo.iosInfo;
      return MetadataSnapshot(
        platform: 'ios',
        device: classifyIosDeviceFormFactor(info),
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
    this.device = 'unknown',
    required this.appVersion,
    required this.osVersion,
    required Locale fallbackLocale,
  }) : fallbackLocaleTag = _localeToLanguageTag(fallbackLocale);

  final String platform;
  final String device;
  final String? appVersion;
  final String? osVersion;
  final String? fallbackLocaleTag;

  @override
  Future<MetadataSnapshot> load() async {
    return MetadataSnapshot(
      platform: platform,
      device: device,
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

@visibleForTesting
String classifyAndroidDeviceFormFactor(AndroidDeviceInfo info) {
  final hints = _normalizedDeviceHints(<String>[
    info.model,
    info.device,
    info.product,
    info.manufacturer,
    info.brand,
  ]);
  if (_containsDeviceHint(hints, <String>['tablet', 'tab', 'pad'])) {
    return 'tablet';
  }
  if (_containsDeviceHint(hints, <String>['phone'])) {
    return 'phone';
  }

  final features = info.systemFeatures
      .map((feature) => feature.trim().toLowerCase())
      .where((feature) => feature.isNotEmpty)
      .toSet();
  if (features.any((feature) {
    return feature == 'android.hardware.type.automotive' ||
        feature == 'android.hardware.type.television' ||
        feature == 'android.hardware.type.watch' ||
        feature == 'android.software.leanback';
  })) {
    return 'unknown';
  }
  if (features.contains('android.hardware.telephony')) {
    return 'phone';
  }
  if (features.contains('android.hardware.touchscreen')) {
    return 'tablet';
  }

  return 'unknown';
}

@visibleForTesting
String classifyIosDeviceFormFactor(IosDeviceInfo info) {
  final hints = _normalizedDeviceHints(<String>[
    info.model,
    info.localizedModel,
    info.utsname.machine,
  ]);
  if (_containsDeviceHint(hints, <String>['ipad'])) {
    return 'tablet';
  }
  if (_containsDeviceHint(hints, <String>['iphone', 'ipod'])) {
    return 'phone';
  }

  return 'unknown';
}

List<String> _normalizedDeviceHints(List<String> hints) {
  return hints
      .map((hint) => hint.trim().toLowerCase())
      .where((hint) => hint.isNotEmpty)
      .toList(growable: false);
}

bool _containsDeviceHint(List<String> hints, List<String> needles) {
  return hints.any((hint) => needles.any((needle) => hint.contains(needle)));
}
