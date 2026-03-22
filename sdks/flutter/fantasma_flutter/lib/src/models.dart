import 'dart:convert';

final class QueuedEvent {
  const QueuedEvent({
    this.id,
    required this.event,
    required this.timestamp,
    required this.installId,
    required this.platform,
    required this.appVersion,
    required this.osVersion,
    required this.locale,
  });

  final int? id;
  final String event;
  final String timestamp;
  final String installId;
  final String platform;
  final String? appVersion;
  final String? osVersion;
  final String? locale;

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'event': event,
      'timestamp': timestamp,
      'install_id': installId,
      'platform': platform,
      'app_version': appVersion,
      'os_version': osVersion,
      'locale': locale,
    };
  }

  String toPayload() => jsonEncode(toJson());

  QueuedEvent copyWithId(int id) {
    return QueuedEvent(
      id: id,
      event: event,
      timestamp: timestamp,
      installId: installId,
      platform: platform,
      appVersion: appVersion,
      osVersion: osVersion,
      locale: locale,
    );
  }

  factory QueuedEvent.fromPayload({
    required int id,
    required String payload,
  }) {
    final decoded = jsonDecode(payload) as Map<String, Object?>;
    return QueuedEvent(
      id: id,
      event: decoded['event'] as String,
      timestamp: decoded['timestamp'] as String,
      installId: decoded['install_id'] as String,
      platform: decoded['platform'] as String,
      appVersion: decoded['app_version'] as String?,
      osVersion: decoded['os_version'] as String?,
      locale: decoded['locale'] as String?,
    );
  }
}

final class MetadataSnapshot {
  const MetadataSnapshot({
    required this.platform,
    required this.appVersion,
    required this.osVersion,
    required this.fallbackLocale,
  });

  final String platform;
  final String? appVersion;
  final String? osVersion;
  final String? fallbackLocale;
}
