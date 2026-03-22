import 'dart:async';
import 'dart:ui';

import 'package:flutter_test/flutter_test.dart';
import 'package:fantasma_flutter/fantasma_flutter.dart';

void main() {
  group('FantasmaConfig', () {
    test('normalizes destination and preserves explicit storage namespace', () {
      final config = FantasmaConfig(
        serverUrl: 'HTTPS://Example.com///',
        writeKey: '  write-key  ',
        storageNamespace: 'primary',
      );

      expect(config.normalizedServerUrl, 'https://example.com');
      expect(config.normalizedWriteKey, 'write-key');
      expect(config.storageNamespace, 'primary');
      expect(config.destinationSignature, 'https://example.com|write-key');
    });

    test('rejects blank storage namespace', () {
      expect(
        () => FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: '   ',
        ),
        throwsA(isA<FantasmaConfigException>()),
      );
    });
  });

  group('FantasmaClient', () {
    test('snapshots locale when track enqueues an event', () async {
      final store = InMemoryFantasmaStore();
      Locale locale = const Locale('en', 'GB');
      final client = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'locale-snapshot',
          localeProvider: () => locale,
        ),
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.2.3',
            osVersion: '16',
            fallbackLocale: Locale('pt', 'PT'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await client.track('app_open');
      locale = const Locale('fr', 'FR');
      await client.track('settings_open');

      expect(
        store.pendingEvents.map((event) => event.locale).toList(),
        <String?>['en-GB', 'fr-FR'],
      );
    });

    test('persists before explicit flush and clears uploaded rows on 202', () async {
      final store = InMemoryFantasmaStore();
      final transport = RecordingTransport(
        responses: <TransportResponse>[const TransportResponse.accepted()],
      );
      final client = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'flush-success',
        ),
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'ios',
            appVersion: '2.0.0',
            osVersion: '18.0',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: transport,
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await client.track('app_open');

      expect(store.pendingEvents, hasLength(1));
      expect(transport.recordedBatches, isEmpty);

      await client.flush();

      expect(transport.recordedBatches, hasLength(1));
      expect(store.pendingEvents, isEmpty);
    });

    test('threshold-triggered flush does not make track surface upload errors', () async {
      final store = InMemoryFantasmaStore();
      final transport = RecordingTransport(
        responses: <TransportResponse>[
          const TransportResponse.blocked(statusCode: 401),
        ],
      );
      final client = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'threshold-best-effort',
        ),
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'ios',
            appVersion: '2.0.0',
            osVersion: '18.0',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: transport,
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
          uploadBatchSize: 1,
        ),
      );

      await client.track('app_open');
      await Future<void>.delayed(Duration.zero);

      expect(transport.recordedBatches, hasLength(1));
      expect(store.pendingEvents, hasLength(1));
      expect(store.blockedDestinationSignature, client.config.destinationSignature);
    });

    test('malformed 202 with mismatched accepted count preserves queued rows', () async {
      final store = InMemoryFantasmaStore();
      final transport = RecordingTransport(
        responses: <TransportResponse>[
          const TransportResponse(statusCode: 202, body: '{"accepted":1}'),
        ],
      );
      final client = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'accepted-count-mismatch',
        ),
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: transport,
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await client.track('app_open');
      await client.track('settings_open');

      await expectLater(client.flush(), throwsA(isA<FantasmaUploadException>()));

      expect(transport.recordedBatches, hasLength(1));
      expect(transport.recordedBatches.single, hasLength(2));
      expect(store.pendingEvents, hasLength(2));
    });

    test('marks blocked destination and keeps it blocked across restart', () async {
      final store = InMemoryFantasmaStore();
      final blockedTransport = RecordingTransport(
        responses: <TransportResponse>[
          const TransportResponse.blocked(statusCode: 401),
        ],
      );
      final config = FantasmaConfig(
        serverUrl: 'https://example.com',
        writeKey: 'write-key',
        storageNamespace: 'blocked-state',
      );

      final firstClient = FantasmaClient.test(
        config: config,
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: blockedTransport,
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await firstClient.track('app_open');
      await expectLater(firstClient.flush(), throwsA(isA<FantasmaUploadException>()));
      await firstClient.close();

      expect(store.blockedDestinationSignature, config.destinationSignature);

      final secondClient = FantasmaClient.test(
        config: config,
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:01:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await expectLater(secondClient.flush(), throwsA(isA<FantasmaUploadException>()));
    });

    test('keeps same-destination clients isolated by storage namespace', () async {
      final firstStore = InMemoryFantasmaStore();
      final secondStore = InMemoryFantasmaStore();

      final firstClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'first',
        ),
        dependencies: FantasmaDependencies.test(
          store: firstStore,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );
      final secondClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'second',
        ),
        dependencies: FantasmaDependencies.test(
          store: secondStore,
          clock: () => DateTime.parse('2026-03-22T12:00:01Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await firstClient.track('app_open');
      await secondClient.track('settings_open');
      await firstClient.clear();

      expect(firstStore.pendingEvents.single.event, 'app_open');
      expect(secondStore.pendingEvents.single.event, 'settings_open');
      expect(firstStore.installId, isNot(equals(secondStore.installId)));
    });

    test('coalesces overlapping flush calls onto one upload cycle', () async {
      final store = InMemoryFantasmaStore();
      final gate = Completer<void>();
      final transport = RecordingTransport(
        onSend: () async {
          await gate.future;
        },
        responses: <TransportResponse>[const TransportResponse.accepted()],
      );
      final client = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'flush-coalesce',
        ),
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'ios',
            appVersion: '1.0.0',
            osVersion: '18.0',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: transport,
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await client.track('app_open');

      final firstFlush = client.flush();
      final secondFlush = client.flush();
      gate.complete();
      await Future.wait(<Future<void>>[firstFlush, secondFlush]);

      expect(transport.recordedBatches, hasLength(1));
      expect(store.pendingEvents, isEmpty);
    });

    test('lifecycle pause schedules a best-effort flush attempt', () async {
      final store = InMemoryFantasmaStore();
      final lifecycle = ManualLifecycleCallbacks();
      final transport = RecordingTransport(
        responses: <TransportResponse>[const TransportResponse.accepted()],
      );
      final client = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'lifecycle',
        ),
        dependencies: FantasmaDependencies.test(
          store: store,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: transport,
          lifecycle: lifecycle,
          periodicFlushInterval: Duration.zero,
        ),
      );

      await client.track('app_open');
      await lifecycle.emitPaused();

      expect(transport.recordedBatches, hasLength(1));
      expect(store.pendingEvents, isEmpty);
    });

    test('rejects a second live client for the same storage namespace', () async {
      final sharedStore = InMemoryFantasmaStore();
      final openStoreCalls = <String>[];
      Future<FantasmaStore> openStore(String namespace) async {
        openStoreCalls.add(namespace);
        return sharedStore;
      }

      final firstClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'shared',
        ),
        dependencies: FantasmaDependencies(
          openStore: openStore,
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          newInstallId: () => 'install-a',
          periodicFlushInterval: Duration.zero,
          uploadBatchSize: 100,
        ),
      );
      final secondClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'shared',
        ),
        dependencies: FantasmaDependencies(
          openStore: openStore,
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          clock: () => DateTime.parse('2026-03-22T12:00:01Z'),
          newInstallId: () => 'install-b',
          periodicFlushInterval: Duration.zero,
          uploadBatchSize: 100,
        ),
      );

      await firstClient.track('app_open');
      await expectLater(
        secondClient.track('settings_open'),
        throwsA(isA<FantasmaStorageNamespaceInUseException>()),
      );
      await firstClient.close();

      expect(openStoreCalls, <String>['shared']);
    });

    test('close releases the storage namespace for reuse', () async {
      final firstStore = InMemoryFantasmaStore();
      final secondStore = InMemoryFantasmaStore();
      final firstClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'reusable',
        ),
        dependencies: FantasmaDependencies.test(
          store: firstStore,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await firstClient.track('app_open');
      await firstClient.close();

      final secondClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'reusable',
        ),
        dependencies: FantasmaDependencies.test(
          store: secondStore,
          clock: () => DateTime.parse('2026-03-22T12:00:01Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await secondClient.track('settings_open');

      expect(secondStore.pendingEvents.single.event, 'settings_open');
    });

    test('close still closes the store when lifecycle detach fails', () async {
      final trackingStore = _TrackingStore();
      final firstClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'teardown-failure',
        ),
        dependencies: FantasmaDependencies.test(
          store: trackingStore,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: _FailingLifecycleCallbacks(onDetach: 'detach failed'),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await firstClient.track('app_open');
      await expectLater(firstClient.close(), throwsA(isA<StateError>()));
      expect(trackingStore.closeCallCount, 1);

      final secondClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'teardown-failure',
        ),
        dependencies: FantasmaDependencies.test(
          store: InMemoryFantasmaStore(),
          clock: () => DateTime.parse('2026-03-22T12:00:01Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await secondClient.track('settings_open');
    });

    test('close releases the storage namespace even when store close fails', () async {
      final failingStore = _FailingStore(onClose: 'store close failed');
      final firstClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'store-close-failure',
        ),
        dependencies: FantasmaDependencies.test(
          store: failingStore,
          clock: () => DateTime.parse('2026-03-22T12:00:00Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await firstClient.track('app_open');
      await expectLater(firstClient.close(), throwsA(isA<StateError>()));
      expect(failingStore.closeCallCount, 1);

      final secondClient = FantasmaClient.test(
        config: FantasmaConfig(
          serverUrl: 'https://example.com',
          writeKey: 'write-key',
          storageNamespace: 'store-close-failure',
        ),
        dependencies: FantasmaDependencies.test(
          store: InMemoryFantasmaStore(),
          clock: () => DateTime.parse('2026-03-22T12:00:01Z'),
          metadataProvider: StaticMetadataProvider(
            platform: 'android',
            appVersion: '1.0.0',
            osVersion: '16',
            fallbackLocale: Locale('en', 'US'),
          ),
          transport: RecordingTransport(),
          lifecycle: ManualLifecycleCallbacks(),
          periodicFlushInterval: Duration.zero,
        ),
      );

      await secondClient.track('settings_open');
    });
  });
}

final class _FailingStore implements FantasmaStore {
  _FailingStore({required this.onClose});

  final String onClose;
  final InMemoryFantasmaStore _delegate = InMemoryFantasmaStore();
  int closeCallCount = 0;

  @override
  Future<void> clearInstallId() => _delegate.clearInstallId();

  @override
  Future<int> countPending() => _delegate.countPending();

  @override
  Future<void> deleteByIds(List<int> ids) => _delegate.deleteByIds(ids);

  @override
  Future<void> enqueue(QueuedEvent event) => _delegate.enqueue(event);

  @override
  Future<String?> loadBlockedDestinationSignature() =>
      _delegate.loadBlockedDestinationSignature();

  @override
  Future<String?> loadInstallId() => _delegate.loadInstallId();

  @override
  Future<List<QueuedEvent>> peekBatch(int limit) => _delegate.peekBatch(limit);

  @override
  Future<void> saveBlockedDestinationSignature(String? value) =>
      _delegate.saveBlockedDestinationSignature(value);

  @override
  Future<void> saveInstallId(String value) => _delegate.saveInstallId(value);

  @override
  Future<void> close() async {
    closeCallCount += 1;
    throw StateError(onClose);
  }
}

final class _FailingLifecycleCallbacks implements FantasmaLifecycleCallbacks {
  _FailingLifecycleCallbacks({required this.onDetach});

  final String onDetach;

  @override
  Future<void> attach(Future<void> Function() onPause) async {}

  @override
  Future<void> detach() async {
    throw StateError(onDetach);
  }
}

final class _TrackingStore implements FantasmaStore {
  final InMemoryFantasmaStore _delegate = InMemoryFantasmaStore();
  int closeCallCount = 0;

  @override
  Future<void> clearInstallId() => _delegate.clearInstallId();

  @override
  Future<int> countPending() => _delegate.countPending();

  @override
  Future<void> deleteByIds(List<int> ids) => _delegate.deleteByIds(ids);

  @override
  Future<void> enqueue(QueuedEvent event) => _delegate.enqueue(event);

  @override
  Future<String?> loadBlockedDestinationSignature() =>
      _delegate.loadBlockedDestinationSignature();

  @override
  Future<String?> loadInstallId() => _delegate.loadInstallId();

  @override
  Future<List<QueuedEvent>> peekBatch(int limit) => _delegate.peekBatch(limit);

  @override
  Future<void> saveBlockedDestinationSignature(String? value) =>
      _delegate.saveBlockedDestinationSignature(value);

  @override
  Future<void> saveInstallId(String value) => _delegate.saveInstallId(value);

  @override
  Future<void> close() async {
    closeCallCount += 1;
  }
}
