import 'dart:async';

import 'package:flutter/foundation.dart';

import 'config.dart';
import 'dependencies.dart';
import 'errors.dart';
import 'lifecycle.dart';
import 'metadata.dart';
import 'models.dart';
import 'storage.dart';
import 'transport.dart';

final class FantasmaClient {
  static final Set<String> _liveNamespaces = <String>{};

  FantasmaClient(this.config) : _dependencies = FantasmaDependencies.live();

  @visibleForTesting
  FantasmaClient.test({
    required this.config,
    required FantasmaDependencies dependencies,
  }) : _dependencies = dependencies;

  final FantasmaConfig config;
  final FantasmaDependencies _dependencies;

  _FantasmaRuntimeState? _state;
  Future<void> _tail = Future<void>.value();
  bool _closed = false;

  Future<void> track(String eventName) {
    return _enqueue(() async {
      _ensureOpen();
      final state = await _ensureState();
      final normalizedName = eventName.trim();
      if (normalizedName.isEmpty) {
        throw const FantasmaInvalidEventNameException();
      }

      final localeTag = await _resolveLocale(state.metadata);
      final installId = await _currentInstallId(state.store);
      final event = QueuedEvent(
        event: normalizedName,
        timestamp: _timestamp(_dependencies.clock().toUtc()),
        installId: installId,
        platform: state.metadata.platform,
        device: state.metadata.device,
        appVersion: state.metadata.appVersion,
        osVersion: state.metadata.osVersion,
        locale: localeTag,
      );

      await state.store.enqueue(event);

      if (await state.store.countPending() >= _dependencies.uploadBatchSize) {
        unawaited(_scheduleBestEffortFlush());
      }
    });
  }

  Future<void> flush() {
    return _enqueue(() async {
      _ensureOpen();
      final state = await _ensureState();
      await _flushInternal(state, bestEffort: false);
    });
  }

  Future<void> clear() {
    return _enqueue(() async {
      _ensureOpen();
      final state = await _ensureState();
      await state.store.clearInstallId();
      await state.store.saveInstallId(_dependencies.newInstallId());
    });
  }

  Future<void> close() {
    return _enqueue(() async {
      if (_closed) {
        return;
      }
      _closed = true;
      final state = _state;
      _state = null;
      state?.periodicFlushTimer?.cancel();
      if (state != null) {
        Object? firstError;
        StackTrace? firstStackTrace;
        try {
          await state.lifecycle.detach();
        } catch (error, stackTrace) {
          firstError = error;
          firstStackTrace = stackTrace;
        }
        try {
          await state.store.close();
        } catch (error, stackTrace) {
          firstError ??= error;
          firstStackTrace ??= stackTrace;
        } finally {
          _releaseNamespace(config.storageNamespace);
        }
        if (firstError != null) {
          Error.throwWithStackTrace(firstError, firstStackTrace!);
        }
      }
    });
  }

  Future<void> _scheduleBestEffortFlush() {
    return _enqueue(() async {
      if (_closed) {
        return;
      }
      final state = await _ensureState();
      try {
        await _flushInternal(state, bestEffort: true);
      } catch (_) {
        return;
      }
    });
  }

  Future<void> _flushInternal(
    _FantasmaRuntimeState state, {
    required bool bestEffort,
  }) async {
    final blockedSignature = await state.store.loadBlockedDestinationSignature();
    if (blockedSignature == config.destinationSignature) {
      throw const FantasmaUploadException(
        'Destination is blocked for this client storage namespace.',
      );
    }

    while (true) {
      final batch = await state.store.peekBatch(_dependencies.uploadBatchSize);
      if (batch.isEmpty) {
        return;
      }
      final response = await _dependencies.transport.send(
        config: config,
        events: batch,
      );
      switch (response.classify(batchCount: batch.length)) {
        case UploadDisposition.success:
          await state.store.deleteByIds(
            batch.map((event) => event.id!).toList(growable: false),
          );
        case UploadDisposition.blockedDestination:
          await state.store.saveBlockedDestinationSignature(
            config.destinationSignature,
          );
          throw const FantasmaUploadException(
            'Fantasma rejected the destination as permanently blocked.',
          );
        case UploadDisposition.retryableFailure:
          if (bestEffort) {
            return;
          }
          throw const FantasmaUploadException(
            'Fantasma upload failed and the batch remains queued.',
          );
      }
    }
  }

  Future<String> _currentInstallId(FantasmaStore store) async {
    final existing = await store.loadInstallId();
    if (existing != null && existing.isNotEmpty) {
      return existing;
    }
    final created = _dependencies.newInstallId();
    await store.saveInstallId(created);
    return created;
  }

  Future<String?> _resolveLocale(MetadataSnapshot metadata) async {
    final provided = await config.localeProvider?.call();
    final locale = localeToLanguageTag(provided);
    if (locale != null && locale.isNotEmpty) {
      return locale;
    }
    return metadata.fallbackLocale;
  }

  Future<_FantasmaRuntimeState> _ensureState() async {
    final existing = _state;
    if (existing != null) {
      return existing;
    }

    _reserveNamespace(config.storageNamespace);
    try {
      final store = await _dependencies.openStore(config.storageNamespace);
      final metadata = await _dependencies.metadataProvider.load();
      final lifecycle = _dependencies.lifecycle;
      final timer = _dependencies.periodicFlushInterval == Duration.zero
          ? null
          : Timer.periodic(
              _dependencies.periodicFlushInterval,
              (_) => unawaited(_scheduleBestEffortFlush()),
            );
      await lifecycle.attach(_scheduleBestEffortFlush);
      final state = _FantasmaRuntimeState(
        store: store,
        metadata: metadata,
        lifecycle: lifecycle,
        periodicFlushTimer: timer,
      );
      _state = state;
      return state;
    } catch (_) {
      _releaseNamespace(config.storageNamespace);
      rethrow;
    }
  }

  Future<void> _enqueue(Future<void> Function() action) {
    final completer = Completer<void>();
    final run = _tail.catchError((_) {}).then((_) => action()).then(
          (_) => completer.complete(),
          onError: (Object error, StackTrace stackTrace) {
            completer.completeError(error, stackTrace);
          },
        );
    _tail = run.catchError((_) {});
    return completer.future;
  }

  void _ensureOpen() {
    if (_closed) {
      throw const FantasmaClosedClientException();
    }
  }

  void _reserveNamespace(String namespace) {
    if (!_liveNamespaces.add(namespace)) {
      throw const FantasmaStorageNamespaceInUseException();
    }
  }

  void _releaseNamespace(String namespace) {
    _liveNamespaces.remove(namespace);
  }

  String _timestamp(DateTime value) => value.toIso8601String();
}

final class _FantasmaRuntimeState {
  const _FantasmaRuntimeState({
    required this.store,
    required this.metadata,
    required this.lifecycle,
    required this.periodicFlushTimer,
  });

  final FantasmaStore store;
  final MetadataSnapshot metadata;
  final FantasmaLifecycleCallbacks lifecycle;
  final Timer? periodicFlushTimer;
}
