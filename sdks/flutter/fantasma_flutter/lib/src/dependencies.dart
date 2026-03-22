import 'dart:async';

import 'lifecycle.dart';
import 'metadata.dart';
import 'storage.dart';
import 'transport.dart';

typedef FantasmaClock = DateTime Function();

final class FantasmaDependencies {
  const FantasmaDependencies({
    required this.openStore,
    required this.metadataProvider,
    required this.transport,
    required this.lifecycle,
    required this.clock,
    required this.newInstallId,
    required this.periodicFlushInterval,
    required this.uploadBatchSize,
  });

  final Future<FantasmaStore> Function(String storageNamespace) openStore;
  final FantasmaMetadataProvider metadataProvider;
  final FantasmaTransport transport;
  final FantasmaLifecycleCallbacks lifecycle;
  final FantasmaClock clock;
  final String Function() newInstallId;
  final Duration periodicFlushInterval;
  final int uploadBatchSize;

  factory FantasmaDependencies.live() {
    return FantasmaDependencies(
      openStore: (String storageNamespace) {
        return SqfliteFantasmaStore.open(storageNamespace: storageNamespace);
      },
      metadataProvider: const LiveMetadataProvider(),
      transport: const HttpFantasmaTransport(),
      lifecycle: FlutterLifecycleCallbacks(),
      clock: DateTime.now,
      newInstallId: generateInstallId,
      periodicFlushInterval: const Duration(seconds: 30),
      uploadBatchSize: 100,
    );
  }

  factory FantasmaDependencies.test({
    required FantasmaStore store,
    required FantasmaMetadataProvider metadataProvider,
    required FantasmaTransport transport,
    required FantasmaLifecycleCallbacks lifecycle,
    required FantasmaClock clock,
    required Duration periodicFlushInterval,
    int uploadBatchSize = 100,
    String Function()? newInstallId,
  }) {
    return FantasmaDependencies(
      openStore: (_) async => store,
      metadataProvider: metadataProvider,
      transport: transport,
      lifecycle: lifecycle,
      clock: clock,
      newInstallId: newInstallId ?? generateInstallId,
      periodicFlushInterval: periodicFlushInterval,
      uploadBatchSize: uploadBatchSize,
    );
  }
}
