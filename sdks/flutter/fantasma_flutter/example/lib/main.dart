import 'dart:async';

import 'package:fantasma_flutter/fantasma_flutter.dart';
import 'package:flutter/material.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const DemoApp());
}

final class DemoApp extends StatefulWidget {
  const DemoApp({super.key});

  @override
  State<DemoApp> createState() => _DemoAppState();
}

final class _DemoAppState extends State<DemoApp> {
  late final FantasmaClient _client;

  @override
  void initState() {
    super.initState();
    _client = FantasmaClient(
      FantasmaConfig(
        serverUrl: 'http://10.0.2.2:8081',
        writeKey: '<ingest-key>',
        storageNamespace: 'demo',
        localeProvider: () => WidgetsBinding.instance.platformDispatcher.locale,
      ),
    );
  }

  @override
  void dispose() {
    unawaited(_client.close());
    super.dispose();
  }

  Future<void> _track() async {
    await _client.track('button_pressed');
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('Fantasma Flutter Demo')),
        body: Center(
          child: FilledButton(
            onPressed: _track,
            child: const Text('Track Event'),
          ),
        ),
      ),
    );
  }
}
