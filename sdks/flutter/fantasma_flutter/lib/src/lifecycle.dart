import 'dart:async';

import 'package:flutter/widgets.dart';

abstract interface class FantasmaLifecycleCallbacks {
  Future<void> attach(Future<void> Function() onPause);
  Future<void> detach();
}

final class FlutterLifecycleCallbacks
    with WidgetsBindingObserver
    implements FantasmaLifecycleCallbacks {
  Future<void> Function()? _onPause;

  @override
  Future<void> attach(Future<void> Function() onPause) async {
    WidgetsFlutterBinding.ensureInitialized();
    _onPause = onPause;
    WidgetsBinding.instance.addObserver(this);
  }

  @override
  Future<void> detach() async {
    WidgetsBinding.instance.removeObserver(this);
    _onPause = null;
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (_onPause == null) {
      return;
    }
    if (state == AppLifecycleState.inactive ||
        state == AppLifecycleState.paused ||
        state == AppLifecycleState.detached) {
      unawaited(_onPause!());
    }
  }
}

final class ManualLifecycleCallbacks implements FantasmaLifecycleCallbacks {
  Future<void> Function()? _onPause;

  @override
  Future<void> attach(Future<void> Function() onPause) async {
    _onPause = onPause;
  }

  @override
  Future<void> detach() async {
    _onPause = null;
  }

  Future<void> emitPaused() async {
    final callback = _onPause;
    if (callback != null) {
      await callback();
    }
  }
}
