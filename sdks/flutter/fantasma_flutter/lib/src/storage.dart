import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:path/path.dart' as path;
import 'package:path_provider/path_provider.dart';
import 'package:sqflite/sqflite.dart';

import 'errors.dart';
import 'models.dart';

abstract interface class FantasmaStore {
  Future<void> enqueue(QueuedEvent event);
  Future<List<QueuedEvent>> peekBatch(int limit);
  Future<void> deleteByIds(List<int> ids);
  Future<int> countPending();
  Future<String?> loadInstallId();
  Future<void> saveInstallId(String value);
  Future<void> clearInstallId();
  Future<String?> loadBlockedDestinationSignature();
  Future<void> saveBlockedDestinationSignature(String? value);
  Future<void> close();
}

final class SqfliteFantasmaStore implements FantasmaStore {
  SqfliteFantasmaStore._(this._database);

  static const _stateInstallIdKey = 'install_id';
  static const _stateBlockedDestinationKey = 'blocked_destination_signature';

  final Database _database;

  static Future<SqfliteFantasmaStore> open({
    required String storageNamespace,
  }) async {
    final directory = await getApplicationSupportDirectory();
    final namespace = _sanitizeNamespace(storageNamespace);
    final containerPath = path.join(directory.path, 'FantasmaFlutter');
    await Directory(containerPath).create(recursive: true);
    final databasePath = path.join(containerPath, '$namespace.sqlite3');
    final database = await openDatabase(
      databasePath,
      version: 1,
      onCreate: (db, _) async {
        await db.execute('''
          CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL
          )
        ''');
        await db.execute('''
          CREATE TABLE state (
            key TEXT PRIMARY KEY,
            value TEXT
          )
        ''');
      },
    );
    return SqfliteFantasmaStore._(database);
  }

  @override
  Future<void> enqueue(QueuedEvent event) async {
    try {
      await _database.insert('events', <String, Object?>{
        'payload': event.toPayload(),
      });
    } on DatabaseException catch (error) {
      throw FantasmaStorageException(error.toString());
    }
  }

  @override
  Future<List<QueuedEvent>> peekBatch(int limit) async {
    try {
      final rows = await _database.query(
        'events',
        columns: <String>['id', 'payload'],
        orderBy: 'id ASC',
        limit: limit,
      );
      return rows.map((row) {
        return QueuedEvent.fromPayload(
          id: row['id']! as int,
          payload: row['payload']! as String,
        );
      }).toList();
    } on DatabaseException catch (error) {
      throw FantasmaStorageException(error.toString());
    }
  }

  @override
  Future<void> deleteByIds(List<int> ids) async {
    if (ids.isEmpty) {
      return;
    }
    try {
      final placeholders = List<String>.filled(ids.length, '?').join(',');
      await _database.delete(
        'events',
        where: 'id IN ($placeholders)',
        whereArgs: ids,
      );
    } on DatabaseException catch (error) {
      throw FantasmaStorageException(error.toString());
    }
  }

  @override
  Future<int> countPending() async {
    try {
      final result = Sqflite.firstIntValue(
        await _database.rawQuery('SELECT COUNT(*) FROM events'),
      );
      return result ?? 0;
    } on DatabaseException catch (error) {
      throw FantasmaStorageException(error.toString());
    }
  }

  @override
  Future<String?> loadInstallId() => _loadState(_stateInstallIdKey);

  @override
  Future<void> saveInstallId(String value) => _saveState(_stateInstallIdKey, value);

  @override
  Future<void> clearInstallId() => _saveState(_stateInstallIdKey, null);

  @override
  Future<String?> loadBlockedDestinationSignature() =>
      _loadState(_stateBlockedDestinationKey);

  @override
  Future<void> saveBlockedDestinationSignature(String? value) =>
      _saveState(_stateBlockedDestinationKey, value);

  Future<String?> _loadState(String key) async {
    try {
      final rows = await _database.query(
        'state',
        columns: <String>['value'],
        where: 'key = ?',
        whereArgs: <Object?>[key],
        limit: 1,
      );
      if (rows.isEmpty) {
        return null;
      }
      return rows.single['value'] as String?;
    } on DatabaseException catch (error) {
      throw FantasmaStorageException(error.toString());
    }
  }

  Future<void> _saveState(String key, String? value) async {
    try {
      await _database.insert(
        'state',
        <String, Object?>{'key': key, 'value': value},
        conflictAlgorithm: ConflictAlgorithm.replace,
      );
    } on DatabaseException catch (error) {
      throw FantasmaStorageException(error.toString());
    }
  }

  @override
  Future<void> close() => _database.close();
}

final class InMemoryFantasmaStore implements FantasmaStore {
  final List<QueuedEvent> _events = <QueuedEvent>[];
  final Map<String, String?> _state = <String, String?>{};
  int _nextId = 1;

  List<QueuedEvent> get pendingEvents => List<QueuedEvent>.unmodifiable(_events);

  String? get installId => _state['install_id'];

  String? get blockedDestinationSignature =>
      _state['blocked_destination_signature'];

  @override
  Future<void> enqueue(QueuedEvent event) async {
    _events.add(event.copyWithId(_nextId));
    _nextId += 1;
  }

  @override
  Future<List<QueuedEvent>> peekBatch(int limit) async {
    return _events.take(limit).toList(growable: false);
  }

  @override
  Future<void> deleteByIds(List<int> ids) async {
    _events.removeWhere((event) => ids.contains(event.id));
  }

  @override
  Future<int> countPending() async => _events.length;

  @override
  Future<String?> loadInstallId() async => _state['install_id'];

  @override
  Future<void> saveInstallId(String value) async {
    _state['install_id'] = value;
  }

  @override
  Future<void> clearInstallId() async {
    _state.remove('install_id');
  }

  @override
  Future<String?> loadBlockedDestinationSignature() async =>
      _state['blocked_destination_signature'];

  @override
  Future<void> saveBlockedDestinationSignature(String? value) async {
    if (value == null) {
      _state.remove('blocked_destination_signature');
      return;
    }
    _state['blocked_destination_signature'] = value;
  }

  @override
  Future<void> close() async {}
}

String generateInstallId() {
  final random = Random.secure();
  final bytes = List<int>.generate(16, (_) => random.nextInt(256));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  final hex = bytes
      .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
      .join();
  return [
    hex.substring(0, 8),
    hex.substring(8, 12),
    hex.substring(12, 16),
    hex.substring(16, 20),
    hex.substring(20, 32),
  ].join('-');
}

String _sanitizeNamespace(String value) {
  final bytes = utf8.encode(value);
  final encoded = base64Url.encode(bytes).replaceAll('=', '');
  return encoded;
}
