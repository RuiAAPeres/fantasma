import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import 'config.dart';
import 'models.dart';

abstract interface class FantasmaTransport {
  Future<TransportResponse> send({
    required FantasmaConfig config,
    required List<QueuedEvent> events,
  });
}

final class HttpFantasmaTransport implements FantasmaTransport {
  const HttpFantasmaTransport({http.Client? client}) : _client = client;

  final http.Client? _client;

  @override
  Future<TransportResponse> send({
    required FantasmaConfig config,
    required List<QueuedEvent> events,
  }) async {
    final client = _client ?? http.Client();
    final uri = Uri.parse('${config.normalizedServerUrl}/v1/events');
    try {
      final response = await client.post(
        uri,
        headers: <String, String>{
          'content-type': 'application/json',
          'x-fantasma-key': config.normalizedWriteKey,
        },
        body: jsonEncode(<String, Object?>{
          'events': events.map((event) => event.toJson()).toList(),
        }),
      );
      return TransportResponse(
        statusCode: response.statusCode,
        body: response.body,
      );
    } catch (error) {
      return TransportResponse.transportFailure(error.toString());
    } finally {
      if (_client == null) {
        client.close();
      }
    }
  }
}

enum UploadDisposition {
  success,
  retryableFailure,
  blockedDestination,
}

final class TransportResponse {
  const TransportResponse({
    required this.statusCode,
    required this.body,
    this.transportError,
  });

  const TransportResponse.accepted({this.body = '{"accepted":1}'})
      : statusCode = 202,
        transportError = null;

  const TransportResponse.blocked({
    required int statusCode,
    this.body = '{"error":"invalid_request"}',
  })  : statusCode = statusCode,
        transportError = null;

  const TransportResponse.transportFailure(String message)
      : statusCode = null,
        body = null,
        transportError = message;

  final int? statusCode;
  final String? body;
  final String? transportError;

  UploadDisposition classify({required int batchCount}) {
    if (transportError != null) {
      return UploadDisposition.retryableFailure;
    }
    if (statusCode == 202 && _hasAcceptedCount(body, batchCount)) {
      return UploadDisposition.success;
    }
    if (statusCode == 401 || statusCode == 422) {
      return UploadDisposition.blockedDestination;
    }
    if (statusCode == 409 && _isPendingDeletion(body)) {
      return UploadDisposition.blockedDestination;
    }
    return UploadDisposition.retryableFailure;
  }

  bool _hasAcceptedCount(String? rawBody, int batchCount) {
    if (rawBody == null) {
      return false;
    }
    try {
      final decoded = jsonDecode(rawBody);
      return decoded is Map<String, Object?> &&
          decoded['accepted'] is int &&
          decoded['accepted'] == batchCount;
    } catch (_) {
      return false;
    }
  }

  bool _isPendingDeletion(String? rawBody) {
    if (rawBody == null) {
      return false;
    }
    try {
      final decoded = jsonDecode(rawBody);
      return decoded is Map<String, Object?> &&
          decoded['error'] == 'project_pending_deletion';
    } catch (_) {
      return false;
    }
  }
}

final class RecordingTransport implements FantasmaTransport {
  RecordingTransport({
    List<TransportResponse>? responses,
    this.onSend,
  }) : _responses = responses ?? <TransportResponse>[];

  final List<TransportResponse> _responses;
  final Future<void> Function()? onSend;
  final List<List<QueuedEvent>> recordedBatches = <List<QueuedEvent>>[];

  @override
  Future<TransportResponse> send({
    required FantasmaConfig config,
    required List<QueuedEvent> events,
  }) async {
    recordedBatches.add(List<QueuedEvent>.from(events));
    if (onSend != null) {
      await onSend!();
    }
    if (_responses.isEmpty) {
      return const TransportResponse.accepted();
    }
    return _responses.removeAt(0);
  }
}
