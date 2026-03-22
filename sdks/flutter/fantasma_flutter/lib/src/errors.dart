sealed class FantasmaException implements Exception {
  const FantasmaException(this.message);

  final String message;

  @override
  String toString() => 'FantasmaException($message)';
}

final class FantasmaConfigException extends FantasmaException {
  const FantasmaConfigException(super.message);
}

final class FantasmaClosedClientException extends FantasmaException {
  const FantasmaClosedClientException()
      : super('FantasmaClient has already been closed.');
}

final class FantasmaInvalidEventNameException extends FantasmaException {
  const FantasmaInvalidEventNameException()
      : super('Event name must not be blank.');
}

final class FantasmaUploadException extends FantasmaException {
  const FantasmaUploadException(super.message);
}

final class FantasmaStorageException extends FantasmaException {
  const FantasmaStorageException(super.message);
}

final class FantasmaStorageNamespaceInUseException extends FantasmaException {
  const FantasmaStorageNamespaceInUseException()
      : super('Another live FantasmaClient is already using this storage namespace.');
}

final class FantasmaPlatformException extends FantasmaException {
  const FantasmaPlatformException(super.message);
}
