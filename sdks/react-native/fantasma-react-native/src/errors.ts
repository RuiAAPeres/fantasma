export type FantasmaErrorCode =
  | "invalid_write_key"
  | "unsupported_server_url"
  | "invalid_event_name"
  | "encoding_failed"
  | "invalid_response"
  | "upload_failed"
  | "storage_failure"
  | "closed_client"
  | "client_already_active";

export class FantasmaError extends Error {
  readonly code: FantasmaErrorCode;
  readonly nativeMessage?: string;

  constructor(
    code: FantasmaErrorCode,
    message: string,
    nativeMessage?: string,
  ) {
    super(message);
    this.name = "FantasmaError";
    this.code = code;
    this.nativeMessage = nativeMessage;
  }
}

export class FantasmaInvalidWriteKeyError extends FantasmaError {
  constructor() {
    super(
      "invalid_write_key",
      "Fantasma write key must not be blank.",
    );
    this.name = "FantasmaInvalidWriteKeyError";
  }
}

export class FantasmaUnsupportedServerUrlError extends FantasmaError {
  constructor() {
    super(
      "unsupported_server_url",
      "Fantasma server URL must use http or https.",
    );
    this.name = "FantasmaUnsupportedServerUrlError";
  }
}

export class FantasmaInvalidEventNameError extends FantasmaError {
  constructor() {
    super(
      "invalid_event_name",
      "Fantasma event name must not be blank.",
    );
    this.name = "FantasmaInvalidEventNameError";
  }
}

export class FantasmaEncodingFailedError extends FantasmaError {
  constructor(nativeMessage?: string) {
    super(
      "encoding_failed",
      "Fantasma could not encode the event payload.",
      nativeMessage,
    );
    this.name = "FantasmaEncodingFailedError";
  }
}

export class FantasmaInvalidResponseError extends FantasmaError {
  constructor(nativeMessage?: string) {
    super(
      "invalid_response",
      "Fantasma received an invalid ingest response.",
      nativeMessage,
    );
    this.name = "FantasmaInvalidResponseError";
  }
}

export class FantasmaUploadFailedError extends FantasmaError {
  constructor(nativeMessage?: string) {
    super(
      "upload_failed",
      "Fantasma could not upload the current event batch.",
      nativeMessage,
    );
    this.name = "FantasmaUploadFailedError";
  }
}

export class FantasmaStorageFailureError extends FantasmaError {
  constructor(nativeMessage?: string) {
    super(
      "storage_failure",
      nativeMessage ?? "Fantasma storage failed.",
      nativeMessage,
    );
    this.name = "FantasmaStorageFailureError";
  }
}

export class FantasmaClosedClientError extends FantasmaError {
  constructor() {
    super("closed_client", "Fantasma client has already been closed.");
    this.name = "FantasmaClosedClientError";
  }
}

export class FantasmaClientAlreadyActiveError extends FantasmaError {
  constructor() {
    super(
      "client_already_active",
      "Fantasma only allows one live React Native client per process.",
    );
    this.name = "FantasmaClientAlreadyActiveError";
  }
}

export function mapFantasmaError(error: unknown): FantasmaError {
  if (error instanceof FantasmaError) {
    return error;
  }

  const value =
    typeof error === "object" && error !== null
      ? (error as Record<string, unknown>)
      : undefined;
  const code = typeof value?.code === "string" ? value.code : undefined;
  const message = typeof value?.message === "string" ? value.message : undefined;

  switch (code) {
    case "invalid_write_key":
      return new FantasmaInvalidWriteKeyError();
    case "unsupported_server_url":
      return new FantasmaUnsupportedServerUrlError();
    case "invalid_event_name":
      return new FantasmaInvalidEventNameError();
    case "encoding_failed":
      return new FantasmaEncodingFailedError(message);
    case "invalid_response":
      return new FantasmaInvalidResponseError(message);
    case "upload_failed":
      return new FantasmaUploadFailedError(message);
    case "storage_failure":
      return new FantasmaStorageFailureError(message);
    case "closed_client":
      return new FantasmaClosedClientError();
    case "client_already_active":
      return new FantasmaClientAlreadyActiveError();
    default:
      return new FantasmaStorageFailureError(message);
  }
}
