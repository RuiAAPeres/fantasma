package com.fantasma.sdk

/**
 * Typed Fantasma SDK failures.
 */
public sealed class FantasmaException(
    message: String? = null,
    cause: Throwable? = null,
) : Exception(message, cause) {
    public data object InvalidWriteKey : FantasmaException("Fantasma write key must not be blank.")

    public data object UnsupportedServerUrl : FantasmaException(
        "Fantasma server URL must use http or https.",
    )

    public data object InvalidEventName : FantasmaException("Fantasma event name must not be blank.")

    public data object EncodingFailed : FantasmaException("Fantasma could not encode the event payload.")

    public data object InvalidResponse : FantasmaException("Fantasma received an invalid ingest response.")

    public data object UploadFailed : FantasmaException("Fantasma could not upload the current event batch.")

    public data class StorageFailure(
        val detail: String,
    ) : FantasmaException(detail)

    public data object ClosedClient : FantasmaException("Fantasma client has already been closed.")
}
