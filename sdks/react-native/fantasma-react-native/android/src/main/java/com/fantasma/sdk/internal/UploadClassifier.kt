package com.fantasma.sdk.internal

import com.fantasma.sdk.FantasmaException
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

internal object UploadClassifier {
    private const val ACCEPTED_STATUS_CODE: Int = 202
    private const val UNAUTHORIZED_STATUS_CODE: Int = 401
    private const val VALIDATION_FAILURE_STATUS_CODE: Int = 422
    private const val CONFLICT_STATUS_CODE: Int = 409

    private val json: Json = Json { ignoreUnknownKeys = true }

    @Suppress("ReturnCount")
    fun classify(
        statusCode: Int,
        responseBody: ByteArray,
        batchCount: Int,
    ): UploadDisposition {
        if (statusCode == ACCEPTED_STATUS_CODE) {
            val accepted =
                try {
                    json.decodeFromString<AcceptedEnvelope>(responseBody.decodeToString())
                } catch (_: Exception) {
                    throw FantasmaException.InvalidResponse
                }

            if (accepted.accepted != batchCount) {
                throw FantasmaException.InvalidResponse
            }

            return UploadDisposition.Success
        }

        if (statusCode == UNAUTHORIZED_STATUS_CODE || statusCode == VALIDATION_FAILURE_STATUS_CODE) {
            return UploadDisposition.BlockedDestination
        }

        if (statusCode == CONFLICT_STATUS_CODE) {
            val conflict =
                runCatching {
                    json.decodeFromString<ErrorEnvelope>(responseBody.decodeToString())
                }.getOrNull()

            if (conflict?.error == "project_pending_deletion") {
                return UploadDisposition.BlockedDestination
            }
        }

        return UploadDisposition.RetryableFailure
    }

    @Serializable
    private data class AcceptedEnvelope(
        val accepted: Int,
    )

    @Serializable
    private data class ErrorEnvelope(
        @SerialName("error")
        val error: String,
    )
}
