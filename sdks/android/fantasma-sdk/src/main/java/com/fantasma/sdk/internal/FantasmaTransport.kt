package com.fantasma.sdk.internal

internal interface FantasmaTransport {
    suspend fun send(request: FantasmaRequest): FantasmaResponse
}

internal data class FantasmaRequest(
    val url: String,
    val writeKey: String,
    val body: ByteArray,
)

internal data class FantasmaResponse(
    val statusCode: Int,
    val body: ByteArray,
)
