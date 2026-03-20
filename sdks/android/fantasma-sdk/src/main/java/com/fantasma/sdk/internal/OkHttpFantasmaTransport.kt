package com.fantasma.sdk.internal

import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

internal class OkHttpFantasmaTransport(
    private val client: OkHttpClient = OkHttpClient(),
) : FantasmaTransport {
    override suspend fun send(request: FantasmaRequest): FantasmaResponse =
        suspendCancellableCoroutine { continuation ->
            val call =
                client.newCall(
                    Request.Builder()
                        .url(request.url)
                        .header("Content-Type", "application/json")
                        .header("X-Fantasma-Key", request.writeKey)
                        .post(request.body.toRequestBody("application/json".toMediaType()))
                        .build(),
                )

            continuation.invokeOnCancellation { call.cancel() }

            call.enqueue(
                object : Callback {
                    override fun onFailure(
                        call: Call,
                        e: IOException,
                    ) {
                        if (continuation.isCancelled) {
                            return
                        }
                        continuation.resumeWithException(e)
                    }

                    override fun onResponse(
                        call: Call,
                        response: Response,
                    ) {
                        response.use {
                            continuation.resume(
                                FantasmaResponse(
                                    statusCode = it.code,
                                    body = it.body?.bytes() ?: ByteArray(0),
                                ),
                            )
                        }
                    }
                },
            )
        }
}
