package com.fantasma.sdk.internal

import com.fantasma.sdk.FantasmaException
import java.net.URI

internal data class NormalizedDestination(
    val serverUrl: String,
    val writeKey: String,
) {
    val signature: String = "$serverUrl|$writeKey"

    companion object {
        @Suppress("ThrowsCount")
        fun from(
            serverUrl: String,
            writeKey: String,
        ): NormalizedDestination {
            val normalizedWriteKey = writeKey.trim()
            if (normalizedWriteKey.isEmpty()) {
                throw FantasmaException.InvalidWriteKey
            }

            val uri =
                try {
                    URI(serverUrl)
                } catch (_: IllegalArgumentException) {
                    throw FantasmaException.UnsupportedServerUrl
                }

            val normalizedScheme = uri.scheme?.lowercase()
            if (normalizedScheme != "http" && normalizedScheme != "https") {
                throw FantasmaException.UnsupportedServerUrl
            }

            val normalizedHost = uri.host?.lowercase() ?: throw FantasmaException.UnsupportedServerUrl
            val normalizedPath =
                uri.rawPath
                    ?.trimEnd('/')
                    ?.takeIf { it.isNotEmpty() && it != "/" }
                    ?: ""
            val normalizedPort = if (uri.port >= 0) ":${uri.port}" else ""
            val normalizedQuery = uri.rawQuery?.let { "?$it" } ?: ""

            return NormalizedDestination(
                serverUrl = "$normalizedScheme://$normalizedHost$normalizedPort$normalizedPath$normalizedQuery",
                writeKey = normalizedWriteKey,
            )
        }
    }
}
