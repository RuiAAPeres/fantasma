package com.fantasma.sdk

internal interface FantasmaClientDelegate {
    suspend fun track(
        eventName: String,
        properties: Map<String, String>?,
    ): Unit

    suspend fun flush(): Unit

    suspend fun clear(): Unit

    fun close(): Unit
}
