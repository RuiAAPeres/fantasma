package com.fantasma.sdk.internal

import com.fantasma.sdk.FantasmaClientDelegate

internal class RuntimeClientDelegate(
    private val runtime: FantasmaRuntime,
    private val onClose: () -> Unit,
) : FantasmaClientDelegate {
    override suspend fun track(eventName: String) {
        runtime.track(eventName)
    }

    override suspend fun flush() {
        runtime.flush()
    }

    override suspend fun clear() {
        runtime.clear()
    }

    override fun close() {
        onClose()
    }
}
