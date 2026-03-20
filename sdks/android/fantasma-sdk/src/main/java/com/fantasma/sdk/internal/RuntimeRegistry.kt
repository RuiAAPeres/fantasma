package com.fantasma.sdk.internal

internal class RuntimeRegistry<T> {
    private val runtimes: MutableMap<String, RuntimeEntry<T>> = linkedMapOf()

    @Synchronized
    fun acquire(
        signature: String,
        factory: () -> T,
    ): RuntimeLease<T> {
        val existing = runtimes[signature]
        if (existing != null) {
            existing.handleCount += 1
            return RuntimeLease(existing.runtime, existing.handleCount)
        }

        val runtime = factory()
        runtimes[signature] = RuntimeEntry(runtime = runtime, handleCount = 1)
        return RuntimeLease(runtime, 1)
    }

    @Synchronized
    @Suppress("ReturnCount")
    fun release(
        signature: String,
        runtime: T,
    ): Int? {
        val existing = runtimes[signature] ?: return null
        if (existing.runtime !== runtime) {
            return null
        }
        existing.handleCount -= 1
        if (existing.handleCount <= 0) {
            runtimes.remove(signature)
            return 0
        }
        return existing.handleCount
    }

    @Synchronized
    fun evict(
        signature: String,
        runtime: T,
    ): T? {
        val existing = runtimes[signature]
        return if (existing != null && existing.runtime === runtime) {
            runtimes.remove(signature)
            existing.runtime
        } else {
            null
        }
    }

    @Synchronized
    fun get(signature: String): T? = runtimes[signature]?.runtime

    private data class RuntimeEntry<T>(
        val runtime: T,
        var handleCount: Int,
    )
}

internal data class RuntimeLease<T>(
    val runtime: T,
    val handleCount: Int,
)
