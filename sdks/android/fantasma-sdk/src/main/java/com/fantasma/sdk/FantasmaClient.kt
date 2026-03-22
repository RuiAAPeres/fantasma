package com.fantasma.sdk

import android.content.Context
import com.fantasma.sdk.internal.FantasmaRuntimeRegistry
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Instance-first Android Fantasma client.
 */
public class FantasmaClient internal constructor(
    private val delegate: FantasmaClientDelegate,
) {
    private val closed: AtomicBoolean = AtomicBoolean(false)

    public constructor(
        context: Context,
        config: FantasmaConfig,
    ) : this(FantasmaRuntimeRegistry.acquire(context.applicationContext, config))

    /**
     * Persist one event and schedule upload if needed.
     */
    public suspend fun track(eventName: String) {
        ensureOpen()
        delegate.track(eventName)
    }

    /**
     * Attempt to upload the current durable queue.
     */
    public suspend fun flush() {
        ensureOpen()
        delegate.flush()
    }

    /**
     * Rotate the install identity for future queued events.
     */
    public suspend fun clear() {
        ensureOpen()
        delegate.clear()
    }

    /**
     * Release this client handle and stop using it.
     */
    public fun close() {
        if (!closed.compareAndSet(false, true)) {
            return
        }
        delegate.close()
    }

    private fun ensureOpen() {
        if (closed.get()) {
            throw FantasmaException.ClosedClient
        }
    }
}
