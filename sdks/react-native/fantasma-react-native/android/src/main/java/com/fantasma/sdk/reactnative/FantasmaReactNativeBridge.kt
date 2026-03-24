package com.fantasma.sdk.reactnative

import android.content.Context
import com.fantasma.sdk.FantasmaClient
import com.fantasma.sdk.FantasmaConfig
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public interface FantasmaReactNativeClient {
    public suspend fun track(eventName: String)

    public suspend fun flush()

    public suspend fun clear()

    public fun close()
}

private class FantasmaReactNativeClientAdapter(
    private val client: FantasmaClient,
) : FantasmaReactNativeClient {
    override suspend fun track(eventName: String) {
        client.track(eventName)
    }

    override suspend fun flush() {
        client.flush()
    }

    override suspend fun clear() {
        client.clear()
    }

    override fun close() {
        client.close()
    }
}

public class FantasmaReactNativeBridge internal constructor(
    private val context: Context?,
    private val createClient: (Context?, FantasmaConfig) -> FantasmaReactNativeClient,
) {
    private val mutex = Mutex()

    private var activeClient: FantasmaReactNativeClient? = null
    private var activeConfig: FantasmaConfig? = null
    private val retiringClients = linkedSetOf<FantasmaReactNativeClient>()
    private val inFlightOperationCounts = linkedMapOf<FantasmaReactNativeClient, Int>()

    public constructor(
        context: Context,
    ) : this(
        context = context.applicationContext,
        createClient = { clientContext, config ->
            val resolvedContext =
                requireNotNull(clientContext) {
                    "Fantasma React Native bridge requires an Android context."
                }
            FantasmaReactNativeClientAdapter(FantasmaClient(resolvedContext, config))
        },
    )

    internal constructor(
        createClient: (Context?, FantasmaConfig) -> FantasmaReactNativeClient,
    ) : this(
        context = null,
        createClient = createClient,
    )

    public suspend fun open(config: FantasmaConfig) {
        var clientToClose: FantasmaReactNativeClient? = null
        mutex.withLock {
            if (activeClient != null && activeConfig == config) {
                return@withLock
            }

            val previousActiveClient = activeClient
            if (previousActiveClient != null) {
                if ((inFlightOperationCounts[previousActiveClient] ?: 0) > 0) {
                    retiringClients += previousActiveClient
                } else {
                    clientToClose = previousActiveClient
                }
            }

            activeClient = createClient(context, config)
            activeConfig = config
        }

        clientToClose?.close()
    }

    public suspend fun track(eventName: String) {
        val client = beginOperation()
        try {
            client.track(eventName)
        } finally {
            finishOperation(client)
        }
    }

    public suspend fun flush() {
        val client = beginOperation()
        try {
            client.flush()
        } finally {
            finishOperation(client)
        }
    }

    public suspend fun clear() {
        val client = beginOperation()
        try {
            client.clear()
        } finally {
            finishOperation(client)
        }
    }

    public suspend fun close() {
        val clientToClose: FantasmaReactNativeClient?
        mutex.withLock {
            val currentActiveClient = activeClient
            if (currentActiveClient == null) {
                return
            }

            if ((inFlightOperationCounts[currentActiveClient] ?: 0) > 0) {
                retiringClients += currentActiveClient
                clientToClose = null
            } else {
                clientToClose = currentActiveClient
            }

            activeClient = null
            activeConfig = null
        }

        clientToClose?.close()
    }

    private suspend fun beginOperation(): FantasmaReactNativeClient {
        return mutex.withLock {
            val client = activeClient ?: throw com.fantasma.sdk.FantasmaException.ClosedClient
            inFlightOperationCounts[client] = (inFlightOperationCounts[client] ?: 0) + 1
            client
        }
    }

    private suspend fun finishOperation(client: FantasmaReactNativeClient) {
        var clientToClose: FantasmaReactNativeClient? = null
        mutex.withLock {
            val remainingOperations = (inFlightOperationCounts[client] ?: 0) - 1
            if (remainingOperations > 0) {
                inFlightOperationCounts[client] = remainingOperations
                return@withLock
            }

            inFlightOperationCounts.remove(client)
            if (retiringClients.remove(client)) {
                clientToClose = client
            }
        }

        clientToClose?.close()
    }
}
