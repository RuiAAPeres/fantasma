package com.fantasma.sdk.reactnative

import android.content.Context
import com.fantasma.sdk.FantasmaConfig
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

internal class FantasmaReactNativeBridgeTest {
    @Test
    internal fun `close during in-flight work keeps the old client alive for replacement handoff`() =
        runTest {
            val firstClient = FakeBridgeClient()
            val secondClient = FakeBridgeClient()
            val createdClients = mutableListOf<FantasmaConfig>()
            val bridge =
                FantasmaReactNativeBridge(
                    createClient = { _: Context?, config: FantasmaConfig ->
                        createdClients += config
                        if (createdClients.size == 1) {
                            firstClient
                        } else {
                            secondClient
                        }
                    },
                )

            val firstConfig =
                FantasmaConfig(
                    serverUrl = "https://api.usefantasma.com",
                    writeKey = "fg_ing_primary",
                )
            val secondConfig =
                FantasmaConfig(
                    serverUrl = "https://api.usefantasma.com",
                    writeKey = "fg_ing_replacement",
                )

            bridge.open(firstConfig)
            val inFlightFlush = async { bridge.flush() }
            firstClient.waitUntilFlushStarts()

            bridge.close()
            bridge.open(secondConfig)

            assertEquals(false, firstClient.closedBeforeFlushCompleted)
            firstClient.releaseFlush()
            inFlightFlush.await()

            assertEquals(listOf(firstConfig, secondConfig), createdClients)
            assertEquals(1, firstClient.closeCallCount)
            assertEquals(0, secondClient.closeCallCount)
        }

    @Test
    internal fun `close without replacement shuts down the active client`() =
        runTest {
            val client = FakeBridgeClient()
            val bridge =
                FantasmaReactNativeBridge(
                    createClient = { _: Context?, _: FantasmaConfig -> client },
                )
            val config =
                FantasmaConfig(
                    serverUrl = "https://api.usefantasma.com",
                    writeKey = "fg_ing_primary",
                )

            bridge.open(config)
            bridge.close()

            assertEquals(1, client.closeCallCount)
        }
}

private class FakeBridgeClient : FantasmaReactNativeClient {
    private val flushStarted = CompletableDeferred<Unit>()
    private val releaseFlush = CompletableDeferred<Unit>()
    private var flushCompleted = false
    var closeCallCount: Int = 0
        private set
    var closedBeforeFlushCompleted: Boolean = false
        private set

    override suspend fun track(eventName: String) = Unit

    override suspend fun flush() {
        flushStarted.complete(Unit)
        releaseFlush.await()
        flushCompleted = true
    }

    override suspend fun clear() = Unit

    override fun close() {
        if (!flushCompleted) {
            closedBeforeFlushCompleted = true
        }
        closeCallCount += 1
    }

    suspend fun waitUntilFlushStarts() {
        flushStarted.await()
    }

    fun releaseFlush() {
        releaseFlush.complete(Unit)
    }
}
