package com.fantasma.sdk

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

internal class FantasmaClientTest {
    @Test
    internal fun `closed client rejects future operations deterministically`() =
        runTest {
            val delegate = RecordingDelegate()
            val client = FantasmaClient(delegate)

            client.close()

            assertFailsWith<FantasmaException.ClosedClient> {
                client.track("app_open")
            }
            assertFailsWith<FantasmaException.ClosedClient> {
                client.flush()
            }
            assertFailsWith<FantasmaException.ClosedClient> {
                client.clear()
            }
        }

    private class RecordingDelegate : FantasmaClientDelegate {
        override suspend fun track(
            eventName: String,
            properties: Map<String, String>?,
        ): Unit = Unit

        override suspend fun flush(): Unit = Unit

        override suspend fun clear(): Unit = Unit

        override fun close(): Unit = Unit
    }
}
