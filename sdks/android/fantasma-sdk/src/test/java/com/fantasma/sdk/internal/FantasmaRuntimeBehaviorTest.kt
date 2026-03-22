package com.fantasma.sdk.internal

import android.content.Context
import android.content.res.Configuration
import android.os.Build
import android.os.LocaleList
import androidx.test.core.app.ApplicationProvider
import com.fantasma.sdk.FantasmaClient
import com.fantasma.sdk.FantasmaConfig
import com.fantasma.sdk.FantasmaException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import java.util.Locale

@RunWith(RobolectricTestRunner::class)
internal class FantasmaRuntimeBehaviorTest {
    @Test
    internal fun `creating a client for a different destination supersedes older handles`() =
        runTest {
            val context = ApplicationProvider.getApplicationContext<Context>()
            val firstConfig =
                FantasmaConfig(
                    serverUrl = "https://api.usefantasma.com",
                    writeKey = "fg_ing_superseded_a",
                )
            val secondConfig =
                FantasmaConfig(
                    serverUrl = "https://api.usefantasma.com",
                    writeKey = "fg_ing_superseded_b",
                )
            val firstSignature = NormalizedDestination.from(firstConfig.serverUrl, firstConfig.writeKey).signature
            val secondSignature = NormalizedDestination.from(secondConfig.serverUrl, secondConfig.writeKey).signature
            FantasmaDatabaseFactory.deleteDatabase(context, firstSignature)
            FantasmaDatabaseFactory.deleteDatabase(context, secondSignature)

            val firstClient = FantasmaClient(context, firstConfig)
            val secondClient = FantasmaClient(context, secondConfig)

            try {
                assertFailsWith<FantasmaException.ClosedClient> {
                    firstClient.track("stale_destination")
                }
                secondClient.track("fresh_destination")
            } finally {
                firstClient.close()
                secondClient.close()
                FantasmaDatabaseFactory.deleteDatabase(context, firstSignature)
                FantasmaDatabaseFactory.deleteDatabase(context, secondSignature)
            }
        }

    @Test
    internal fun `switching back to an earlier destination creates a fresh usable runtime`() =
        runTest {
            val context = ApplicationProvider.getApplicationContext<Context>()
            val firstConfig =
                FantasmaConfig(
                    serverUrl = "https://api.usefantasma.com",
                    writeKey = "fg_ing_return_a",
                )
            val secondConfig =
                FantasmaConfig(
                    serverUrl = "https://api.usefantasma.com",
                    writeKey = "fg_ing_return_b",
                )
            val firstSignature = NormalizedDestination.from(firstConfig.serverUrl, firstConfig.writeKey).signature
            val secondSignature = NormalizedDestination.from(secondConfig.serverUrl, secondConfig.writeKey).signature
            FantasmaDatabaseFactory.deleteDatabase(context, firstSignature)
            FantasmaDatabaseFactory.deleteDatabase(context, secondSignature)

            val firstClient = FantasmaClient(context, firstConfig)
            val secondClient = FantasmaClient(context, secondConfig)
            val returnedFirstClient = FantasmaClient(context, firstConfig)

            try {
                returnedFirstClient.track("fresh_destination_again")
                firstClient.close()
                returnedFirstClient.track("fresh_after_stale_close")
            } finally {
                firstClient.close()
                secondClient.close()
                returnedFirstClient.close()
                FantasmaDatabaseFactory.deleteDatabase(context, firstSignature)
                FantasmaDatabaseFactory.deleteDatabase(context, secondSignature)
            }
        }

    @Test
    internal fun `explicit flush waits for an in-flight upload to finish`() =
        runTest {
            val transport = SuspendedTransport()
            val harness = RuntimeHarness.create(transport)

            try {
                harness.runtime.track("app_open")

                val firstFlush = async(Dispatchers.Default) { harness.runtime.flush() }
                transport.waitUntilSendStarts()

                val secondFlush = async(Dispatchers.Default) { harness.runtime.flush() }
                yield()

                assertFalse(secondFlush.isCompleted)

                transport.release()
                firstFlush.await()
                secondFlush.await()

                assertEquals(0, harness.queue.count())
            } finally {
                harness.close()
            }
        }

    @Test
    internal fun `stale close after superseding an in-flight runtime does not interrupt retirement`() =
        runTest {
            val transport = SuspendedTransport()
            val harness = RuntimeHarness.create(transport)
            val registry = RuntimeRegistry<FantasmaRuntime>()
            registry.acquire(harness.destination.signature) { harness.runtime }

            try {
                harness.runtime.track("app_open")

                val inFlightFlush = async(Dispatchers.Default) { harness.runtime.flush() }
                transport.waitUntilSendStarts()

                registry.evict(harness.destination.signature, harness.runtime)
                harness.runtime.markSuperseded()

                assertNull(registry.release(harness.destination.signature, harness.runtime))
                yield()
                assertFalse(inFlightFlush.isCompleted)

                transport.release()
                inFlightFlush.await()

                assertFailsWith<FantasmaException.ClosedClient> {
                    harness.runtime.track("stale_after_retirement")
                }
            } finally {
                harness.close()
            }
        }

    @Test
    internal fun `event locale uses the app context locale instead of the process default`() =
        runTest {
            val baseContext = ApplicationProvider.getApplicationContext<Context>()
            val configuration = Configuration(baseContext.resources.configuration)
            val appLocale = Locale.forLanguageTag("pt-PT")
            val originalDefault = Locale.getDefault()
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                configuration.setLocales(LocaleList(appLocale))
            } else {
                @Suppress("DEPRECATION")
                run {
                    configuration.locale = appLocale
                }
            }
            val localizedContext = baseContext.createConfigurationContext(configuration)
            val harness = RuntimeHarness.create(NoopTransport(), context = localizedContext)

            try {
                Locale.setDefault(Locale.US)
                harness.runtime.track("app_open")

                val row = harness.queue.peek(limit = 1).single()
                val payload =
                    kotlinx.serialization.json.Json
                        .parseToJsonElement(row.payload.decodeToString())
                        .jsonObject

                assertEquals("pt-PT", payload["locale"]?.jsonPrimitive?.content)
            } finally {
                Locale.setDefault(originalDefault)
                harness.close()
            }
        }

    private class SuspendedTransport : FantasmaTransport {
        private val sendStarted: CompletableDeferred<Unit> = CompletableDeferred()
        private val releaseSend: CompletableDeferred<Unit> = CompletableDeferred()

        override suspend fun send(request: FantasmaRequest): FantasmaResponse {
            sendStarted.complete(Unit)
            releaseSend.await()
            return FantasmaResponse(
                statusCode = 202,
                body = """{"accepted":1}""".encodeToByteArray(),
            )
        }

        suspend fun waitUntilSendStarts() {
            sendStarted.await()
        }

        fun release() {
            releaseSend.complete(Unit)
        }
    }

    private class NoopTransport : FantasmaTransport {
        override suspend fun send(request: FantasmaRequest): FantasmaResponse {
            return FantasmaResponse(
                statusCode = 202,
                body = """{"accepted":1}""".encodeToByteArray(),
            )
        }
    }

    private class RuntimeHarness(
        val context: Context,
        val destination: NormalizedDestination,
        val database: FantasmaDatabase,
        val queue: SQLiteEventQueue,
        val runtime: FantasmaRuntime,
    ) {
        fun close() {
            runtime.shutdown()
            FantasmaDatabaseFactory.deleteDatabase(context, destination.signature)
        }

        companion object {
            fun create(
                transport: FantasmaTransport,
                context: Context = ApplicationProvider.getApplicationContext(),
            ): RuntimeHarness {
                val destination =
                    NormalizedDestination.from(
                        serverUrl = "https://api.usefantasma.com",
                        writeKey = "fg_ing_flush_wait",
                    )
                FantasmaDatabaseFactory.deleteDatabase(context, destination.signature)
                val database = FantasmaDatabaseFactory.open(context, destination.signature)
                val queue = SQLiteEventQueue(database = database, destinationSignature = destination.signature)
                val identityStore = IdentityStore(context)
                val uploader = EventUploader(queue = queue, transport = transport)
                val constructor =
                    FantasmaRuntime::class.java.getDeclaredConstructor(
                        Context::class.java,
                        NormalizedDestination::class.java,
                        FantasmaDatabase::class.java,
                        SQLiteEventQueue::class.java,
                        IdentityStore::class.java,
                        EventUploader::class.java,
                    )
                constructor.isAccessible = true
                val runtime =
                    constructor.newInstance(
                        context,
                        destination,
                        database,
                        queue,
                        identityStore,
                        uploader,
                    )
                return RuntimeHarness(context, destination, database, queue, runtime)
            }
        }
    }
}
