package com.fantasma.sdk.internal

import android.content.Context
import android.os.Build
import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner
import com.fantasma.sdk.FantasmaException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicBoolean

@Suppress("TooManyFunctions")
internal class FantasmaRuntime private constructor(
    private val context: Context,
    private val destination: NormalizedDestination,
    private val database: FantasmaDatabase,
    private val queue: SQLiteEventQueue,
    private val identityStore: IdentityStore,
    private val uploader: EventUploader,
) {
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mutex: Mutex = Mutex()
    private val isShuttingDown: AtomicBoolean = AtomicBoolean(false)
    private val isSuperseded: AtomicBoolean = AtomicBoolean(false)
    private val json: Json = Json { explicitNulls = false }

    private var installId: String? = null
    private var flushInProgress: Boolean = false
    private val flushWaiters: MutableList<CompletableDeferred<Result<Unit>>> = mutableListOf()

    private val backgroundObserver: DefaultLifecycleObserver =
        object : DefaultLifecycleObserver {
            override fun onStop(owner: LifecycleOwner) {
                scope.launch {
                    runCatching { flushInternal(waitForInFlight = false) }
                }
            }
        }

    init {
        runOnMainThread {
            ProcessLifecycleOwner.get().lifecycle.addObserver(backgroundObserver)
        }
        scope.launch {
            while (true) {
                delay(PERIODIC_FLUSH_INTERVAL_MS)
                runCatching { flushInternal(waitForInFlight = false) }
            }
        }
    }

    suspend fun track(
        eventName: String,
        properties: Map<String, String>?,
    ) {
        ensureRuntimeOpen()

        val normalizedName = eventName.trim()
        if (normalizedName.isEmpty()) {
            throw FantasmaException.InvalidEventName
        }

        val validatedProperties = EventPropertyValidator.validate(properties)
        val payload = eventPayload(normalizedName, validatedProperties)
        queue.enqueue(payload = payload, createdAt = Instant.now().epochSecond)

        if (queue.count() >= UPLOAD_BATCH_SIZE) {
            flushInternal(waitForInFlight = false)
        }
    }

    suspend fun flush() {
        ensureRuntimeOpen()
        flushInternal(waitForInFlight = true)
    }

    suspend fun clear() {
        ensureRuntimeOpen()
        installId = identityStore.clear()
    }

    fun shutdown() {
        if (!isShuttingDown.compareAndSet(false, true)) {
            return
        }
        runOnMainThread {
            ProcessLifecycleOwner.get().lifecycle.removeObserver(backgroundObserver)
        }
        scope.cancel()
        database.close()
    }

    fun markSuperseded() {
        if (!isSuperseded.compareAndSet(false, true)) {
            return
        }
        scope.launch {
            val shouldRetire =
                mutex.withLock {
                    if (!flushInProgress) {
                        queue.reset()
                        true
                    } else {
                        false
                    }
                }
            if (shouldRetire) {
                shutdown()
            }
        }
    }

    @Suppress("ReturnCount", "ThrowsCount")
    private suspend fun flushInternal(waitForInFlight: Boolean) {
        when (val start = beginFlush(waitForInFlight)) {
            FlushStart.Skip -> return
            is FlushStart.Wait -> {
                start.waiter.await().getOrThrow()
                return
            }
            FlushStart.StartNow -> Unit
        }

        val flushResult = runCatching { flushLoop() }
        finishFlush(flushResult)
        flushResult.getOrThrow()
    }

    private suspend fun eventPayload(
        eventName: String,
        properties: Map<String, String>?,
    ): ByteArray {
        val identity = installId ?: identityStore.load().also { installId = it }
        return try {
            json.encodeToString(
                EventEnvelope(
                    event = eventName,
                    timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()),
                    installId = identity,
                    platform = "android",
                    appVersion = appVersion(),
                    osVersion = Build.VERSION.RELEASE,
                    properties = properties,
                ),
            ).encodeToByteArray()
        } catch (_: Exception) {
            throw FantasmaException.EncodingFailed
        }
    }

    private fun ensureRuntimeOpen() {
        if (isShuttingDown.get() || isSuperseded.get()) {
            throw FantasmaException.ClosedClient
        }
    }

    private suspend fun beginFlush(waitForInFlight: Boolean): FlushStart {
        return mutex.withLock {
            if (!flushInProgress) {
                flushInProgress = true
                return@withLock FlushStart.StartNow
            }

            if (!waitForInFlight) {
                return@withLock FlushStart.Skip
            }

            val waiter = CompletableDeferred<Result<Unit>>()
            flushWaiters += waiter
            FlushStart.Wait(waiter)
        }
    }

    @Suppress("ThrowsCount")
    private suspend fun flushLoop() {
        if (queue.blockedDestinationSignature() == destination.signature) {
            throw FantasmaException.UploadFailed
        }

        while (true) {
            if (isSuperseded.get()) {
                queue.reset()
                return
            }

            val batch = uploader.makeBatch(limit = UPLOAD_BATCH_SIZE) ?: return
            when (uploader.upload(batch, destination)) {
                UploadDisposition.Success -> Unit
                UploadDisposition.RetryableFailure -> throw FantasmaException.UploadFailed
                UploadDisposition.BlockedDestination -> {
                    queue.markBlockedDestinationSignature(destination.signature)
                    throw FantasmaException.UploadFailed
                }
            }
        }
    }

    private suspend fun finishFlush(result: Result<Unit>) {
        val shouldRetire =
            mutex.withLock {
                flushInProgress = false
                if (isSuperseded.get()) {
                    queue.reset()
                    true
                } else {
                    false
                }
            }
        resumeFlushWaiters(result)
        if (shouldRetire) {
            shutdown()
        }
    }

    private fun resumeFlushWaiters(result: Result<Unit>) {
        val waiters =
            buildList {
                addAll(flushWaiters)
                flushWaiters.clear()
            }
        waiters.forEach { waiter -> waiter.complete(result) }
    }

    private fun appVersion(): String? {
        val packageInfo = context.packageManager.getPackageInfo(context.packageName, 0)
        return packageInfo.versionName?.takeIf { it.isNotBlank() }
    }

    private fun runOnMainThread(block: () -> Unit) {
        if (Looper.myLooper() == Looper.getMainLooper()) {
            block()
            return
        }

        Handler(Looper.getMainLooper()).post(block)
    }

    companion object {
        private const val PERIODIC_FLUSH_INTERVAL_MS: Long = 30_000L
        private const val UPLOAD_BATCH_SIZE: Int = 100

        fun create(
            context: Context,
            destination: NormalizedDestination,
        ): FantasmaRuntime {
            val database = FantasmaDatabaseFactory.open(context, destination.signature)
            val queue = SQLiteEventQueue(database = database, destinationSignature = destination.signature)
            val identityStore = IdentityStore(context)
            val uploader = EventUploader(queue = queue, transport = OkHttpFantasmaTransport())

            return FantasmaRuntime(
                context = context,
                destination = destination,
                database = database,
                queue = queue,
                identityStore = identityStore,
                uploader = uploader,
            )
        }
    }

    private sealed interface FlushStart {
        data object StartNow : FlushStart

        data object Skip : FlushStart

        data class Wait(
            val waiter: CompletableDeferred<Result<Unit>>,
        ) : FlushStart
    }
}
