package com.fantasma.reactnative

import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.fantasma.sdk.FantasmaConfig
import com.fantasma.sdk.FantasmaException
import com.fantasma.sdk.reactnative.FantasmaReactNativeBridge
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch

public class FantasmaReactNativeModule(
    reactContext: ReactApplicationContext,
) : ReactContextBaseJavaModule(reactContext) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val bridge = FantasmaReactNativeBridge(reactContext.applicationContext)

    override fun getName(): String = "FantasmaReactNative"

    @ReactMethod
    public fun open(
        serverUrl: String,
        writeKey: String,
        promise: Promise,
    ) {
        scope.launch {
            runCatching {
                bridge.open(
                    FantasmaConfig(
                        serverUrl = serverUrl,
                        writeKey = writeKey,
                    ),
                )
            }.fold(
                onSuccess = { promise.resolve(null) },
                onFailure = { promise.rejectBridgeError(it) },
            )
        }
    }

    @ReactMethod
    public fun track(
        eventName: String,
        promise: Promise,
    ) {
        scope.launch {
            runCatching { bridge.track(eventName) }.fold(
                onSuccess = { promise.resolve(null) },
                onFailure = { promise.rejectBridgeError(it) },
            )
        }
    }

    @ReactMethod
    public fun flush(promise: Promise) {
        scope.launch {
            runCatching { bridge.flush() }.fold(
                onSuccess = { promise.resolve(null) },
                onFailure = { promise.rejectBridgeError(it) },
            )
        }
    }

    @ReactMethod
    public fun clear(promise: Promise) {
        scope.launch {
            runCatching { bridge.clear() }.fold(
                onSuccess = { promise.resolve(null) },
                onFailure = { promise.rejectBridgeError(it) },
            )
        }
    }

    @ReactMethod
    public fun close(promise: Promise) {
        scope.launch {
            runCatching { bridge.close() }.fold(
                onSuccess = { promise.resolve(null) },
                onFailure = { promise.rejectBridgeError(it) },
            )
        }
    }

    override fun invalidate() {
        super.invalidate()
        scope.cancel()
    }
}

private fun Promise.rejectBridgeError(error: Throwable) {
    when (error) {
        FantasmaException.InvalidWriteKey -> reject(
            "invalid_write_key",
            error.message,
            error,
        )
        FantasmaException.UnsupportedServerUrl -> reject(
            "unsupported_server_url",
            error.message,
            error,
        )
        FantasmaException.InvalidEventName -> reject(
            "invalid_event_name",
            error.message,
            error,
        )
        FantasmaException.EncodingFailed -> reject(
            "encoding_failed",
            error.message,
            error,
        )
        FantasmaException.InvalidResponse -> reject(
            "invalid_response",
            error.message,
            error,
        )
        FantasmaException.UploadFailed -> reject(
            "upload_failed",
            error.message,
            error,
        )
        is FantasmaException.StorageFailure -> reject(
            "storage_failure",
            error.detail,
            error,
        )
        FantasmaException.ClosedClient -> reject(
            "closed_client",
            error.message,
            error,
        )
    }
}
