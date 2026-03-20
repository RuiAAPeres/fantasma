package com.fantasma.sdk.internal

import android.content.Context
import com.fantasma.sdk.FantasmaClientDelegate
import com.fantasma.sdk.FantasmaConfig

internal object FantasmaRuntimeRegistry {
    private val registry: RuntimeRegistry<FantasmaRuntime> = RuntimeRegistry()

    @Synchronized
    fun acquire(
        context: Context,
        config: FantasmaConfig,
    ): FantasmaClientDelegate {
        val normalized = NormalizedDestination.from(config.serverUrl, config.writeKey)
        val metadataStore = RuntimeMetadataStore(context)
        val previousSignature = metadataStore.swapActiveDestinationSignature(normalized.signature)
        if (previousSignature != null && previousSignature != normalized.signature) {
            val existingRuntime = registry.get(previousSignature)
            if (existingRuntime != null) {
                registry.evict(previousSignature, existingRuntime)
                existingRuntime.markSuperseded()
            } else {
                FantasmaDatabaseFactory.deleteDatabase(context, previousSignature)
            }
        }

        val lease =
            registry.acquire(normalized.signature) {
                FantasmaRuntime.create(
                    context = context,
                    destination = normalized,
                )
            }

        return RuntimeClientDelegate(
            runtime = lease.runtime,
            onClose = {
                release(normalized.signature, lease.runtime)
            },
        )
    }

    @Synchronized
    private fun release(
        signature: String,
        runtime: FantasmaRuntime,
    ) {
        val remainingHandles = registry.release(signature, runtime) ?: return
        if (remainingHandles == 0) {
            runtime.shutdown()
        }
    }
}
