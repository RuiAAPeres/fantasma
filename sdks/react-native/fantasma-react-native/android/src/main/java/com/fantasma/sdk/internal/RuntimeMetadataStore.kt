package com.fantasma.sdk.internal

import android.content.Context
import androidx.core.content.edit

internal class RuntimeMetadataStore(
    context: Context,
) {
    private val preferences = context.getSharedPreferences("fantasma-sdk-runtime", Context.MODE_PRIVATE)

    fun swapActiveDestinationSignature(newSignature: String): String? {
        val previous = preferences.getString(ACTIVE_DESTINATION_SIGNATURE, null)
        preferences.edit(commit = true) {
            putString(ACTIVE_DESTINATION_SIGNATURE, newSignature)
        }
        return previous
    }

    private companion object {
        const val ACTIVE_DESTINATION_SIGNATURE: String = "active_destination_signature"
    }
}
