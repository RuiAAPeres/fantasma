package com.fantasma.sdk.internal

import android.content.Context
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import kotlinx.coroutines.flow.first
import java.util.UUID

private val Context.identityDataStore by preferencesDataStore(name = "fantasma-sdk-identity")

internal class IdentityStore(
    private val context: Context,
) {
    suspend fun load(): String {
        val current = context.identityDataStore.data.first()[INSTALL_ID_KEY]?.trim().orEmpty()
        if (current.isNotEmpty()) {
            return current
        }

        val generated = newIdentifier()
        context.identityDataStore.edit { preferences ->
            preferences[INSTALL_ID_KEY] = generated
        }
        return generated
    }

    suspend fun clear(): String {
        val generated = newIdentifier()
        context.identityDataStore.edit { preferences ->
            preferences[INSTALL_ID_KEY] = generated
        }
        return generated
    }

    private fun newIdentifier(): String = UUID.randomUUID().toString().lowercase()

    private companion object {
        val INSTALL_ID_KEY = stringPreferencesKey("install_id")
    }
}
