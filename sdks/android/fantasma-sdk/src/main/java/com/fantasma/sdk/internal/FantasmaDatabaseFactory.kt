package com.fantasma.sdk.internal

import android.content.Context
import androidx.room.Room
import java.security.MessageDigest

internal object FantasmaDatabaseFactory {
    fun open(
        context: Context,
        signature: String,
    ): FantasmaDatabase {
        return Room.databaseBuilder(
            context,
            FantasmaDatabase::class.java,
            databaseName(signature),
        ).build()
    }

    fun deleteDatabase(
        context: Context,
        signature: String,
    ) {
        context.deleteDatabase(databaseName(signature))
    }

    private fun databaseName(signature: String): String {
        return "fantasma-${sha256Hex(signature)}.db"
    }

    private fun sha256Hex(value: String): String {
        val digest = MessageDigest.getInstance("SHA-256").digest(value.toByteArray())
        return digest.joinToString(separator = "") { byte -> "%02x".format(byte) }
    }
}
