package com.fantasma.sdk.internal

import com.fantasma.sdk.FantasmaException

internal object EventPropertyValidator {
    private const val MAX_PROPERTY_COUNT: Int = 2
    private const val MAX_KEY_LENGTH: Int = 63

    private val reservedKeys: Set<String> =
        setOf(
            "project_id",
            "event",
            "timestamp",
            "install_id",
            "properties",
            "metric",
            "granularity",
            "start",
            "end",
            "start_date",
            "end_date",
            "group_by",
            "platform",
            "app_version",
            "os_version",
        )

    @Suppress("ThrowsCount")
    fun validate(properties: Map<String, String>?): Map<String, String>? {
        if (properties.isNullOrEmpty()) {
            return null
        }

        if (properties.size > MAX_PROPERTY_COUNT) {
            throw FantasmaException.InvalidPropertyCount
        }

        properties.keys.forEach { key ->
            if (!isValidKey(key)) {
                throw FantasmaException.InvalidPropertyName
            }
            if (key in reservedKeys) {
                throw FantasmaException.ReservedPropertyName
            }
        }

        return properties
    }

    @Suppress("ReturnCount")
    private fun isValidKey(key: String): Boolean {
        if (key.isEmpty() || key.length > MAX_KEY_LENGTH) {
            return false
        }

        if (key.first() !in 'a'..'z') {
            return false
        }

        return key.drop(1).all { character ->
            character in 'a'..'z' || character in '0'..'9' || character == '_'
        }
    }
}
