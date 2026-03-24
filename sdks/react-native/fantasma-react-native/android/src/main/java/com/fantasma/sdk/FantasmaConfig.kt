package com.fantasma.sdk

/**
 * Immutable Fantasma destination configuration for one Android client instance.
 */
public data class FantasmaConfig(
    val serverUrl: String,
    val writeKey: String,
)
