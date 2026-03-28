package com.fantasma.sdk.internal

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
internal data class EventEnvelope(
    val event: String,
    val timestamp: String,
    @SerialName("install_id")
    val installId: String,
    val platform: String,
    val device: String,
    @SerialName("app_version")
    val appVersion: String?,
    @SerialName("os_version")
    val osVersion: String?,
    val locale: String?,
)
