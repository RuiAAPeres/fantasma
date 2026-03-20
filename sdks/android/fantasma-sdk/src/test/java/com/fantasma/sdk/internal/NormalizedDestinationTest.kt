package com.fantasma.sdk.internal

import com.fantasma.sdk.FantasmaException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class NormalizedDestinationTest {
    @Test
    internal fun `normalization trims write key and preserves equivalent destinations`() {
        val normalized =
            NormalizedDestination.from(
                serverUrl = "HTTPS://API.USEFANTASMA.COM///",
                writeKey = "  fg_ing_test  ",
            )

        assertEquals("https://api.usefantasma.com", normalized.serverUrl)
        assertEquals("fg_ing_test", normalized.writeKey)
        assertEquals("https://api.usefantasma.com|fg_ing_test", normalized.signature)
    }

    @Test
    internal fun `normalization preserves path differences as different destinations`() {
        val base =
            NormalizedDestination.from(
                serverUrl = "https://api.usefantasma.com",
                writeKey = "fg_ing_test",
            )
        val scoped =
            NormalizedDestination.from(
                serverUrl = "https://api.usefantasma.com/tenant-a",
                writeKey = "fg_ing_test",
            )

        assertEquals("https://api.usefantasma.com", base.serverUrl)
        assertEquals("https://api.usefantasma.com/tenant-a", scoped.serverUrl)
    }

    @Test
    internal fun `normalization rejects empty write keys`() {
        assertFailsWith<FantasmaException.InvalidWriteKey> {
            NormalizedDestination.from(
                serverUrl = "https://api.usefantasma.com",
                writeKey = "   ",
            )
        }
    }

    @Test
    internal fun `normalization rejects unsupported urls`() {
        assertFailsWith<FantasmaException.UnsupportedServerUrl> {
            NormalizedDestination.from(
                serverUrl = "ftp://api.usefantasma.com",
                writeKey = "fg_ing_test",
            )
        }
    }
}
