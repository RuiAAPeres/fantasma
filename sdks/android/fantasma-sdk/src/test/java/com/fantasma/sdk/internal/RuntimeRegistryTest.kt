package com.fantasma.sdk.internal

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame

internal class RuntimeRegistryTest {
    @Test
    internal fun `same normalized destination shares one runtime entry`() {
        val registry = RuntimeRegistry<String>()
        val first = registry.acquire("https://api.usefantasma.com|fg_ing_test") { "runtime-a" }
        val second = registry.acquire("https://api.usefantasma.com|fg_ing_test") { "runtime-b" }

        assertSame(first.runtime, second.runtime)
        assertEquals(2, second.handleCount)
    }

    @Test
    internal fun `different normalized destinations stay isolated`() {
        val registry = RuntimeRegistry<String>()
        val first = registry.acquire("https://api.usefantasma.com|fg_ing_a") { "runtime-a" }
        val second = registry.acquire("https://api.usefantasma.com|fg_ing_b") { "runtime-b" }

        assertNotSame(first.runtime, second.runtime)
        assertEquals(1, first.handleCount)
        assertEquals(1, second.handleCount)
    }

    @Test
    internal fun `releasing a stale runtime handle does not touch a newer runtime for the same signature`() {
        val registry = RuntimeRegistry<String>()
        val first = registry.acquire("https://api.usefantasma.com|fg_ing_a") { "runtime-a" }

        registry.evict("https://api.usefantasma.com|fg_ing_a", first.runtime)
        val replacement = registry.acquire("https://api.usefantasma.com|fg_ing_a") { "runtime-b" }

        assertNull(registry.release("https://api.usefantasma.com|fg_ing_a", first.runtime))
        assertSame(replacement.runtime, registry.get("https://api.usefantasma.com|fg_ing_a"))
    }
}
