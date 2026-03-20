package com.fantasma.sdk.internal

import com.fantasma.sdk.FantasmaException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class EventPropertyValidatorTest {
    @Test
    internal fun `empty properties normalize to null`() {
        assertEquals(null, EventPropertyValidator.validate(emptyMap()))
        assertEquals(null, EventPropertyValidator.validate(null))
    }

    @Test
    internal fun `validator rejects more than two properties`() {
        assertFailsWith<FantasmaException.InvalidPropertyCount> {
            EventPropertyValidator.validate(
                mapOf(
                    "screen" to "Home",
                    "region" to "EU",
                    "plan" to "pro",
                ),
            )
        }
    }

    @Test
    internal fun `validator rejects invalid property names`() {
        assertFailsWith<FantasmaException.InvalidPropertyName> {
            EventPropertyValidator.validate(mapOf("Screen-Name" to "Home"))
        }
    }

    @Test
    internal fun `validator rejects reserved property names`() {
        assertFailsWith<FantasmaException.ReservedPropertyName> {
            EventPropertyValidator.validate(mapOf("platform" to "android"))
        }
    }
}
