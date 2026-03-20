package com.fantasma.sdk.internal

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
internal class IdentityStoreInstrumentedTest {
    @Test
    internal fun clearRotatesInstallIdentityAndFutureLoadsReturnTheNewValue(): Unit =
        runTest {
            val context = ApplicationProvider.getApplicationContext<android.content.Context>()
            val store = IdentityStore(context)

            val first = store.load()
            val second = store.clear()
            val third = store.load()

            assertNotEquals(first, second)
            assertEquals(second, third)
        }
}
