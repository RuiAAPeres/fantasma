package com.fantasma.sdk.internal

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

@RunWith(AndroidJUnit4::class)
internal class SQLiteEventQueueInstrumentedTest {
    @Test
    internal fun queuePersistsRowsAcrossReopenForTheSameDestinationSignature(): Unit =
        runTest {
            val context = ApplicationProvider.getApplicationContext<android.content.Context>()
            val signature = "https://api.usefantasma.com|${UUID.randomUUID()}"
            FantasmaDatabaseFactory.deleteDatabase(context, signature)

            val firstDatabase = FantasmaDatabaseFactory.open(context, signature)
            val firstQueue = SQLiteEventQueue(firstDatabase, signature)
            firstQueue.enqueue("""{"event":"app_open"}""".encodeToByteArray(), createdAt = 1)
            firstDatabase.close()

            val reopenedDatabase = FantasmaDatabaseFactory.open(context, signature)
            val reopenedQueue = SQLiteEventQueue(reopenedDatabase, signature)
            val rows = reopenedQueue.peek(limit = 10)

            assertEquals(1, rows.size)
            assertEquals(1, rows.first().createdAt)

            reopenedDatabase.close()
            FantasmaDatabaseFactory.deleteDatabase(context, signature)
        }
}
