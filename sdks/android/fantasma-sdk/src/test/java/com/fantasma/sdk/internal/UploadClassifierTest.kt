package com.fantasma.sdk.internal

import com.fantasma.sdk.FantasmaException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class UploadClassifierTest {
    @Test
    internal fun `valid accepted response is success`() {
        assertEquals(
            UploadDisposition.Success,
            UploadClassifier.classify(
                statusCode = 202,
                responseBody = """{"accepted":2}""".encodeToByteArray(),
                batchCount = 2,
            ),
        )
    }

    @Test
    internal fun `accepted count mismatch is invalid response`() {
        assertFailsWith<FantasmaException.InvalidResponse> {
            UploadClassifier.classify(
                statusCode = 202,
                responseBody = """{"accepted":1}""".encodeToByteArray(),
                batchCount = 2,
            )
        }
    }

    @Test
    internal fun `unauthorized and validation failures block destination`() {
        assertEquals(
            UploadDisposition.BlockedDestination,
            UploadClassifier.classify(
                statusCode = 401,
                responseBody = """{"error":"unauthorized"}""".encodeToByteArray(),
                batchCount = 1,
            ),
        )
        assertEquals(
            UploadDisposition.BlockedDestination,
            UploadClassifier.classify(
                statusCode = 422,
                responseBody = """{"error":"invalid_request"}""".encodeToByteArray(),
                batchCount = 1,
            ),
        )
    }

    @Test
    internal fun `pending deletion conflict blocks destination but unrelated conflict retries`() {
        assertEquals(
            UploadDisposition.BlockedDestination,
            UploadClassifier.classify(
                statusCode = 409,
                responseBody = """{"error":"project_pending_deletion"}""".encodeToByteArray(),
                batchCount = 1,
            ),
        )
        assertEquals(
            UploadDisposition.RetryableFailure,
            UploadClassifier.classify(
                statusCode = 409,
                responseBody = """{"error":"project_busy"}""".encodeToByteArray(),
                batchCount = 1,
            ),
        )
    }
}
