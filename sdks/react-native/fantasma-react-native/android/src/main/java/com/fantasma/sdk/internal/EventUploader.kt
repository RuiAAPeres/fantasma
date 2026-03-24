package com.fantasma.sdk.internal

internal data class UploadBatch(
    val rowIds: List<Long>,
    val body: ByteArray,
    val count: Int,
)

internal class EventUploader(
    private val queue: SQLiteEventQueue,
    private val transport: FantasmaTransport,
) {
    suspend fun makeBatch(limit: Int): UploadBatch? {
        val rows = queue.peek(limit)
        if (rows.isEmpty()) {
            return null
        }

        val body =
            buildString {
                append("{\"events\":[")
                rows.forEachIndexed { index, row ->
                    if (index > 0) {
                        append(',')
                    }
                    append(row.payload.decodeToString())
                }
                append("]}")
            }.encodeToByteArray()

        return UploadBatch(
            rowIds = rows.map { it.id },
            body = body,
            count = rows.size,
        )
    }

    suspend fun upload(
        batch: UploadBatch,
        destination: NormalizedDestination,
    ): UploadDisposition {
        val response =
            try {
                transport.send(
                    FantasmaRequest(
                        url = "${destination.serverUrl}/v1/events",
                        writeKey = destination.writeKey,
                        body = batch.body,
                    ),
                )
            } catch (_: Exception) {
                return UploadDisposition.RetryableFailure
            }

        val disposition = UploadClassifier.classify(response.statusCode, response.body, batch.count)
        if (disposition == UploadDisposition.Success) {
            queue.delete(batch.rowIds)
        }
        return disposition
    }
}
