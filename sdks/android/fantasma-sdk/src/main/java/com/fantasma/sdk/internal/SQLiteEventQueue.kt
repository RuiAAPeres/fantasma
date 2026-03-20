package com.fantasma.sdk.internal

import androidx.room.withTransaction
import com.fantasma.sdk.FantasmaException

internal data class QueuedEventRow(
    val id: Long,
    val payload: ByteArray,
    val createdAt: Long,
)

internal class SQLiteEventQueue(
    private val database: FantasmaDatabase,
    private val destinationSignature: String,
) {
    private val dao: FantasmaQueueDao = database.queueDao()

    suspend fun enqueue(
        payload: ByteArray,
        createdAt: Long,
    ) {
        database.withTransaction {
            val existingSignature = dao.readState(QueueStateKey.DestinationSignature.value)
            if (existingSignature != null && existingSignature != destinationSignature) {
                throw FantasmaException.StorageFailure("queue destination mismatch")
            }

            dao.upsertState(QueueStateEntity(QueueStateKey.DestinationSignature.value, destinationSignature))
            dao.insertEvent(
                QueuedEventEntity(
                    payload = payload,
                    createdAt = createdAt,
                ),
            )
        }
    }

    suspend fun peek(limit: Int): List<QueuedEventRow> {
        return dao.peek(limit).map { row ->
            QueuedEventRow(id = row.id, payload = row.payload, createdAt = row.createdAt)
        }
    }

    suspend fun count(): Int = dao.count()

    suspend fun delete(ids: List<Long>) {
        if (ids.isEmpty()) {
            return
        }

        database.withTransaction {
            dao.delete(ids)
            if (dao.count() == 0) {
                dao.deleteState(QueueStateKey.DestinationSignature.value)
            }
        }
    }

    suspend fun reset() {
        database.withTransaction {
            dao.deleteAllEvents()
            dao.deleteState(QueueStateKey.DestinationSignature.value)
            dao.deleteState(QueueStateKey.BlockedDestinationSignature.value)
        }
    }

    suspend fun blockedDestinationSignature(): String? = dao.readState(QueueStateKey.BlockedDestinationSignature.value)

    suspend fun markBlockedDestinationSignature(signature: String) {
        dao.upsertState(QueueStateEntity(QueueStateKey.BlockedDestinationSignature.value, signature))
    }

    private enum class QueueStateKey(val value: String) {
        DestinationSignature("destination_signature"),
        BlockedDestinationSignature("blocked_destination_signature"),
    }
}
