package com.fantasma.sdk.internal

import androidx.room.Dao
import androidx.room.Database
import androidx.room.Entity
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.PrimaryKey
import androidx.room.Query
import androidx.room.RoomDatabase

@Entity(tableName = "events")
internal data class QueuedEventEntity(
    @PrimaryKey(autoGenerate = true)
    val id: Long = 0,
    val payload: ByteArray,
    val createdAt: Long,
)

@Entity(tableName = "queue_state")
internal data class QueueStateEntity(
    @PrimaryKey
    val key: String,
    val value: String,
)

@Dao
internal interface FantasmaQueueDao {
    @Insert
    suspend fun insertEvent(event: QueuedEventEntity): Long

    @Query("SELECT id, payload, createdAt FROM events ORDER BY id ASC LIMIT :limit")
    suspend fun peek(limit: Int): List<QueuedEventEntity>

    @Query("SELECT COUNT(*) FROM events")
    suspend fun count(): Int

    @Query("DELETE FROM events WHERE id IN (:ids)")
    suspend fun delete(ids: List<Long>): Int

    @Query("DELETE FROM events")
    suspend fun deleteAllEvents(): Int

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertState(state: QueueStateEntity): Unit

    @Query("SELECT value FROM queue_state WHERE `key` = :key LIMIT 1")
    suspend fun readState(key: String): String?

    @Query("DELETE FROM queue_state WHERE `key` = :key")
    suspend fun deleteState(key: String): Int
}

@Database(
    entities = [QueuedEventEntity::class, QueueStateEntity::class],
    version = 1,
    exportSchema = true,
)
internal abstract class FantasmaDatabase : RoomDatabase() {
    abstract fun queueDao(): FantasmaQueueDao
}
