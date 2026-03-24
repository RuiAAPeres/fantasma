import SQLite3
import Foundation

private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
// A brief busy timeout lets transient second-connection checks wait out in-flight writes.
private let sqliteBusyTimeoutMilliseconds: Int32 = 1_000

private final class SQLiteDatabaseHandle {
    let pointer: OpaquePointer?

    init(pointer: OpaquePointer?) {
        self.pointer = pointer
    }

    deinit {
        sqlite3_close(pointer)
    }
}

internal struct QueuedEventRow: Equatable, Sendable {
    let id: Int64
    let payload: Data
    let createdAt: Int64
}

internal actor SQLiteEventQueue {
    private enum QueueStateKey {
        static let destinationSignature = "destination_signature"
        static let blockedDestinationSignature = "blocked_destination_signature"
    }

    private let database: SQLiteDatabaseHandle

    init(databaseURL: URL) throws {
        do {
            try FileManager.default.createDirectory(
                at: databaseURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
        } catch {
            throw FantasmaError.storageFailure(error.localizedDescription)
        }

        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        var database: OpaquePointer?
        if sqlite3_open_v2(databaseURL.path, &database, flags, nil) != SQLITE_OK {
            let message = database.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "unknown sqlite error"
            sqlite3_close(database)
            throw FantasmaError.storageFailure(message)
        }

        self.database = SQLiteDatabaseHandle(pointer: database)
        sqlite3_busy_timeout(self.database.pointer, sqliteBusyTimeoutMilliseconds)

        var errorMessage: UnsafeMutablePointer<Int8>?
        if sqlite3_exec(
            self.database.pointer,
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload BLOB NOT NULL,
                created_at INTEGER NOT NULL
            );
            """,
            nil,
            nil,
            &errorMessage
        ) != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "unknown sqlite error"
            sqlite3_free(errorMessage)
            throw FantasmaError.storageFailure(message)
        }

        errorMessage = nil
        if sqlite3_exec(
            self.database.pointer,
            """
            CREATE TABLE IF NOT EXISTS queue_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """,
            nil,
            nil,
            &errorMessage
        ) != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "unknown sqlite error"
            sqlite3_free(errorMessage)
            throw FantasmaError.storageFailure(message)
        }
    }

    func enqueue(payload: Data, createdAt: Int64, destinationSignature: String) throws {
        try inTransaction {
            let existingSignature = try readQueueState(for: QueueStateKey.destinationSignature)
            if let existingSignature, existingSignature != destinationSignature {
                throw FantasmaError.storageFailure("queue destination mismatch")
            }

            try writeQueueState(destinationSignature, for: QueueStateKey.destinationSignature)

            let statement = try prepare("INSERT INTO events (payload, created_at) VALUES (?, ?);")
            defer { sqlite3_finalize(statement) }

            try bind(payload, to: statement, at: 1)
            try bind(createdAt, to: statement, at: 2)
            try step(statement)
        }
    }

    func peek(limit: Int) throws -> [QueuedEventRow] {
        let statement = try prepare("""
            SELECT id, payload, created_at
            FROM events
            ORDER BY id ASC
            LIMIT ?;
            """)
        defer { sqlite3_finalize(statement) }

        try bind(Int64(limit), to: statement, at: 1)

        var rows: [QueuedEventRow] = []
        while sqlite3_step(statement) == SQLITE_ROW {
            let id = sqlite3_column_int64(statement, 0)
            let byteCount = Int(sqlite3_column_bytes(statement, 1))
            let createdAt = sqlite3_column_int64(statement, 2)
            guard let bytes = sqlite3_column_blob(statement, 1) else {
                rows.append(QueuedEventRow(id: id, payload: Data(), createdAt: createdAt))
                continue
            }
            rows.append(QueuedEventRow(
                id: id,
                payload: Data(bytes: bytes, count: byteCount),
                createdAt: createdAt
            ))
        }

        return rows
    }

    func count() throws -> Int {
        let statement = try prepare("SELECT COUNT(*) FROM events;")
        defer { sqlite3_finalize(statement) }

        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw FantasmaError.storageFailure("failed to count queued events")
        }

        return Int(sqlite3_column_int64(statement, 0))
    }

    func delete(ids: [Int64]) throws {
        guard !ids.isEmpty else {
            return
        }

        try inTransaction {
            let placeholders = ids.map { _ in "?" }.joined(separator: ",")
            let statement = try prepare("DELETE FROM events WHERE id IN (\(placeholders));")
            defer { sqlite3_finalize(statement) }

            for (index, id) in ids.enumerated() {
                try bind(id, to: statement, at: Int32(index + 1))
            }

            try step(statement)

            if try rowCount() == 0 {
                try deleteQueueState(for: QueueStateKey.destinationSignature)
            }
        }
    }

    func reset() throws {
        try inTransaction {
            try execute("DELETE FROM events;")
            try deleteQueueState(for: QueueStateKey.destinationSignature)
            try deleteQueueState(for: QueueStateKey.blockedDestinationSignature)
        }
    }

    func destinationSignature() throws -> String? {
        let count = try rowCount()
        guard count > 0 else {
            if try readQueueState(for: QueueStateKey.destinationSignature) != nil {
                try deleteQueueState(for: QueueStateKey.destinationSignature)
            }
            return nil
        }

        return try readQueueState(for: QueueStateKey.destinationSignature)
    }

    func blockedDestinationSignature() throws -> String? {
        try readQueueState(for: QueueStateKey.blockedDestinationSignature)
    }

    func markBlockedDestinationSignature(_ signature: String) throws {
        try writeQueueState(signature, for: QueueStateKey.blockedDestinationSignature)
    }

    private func execute(_ sql: String) throws {
        var errorMessage: UnsafeMutablePointer<Int8>?
        if sqlite3_exec(database.pointer, sql, nil, nil, &errorMessage) != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "unknown sqlite error"
            sqlite3_free(errorMessage)
            throw FantasmaError.storageFailure(message)
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer? {
        var statement: OpaquePointer?
        if sqlite3_prepare_v2(database.pointer, sql, -1, &statement, nil) != SQLITE_OK {
            throw FantasmaError.storageFailure(lastErrorMessage())
        }
        return statement
    }

    private func bind(_ value: Data, to statement: OpaquePointer?, at index: Int32) throws {
        let result = value.withUnsafeBytes { buffer in
            sqlite3_bind_blob(statement, index, buffer.baseAddress, Int32(value.count), sqliteTransient)
        }
        guard result == SQLITE_OK else {
            throw FantasmaError.storageFailure(lastErrorMessage())
        }
    }

    private func bind(_ value: Int64, to statement: OpaquePointer?, at index: Int32) throws {
        guard sqlite3_bind_int64(statement, index, value) == SQLITE_OK else {
            throw FantasmaError.storageFailure(lastErrorMessage())
        }
    }

    private func step(_ statement: OpaquePointer?) throws {
        let result = sqlite3_step(statement)
        guard result == SQLITE_DONE else {
            throw FantasmaError.storageFailure(lastErrorMessage())
        }
    }

    private func rowCount() throws -> Int {
        let statement = try prepare("SELECT COUNT(*) FROM events;")
        defer { sqlite3_finalize(statement) }

        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw FantasmaError.storageFailure("failed to count queued events")
        }

        return Int(sqlite3_column_int64(statement, 0))
    }

    private func readQueueState(for key: String) throws -> String? {
        let statement = try prepare("SELECT value FROM queue_state WHERE key = ? LIMIT 1;")
        defer { sqlite3_finalize(statement) }

        try bind(key, to: statement, at: 1)

        guard sqlite3_step(statement) == SQLITE_ROW else {
            return nil
        }

        guard let text = sqlite3_column_text(statement, 0) else {
            return nil
        }

        return String(cString: text)
    }

    private func writeQueueState(_ value: String, for key: String) throws {
        let statement = try prepare("""
            INSERT INTO queue_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value;
            """)
        defer { sqlite3_finalize(statement) }

        try bind(key, to: statement, at: 1)
        try bind(value, to: statement, at: 2)
        try step(statement)
    }

    private func deleteQueueState(for key: String) throws {
        let statement = try prepare("DELETE FROM queue_state WHERE key = ?;")
        defer { sqlite3_finalize(statement) }

        try bind(key, to: statement, at: 1)
        try step(statement)
    }

    private func inTransaction<T>(_ operation: () throws -> T) throws -> T {
        try execute("BEGIN IMMEDIATE TRANSACTION;")
        do {
            let result = try operation()
            try execute("COMMIT;")
            return result
        } catch {
            try? execute("ROLLBACK;")
            throw error
        }
    }

    private func lastErrorMessage() -> String {
        guard let database = database.pointer else {
            return "database is not open"
        }
        return String(cString: sqlite3_errmsg(database))
    }

    private func bind(_ value: String, to statement: OpaquePointer?, at index: Int32) throws {
        guard sqlite3_bind_text(statement, index, value, -1, sqliteTransient) == SQLITE_OK else {
            throw FantasmaError.storageFailure(lastErrorMessage())
        }
    }
}
