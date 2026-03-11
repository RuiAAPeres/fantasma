import CSQLite
import Foundation

private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

internal struct QueuedEventRow: Equatable, Sendable {
    let id: Int64
    let payload: Data
    let createdAt: Int64
}

internal final class SQLiteEventQueue: @unchecked Sendable {
    private var database: OpaquePointer?

    init(databaseURL: URL) throws {
        try FileManager.default.createDirectory(
            at: databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        var database: OpaquePointer?
        if sqlite3_open_v2(databaseURL.path, &database, flags, nil) != SQLITE_OK {
            let message = database.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "unknown sqlite error"
            sqlite3_close(database)
            throw FantasmaSDKError.sqlite(message)
        }

        self.database = database

        try execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload BLOB NOT NULL,
                created_at INTEGER NOT NULL
            );
            """)
    }

    deinit {
        sqlite3_close(database)
    }

    func enqueue(payload: Data, createdAt: Int64) throws {
        let statement = try prepare("INSERT INTO events (payload, created_at) VALUES (?, ?);")
        defer { sqlite3_finalize(statement) }

        try bind(payload, to: statement, at: 1)
        try bind(createdAt, to: statement, at: 2)
        try step(statement)
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
            throw FantasmaSDKError.sqlite("failed to count queued events")
        }

        return Int(sqlite3_column_int64(statement, 0))
    }

    func delete(ids: [Int64]) throws {
        guard !ids.isEmpty else {
            return
        }

        let placeholders = ids.map { _ in "?" }.joined(separator: ",")
        let statement = try prepare("DELETE FROM events WHERE id IN (\(placeholders));")
        defer { sqlite3_finalize(statement) }

        for (index, id) in ids.enumerated() {
            try bind(id, to: statement, at: Int32(index + 1))
        }

        try step(statement)
    }

    private func execute(_ sql: String) throws {
        var errorMessage: UnsafeMutablePointer<Int8>?
        if sqlite3_exec(database, sql, nil, nil, &errorMessage) != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "unknown sqlite error"
            sqlite3_free(errorMessage)
            throw FantasmaSDKError.sqlite(message)
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer? {
        var statement: OpaquePointer?
        if sqlite3_prepare_v2(database, sql, -1, &statement, nil) != SQLITE_OK {
            throw FantasmaSDKError.sqlite(lastErrorMessage())
        }
        return statement
    }

    private func bind(_ value: Data, to statement: OpaquePointer?, at index: Int32) throws {
        let result = value.withUnsafeBytes { buffer in
            sqlite3_bind_blob(statement, index, buffer.baseAddress, Int32(value.count), sqliteTransient)
        }
        guard result == SQLITE_OK else {
            throw FantasmaSDKError.sqlite(lastErrorMessage())
        }
    }

    private func bind(_ value: Int64, to statement: OpaquePointer?, at index: Int32) throws {
        guard sqlite3_bind_int64(statement, index, value) == SQLITE_OK else {
            throw FantasmaSDKError.sqlite(lastErrorMessage())
        }
    }

    private func step(_ statement: OpaquePointer?) throws {
        let result = sqlite3_step(statement)
        guard result == SQLITE_DONE else {
            throw FantasmaSDKError.sqlite(lastErrorMessage())
        }
    }

    private func lastErrorMessage() -> String {
        guard let database else {
            return "database is not open"
        }
        return String(cString: sqlite3_errmsg(database))
    }
}
