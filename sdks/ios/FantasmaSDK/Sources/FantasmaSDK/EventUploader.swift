import Foundation

struct EventUploader: Sendable {
    private let queue: SQLiteEventQueue
    private let transport: any FantasmaTransport

    init(queue: SQLiteEventQueue, transport: any FantasmaTransport) {
        self.queue = queue
        self.transport = transport
    }

    func makeBatch(limit: Int) async throws -> UploadBatch? {
        let rows = try await queue.peek(limit: limit)
        guard !rows.isEmpty else {
            return nil
        }

        let ids = rows.map(\.id)
        var body = Data("{\"events\":[".utf8)
        for (index, row) in rows.enumerated() {
            if index > 0 {
                body.append(0x2C)
            }
            body.append(row.payload)
        }
        body.append(Data("]}".utf8))

        return UploadBatch(rowIDs: ids, body: body, count: rows.count)
    }

    func upload(_ batch: UploadBatch, configuration: FantasmaConfiguration) async throws {
        var request = URLRequest(url: configuration.serverURL.appendingPathComponent("v1/events"))
        request.httpMethod = "POST"
        request.httpBody = batch.body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(configuration.writeKey, forHTTPHeaderField: "X-Fantasma-Key")

        let data: Data
        let response: HTTPURLResponse

        do {
            (data, response) = try await transport.send(request: request)
        } catch let error as FantasmaError {
            if error == .invalidResponse {
                throw error
            }
            throw FantasmaError.uploadFailed
        } catch {
            throw FantasmaError.uploadFailed
        }

        guard response.statusCode == 202 else {
            throw FantasmaError.uploadFailed
        }

        guard
            let accepted = try? JSONDecoder().decode(AcceptedResponse.self, from: data),
            accepted.accepted == batch.count
        else {
            throw FantasmaError.uploadFailed
        }

        try await queue.delete(ids: batch.rowIDs)
    }
}
