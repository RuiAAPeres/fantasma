import Foundation

actor EventUploader {
    private let queue: SQLiteEventQueue
    private let transport: any FantasmaTransport

    init(queue: SQLiteEventQueue, transport: any FantasmaTransport) {
        self.queue = queue
        self.transport = transport
    }

    func makeBatch(limit: Int) throws -> UploadBatch? {
        let rows = try queue.peek(limit: limit)
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

    func upload(_ batch: UploadBatch, configuration: FantasmaConfiguration) async -> Bool {
        var request = URLRequest(url: configuration.serverURL.appendingPathComponent("v1/events"))
        request.httpMethod = "POST"
        request.httpBody = batch.body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(configuration.writeKey, forHTTPHeaderField: "X-Fantasma-Key")

        do {
            let (data, response) = try await transport.send(request: request)
            guard response.statusCode == 202 else {
                return false
            }

            guard
                let accepted = try? JSONDecoder().decode(AcceptedResponse.self, from: data),
                accepted.accepted == batch.count
            else {
                return false
            }

            try queue.delete(ids: batch.rowIDs)
            return true
        } catch {
            return false
        }
    }
}
