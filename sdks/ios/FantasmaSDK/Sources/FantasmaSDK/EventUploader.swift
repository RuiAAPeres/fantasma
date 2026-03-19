import Foundation

enum UploadDisposition: Sendable, Equatable {
    case success
    case retryableFailure
    case blockedDestination
}

private struct UploadErrorEnvelope: Decodable {
    let error: String
}

struct EventUploader: Sendable {
    private let queue: SQLiteEventQueue
    private let transport: FantasmaTransport
    private let afterBuildBatch: @Sendable () async -> Void

    init(
        queue: SQLiteEventQueue,
        transport: FantasmaTransport,
        afterBuildBatch: @escaping @Sendable () async -> Void = {}
    ) {
        self.queue = queue
        self.transport = transport
        self.afterBuildBatch = afterBuildBatch
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

        await afterBuildBatch()
        return UploadBatch(rowIDs: ids, body: body, count: rows.count)
    }

    func upload(_ batch: UploadBatch, configuration: FantasmaConfiguration) async throws -> UploadDisposition {
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
            return .retryableFailure
        } catch {
            return .retryableFailure
        }

        guard response.statusCode == 202 else {
            if response.statusCode == 409,
               let envelope = try? JSONDecoder().decode(UploadErrorEnvelope.self, from: data),
               envelope.error == "project_pending_deletion"
            {
                return .blockedDestination
            }
            return .retryableFailure
        }

        let accepted: AcceptedResponse
        do {
            accepted = try JSONDecoder().decode(AcceptedResponse.self, from: data)
        } catch {
            throw FantasmaError.invalidResponse
        }

        guard accepted.accepted == batch.count else {
            throw FantasmaError.invalidResponse
        }

        try await queue.delete(ids: batch.rowIDs)
        return .success
    }
}
