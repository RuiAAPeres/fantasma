import Foundation

internal protocol FantasmaTransport: Sendable {
    func send(request: URLRequest) async throws -> (Data, HTTPURLResponse)
}

internal final class URLSessionTransport: FantasmaTransport, @unchecked Sendable {
    private let session: URLSession

    init(session: URLSession = .shared) {
        self.session = session
    }

    func send(request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await session.data(for: request)
        guard let response = response as? HTTPURLResponse else {
            throw FantasmaSDKError.invalidResponse
        }
        return (data, response)
    }
}
