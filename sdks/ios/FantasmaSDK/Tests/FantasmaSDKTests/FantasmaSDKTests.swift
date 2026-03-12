import Foundation
import Testing
@testable import FantasmaSDK

@Suite("Fantasma SDK Internal Tests")
struct FantasmaSDKInternalTests {
    @Test("queue persists rows across reopen")
    func queuePersistence() throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        let queue = try harness.makeQueue()
        try queue.enqueue(payload: try harness.payload(event: "app_open", installId: "install-a"), createdAt: 1)

        let reopenedQueue = try harness.makeQueue()
        let rows = try reopenedQueue.peek(limit: 10)

        #expect(rows.count == 1)
        #expect(rows.first?.createdAt == 1)
    }

    @Test("uploader builds a 50 event batch and leaves remainder queued")
    func uploaderBatching() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        let queue = try harness.makeQueue()
        for index in 0..<55 {
            try queue.enqueue(
                payload: try harness.payload(event: "button_pressed_\(index)", installId: "install-a"),
                createdAt: Int64(index)
            )
        }

        let uploader = EventUploader(queue: queue, transport: RecordingTransport())
        let batch = try #require(await uploader.makeBatch(limit: 50))

        #expect(batch.rowIDs.count == 50)
        #expect(try queue.count() == 55)
    }
}

@Suite("Fantasma SDK Behavior Tests", .serialized)
struct FantasmaSDKBehaviorTests {
    @Test("public API persists queued event snapshots with generated identity")
    func publicAPIQueuesSnapshot() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        Fantasma.configure(serverURL: "http://localhost:8081", writeKey: "fg_ing_test")
        Fantasma.track("screen_view", properties: ["screen": "Home"])

        let queue = try harness.makeQueue()
        let row = try #require(queue.peek(limit: 1).first)
        let event = try JSONDecoder().decode(EventEnvelope.self, from: row.payload)

        #expect(event.event == "screen_view")
        #expect(event.installId == "install-a")
        #expect(event.platform == "ios")
        #expect(event.appVersion == "1.2.3")
        #expect(event.osVersion == "18.3")
        #expect(event.properties == ["screen": "Home"])
    }

    @Test("clear rotates identity for future events without mutating queued rows")
    func clearPreservesQueuedRows() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: transport)

        Fantasma.configure(serverURL: "http://localhost:8081", writeKey: "fg_ing_test")
        Fantasma.track("before_clear")
        Fantasma.clear()
        Fantasma.track("after_clear")

        let queue = try harness.makeQueue()
        let rows = try queue.peek(limit: 10)
        let first = try JSONDecoder().decode(EventEnvelope.self, from: try #require(rows.first?.payload))
        let second = try JSONDecoder().decode(EventEnvelope.self, from: try #require(rows.last?.payload))

        #expect(rows.count == 2)
        #expect(first.installId == "install-a")
        #expect(second.installId == "install-b")
    }

    @Test("flush removes queued rows after a 202 accepted response")
    func flushDeletesOnSuccess() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(transport: transport)

        Fantasma.configure(serverURL: "http://localhost:8081", writeKey: "fg_ing_test")
        Fantasma.track("app_open")
        Fantasma.flush()

        try await waitUntil {
            await transport.requests().count == 1
        }

        let queue = try harness.makeQueue()
        try await waitUntil {
            (try? queue.count()) == 0
        }
    }
    @Test("public API sends the Fantasma ingest contract to a local HTTP server")
    func ingestContractRoundTrip() async throws {
        let server = try LocalEventServer()
        defer { server.stop() }

        let serverURL = try await server.start()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        let liveTransport = URLSessionTransport(session: .shared)
        try await harness.installSharedCore(transport: liveTransport)

        Fantasma.configure(serverURL: serverURL.absoluteString, writeKey: "fg_ing_test")
        Fantasma.track("app_open", properties: ["screen": "Home"])
        Fantasma.flush()

        let request = try await server.waitForRequest()
        let body = try JSONDecoder().decode(EventBatch.self, from: request.body)
        let event = try #require(body.events.first)

        #expect(request.method == "POST")
        #expect(request.path == "/v1/events")
        #expect(request.headers["content-type"] == "application/json")
        #expect(request.headers["x-fantasma-key"] == "fg_ing_test")
        #expect(body.events.count == 1)
        #expect(event.event == "app_open")
        #expect(event.installId == "install-a")
        #expect(event.platform == "ios")
        #expect(event.osVersion == "18.3")
        #expect(event.properties == ["screen": "Home"])

        let queue = try harness.makeQueue()
        try await waitUntil {
            (try? queue.count()) == 0
        }
    }
}

private struct EventBatch: Decodable {
    let events: [EventEnvelope]
}

private actor RecordingTransport: FantasmaTransport {
    private var responses: [Result<(Data, HTTPURLResponse), Error>] = []
    private var capturedRequests: [URLRequest] = []

    func enqueue(response: Result<(Data, HTTPURLResponse), Error>) {
        responses.append(response)
    }

    func requests() -> [URLRequest] {
        capturedRequests
    }

    func send(request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        capturedRequests.append(request)
        guard !responses.isEmpty else {
            throw TestError.noPlannedResponse
        }
        return try responses.removeFirst().get()
    }
}

private final class IdentifierSequence: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String]

    init(_ values: [String]) {
        self.values = values
    }

    func next() -> String {
        lock.lock()
        defer { lock.unlock() }
        return values.removeFirst()
    }
}

private struct TestHarness {
    let rootDirectory: URL
    let databaseURL: URL
    let defaultsName: String
    let fixedDate: Date
    private let identifiers = IdentifierSequence(["install-a", "install-b", "install-c"])

    init() {
        rootDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        databaseURL = rootDirectory.appendingPathComponent("fantasma.sqlite")
        defaultsName = "FantasmaSDKTests.\(UUID().uuidString)"
        fixedDate = makeFixedDate()

        let userDefaults = UserDefaults(suiteName: defaultsName)!
        userDefaults.removePersistentDomain(forName: defaultsName)
    }

    func cleanup() {
        UserDefaults(suiteName: defaultsName)?.removePersistentDomain(forName: defaultsName)
        try? FileManager.default.removeItem(at: rootDirectory)
    }

    func makeQueue() throws -> SQLiteEventQueue {
        try SQLiteEventQueue(databaseURL: databaseURL)
    }

    func installSharedCore(transport: any FantasmaTransport) async throws {
        let userDefaults = try #require(UserDefaults(suiteName: defaultsName))
        userDefaults.removePersistentDomain(forName: defaultsName)
        let core = makeCore(userDefaults: userDefaults, transport: transport)
        await Fantasma.replaceSharedCoreForTesting(core)
    }

    func payload(event: String, installId: String) throws -> Data {
        try JSONEncoder().encode(
            EventEnvelope(
                event: event,
                timestamp: "2026-01-01T00:00:00.000Z",
                installId: installId,
                platform: "ios",
                appVersion: "1.2.3",
                osVersion: "18.3",
                properties: nil
            )
        )
    }

    private func makeCore(
        userDefaults: UserDefaults,
        transport: any FantasmaTransport
    ) -> FantasmaCore {
        let dependencies = FantasmaDependencies(
            userDefaults: userDefaults,
            databaseURL: databaseURL,
            transport: transport,
            now: { fixedDate },
            appVersion: { "1.2.3" },
            osVersion: { "18.3" },
            newIdentifier: { identifiers.next() },
            timerInterval: 3_600,
            uploadBatchSize: 50
        )
        let identityStore = IdentityStore(
            userDefaults: userDefaults,
            newIdentifier: { identifiers.next() }
        )
        let queue = try! SQLiteEventQueue(databaseURL: databaseURL)
        let uploader = EventUploader(queue: queue, transport: transport)
        return FantasmaCore(
            dependencies: dependencies,
            identityStore: identityStore,
            eventQueue: queue,
            uploader: uploader
        )
    }
}

private struct CapturedRequest: Sendable {
    let method: String
    let path: String
    let headers: [String: String]
    let body: Data
}

private final class LocalEventServer: @unchecked Sendable {
    private let rootDirectory: URL
    private let captureURL: URL
    private let readyURL: URL
    private let scriptURL: URL
    private let process = Process()

    init() throws {
        rootDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        captureURL = rootDirectory.appendingPathComponent("request.json")
        readyURL = rootDirectory.appendingPathComponent("ready.txt")
        scriptURL = rootDirectory.appendingPathComponent("server.py")

        try FileManager.default.createDirectory(at: rootDirectory, withIntermediateDirectories: true)
        try """
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

capture_path = sys.argv[1]
ready_path = sys.argv[2]

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        payload = {
            "method": "POST",
            "path": self.path,
            "headers": {key.lower(): value for key, value in self.headers.items()},
            "body": body.decode("utf-8"),
        }
        with open(capture_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)

        try:
            accepted = len(json.loads(body.decode("utf-8")).get("events", []))
        except Exception:
            accepted = 0

        response = json.dumps({"accepted": accepted}).encode("utf-8")
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format, *args):
        return

server = HTTPServer(("127.0.0.1", 0), Handler)
with open(ready_path, "w", encoding="utf-8") as handle:
    handle.write(str(server.server_port))
server.handle_request()
""".write(to: scriptURL, atomically: true, encoding: .utf8)
    }

    func start() async throws -> URL {
        process.executableURL = URL(fileURLWithPath: "/opt/homebrew/bin/python3")
        process.arguments = [scriptURL.path, captureURL.path, readyURL.path]
        try process.run()

        try await waitUntil(timeoutNanoseconds: 2_000_000_000) {
            FileManager.default.fileExists(atPath: self.readyURL.path)
        }

        let portText = try String(contentsOf: readyURL, encoding: .utf8)
        let port = Int(portText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0
        return try #require(URL(string: "http://127.0.0.1:\(port)"))
    }

    func stop() {
        if process.isRunning {
            process.terminate()
        }
        try? FileManager.default.removeItem(at: rootDirectory)
    }

    func waitForRequest() async throws -> CapturedRequest {
        try await waitUntil(timeoutNanoseconds: 2_000_000_000) {
            FileManager.default.fileExists(atPath: self.captureURL.path)
        }
        let data = try Data(contentsOf: captureURL)
        let payload = try JSONDecoder().decode(CapturedRequestFile.self, from: data)
        return CapturedRequest(
            method: payload.method,
            path: payload.path,
            headers: payload.headers,
            body: Data(payload.body.utf8)
        )
    }
}

private struct CapturedRequestFile: Decodable {
    let method: String
    let path: String
    let headers: [String: String]
    let body: String
}

private enum TestError: Error {
    case noPlannedResponse
}

private func successResponse(accepted: Int) -> (Data, HTTPURLResponse) {
    let payload = try! JSONEncoder().encode(["accepted": accepted])
    let response = HTTPURLResponse(
        url: URL(string: "http://localhost:8081/v1/events")!,
        statusCode: 202,
        httpVersion: nil,
        headerFields: nil
    )!
    return (payload, response)
}

private func makeFixedDate() -> Date {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter.date(from: "2026-01-01T00:00:00.000Z")!
}

private func waitUntil(
    timeoutNanoseconds: UInt64 = 2_000_000_000,
    condition: @escaping @Sendable () async -> Bool
) async throws {
    let deadline = DispatchTime.now().uptimeNanoseconds + timeoutNanoseconds
    while DispatchTime.now().uptimeNanoseconds < deadline {
        if await condition() {
            return
        }
        try await Task.sleep(nanoseconds: 25_000_000)
    }
    throw TestError.noPlannedResponse
}
