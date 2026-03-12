import Foundation

#if canImport(UIKit)
import UIKit
#endif

public enum FantasmaError: Error, Equatable, Sendable {
    case notConfigured
    case invalidWriteKey
    case unsupportedServerURL
    case invalidEventName
    case encodingFailed
    case invalidResponse
    case uploadFailed
    case storageFailure(String)
}

struct FantasmaConfiguration: Sendable {
    let serverURL: URL
    let writeKey: String
}

struct EventEnvelope: Codable, Equatable, Sendable {
    let event: String
    let timestamp: String
    let installId: String
    let platform: String
    let appVersion: String?
    let osVersion: String?
    let properties: [String: String]?

    enum CodingKeys: String, CodingKey {
        case event
        case timestamp
        case installId = "install_id"
        case platform
        case appVersion = "app_version"
        case osVersion = "os_version"
        case properties
    }
}

struct AcceptedResponse: Decodable, Equatable, Sendable {
    let accepted: Int
}

struct UploadBatch: Sendable {
    let rowIDs: [Int64]
    let body: Data
    let count: Int
}

struct FantasmaDependencies: Sendable {
    let databaseURL: URL
    let transport: any FantasmaTransport
    let now: @Sendable () -> Date
    let appVersion: @Sendable () -> String?
    let osVersion: @Sendable () -> String?
    let newIdentifier: @Sendable () -> String
    let timerInterval: Duration
    let uploadBatchSize: Int

    static func live() -> FantasmaDependencies {
        FantasmaDependencies(
            databaseURL: defaultDatabaseURL(),
            transport: URLSessionTransport(session: .shared),
            now: { Date() },
            appVersion: {
                let bundle = Bundle.main
                return bundle.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String
                    ?? bundle.object(forInfoDictionaryKey: "CFBundleVersion") as? String
            },
            osVersion: {
                let version = ProcessInfo.processInfo.operatingSystemVersion
                if version.patchVersion > 0 {
                    return "\(version.majorVersion).\(version.minorVersion).\(version.patchVersion)"
                }
                return "\(version.majorVersion).\(version.minorVersion)"
            },
            newIdentifier: { UUID().uuidString.lowercased() },
            timerInterval: .seconds(10),
            uploadBatchSize: 50
        )
    }

    private static func defaultDatabaseURL() -> URL {
        let fileManager = FileManager.default
        let baseURL = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? fileManager.temporaryDirectory
        let directoryURL = baseURL.appendingPathComponent("FantasmaSDK", isDirectory: true)
        try? fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        return directoryURL.appendingPathComponent("events.sqlite3")
    }
}

enum FantasmaJSON {
    static func timestamp(from date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: date)
    }
}

actor FantasmaCore {
    private let dependencies: FantasmaDependencies
    private let identityStore: IdentityStore
    private let eventQueue: SQLiteEventQueue
    private let uploader: EventUploader

    private var configuration: FantasmaConfiguration?
    private var identity: IdentityState?
    private var periodicFlushTask: Task<Void, Never>?
    private var backgroundFlushTask: Task<Void, Never>?
    private var uploadInFlight = false
    private var pendingUpload = false
    private var flushWaiters: [CheckedContinuation<Void, Error>] = []

    static func live() throws -> FantasmaCore {
        let dependencies = FantasmaDependencies.live()
        let identityStore = IdentityStore(
            provider: .standard,
            newIdentifier: dependencies.newIdentifier
        )
        let queue = try SQLiteEventQueue(databaseURL: dependencies.databaseURL)
        let uploader = EventUploader(queue: queue, transport: dependencies.transport)
        return FantasmaCore(
            dependencies: dependencies,
            identityStore: identityStore,
            eventQueue: queue,
            uploader: uploader
        )
    }

    init(
        dependencies: FantasmaDependencies,
        identityStore: IdentityStore,
        eventQueue: SQLiteEventQueue,
        uploader: EventUploader
    ) {
        self.dependencies = dependencies
        self.identityStore = identityStore
        self.eventQueue = eventQueue
        self.uploader = uploader
    }

    func shutdown() {
        periodicFlushTask?.cancel()
        periodicFlushTask = nil
        backgroundFlushTask?.cancel()
        backgroundFlushTask = nil
    }

    func configure(serverURL: URL, writeKey: String) throws {
        let normalizedWriteKey = writeKey.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedWriteKey.isEmpty else {
            throw FantasmaError.invalidWriteKey
        }

        let normalizedScheme = serverURL.scheme?.lowercased()
        guard normalizedScheme == "http" || normalizedScheme == "https" else {
            throw FantasmaError.unsupportedServerURL
        }

        configuration = FantasmaConfiguration(serverURL: serverURL, writeKey: normalizedWriteKey)
        installTimerIfNeeded()
        installBackgroundFlushTaskIfNeeded()
    }

    func track(_ eventName: String, properties: [String: String]?) async throws {
        guard configuration != nil else {
            throw FantasmaError.notConfigured
        }

        let name = eventName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else {
            throw FantasmaError.invalidEventName
        }

        let now = dependencies.now()
        let identity = await currentIdentity()
        let event = EventEnvelope(
            event: name,
            timestamp: FantasmaJSON.timestamp(from: now),
            installId: identity.installID,
            platform: "ios",
            appVersion: dependencies.appVersion(),
            osVersion: dependencies.osVersion(),
            properties: properties?.isEmpty == false ? properties : nil
        )

        let payload: Data
        do {
            payload = try JSONEncoder().encode(event)
        } catch {
            throw FantasmaError.encodingFailed
        }

        try await eventQueue.enqueue(payload: payload, createdAt: Int64(now.timeIntervalSince1970))

        if try await eventQueue.count() >= dependencies.uploadBatchSize {
            scheduleBackgroundFlush()
        }
    }

    func clear() async {
        identity = await identityStore.clear()
    }

    func flush() async throws {
        try await flush(allowUnconfigured: false, waitForInFlight: true)
    }

    private func installTimerIfNeeded() {
        guard periodicFlushTask == nil else {
            return
        }

        let interval = dependencies.timerInterval
        periodicFlushTask = Task {
            let clock = ContinuousClock()
            while !Task.isCancelled {
                try? await clock.sleep(for: interval)
                try? await self.flush(allowUnconfigured: true, waitForInFlight: false)
            }
        }
    }

    private func installBackgroundFlushTaskIfNeeded() {
        guard backgroundFlushTask == nil else {
            return
        }

        #if canImport(UIKit)
        backgroundFlushTask = Task {
            for await _ in NotificationCenter.default.notifications(
                named: UIApplication.didEnterBackgroundNotification
            ) {
                if Task.isCancelled {
                    return
                }
                try? await self.flush(allowUnconfigured: true, waitForInFlight: false)
            }
        }
        #endif
    }

    private func currentIdentity() async -> IdentityState {
        if let identity {
            return identity
        }

        let identity = await identityStore.load()
        self.identity = identity
        return identity
    }

    private func flush(
        allowUnconfigured: Bool,
        waitForInFlight: Bool
    ) async throws {
        guard !uploadInFlight else {
            pendingUpload = true
            if waitForInFlight {
                try await withCheckedThrowingContinuation { continuation in
                    flushWaiters.append(continuation)
                }
            }
            return
        }

        guard let configuration else {
            if allowUnconfigured {
                return
            }
            throw FantasmaError.notConfigured
        }

        do {
            uploadInFlight = true
            defer { uploadInFlight = false }

            while true {
                pendingUpload = false

                guard let batch = try await uploader.makeBatch(limit: dependencies.uploadBatchSize) else {
                    resumeFlushWaiters()
                    return
                }

                try await uploader.upload(batch, configuration: configuration)
                let queueCount = try await eventQueue.count()
                if !(pendingUpload || queueCount > 0) {
                    resumeFlushWaiters()
                    return
                }
            }
        } catch {
            failFlushWaiters(error)
            throw error
        }
    }

    private func scheduleBackgroundFlush() {
        Task {
            try? await self.flush(allowUnconfigured: true, waitForInFlight: false)
        }
    }

    private func resumeFlushWaiters() {
        let waiters = flushWaiters
        flushWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func failFlushWaiters(_ error: Error) {
        let waiters = flushWaiters
        flushWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume(throwing: error)
        }
    }
}
