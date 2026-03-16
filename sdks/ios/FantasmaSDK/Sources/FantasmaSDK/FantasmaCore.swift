import Foundation

#if canImport(UIKit)
import UIKit
#endif

public enum FantasmaError: Error, Equatable, Sendable {
    case notConfigured
    case invalidWriteKey
    case unsupportedServerURL
    case invalidEventName
    case invalidPropertyCount
    case invalidPropertyName
    case reservedPropertyName
    case encodingFailed
    case invalidResponse
    case uploadFailed
    case storageFailure(String)
}

struct FantasmaConfiguration: Equatable, Sendable {
    let serverURL: URL
    let writeKey: String

    var destinationSignature: String {
        "\(serverURL.absoluteString)|\(writeKey)"
    }

    static func normalized(serverURL: URL, writeKey: String) throws -> FantasmaConfiguration {
        let normalizedWriteKey = writeKey.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedWriteKey.isEmpty else {
            throw FantasmaError.invalidWriteKey
        }

        guard var components = URLComponents(url: serverURL, resolvingAgainstBaseURL: false) else {
            throw FantasmaError.unsupportedServerURL
        }

        let normalizedScheme = components.scheme?.lowercased()
        guard normalizedScheme == "http" || normalizedScheme == "https" else {
            throw FantasmaError.unsupportedServerURL
        }

        components.scheme = normalizedScheme
        if let host = components.host {
            components.host = host.lowercased()
        }
        if components.percentEncodedPath == "/" {
            components.percentEncodedPath = ""
        } else {
            components.percentEncodedPath = components.percentEncodedPath.trimmingTrailingSlashes()
        }

        guard let normalizedURL = components.url else {
            throw FantasmaError.unsupportedServerURL
        }

        return FantasmaConfiguration(serverURL: normalizedURL, writeKey: normalizedWriteKey)
    }
}

private enum EventPropertyValidation {
    static let reservedKeys: Set<String> = [
        "project_id",
        "event",
        "timestamp",
        "install_id",
        "properties",
        "metric",
        "granularity",
        "start",
        "end",
        "start_date",
        "end_date",
        "group_by",
        "platform",
        "app_version",
        "os_version",
    ]

    static func validated(_ properties: [String: String]?) throws -> [String: String]? {
        guard let properties, !properties.isEmpty else {
            return nil
        }

        guard properties.count <= 4 else {
            throw FantasmaError.invalidPropertyCount
        }

        for key in properties.keys {
            guard isValidKey(key) else {
                throw FantasmaError.invalidPropertyName
            }
            guard !reservedKeys.contains(key) else {
                throw FantasmaError.reservedPropertyName
            }
        }

        return properties
    }

    static func isValidKey(_ key: String) -> Bool {
        guard let first = key.unicodeScalars.first, key.count <= 63 else {
            return false
        }

        guard first.value >= 97 && first.value <= 122 else {
            return false
        }

        for scalar in key.unicodeScalars.dropFirst() {
            let value = scalar.value
            let isLowercaseLetter = value >= 97 && value <= 122
            let isDigit = value >= 48 && value <= 57
            let isUnderscore = value == 95
            if !(isLowercaseLetter || isDigit || isUnderscore) {
                return false
            }
        }

        return true
    }
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
    let transport: FantasmaTransport
    let now: @Sendable () -> Date
    let appVersion: @Sendable () -> String?
    let osVersion: @Sendable () -> String?
    let newIdentifier: @Sendable () -> String
    let timerInterval: Duration
    let uploadBatchSize: Int
    let beforeEnqueue: @Sendable () async -> Void
    let beforeUploadBoundary: @Sendable () async -> Void

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
            uploadBatchSize: 50,
            beforeEnqueue: {},
            beforeUploadBoundary: {}
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
    private var flushInProgress = false
    private var pendingUpload = false
    private var trackOperationsInProgress = 0
    private var reconfigurationInProgress = false
    private var pendingReconfiguration: FantasmaConfiguration?
    private var flushWaiters: [CheckedContinuation<Void, Error>] = []
    private var reconfigurationWaiters: [CheckedContinuation<Void, Error>] = []

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

    func configure(serverURL: URL, writeKey: String) async throws {
        let configuration = try FantasmaConfiguration.normalized(serverURL: serverURL, writeKey: writeKey)

        while true {
            if reconfigurationInProgress {
                try await waitForReconfiguration()
                continue
            }

            if self.configuration == configuration {
                installTimerIfNeeded()
                installBackgroundFlushTaskIfNeeded()
                return
            }

            if flushInProgress || trackOperationsInProgress > 0 {
                reconfigurationInProgress = true
                pendingReconfiguration = configuration
                try await waitForReconfiguration()
                continue
            }

            reconfigurationInProgress = true
            do {
                try await applyConfigurationChange(to: configuration)
                finishReconfiguration()
                return
            } catch {
                failReconfiguration(error)
                throw error
            }
        }
    }

    func track(_ eventName: String, properties: [String: String]?) async throws {
        guard configuration != nil else {
            throw FantasmaError.notConfigured
        }

        let name = eventName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else {
            throw FantasmaError.invalidEventName
        }

        trackOperationsInProgress += 1
        do {
            let validatedProperties = try EventPropertyValidation.validated(properties)

            let now = dependencies.now()
            let identity = await currentIdentity()
            let event = EventEnvelope(
                event: name,
                timestamp: FantasmaJSON.timestamp(from: now),
                installId: identity.installID,
                platform: "ios",
                appVersion: dependencies.appVersion(),
                osVersion: dependencies.osVersion(),
                properties: validatedProperties
            )

            let payload: Data
            do {
                payload = try JSONEncoder().encode(event)
            } catch {
                throw FantasmaError.encodingFailed
            }

            await dependencies.beforeEnqueue()
            guard let configuration else {
                throw FantasmaError.notConfigured
            }
            try await eventQueue.enqueue(
                payload: payload,
                createdAt: Int64(now.timeIntervalSince1970),
                destinationSignature: configuration.destinationSignature
            )

            if try await eventQueue.count() >= dependencies.uploadBatchSize {
                scheduleBackgroundFlush()
            }
        } catch {
            trackOperationsInProgress -= 1
            _ = try? await applyPendingReconfigurationIfNeeded()
            throw error
        }

        trackOperationsInProgress -= 1
        _ = try await applyPendingReconfigurationIfNeeded()
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
        guard !flushInProgress else {
            pendingUpload = true
            if waitForInFlight {
                try await withCheckedThrowingContinuation { continuation in
                    flushWaiters.append(continuation)
                }
            }
            return
        }

        guard configuration != nil else {
            if allowUnconfigured {
                return
            }
            throw FantasmaError.notConfigured
        }

        do {
            flushInProgress = true
            defer { flushInProgress = false }

            while true {
                if try await applyPendingReconfigurationIfNeeded(allowWhileFlushing: true) {
                    resumeFlushWaiters()
                    return
                }

                pendingUpload = false

                guard let configuration = self.configuration else {
                    if allowUnconfigured {
                        resumeFlushWaiters()
                        return
                    }
                    throw FantasmaError.notConfigured
                }

                guard let batch = try await uploader.makeBatch(limit: dependencies.uploadBatchSize) else {
                    if try await applyPendingReconfigurationIfNeeded(allowWhileFlushing: true) {
                        resumeFlushWaiters()
                        return
                    }
                    resumeFlushWaiters()
                    return
                }

                await dependencies.beforeUploadBoundary()
                if try await applyPendingReconfigurationIfNeeded(allowWhileFlushing: true) {
                    resumeFlushWaiters()
                    return
                }

                do {
                    try await uploader.upload(batch, configuration: configuration)
                } catch {
                    do {
                        _ = try await applyPendingReconfigurationIfNeeded(allowWhileFlushing: true)
                    } catch {
                        failFlushWaiters(error)
                        throw error
                    }

                    failFlushWaiters(error)
                    throw error
                }

                if try await applyPendingReconfigurationIfNeeded(allowWhileFlushing: true) {
                    resumeFlushWaiters()
                    return
                }

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

    private func applyConfigurationChange(to configuration: FantasmaConfiguration) async throws {
        let persistedDestinationSignature = try await eventQueue.destinationSignature()
        let shouldDiscardQueuedRows =
            persistedDestinationSignature != nil
            && persistedDestinationSignature != configuration.destinationSignature
        if shouldDiscardQueuedRows {
            try await eventQueue.reset()
            pendingUpload = false
        }

        self.configuration = configuration
        pendingReconfiguration = nil
        installTimerIfNeeded()
        installBackgroundFlushTaskIfNeeded()
    }

    private func applyPendingReconfigurationIfNeeded(
        allowWhileFlushing: Bool = false
    ) async throws -> Bool {
        guard let configuration = pendingReconfiguration else {
            return false
        }

        guard (allowWhileFlushing || !flushInProgress) && trackOperationsInProgress == 0 else {
            return false
        }

        do {
            try await applyConfigurationChange(to: configuration)
            finishReconfiguration()
            return true
        } catch {
            failReconfiguration(error)
            throw error
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

    private func waitForReconfiguration() async throws {
        try await withCheckedThrowingContinuation { continuation in
            reconfigurationWaiters.append(continuation)
        }
    }

    private func finishReconfiguration() {
        reconfigurationInProgress = false
        let waiters = reconfigurationWaiters
        reconfigurationWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func failReconfiguration(_ error: Error) {
        reconfigurationInProgress = false
        pendingReconfiguration = nil
        let waiters = reconfigurationWaiters
        reconfigurationWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume(throwing: error)
        }
    }
}

private extension String {
    func trimmingTrailingSlashes() -> String {
        guard !isEmpty else {
            return self
        }

        var value = self
        while value.count > 1 && value.hasSuffix("/") {
            value.removeLast()
        }
        return value == "/" ? "" : value
    }
}
