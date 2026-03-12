import Foundation

#if canImport(UIKit)
import UIKit
#endif

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

enum FantasmaSDKError: Error, Equatable {
    case invalidResponse
    case sqlite(String)
}

struct FantasmaDependencies: @unchecked Sendable {
    let userDefaults: UserDefaults
    let databaseURL: URL
    let transport: any FantasmaTransport
    let now: @Sendable () -> Date
    let appVersion: @Sendable () -> String?
    let osVersion: @Sendable () -> String?
    let newIdentifier: @Sendable () -> String
    let timerInterval: TimeInterval
    let uploadBatchSize: Int

    static func live() -> FantasmaDependencies {
        FantasmaDependencies(
            userDefaults: .standard,
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
            timerInterval: 10,
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
    private var identity: IdentityState
    private var periodicFlushTask: Task<Void, Never>?
    private var uploadInFlight = false
    private var pendingUpload = false
    private var backgroundObserver: NSObjectProtocol?

    static func live() -> FantasmaCore {
        let dependencies = FantasmaDependencies.live()
        let identityStore = IdentityStore(
            userDefaults: dependencies.userDefaults,
            newIdentifier: dependencies.newIdentifier
        )
        let queue = try! SQLiteEventQueue(databaseURL: dependencies.databaseURL)
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
        self.identity = identityStore.load()
    }

    func shutdown() {
        periodicFlushTask?.cancel()
        periodicFlushTask = nil
        if let backgroundObserver {
            NotificationCenter.default.removeObserver(backgroundObserver)
            self.backgroundObserver = nil
        }
    }

    func configure(serverURL: String, writeKey: String) {
        guard
            let normalizedURL = URL(string: serverURL.trimmingCharacters(in: .whitespacesAndNewlines)),
            !writeKey.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else {
            return
        }

        configuration = FantasmaConfiguration(serverURL: normalizedURL, writeKey: writeKey)
        installTimerIfNeeded()
        installBackgroundObserverIfNeeded()
    }

    func track(_ eventName: String, properties: [String: String]?) {
        guard configuration != nil else {
            return
        }

        let name = eventName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else {
            return
        }

        let now = dependencies.now()
        let event = EventEnvelope(
            event: name,
            timestamp: FantasmaJSON.timestamp(from: now),
            installId: identity.installID,
            platform: "ios",
            appVersion: dependencies.appVersion(),
            osVersion: dependencies.osVersion(),
            properties: properties?.isEmpty == false ? properties : nil
        )

        guard let payload = try? JSONEncoder().encode(event) else {
            return
        }

        try? eventQueue.enqueue(payload: payload, createdAt: Int64(now.timeIntervalSince1970))
        if let queuedCount = try? eventQueue.count(), queuedCount >= dependencies.uploadBatchSize {
            Task {
                await flush()
            }
        }
    }

    func clear() {
        identity = identityStore.clear()
    }

    func flush() async {
        guard !uploadInFlight else {
            pendingUpload = true
            return
        }

        uploadInFlight = true
        defer { uploadInFlight = false }

        while true {
            pendingUpload = false

            let batch: UploadBatch
            do {
                guard let nextBatch = try await uploader.makeBatch(limit: dependencies.uploadBatchSize) else {
                    return
                }
                batch = nextBatch
            } catch {
                return
            }

            guard let configuration else {
                return
            }

            let succeeded = await uploader.upload(batch, configuration: configuration)
            let queueCount = (try? eventQueue.count()) ?? 0
            if !(pendingUpload || (succeeded && queueCount > 0)) {
                return
            }
        }
    }

    private func installTimerIfNeeded() {
        guard periodicFlushTask == nil else {
            return
        }

        let intervalNanoseconds = UInt64(dependencies.timerInterval * 1_000_000_000)
        periodicFlushTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: intervalNanoseconds)
                guard let self else {
                    return
                }
                await self.flush()
            }
        }
    }

    private func installBackgroundObserverIfNeeded() {
        guard backgroundObserver == nil else {
            return
        }

        #if canImport(UIKit)
        backgroundObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.didEnterBackgroundNotification,
            object: nil,
            queue: nil
        ) { [weak self] _ in
            guard let self else {
                return
            }

            Task {
                await self.flush()
            }
        }
        #endif
    }
}
