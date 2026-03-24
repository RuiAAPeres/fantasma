import Foundation

internal protocol FantasmaReactNativeBridgeCore: Actor {
    func configure(configuration: FantasmaConfiguration) async throws
    func track(_ eventName: String) async throws
    func flush() async throws
    func clear() async
    func shutdown() async
}

extension FantasmaCore: FantasmaReactNativeBridgeCore {
    func configure(configuration: FantasmaConfiguration) async throws {
        try await configure(
            serverURL: configuration.serverURL,
            writeKey: configuration.writeKey
        )
    }
}

public actor FantasmaReactNativeBridge {
    public static let shared = FantasmaReactNativeBridge()

    private let makeCore: @Sendable () async throws -> FantasmaReactNativeBridgeCore

    private var activeCore: FantasmaReactNativeBridgeCore?
    private var activeConfiguration: FantasmaConfiguration?
    private var closingCore: FantasmaReactNativeBridgeCore?
    private var operationCount = 0

    public init() {
        self.makeCore = { try FantasmaCore.live() }
    }

    internal init(
        makeCore: @escaping @Sendable () async throws -> FantasmaReactNativeBridgeCore
    ) {
        self.makeCore = makeCore
    }

    public func open(serverURL: URL, writeKey: String) async throws {
        let configuration = try FantasmaConfiguration.normalized(
            serverURL: serverURL,
            writeKey: writeKey
        )

        if let activeConfiguration, activeConfiguration == configuration, activeCore != nil {
            return
        }

        try await performOpen(configuration)
    }

    public func track(_ eventName: String) async throws {
        let core = try await beginOperation()
        do {
            try await core.track(eventName)
        } catch {
            await finishOperation()
            throw error
        }
        await finishOperation()
    }

    public func flush() async throws {
        let core = try await beginOperation()
        do {
            try await core.flush()
        } catch {
            await finishOperation()
            throw error
        }
        await finishOperation()
    }

    public func clear() async throws {
        let core = try await beginOperation()
        await core.clear()
        await finishOperation()
    }

    public func close() async {
        if operationCount == 0 {
            let core = activeCore ?? closingCore
            activeCore = nil
            activeConfiguration = nil
            closingCore = nil
            await core?.shutdown()
            return
        }

        if closingCore == nil {
            closingCore = activeCore
            activeCore = nil
            activeConfiguration = nil
        }
    }

    private func performOpen(_ configuration: FantasmaConfiguration) async throws {
        if let activeCore {
            try await activeCore.configure(configuration: configuration)
            activeConfiguration = configuration
            return
        }

        if let closingCore {
            self.closingCore = nil
            activeCore = closingCore
            try await closingCore.configure(configuration: configuration)
            activeConfiguration = configuration
            return
        }

        let core = try await makeCore()
        try await core.configure(configuration: configuration)
        activeCore = core
        activeConfiguration = configuration
    }

    private func beginOperation() async throws -> FantasmaReactNativeBridgeCore {
        guard let core = activeCore else {
            throw FantasmaError.notConfigured
        }

        operationCount += 1
        return core
    }

    private func finishOperation() async {
        operationCount -= 1
        guard operationCount == 0, activeCore == nil, let core = closingCore else {
            return
        }

        closingCore = nil
        await core.shutdown()
    }
}
