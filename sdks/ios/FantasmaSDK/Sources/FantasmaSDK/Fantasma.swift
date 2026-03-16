import Foundation

private struct FantasmaRuntimeEnvironment: Sendable {
    let makeCore: @Sendable () throws -> FantasmaCore
    let clearIdentity: @Sendable () async -> Void

    static let live = FantasmaRuntimeEnvironment(
        makeCore: { try FantasmaCore.live() },
        clearIdentity: {
            let store = IdentityStore(
                provider: .standard,
                newIdentifier: { UUID().uuidString.lowercased() }
            )
            _ = await store.clear()
        }
    )
}

private actor FantasmaRuntime {
    private var core: FantasmaCore?
    private var environment = FantasmaRuntimeEnvironment.live

    func sharedCore() throws -> FantasmaCore {
        if let core {
            return core
        }

        let core = try environment.makeCore()
        self.core = core
        return core
    }

    func replaceCore(_ newCore: FantasmaCore?) async {
        let previousCore = core
        core = newCore
        if let previousCore {
            await previousCore.shutdown()
        }
    }

    func clear() async {
        if let core {
            await core.clear()
            return
        }

        await environment.clearIdentity()
    }

    func installEnvironment(_ environment: FantasmaRuntimeEnvironment) {
        self.environment = environment
    }

    func resetEnvironment() {
        environment = .live
    }
}

public enum Fantasma {
    private static let runtime = FantasmaRuntime()

    public static func configure(serverURL: URL, writeKey: String) async throws {
        let core = try await runtime.sharedCore()
        try await core.configure(serverURL: serverURL, writeKey: writeKey)
    }

    public static func track(_ eventName: String, properties: [String: String]? = nil) async throws {
        let core = try await runtime.sharedCore()
        try await core.track(eventName, properties: properties)
    }

    public static func flush() async throws {
        let core = try await runtime.sharedCore()
        try await core.flush()
    }

    public static func clear() async {
        await runtime.clear()
    }

    internal static func replaceSharedCoreForTesting(_ core: FantasmaCore) async {
        await runtime.replaceCore(core)
    }

    internal static func resetSharedCoreForTesting() async {
        await runtime.replaceCore(nil)
    }

    internal static func installRuntimeEnvironmentForTesting(
        makeCore: @escaping @Sendable () throws -> FantasmaCore,
        clearIdentity: @escaping @Sendable () async -> Void
    ) async {
        await runtime.installEnvironment(
            FantasmaRuntimeEnvironment(
                makeCore: makeCore,
                clearIdentity: clearIdentity
            )
        )
    }

    internal static func pendingConfigurationMatchesForTesting(
        serverURL: URL,
        writeKey: String
    ) async throws -> Bool {
        let configuration = try FantasmaConfiguration.normalized(
            serverURL: serverURL,
            writeKey: writeKey
        )
        let core = try await runtime.sharedCore()
        return await core.pendingConfigurationMatchesForTesting(configuration)
    }

    internal static func resetRuntimeEnvironmentForTesting() async {
        await runtime.resetEnvironment()
        await runtime.replaceCore(nil)
    }
}
