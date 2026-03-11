import Foundation

private actor FantasmaRuntime {
    private var core = FantasmaCore.live()

    func currentCore() -> FantasmaCore {
        core
    }

    func replaceCore(_ newCore: FantasmaCore) async {
        let previousCore = core
        core = newCore
        await previousCore.shutdown()
    }
}

public enum Fantasma {
    private static let runtime = FantasmaRuntime()

    public static func configure(serverURL: String, writeKey: String) {
        wait {
            let core = await runtime.currentCore()
            await core.configure(serverURL: serverURL, writeKey: writeKey)
        }
    }

    public static func track(_ eventName: String, properties: [String: String]? = nil) {
        wait {
            let core = await runtime.currentCore()
            await core.track(eventName, properties: properties)
        }
    }

    public static func identify(_ userId: String) {
        wait {
            let core = await runtime.currentCore()
            await core.identify(userId)
        }
    }

    public static func flush() {
        Task {
            let core = await runtime.currentCore()
            await core.flush()
        }
    }

    public static func clear() {
        wait {
            let core = await runtime.currentCore()
            await core.clear()
        }
    }

    internal static func replaceSharedCoreForTesting(_ core: FantasmaCore) async {
        await runtime.replaceCore(core)
    }

    internal static func resetSharedCoreForTesting() async {
        await runtime.replaceCore(FantasmaCore.live())
    }

    private static func wait(
        _ operation: @escaping @Sendable () async -> Void
    ) {
        let semaphore = DispatchSemaphore(value: 0)
        Task {
            await operation()
            semaphore.signal()
        }
        semaphore.wait()
    }
}
