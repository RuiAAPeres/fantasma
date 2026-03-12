import Foundation

struct IdentityState: Equatable, Sendable {
    let installID: String
}

struct UserDefaultsProvider: Sendable {
    let suiteName: String?

    static let standard = UserDefaultsProvider(suiteName: nil)

    func userDefaults() -> UserDefaults {
        if let suiteName, let userDefaults = UserDefaults(suiteName: suiteName) {
            return userDefaults
        }
        return .standard
    }
}

actor IdentityStore {
    private enum Keys {
        static let installID = "dev.fantasma.sdk.install-id"
    }

    private let provider: UserDefaultsProvider
    private let newIdentifier: @Sendable () -> String

    init(provider: UserDefaultsProvider, newIdentifier: @escaping @Sendable () -> String) {
        self.provider = provider
        self.newIdentifier = newIdentifier
    }

    func load() -> IdentityState {
        let installID = readOrCreate(key: Keys.installID)
        return IdentityState(installID: installID)
    }

    func clear() -> IdentityState {
        let installID = newIdentifier()
        provider.userDefaults().set(installID, forKey: Keys.installID)
        return IdentityState(installID: installID)
    }

    private func readOrCreate(key: String) -> String {
        let userDefaults = provider.userDefaults()
        if let existing = normalized(userDefaults.string(forKey: key)) {
            return existing
        }

        let value = newIdentifier()
        userDefaults.set(value, forKey: key)
        return value
    }

    private func normalized(_ value: String?) -> String? {
        guard let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines), !trimmed.isEmpty else {
            return nil
        }
        return trimmed
    }
}
