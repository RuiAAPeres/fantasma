import Foundation

struct IdentityState: Equatable, Sendable {
    let installID: String
}

final class IdentityStore: @unchecked Sendable {
    private enum Keys {
        static let installID = "dev.fantasma.sdk.install-id"
    }

    private let userDefaults: UserDefaults
    private let newIdentifier: @Sendable () -> String

    init(userDefaults: UserDefaults, newIdentifier: @escaping @Sendable () -> String) {
        self.userDefaults = userDefaults
        self.newIdentifier = newIdentifier
    }

    func load() -> IdentityState {
        let installID = readOrCreate(key: Keys.installID)
        return IdentityState(installID: installID)
    }

    func clear() -> IdentityState {
        let installID = newIdentifier()
        userDefaults.set(installID, forKey: Keys.installID)
        return IdentityState(installID: installID)
    }

    private func readOrCreate(key: String) -> String {
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
