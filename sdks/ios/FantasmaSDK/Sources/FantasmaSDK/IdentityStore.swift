import Foundation

struct IdentityState: Equatable, Sendable {
    let installID: String
    let userID: String?
    let sessionID: String
}

final class IdentityStore: @unchecked Sendable {
    private enum Keys {
        static let installID = "dev.fantasma.sdk.install-id"
        static let userID = "dev.fantasma.sdk.user-id"
        static let sessionID = "dev.fantasma.sdk.session-id"
    }

    private let userDefaults: UserDefaults
    private let newIdentifier: @Sendable () -> String

    init(userDefaults: UserDefaults, newIdentifier: @escaping @Sendable () -> String) {
        self.userDefaults = userDefaults
        self.newIdentifier = newIdentifier
    }

    func load() -> IdentityState {
        let installID = readOrCreate(key: Keys.installID)
        let sessionID = readOrCreate(key: Keys.sessionID)
        let userID = normalized(userDefaults.string(forKey: Keys.userID))
        return IdentityState(installID: installID, userID: userID, sessionID: sessionID)
    }

    func identify(_ userID: String) -> IdentityState {
        let value = normalized(userID)
        userDefaults.set(value, forKey: Keys.userID)
        let state = load()
        return IdentityState(installID: state.installID, userID: value, sessionID: state.sessionID)
    }

    func clear() -> IdentityState {
        let installID = newIdentifier()
        let sessionID = newIdentifier()
        userDefaults.set(installID, forKey: Keys.installID)
        userDefaults.set(sessionID, forKey: Keys.sessionID)
        userDefaults.removeObject(forKey: Keys.userID)
        return IdentityState(installID: installID, userID: nil, sessionID: sessionID)
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
