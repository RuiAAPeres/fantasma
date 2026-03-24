import Foundation

#if canImport(React)
import React

@objc(FantasmaReactNative)
final class FantasmaReactNativeModule: NSObject {
    private let bridge = FantasmaReactNativeBridge.shared

    @objc
    static func requiresMainQueueSetup() -> Bool {
        false
    }

    @objc(open:writeKey:resolver:rejecter:)
    func open(
        serverURL: String,
        writeKey: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        Task {
            do {
                guard let url = URL(string: serverURL) else {
                    throw FantasmaError.unsupportedServerURL
                }
                try await bridge.open(serverURL: url, writeKey: writeKey)
                resolve(nil)
            } catch {
                rejectFantasmaError(error, reject: reject)
            }
        }
    }

    @objc(track:resolver:rejecter:)
    func track(
        eventName: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        Task {
            do {
                try await bridge.track(eventName)
                resolve(nil)
            } catch {
                rejectFantasmaError(error, reject: reject)
            }
        }
    }

    @objc(flush:rejecter:)
    func flush(
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        Task {
            do {
                try await bridge.flush()
                resolve(nil)
            } catch {
                rejectFantasmaError(error, reject: reject)
            }
        }
    }

    @objc(clear:rejecter:)
    func clear(
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        Task {
            do {
                try await bridge.clear()
                resolve(nil)
            } catch {
                rejectFantasmaError(error, reject: reject)
            }
        }
    }

    @objc(close:rejecter:)
    func close(
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        Task {
            await bridge.close()
            resolve(nil)
        }
    }

    private func rejectFantasmaError(
        _ error: Error,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        if let fantasmaError = error as? FantasmaError {
            switch fantasmaError {
            case .invalidWriteKey:
                reject("invalid_write_key", fantasmaError.localizedDescription, fantasmaError)
            case .unsupportedServerURL:
                reject("unsupported_server_url", fantasmaError.localizedDescription, fantasmaError)
            case .invalidEventName:
                reject("invalid_event_name", fantasmaError.localizedDescription, fantasmaError)
            case .encodingFailed:
                reject("encoding_failed", fantasmaError.localizedDescription, fantasmaError)
            case .invalidResponse:
                reject("invalid_response", fantasmaError.localizedDescription, fantasmaError)
            case .uploadFailed:
                reject("upload_failed", fantasmaError.localizedDescription, fantasmaError)
            case .storageFailure(let detail):
                reject("storage_failure", detail, fantasmaError)
            case .notConfigured:
                reject("closed_client", "Fantasma client has already been closed.", fantasmaError)
            }
            return
        }

        reject("storage_failure", error.localizedDescription, error)
    }
}
#endif
