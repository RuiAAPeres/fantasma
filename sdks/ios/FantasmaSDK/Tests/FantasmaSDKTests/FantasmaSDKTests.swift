import Foundation
#if canImport(Testing)
import Testing
@testable import FantasmaSDK

@Suite("Fantasma SDK Internal Tests")
struct FantasmaSDKInternalTests {
    @Test("queue persists rows across reopen")
    func queuePersistence() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        let queue = try harness.makeQueue()
        try await queue.enqueue(
            payload: try harness.payload(event: "app_open", installId: "install-a"),
            createdAt: 1,
            destinationSignature: localhostDestinationSignature()
        )

        let reopenedQueue = try harness.makeQueue()
        let rows = try await reopenedQueue.peek(limit: 10)

        #expect(rows.count == 1)
        #expect(rows.first?.createdAt == 1)
    }

    @Test("uploader builds a 50 event batch and leaves remainder queued")
    func uploaderBatching() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        let queue = try harness.makeQueue()
        for index in 0..<55 {
            try await queue.enqueue(
                payload: try harness.payload(event: "button_pressed_\(index)", installId: "install-a"),
                createdAt: Int64(index),
                destinationSignature: localhostDestinationSignature()
            )
        }

        let uploader = EventUploader(queue: queue, transport: RecordingTransport())
        let batch = try requireValue(await uploader.makeBatch(limit: 50))

        #expect(batch.rowIDs.count == 50)
        #expect(try await queue.count() == 55)
    }
}

@Suite("Fantasma SDK Behavior Tests", .serialized)
struct FantasmaSDKBehaviorTests {
    @Test("public API exposes async throwing static entrypoints")
    func publicAPISignatures() {
        let configure: (URL, String) async throws -> Void = Fantasma.configure
        let track: (String, [String: String]?) async throws -> Void = Fantasma.track
        let flush: () async throws -> Void = Fantasma.flush
        let clear: () async -> Void = Fantasma.clear

        _ = configure
        _ = track
        _ = flush
        _ = clear
    }

    @Test("public API persists queued event snapshots with generated identity")
    func publicAPIQueuesSnapshot() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("screen_view", properties: ["screen": "Home"])

        let queue = try harness.makeQueue()
        let row = try requireValue(await queue.peek(limit: 1).first)
        let event = try JSONDecoder().decode(EventEnvelope.self, from: row.payload)

        #expect(event.event == "screen_view")
        #expect(event.installId == "install-a")
        #expect(event.platform == "ios")
        #expect(event.appVersion == "1.2.3")
        #expect(event.osVersion == "18.3")
        #expect(event.properties == ["screen": "Home"])
    }

    @Test("clear rotates identity for future events without mutating queued rows")
    func clearPreservesQueuedRows() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("before_clear")
        await Fantasma.clear()
        try await Fantasma.track("after_clear")

        let queue = try harness.makeQueue()
        let rows = try await queue.peek(limit: 10)
        let first = try JSONDecoder().decode(EventEnvelope.self, from: requireValue(rows.first?.payload))
        let second = try JSONDecoder().decode(EventEnvelope.self, from: requireValue(rows.last?.payload))

        #expect(rows.count == 2)
        #expect(first.installId == "install-a")
        #expect(second.installId == "install-b")
    }

    @Test("track throws when configure has not run")
    func trackBeforeConfigureThrows() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        do {
            try await Fantasma.track("screen_view")
            Issue.record("expected notConfigured error")
        } catch let error as FantasmaError {
            #expect(error == .notConfigured)
        }
    }

    @Test("flush throws when configure has not run")
    func flushBeforeConfigureThrows() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        do {
            try await Fantasma.flush()
            Issue.record("expected notConfigured error")
        } catch let error as FantasmaError {
            #expect(error == .notConfigured)
        }
    }

    @Test("configure rejects an empty write key")
    func configureRejectsEmptyWriteKey() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        do {
            try await Fantasma.configure(serverURL: localhostURL(), writeKey: "   ")
            Issue.record("expected invalidWriteKey error")
        } catch let error as FantasmaError {
            #expect(error == .invalidWriteKey)
        }
    }

    @Test("configure rejects unsupported server URLs")
    func configureRejectsUnsupportedServerURL() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        do {
            try await Fantasma.configure(
                serverURL: URL(fileURLWithPath: "/tmp/fantasma"),
                writeKey: "fg_ing_test"
            )
            Issue.record("expected unsupportedServerURL error")
        } catch let error as FantasmaError {
            #expect(error == .unsupportedServerURL)
        }
    }

    @Test("track rejects property maps larger than two keys before enqueue")
    func trackRejectsTooManyProperties() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")

        do {
            try await Fantasma.track(
                "screen_view",
                properties: [
                    "screen": "Home",
                    "region": "EU",
                    "plan": "pro"
                ]
            )
            Issue.record("expected invalidPropertyCount error")
        } catch let error as FantasmaError {
            #expect(error == .invalidPropertyCount)
        }

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)
    }

    @Test("track rejects property names that do not match the public contract before enqueue")
    func trackRejectsInvalidPropertyName() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")

        do {
            try await Fantasma.track("screen_view", properties: ["Screen-Name": "Home"])
            Issue.record("expected invalidPropertyName error")
        } catch let error as FantasmaError {
            #expect(error == .invalidPropertyName)
        }

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)
    }

    @Test("track rejects reserved property names from the public contract before enqueue")
    func trackRejectsReservedPropertyName() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: RecordingTransport())

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")

        do {
            try await Fantasma.track("screen_view", properties: ["metric": "count"])
            Issue.record("expected reservedPropertyName error")
        } catch let error as FantasmaError {
            #expect(error == .reservedPropertyName)
        }

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)
    }

    @Test("reconfiguring with the same normalized destination preserves queued rows")
    func sameDestinationReconfigurePreservesQueue() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: " fg_ing_test ")
        try await Fantasma.track("screen_view")
        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.flush()

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)

        let request = try requireValue(await transport.requests().first)
        #expect(request.value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_test")
    }

    @Test("reconfiguring with a different destination discards queued rows before future uploads")
    func destinationDriftDiscardQueuedRows() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("old_event")

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 1)

        try await Fantasma.configure(serverURL: replacementURL(), writeKey: "fg_ing_next")
        #expect(try await queue.count() == 0)

        try await Fantasma.track("new_event")
        try await Fantasma.flush()

        let request = try requireValue(await transport.requests().first)
        #expect(request.url?.absoluteString == replacementURL().appendingPathComponent("v1/events").absoluteString)
        #expect(request.value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_next")
    }

    @Test("configuring after restart with the same destination preserves persisted queued rows")
    func restartPreservesQueuedRowsForSameDestination() async throws {
        let firstTransport = RecordingTransport()
        let secondTransport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: firstTransport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("old_event")

        await Fantasma.resetSharedCoreForTesting()
        await secondTransport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(transport: secondTransport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.flush()

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)

        let request = try requireValue(await secondTransport.requests().first)
        let batch = try JSONDecoder().decode(EventBatch.self, from: requireValue(request.httpBody))
        #expect(batch.events.count == 1)
        #expect(batch.events.first?.event == "old_event")
        #expect(request.value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_test")
    }

    @Test("configuring after restart with a different destination discards persisted queued rows")
    func restartDestinationDriftDiscardsQueuedRows() async throws {
        let firstTransport = RecordingTransport()
        let secondTransport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: firstTransport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("old_event")

        await Fantasma.resetSharedCoreForTesting()
        await secondTransport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(transport: secondTransport)

        try await Fantasma.configure(serverURL: replacementURL(), writeKey: "fg_ing_next")

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)

        try await Fantasma.track("new_event")
        try await Fantasma.flush()

        let request = try requireValue(await secondTransport.requests().first)
        let batch = try JSONDecoder().decode(EventBatch.self, from: requireValue(request.httpBody))
        #expect(batch.events.count == 1)
        #expect(batch.events.first?.event == "new_event")
        #expect(request.url?.absoluteString == replacementURL().appendingPathComponent("v1/events").absoluteString)
        #expect(request.value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_next")
    }

    @Test("flush removes queued rows after a 202 accepted response")
    func flushDeletesOnSuccess() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("app_open")
        try await Fantasma.flush()

        try await waitUntil {
            await transport.requests().count == 1
        }

        let queue = try harness.makeQueue()
        try await waitUntil {
            (try? await queue.count()) == 0
        }
    }

    @Test("flush throws when upload fails and keeps rows queued")
    func flushThrowsOnUploadFailure() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .failure(TestError.noPlannedResponse))
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("app_open")

        do {
            try await Fantasma.flush()
            Issue.record("expected uploadFailed error")
        } catch let error as FantasmaError {
            #expect(error == .uploadFailed)
        }

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 1)
    }

    @Test("flush throws invalidResponse when a 202 response body is malformed and keeps rows queued")
    func flushRejectsMalformedAcceptedResponse() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(invalidAcceptedResponse(body: Data("not-json".utf8))))
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("app_open")

        do {
            try await Fantasma.flush()
            Issue.record("expected invalidResponse error")
        } catch let error as FantasmaError {
            #expect(error == .invalidResponse)
        }

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 1)
    }

    @Test("flush throws invalidResponse when a 202 response accepts the wrong count and keeps rows queued")
    func flushRejectsAcceptedCountMismatch() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(successResponse(accepted: 0)))
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("app_open")

        do {
            try await Fantasma.flush()
            Issue.record("expected invalidResponse error")
        } catch let error as FantasmaError {
            #expect(error == .invalidResponse)
        }

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 1)
    }

    @Test("threshold auto flush keeps queued rows when upload fails")
    func autoFlushFailureRetainsRows() async throws {
        let transport = RecordingTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .failure(TestError.noPlannedResponse))
        try await harness.installSharedCore(transport: transport, uploadBatchSize: 1)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("app_open")

        try await waitUntil {
            await transport.requests().count == 1
        }

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 1)
    }

    @Test("explicit flush waits for a threshold-triggered auto flush already in flight")
    func explicitFlushWaitsForThresholdAutoFlush() async throws {
        let transport = SuspendedTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: transport, uploadBatchSize: 1)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("app_open")
        try await transport.waitUntilSendStarts()

        let probe = CompletionProbe()
        let explicitFlush = Task {
            defer { Task { await probe.markCompleted() } }
            try await Fantasma.flush()
        }

        await transport.release()

        try await waitUntil(timeoutNanoseconds: 250_000_000) {
            await probe.completed
        }

        try await explicitFlush.value

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)
    }

    @Test("explicit flush waits for an in-flight upload to finish")
    func explicitFlushWaitsForInFlightUpload() async throws {
        let transport = SuspendedTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: transport)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("app_open")

        let firstFlush = Task {
            try await Fantasma.flush()
        }
        try await transport.waitUntilSendStarts()

        let probe = CompletionProbe()
        let secondFlush = Task {
            defer { Task { await probe.markCompleted() } }
            try await Fantasma.flush()
        }

        try await waitUntil(timeoutNanoseconds: 250_000_000) {
            await transport.requests().count == 1
        }
        #expect(await probe.completed == false)

        await transport.release()
        try await firstFlush.value
        try await secondFlush.value

        #expect(await transport.requests().count == 1)

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)
    }

    @Test("reconfiguring during an active upload waits for the current batch, discards remaining queued rows, then swaps destination")
    func destinationDriftDuringActiveUploadWaitsForBatchBoundary() async throws {
        let transport = SuspendedTransport()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        try await harness.installSharedCore(transport: transport, uploadBatchSize: 1)

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("old_event_1")
        try await Fantasma.track("old_event_2")

        let firstFlush = Task {
            try await Fantasma.flush()
        }
        try await transport.waitUntilSendStarts()

        let probe = CompletionProbe()
        let reconfigure = Task {
            defer { Task { await probe.markCompleted() } }
            try await Fantasma.configure(serverURL: replacementURL(), writeKey: "fg_ing_next")
        }

        try await waitUntil(timeoutNanoseconds: 250_000_000) {
            await transport.requests().count == 1
        }
        #expect(await probe.completed == false)

        await transport.release()
        try await firstFlush.value
        try await reconfigure.value

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)

        try await Fantasma.track("new_event")
        let secondFlush = Task {
            try await Fantasma.flush()
        }
        try await waitUntil(timeoutNanoseconds: 250_000_000) {
            await transport.requests().count == 2
        }
        let requests = await transport.requests()
        #expect(requests[0].url?.absoluteString == localhostURL().appendingPathComponent("v1/events").absoluteString)
        #expect(requests[0].value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_test")
        #expect(requests[1].url?.absoluteString == replacementURL().appendingPathComponent("v1/events").absoluteString)
        #expect(requests[1].value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_next")
        await transport.release()
        try await secondFlush.value
    }

    @Test("reconfiguring during batch assembly discards the prepared old-destination batch before it uploads")
    func destinationDriftDuringBatchAssemblyDiscardsPreparedBatch() async throws {
        let transport = RecordingTransport()
        let gate = SuspensionGate()
        let reconfigureStarted = TaskStartSignal()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(
            transport: transport,
            beforeUploadBoundary: {
                await gate.markStarted()
                await gate.waitUntilReleased()
            }
        )

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")
        try await Fantasma.track("old_event")

        let firstFlush = Task {
            try await Fantasma.flush()
        }
        try await gate.waitUntilStarted()

        let probe = CompletionProbe()
        let reconfigure = Task {
            reconfigureStarted.markStarted()
            defer { Task { await probe.markCompleted() } }
            try await Fantasma.configure(serverURL: replacementURL(), writeKey: "fg_ing_next")
        }

        await reconfigureStarted.waitUntilStarted()
        #expect(await probe.completed == false)
        try await waitUntil(timeoutNanoseconds: 250_000_000) {
            (try? await Fantasma.pendingConfigurationMatchesForTesting(
                serverURL: replacementURL(),
                writeKey: "fg_ing_next"
            )) == true
        }
        let inFlightRequestsBeforeRelease = await transport.requests()
        #expect(inFlightRequestsBeforeRelease.count <= 1)

        await gate.release()
        try await firstFlush.value
        try await reconfigure.value

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)

        try await Fantasma.track("new_event")
        try await Fantasma.flush()

        let requests = await transport.requests()
        let request = try requireValue(requests.last)
        #expect(request.url?.absoluteString == replacementURL().appendingPathComponent("v1/events").absoluteString)
        #expect(request.value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_next")
    }

    @Test("reconfiguring while track is preparing an event waits for enqueue completion, then discards the old-destination row")
    func destinationDriftDuringTrackPreparationDiscardsQueuedRow() async throws {
        let transport = RecordingTransport()
        let gate = SuspensionGate()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await transport.enqueue(response: .success(successResponse(accepted: 1)))
        try await harness.installSharedCore(
            transport: transport,
            beforeEnqueue: {
                await gate.markStarted()
                await gate.waitUntilReleased()
            }
        )

        try await Fantasma.configure(serverURL: localhostURL(), writeKey: "fg_ing_test")

        let track = Task {
            try await Fantasma.track("old_event")
        }
        try await gate.waitUntilStarted()

        let probe = CompletionProbe()
        let reconfigure = Task {
            defer { Task { await probe.markCompleted() } }
            try await Fantasma.configure(serverURL: replacementURL(), writeKey: "fg_ing_next")
        }

        #expect(await probe.completed == false)

        await gate.release()
        try await track.value
        try await reconfigure.value

        let queue = try harness.makeQueue()
        #expect(try await queue.count() == 0)

        try await Fantasma.track("new_event")
        try await Fantasma.flush()

        let request = try requireValue(await transport.requests().first)
        #expect(request.url?.absoluteString == replacementURL().appendingPathComponent("v1/events").absoluteString)
        #expect(request.value(forHTTPHeaderField: "X-Fantasma-Key") == "fg_ing_next")
    }

    @Test("clear rotates identity even when live core creation fails")
    func clearRotatesIdentityWhenLiveCoreCreationFails() async throws {
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        await Fantasma.installRuntimeEnvironmentForTesting(
            makeCore: { throw FantasmaError.storageFailure("simulated live-core failure") },
            clearIdentity: {
                let store = IdentityStore(
                    provider: UserDefaultsProvider(suiteName: harness.defaultsName),
                    newIdentifier: { "install-z" }
                )
                _ = await store.clear()
            }
        )

        await Fantasma.clear()

        let userDefaults = try requireValue(UserDefaults(suiteName: harness.defaultsName))
        #expect(userDefaults.string(forKey: "dev.fantasma.sdk.install-id") == "install-z")

        await Fantasma.resetRuntimeEnvironmentForTesting()
    }

    @Test("public API sends the Fantasma ingest contract to a local HTTP server")
    func ingestContractRoundTrip() async throws {
        let server = try LocalEventServer()
        defer { server.stop() }

        let serverURL = try await server.start()
        let harness = TestHarness()
        defer { harness.cleanup() }

        await Fantasma.resetSharedCoreForTesting()
        let liveTransport = URLSessionTransport(session: .shared)
        try await harness.installSharedCore(transport: liveTransport)

        try await Fantasma.configure(serverURL: serverURL, writeKey: "fg_ing_test")
        try await Fantasma.track("app_open", properties: ["screen": "Home"])
        try await Fantasma.flush()

        let request = try await server.waitForRequest()
        let body = try JSONDecoder().decode(EventBatch.self, from: request.body)
        let event = try requireValue(body.events.first)

        #expect(request.method == "POST")
        #expect(request.path == "/v1/events")
        #expect(request.headers["content-type"] == "application/json")
        #expect(request.headers["x-fantasma-key"] == "fg_ing_test")
        #expect(body.events.count == 1)
        #expect(event.event == "app_open")
        #expect(event.installId == "install-a")
        #expect(event.platform == "ios")
        #expect(event.osVersion == "18.3")
        #expect(event.properties == ["screen": "Home"])

        let queue = try harness.makeQueue()
        try await waitUntil {
            (try? await queue.count()) == 0
        }
    }
}

private struct EventBatch: Decodable {
    let events: [EventEnvelope]
}

@Suite("Fantasma SDK Test Helper Tests")
struct FantasmaSDKTestHelperTests {
    @Test("local event server launches python via env")
    func localEventServerUsesPortablePythonLauncher() throws {
        let server = try LocalEventServer()
        defer { server.stop() }

        let command = server.launchCommand
        #expect(command.executableURL.path == "/usr/bin/env")
        #expect(command.arguments.first == "python3")
        #expect(command.arguments.dropFirst().count == 3)
    }
}

private actor RecordingTransport: FantasmaTransport {
    private var responses: [Result<(Data, HTTPURLResponse), Error>] = []
    private var capturedRequests: [URLRequest] = []

    func enqueue(response: Result<(Data, HTTPURLResponse), Error>) {
        responses.append(response)
    }

    func requests() -> [URLRequest] {
        capturedRequests
    }

    func send(request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        capturedRequests.append(request)
        guard !responses.isEmpty else {
            throw TestError.noPlannedResponse
        }
        return try responses.removeFirst().get()
    }
}

private actor CompletionProbe {
    private(set) var completed = false

    func markCompleted() {
        completed = true
    }
}

private final class TaskStartSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var started = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func markStarted() {
        let currentWaiters: [CheckedContinuation<Void, Never>]
        lock.lock()
        if started {
            lock.unlock()
            return
        }

        started = true
        currentWaiters = waiters
        waiters.removeAll(keepingCapacity: false)
        lock.unlock()

        for waiter in currentWaiters {
            waiter.resume()
        }
    }

    func waitUntilStarted() async {
        await withCheckedContinuation { continuation in
            lock.lock()
            if started {
                lock.unlock()
                continuation.resume()
                return
            }

            waiters.append(continuation)
            lock.unlock()
        }
    }
}

private actor SuspensionGate {
    private var started = false
    private var released = false
    private var startContinuation: CheckedContinuation<Void, Error>?
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func markStarted() {
        started = true
        startContinuation?.resume()
        startContinuation = nil
    }

    func waitUntilStarted() async throws {
        if started {
            return
        }

        try await withCheckedThrowingContinuation { continuation in
            startContinuation = continuation
        }
    }

    func waitUntilReleased() async {
        if released {
            return
        }

        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func release() {
        released = true
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

private actor SuspendedTransport: FantasmaTransport {
    private var capturedRequests: [URLRequest] = []
    private var didStartSend = false
    private var sendStartedContinuation: CheckedContinuation<Void, Error>?
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func requests() -> [URLRequest] {
        capturedRequests
    }

    func waitUntilSendStarts() async throws {
        if didStartSend {
            return
        }

        try await withCheckedThrowingContinuation { continuation in
            sendStartedContinuation = continuation
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }

    func send(request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        capturedRequests.append(request)
        didStartSend = true
        sendStartedContinuation?.resume()
        sendStartedContinuation = nil

        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }

        return successResponse(accepted: 1)
    }
}

private final class IdentifierSequence: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String]

    init(_ values: [String]) {
        self.values = values
    }

    func next() -> String {
        lock.lock()
        defer { lock.unlock() }
        return values.removeFirst()
    }
}

private struct TestHarness {
    let rootDirectory: URL
    let databaseURL: URL
    let defaultsName: String
    let fixedDate: Date
    private let identifiers = IdentifierSequence(["install-a", "install-b", "install-c"])

    init() {
        rootDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        databaseURL = rootDirectory.appendingPathComponent("fantasma.sqlite")
        defaultsName = "FantasmaSDKTests.\(UUID().uuidString)"
        fixedDate = makeFixedDate()

        let userDefaults = UserDefaults(suiteName: defaultsName)!
        userDefaults.removePersistentDomain(forName: defaultsName)
    }

    func cleanup() {
        UserDefaults(suiteName: defaultsName)?.removePersistentDomain(forName: defaultsName)
        try? FileManager.default.removeItem(at: rootDirectory)
    }

    func makeQueue() throws -> SQLiteEventQueue {
        try SQLiteEventQueue(databaseURL: databaseURL)
    }

    func installSharedCore(
        transport: FantasmaTransport,
        uploadBatchSize: Int = 50,
        beforeEnqueue: @escaping @Sendable () async -> Void = {},
        afterBuildBatch: @escaping @Sendable () async -> Void = {},
        beforeUploadBoundary: @escaping @Sendable () async -> Void = {}
    ) async throws {
        let userDefaults = try requireValue(UserDefaults(suiteName: defaultsName))
        userDefaults.removePersistentDomain(forName: defaultsName)
        let core = try makeCore(
            transport: transport,
            uploadBatchSize: uploadBatchSize,
            beforeEnqueue: beforeEnqueue,
            afterBuildBatch: afterBuildBatch,
            beforeUploadBoundary: beforeUploadBoundary
        )
        await Fantasma.replaceSharedCoreForTesting(core)
    }

    func payload(event: String, installId: String) throws -> Data {
        try JSONEncoder().encode(
            EventEnvelope(
                event: event,
                timestamp: "2026-01-01T00:00:00.000Z",
                installId: installId,
                platform: "ios",
                appVersion: "1.2.3",
                osVersion: "18.3",
                properties: nil
            )
        )
    }

    private func makeCore(
        transport: FantasmaTransport,
        uploadBatchSize: Int,
        beforeEnqueue: @escaping @Sendable () async -> Void,
        afterBuildBatch: @escaping @Sendable () async -> Void,
        beforeUploadBoundary: @escaping @Sendable () async -> Void
    ) throws -> FantasmaCore {
        let dependencies = FantasmaDependencies(
            databaseURL: databaseURL,
            transport: transport,
            now: { fixedDate },
            appVersion: { "1.2.3" },
            osVersion: { "18.3" },
            newIdentifier: { identifiers.next() },
            timerInterval: .seconds(3_600),
            uploadBatchSize: uploadBatchSize,
            beforeEnqueue: beforeEnqueue,
            beforeUploadBoundary: beforeUploadBoundary
        )
        let identityStore = IdentityStore(
            provider: UserDefaultsProvider(suiteName: defaultsName),
            newIdentifier: { identifiers.next() }
        )
        let queue = try SQLiteEventQueue(databaseURL: databaseURL)
        let uploader = EventUploader(
            queue: queue,
            transport: transport,
            afterBuildBatch: afterBuildBatch
        )
        return FantasmaCore(
            dependencies: dependencies,
            identityStore: identityStore,
            eventQueue: queue,
            uploader: uploader
        )
    }
}

#endif

private struct CapturedRequest: Sendable {
    let method: String
    let path: String
    let headers: [String: String]
    let body: Data
}

private struct LocalServerLaunchCommand: Sendable {
    let executableURL: URL
    let arguments: [String]
}

private final class LocalEventServer: @unchecked Sendable {
    private let rootDirectory: URL
    private let captureURL: URL
    private let readyURL: URL
    private let scriptURL: URL
    private let process = Process()

    init() throws {
        rootDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        captureURL = rootDirectory.appendingPathComponent("request.json")
        readyURL = rootDirectory.appendingPathComponent("ready.txt")
        scriptURL = rootDirectory.appendingPathComponent("server.py")

        try FileManager.default.createDirectory(at: rootDirectory, withIntermediateDirectories: true)
        try """
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

capture_path = sys.argv[1]
ready_path = sys.argv[2]

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        payload = {
            "method": "POST",
            "path": self.path,
            "headers": {key.lower(): value for key, value in self.headers.items()},
            "body": body.decode("utf-8"),
        }
        with open(capture_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)

        try:
            accepted = len(json.loads(body.decode("utf-8")).get("events", []))
        except Exception:
            accepted = 0

        response = json.dumps({"accepted": accepted}).encode("utf-8")
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format, *args):
        return

server = HTTPServer(("127.0.0.1", 0), Handler)
with open(ready_path, "w", encoding="utf-8") as handle:
    handle.write(str(server.server_port))
server.handle_request()
""".write(to: scriptURL, atomically: true, encoding: .utf8)
    }

    var launchCommand: LocalServerLaunchCommand {
        LocalServerLaunchCommand(
            executableURL: URL(fileURLWithPath: "/usr/bin/env"),
            arguments: ["python3", scriptURL.path, captureURL.path, readyURL.path]
        )
    }

    func start() async throws -> URL {
        let command = launchCommand
        process.executableURL = command.executableURL
        process.arguments = command.arguments
        try process.run()

        try await waitUntil(timeoutNanoseconds: 2_000_000_000) {
            FileManager.default.fileExists(atPath: self.readyURL.path)
        }

        let portText = try String(contentsOf: readyURL, encoding: .utf8)
        let port = Int(portText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0
        return try requireValue(URL(string: "http://127.0.0.1:\(port)"))
    }

    func stop() {
        if process.isRunning {
            process.terminate()
        }
        try? FileManager.default.removeItem(at: rootDirectory)
    }

    func waitForRequest() async throws -> CapturedRequest {
        try await waitUntil(timeoutNanoseconds: 2_000_000_000) {
            FileManager.default.fileExists(atPath: self.captureURL.path)
        }
        let data = try Data(contentsOf: captureURL)
        let payload = try JSONDecoder().decode(CapturedRequestFile.self, from: data)
        return CapturedRequest(
            method: payload.method,
            path: payload.path,
            headers: payload.headers,
            body: Data(payload.body.utf8)
        )
    }
}

private struct CapturedRequestFile: Decodable {
    let method: String
    let path: String
    let headers: [String: String]
    let body: String
}

private enum TestError: Error {
    case noPlannedResponse
}

private func successResponse(accepted: Int) -> (Data, HTTPURLResponse) {
    let payload = try! JSONEncoder().encode(["accepted": accepted])
    let response = HTTPURLResponse(
        url: URL(string: "http://localhost:8081/v1/events")!,
        statusCode: 202,
        httpVersion: nil,
        headerFields: nil
    )!
    return (payload, response)
}

private func invalidAcceptedResponse(body: Data) -> (Data, HTTPURLResponse) {
    let response = HTTPURLResponse(
        url: URL(string: "http://localhost:8081/v1/events")!,
        statusCode: 202,
        httpVersion: nil,
        headerFields: nil
    )!
    return (body, response)
}

private func makeFixedDate() -> Date {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter.date(from: "2026-01-01T00:00:00.000Z")!
}

private func localhostURL() -> URL {
    URL(string: "http://localhost:8081")!
}

private func replacementURL() -> URL {
    URL(string: "http://localhost:8082")!
}

private func localhostDestinationSignature() -> String {
    "\(localhostURL().absoluteString)|fg_ing_test"
}

private func requireValue<T>(_ value: T?) throws -> T {
    guard let value else {
        throw TestError.noPlannedResponse
    }
    return value
}

private func waitUntil(
    timeoutNanoseconds: UInt64 = 2_000_000_000,
    condition: @escaping @Sendable () async -> Bool
) async throws {
    let deadline = DispatchTime.now().uptimeNanoseconds + timeoutNanoseconds
    while DispatchTime.now().uptimeNanoseconds < deadline {
        if await condition() {
            return
        }
        try await Task.sleep(nanoseconds: 25_000_000)
    }
    throw TestError.noPlannedResponse
}
