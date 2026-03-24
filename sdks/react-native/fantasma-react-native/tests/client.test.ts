import { afterEach, describe, expect, it, vi } from "vitest";

import {
  FantasmaClient,
  FantasmaClientAlreadyActiveError,
  FantasmaClosedClientError,
  FantasmaEncodingFailedError,
  FantasmaInvalidWriteKeyError,
  installFantasmaNativeModuleForTesting,
  resetFantasmaNativeModuleForTesting,
  type FantasmaNativeModule,
} from "../src/index.js";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });
  return { promise, resolve, reject };
}

afterEach(async () => {
  resetFantasmaNativeModuleForTesting();
  await FantasmaClient.resetForTesting();
});

describe("FantasmaClient", () => {
  it("rejects invalid write keys at construction time", () => {
    expect(
      () =>
        new FantasmaClient({
          serverUrl: "https://api.usefantasma.com",
          writeKey: "   ",
        }),
    ).toThrow(FantasmaInvalidWriteKeyError);
  });

  it("throws when a second live client is constructed", () => {
    installFantasmaNativeModuleForTesting(createNoopNativeModule());

    const first = new FantasmaClient({
      serverUrl: "https://api.usefantasma.com",
      writeKey: "fg_ing_primary",
    });

    expect(
      () =>
        new FantasmaClient({
          serverUrl: "https://api.usefantasma.com",
          writeKey: "fg_ing_secondary",
        }),
    ).toThrow(FantasmaClientAlreadyActiveError);

    return first.close();
  });

  it("shares one in-flight native acquisition across concurrent first calls", async () => {
    const acquireGate = deferred<void>();
    const nativeModule = createNoopNativeModule({
      open: vi.fn(async () => {
        await acquireGate.promise;
      }),
    });
    installFantasmaNativeModuleForTesting(nativeModule);

    const client = new FantasmaClient({
      serverUrl: "https://api.usefantasma.com",
      writeKey: "fg_ing_primary",
    });

    const trackPromise = client.track("app_open");
    const flushPromise = client.flush();

    expect(nativeModule.open).toHaveBeenCalledTimes(1);

    acquireGate.resolve();
    await Promise.all([trackPromise, flushPromise]);

    expect(nativeModule.track).toHaveBeenCalledTimes(1);
    expect(nativeModule.flush).toHaveBeenCalledTimes(1);
  });

  it("auto-closes and frees the slot after the first native acquisition fails", async () => {
    installFantasmaNativeModuleForTesting(
      createNoopNativeModule({
        open: vi.fn(async () => {
          throw {
            code: "encoding_failed",
            message: "failed to encode",
          };
        }),
      }),
    );

    const failed = new FantasmaClient({
      serverUrl: "https://api.usefantasma.com",
      writeKey: "fg_ing_primary",
    });

    await expect(failed.track("app_open")).rejects.toThrow(
      FantasmaEncodingFailedError,
    );
    await expect(failed.flush()).rejects.toThrow(FantasmaClosedClientError);

    const replacement = new FantasmaClient({
      serverUrl: "https://api.usefantasma.com",
      writeKey: "fg_ing_replacement",
    });

    await replacement.close();
  });

  it("does not leak the slot when close races with the first native acquisition", async () => {
    const acquireGate = deferred<void>();
    const nativeModule = createNoopNativeModule({
      open: vi.fn(async () => {
        await acquireGate.promise;
      }),
    });
    installFantasmaNativeModuleForTesting(nativeModule);

    const client = new FantasmaClient({
      serverUrl: "https://api.usefantasma.com",
      writeKey: "fg_ing_primary",
    });

    const trackPromise = client.track("app_open");
    const closePromise = client.close();

    acquireGate.resolve();
    await expect(trackPromise).rejects.toThrow(FantasmaClosedClientError);
    await closePromise;
    expect(nativeModule.close).toHaveBeenCalledTimes(1);

    const replacement = new FantasmaClient({
      serverUrl: "https://api.usefantasma.com",
      writeKey: "fg_ing_replacement",
    });
    await replacement.close();
  });
});

function createNoopNativeModule(
  overrides: Partial<FantasmaNativeModule> = {},
): FantasmaNativeModule {
  return {
    open: vi.fn(async () => {}),
    track: vi.fn(async () => {}),
    flush: vi.fn(async () => {}),
    clear: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
    ...overrides,
  };
}
