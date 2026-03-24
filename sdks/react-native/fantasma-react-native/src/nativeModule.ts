import type { FantasmaErrorCode } from "./errors.js";

export type FantasmaConfig = {
  serverUrl: string;
  writeKey: string;
};

export type FantasmaNativeError = {
  code: FantasmaErrorCode;
  message?: string;
};

export type FantasmaNativeModule = {
  open(config: FantasmaConfig): Promise<void>;
  track(eventName: string): Promise<void>;
  flush(): Promise<void>;
  clear(): Promise<void>;
  close(): Promise<void>;
};

function makeUnlinkedModule(): FantasmaNativeModule {
  return {
    async open() {
      throw {
        code: "storage_failure",
        message:
          "Fantasma React Native native bindings are not linked into this app.",
      } satisfies FantasmaNativeError;
    },
    async track() {
      throw {
        code: "storage_failure",
        message:
          "Fantasma React Native native bindings are not linked into this app.",
      } satisfies FantasmaNativeError;
    },
    async flush() {
      throw {
        code: "storage_failure",
        message:
          "Fantasma React Native native bindings are not linked into this app.",
      } satisfies FantasmaNativeError;
    },
    async clear() {
      throw {
        code: "storage_failure",
        message:
          "Fantasma React Native native bindings are not linked into this app.",
      } satisfies FantasmaNativeError;
    },
    async close() {
      return;
    },
  };
}

function loadLinkedNativeModule(): FantasmaNativeModule {
  try {
    const reactNative = require("react-native") as {
      NativeModules?: Record<string, FantasmaNativeModule | undefined>;
    };
    const nativeModule = reactNative.NativeModules?.FantasmaReactNative;
    if (nativeModule !== undefined) {
      return nativeModule;
    }
  } catch {
    // Fall through to the unlinked module.
  }

  return makeUnlinkedModule();
}

let currentNativeModule: FantasmaNativeModule = loadLinkedNativeModule();

export function getFantasmaNativeModule(): FantasmaNativeModule {
  return currentNativeModule;
}

export function installFantasmaNativeModuleForTesting(
  nativeModule: FantasmaNativeModule,
): void {
  currentNativeModule = nativeModule;
}

export function resetFantasmaNativeModuleForTesting(): void {
  currentNativeModule = loadLinkedNativeModule();
}
