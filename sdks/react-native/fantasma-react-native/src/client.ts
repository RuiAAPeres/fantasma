import {
  FantasmaClientAlreadyActiveError,
  FantasmaClosedClientError,
  FantasmaInvalidEventNameError,
  FantasmaInvalidWriteKeyError,
  FantasmaUnsupportedServerUrlError,
  mapFantasmaError,
} from "./errors.js";
import {
  getFantasmaNativeModule,
  type FantasmaConfig,
} from "./nativeModule.js";

type ClientState = "open" | "closing" | "closed";

export class FantasmaClient {
  private static liveClient: FantasmaClient | null = null;

  static async resetForTesting(): Promise<void> {
    await this.liveClient?.close();
    this.liveClient = null;
  }

  readonly config: FantasmaConfig;

  private state: ClientState = "open";
  private acquirePromise: Promise<void> | null = null;
  private closePromise: Promise<void> | null = null;
  private acquired = false;

  constructor(config: FantasmaConfig) {
    this.config = normalizeConfig(config);
    if (FantasmaClient.liveClient !== null) {
      throw new FantasmaClientAlreadyActiveError();
    }
    FantasmaClient.liveClient = this;
  }

  async track(eventName: string): Promise<void> {
    const normalizedName = eventName.trim();
    if (normalizedName.length === 0) {
      throw new FantasmaInvalidEventNameError();
    }

    await this.ensureReady();
    this.ensureOpen();
    await getFantasmaNativeModule().track(normalizedName);
  }

  async flush(): Promise<void> {
    await this.ensureReady();
    this.ensureOpen();
    await getFantasmaNativeModule().flush();
  }

  async clear(): Promise<void> {
    await this.ensureReady();
    this.ensureOpen();
    await getFantasmaNativeModule().clear();
  }

  async close(): Promise<void> {
    if (this.state === "closed") {
      return;
    }
    if (this.closePromise !== null) {
      await this.closePromise;
      return;
    }

    this.state = "closing";
    this.closePromise = (async () => {
      const acquirePromise = this.acquirePromise;
      if (acquirePromise !== null) {
        try {
          await acquirePromise;
        } catch {
          this.finalizeClosed();
          return;
        }
      }

      if (this.acquired) {
        try {
          await getFantasmaNativeModule().close();
        } finally {
          this.acquired = false;
        }
      }

      this.finalizeClosed();
    })();

    await this.closePromise;
  }

  private async ensureReady(): Promise<void> {
    this.ensureUsable();
    if (this.acquired) {
      return;
    }
    if (this.acquirePromise !== null) {
      await this.acquirePromise;
      return;
    }

    const promise = this.startAcquire();
    this.acquirePromise = promise;
    await promise;
  }

  private async startAcquire(): Promise<void> {
    try {
      await getFantasmaNativeModule().open(this.config);
      this.acquired = true;
      if (this.state !== "open") {
        await getFantasmaNativeModule().close();
        this.acquired = false;
        this.finalizeClosed();
        throw new FantasmaClosedClientError();
      }
    } catch (error) {
      if (error instanceof FantasmaClosedClientError) {
        throw error;
      }

      const mapped = mapFantasmaError(error);
      this.acquired = false;
      this.finalizeClosed();
      throw mapped;
    } finally {
      if (this.acquirePromise !== null) {
        this.acquirePromise = null;
      }
    }
  }

  private ensureUsable(): void {
    if (this.state === "closed") {
      throw new FantasmaClosedClientError();
    }
  }

  private ensureOpen(): void {
    if (this.state !== "open") {
      throw new FantasmaClosedClientError();
    }
  }

  private finalizeClosed(): void {
    this.state = "closed";
    if (FantasmaClient.liveClient === this) {
      FantasmaClient.liveClient = null;
    }
  }
}

function normalizeConfig(config: FantasmaConfig): FantasmaConfig {
  const writeKey = config.writeKey.trim();
  if (writeKey.length === 0) {
    throw new FantasmaInvalidWriteKeyError();
  }

  let normalizedUrl: URL;
  try {
    normalizedUrl = new URL(config.serverUrl);
  } catch {
    throw new FantasmaUnsupportedServerUrlError();
  }

  if (
    normalizedUrl.protocol !== "http:" &&
    normalizedUrl.protocol !== "https:"
  ) {
    throw new FantasmaUnsupportedServerUrlError();
  }

  if (normalizedUrl.pathname === "/") {
    normalizedUrl.pathname = "";
  }

  while (normalizedUrl.pathname.length > 1 && normalizedUrl.pathname.endsWith("/")) {
    normalizedUrl.pathname = normalizedUrl.pathname.slice(0, -1);
  }

  return {
    serverUrl: normalizedUrl.toString(),
    writeKey,
  };
}
