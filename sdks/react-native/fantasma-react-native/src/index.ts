export { FantasmaClient } from "./client.js";
export {
  FantasmaClientAlreadyActiveError,
  FantasmaClosedClientError,
  FantasmaEncodingFailedError,
  FantasmaError,
  FantasmaInvalidEventNameError,
  FantasmaInvalidResponseError,
  FantasmaInvalidWriteKeyError,
  FantasmaStorageFailureError,
  FantasmaUnsupportedServerUrlError,
  FantasmaUploadFailedError,
  mapFantasmaError,
} from "./errors.js";
export type { FantasmaErrorCode } from "./errors.js";
export {
  installFantasmaNativeModuleForTesting,
  resetFantasmaNativeModuleForTesting,
} from "./nativeModule.js";
export type {
  FantasmaConfig,
  FantasmaNativeError,
  FantasmaNativeModule,
} from "./nativeModule.js";
