import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = fileURLToPath(new URL("..", import.meta.url));
const syncScriptPath = fileURLToPath(new URL("./sync-native-sources.mjs", import.meta.url));

execFileSync(process.execPath, [syncScriptPath], {
  cwd: packageDir,
  stdio: "inherit",
});

const tempDir = await mkdtemp(path.join(tmpdir(), "fantasma-rn-ios-"));
try {
  const npmEnv = {
    ...process.env,
    npm_config_cache: path.join(tempDir, "npm-cache"),
  };
  delete npmEnv.npm_config_dir;
  delete npmEnv.npm_config_verify_deps_before_run;

  const packedOutput = execFileSync(
    "npm",
    ["pack", "--json", "--dry-run", "--ignore-scripts"],
    {
      cwd: packageDir,
      encoding: "utf8",
      env: npmEnv,
    },
  );
  const packedFiles = new Set(
    JSON.parse(packedOutput)[0].files.map((entry) => entry.path),
  );
  for (const requiredFile of [
    "dist/index.js",
    "dist/index.d.ts",
    "fantasma-react-native.podspec",
    "react-native.config.js",
    "ios/FantasmaReactNativeModule.swift",
    "ios/FantasmaReactNativeModule.m",
    "ios/vendor/FantasmaSDK/FantasmaCore.swift",
  ]) {
    if (!packedFiles.has(requiredFile)) {
      throw new Error(`npm pack is missing required iOS file: ${requiredFile}`);
    }
  }

  const reactDir = path.join(tempDir, "React");
  await mkdir(reactDir, { recursive: true });
  await writeFile(
    path.join(reactDir, "RCTBridgeModule.h"),
    `#import <Foundation/Foundation.h>

typedef void (^RCTPromiseResolveBlock)(id _Nullable result);
typedef void (^RCTPromiseRejectBlock)(NSString * _Nonnull code, NSString * _Nonnull message, NSError * _Nullable error);

@protocol RCTBridgeModule
@end

#define RCT_EXTERN_MODULE(objc_name, objc_supername) objc_name : objc_supername @end @interface objc_name (RCTExternModule) <RCTBridgeModule> @end @implementation objc_name (RCTExternModule)
#define RCT_EXTERN_METHOD(method)
`,
  );
  await writeFile(
    path.join(reactDir, "module.modulemap"),
    `module React {
  header "RCTBridgeModule.h"
  export *
}
`,
  );

  const sdkPath = execFileSync("xcrun", ["--sdk", "macosx", "--show-sdk-path"], {
    encoding: "utf8",
  }).trim();

  const swiftFiles = [
    path.join(packageDir, "ios", "FantasmaReactNativeModule.swift"),
    path.join(packageDir, "ios", "vendor", "FantasmaSDK", "EventUploader.swift"),
    path.join(packageDir, "ios", "vendor", "FantasmaSDK", "Fantasma.swift"),
    path.join(packageDir, "ios", "vendor", "FantasmaSDK", "FantasmaCore.swift"),
    path.join(packageDir, "ios", "vendor", "FantasmaSDK", "FantasmaReactNativeBridge.swift"),
    path.join(packageDir, "ios", "vendor", "FantasmaSDK", "IdentityStore.swift"),
    path.join(packageDir, "ios", "vendor", "FantasmaSDK", "SQLiteQueue.swift"),
    path.join(packageDir, "ios", "vendor", "FantasmaSDK", "Transport.swift"),
  ];

  execFileSync(
    "swiftc",
    [
      "-typecheck",
      "-I",
      tempDir,
      "-module-cache-path",
      path.join(tempDir, "module-cache"),
      ...swiftFiles,
    ],
    {
      cwd: packageDir,
      stdio: "inherit",
      env: {
        ...process.env,
        CLANG_MODULE_CACHE_PATH: path.join(tempDir, "clang-module-cache"),
      },
    },
  );

  execFileSync(
    "clang",
    [
      "-fsyntax-only",
      "-x",
      "objective-c",
      "-fobjc-arc",
      "-fmodules",
      "-fmodules-cache-path=" + path.join(tempDir, "clang-module-cache"),
      "-isysroot",
      sdkPath,
      "-I",
      tempDir,
      path.join(packageDir, "ios", "FantasmaReactNativeModule.m"),
    ],
    {
      cwd: packageDir,
      stdio: "inherit",
      env: {
        ...process.env,
        CLANG_MODULE_CACHE_PATH: path.join(tempDir, "clang-module-cache"),
      },
    },
  );
} finally {
  await rm(tempDir, { recursive: true, force: true });
}

console.log("iOS bridge sources compile and the packed package includes the required files.");
