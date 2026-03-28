import { execFileSync } from "node:child_process";
import { cp, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const packageDir = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = path.resolve(packageDir, "../../..");
const syncScriptPath = fileURLToPath(new URL("./sync-native-sources.mjs", import.meta.url));

execFileSync(process.execPath, [syncScriptPath], {
  cwd: packageDir,
  stdio: "inherit",
});

const reactNativeVersion = execFileSync(
  "node",
  [
    "-p",
    "require('./node_modules/react-native/package.json').version",
  ],
  {
    cwd: packageDir,
    encoding: "utf8",
  },
).trim();

const androidSdkDir = process.env.ANDROID_SDK_ROOT ?? process.env.ANDROID_HOME;
if (androidSdkDir === undefined) {
  throw new Error(
    "bridge:android:check requires ANDROID_SDK_ROOT or ANDROID_HOME to be set.",
  );
}

const tempDir = await mkdtemp(path.join(tmpdir(), "fantasma-rn-android-"));
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
    "react-native.config.js",
    "android/build.gradle.kts",
    "android/settings.gradle.kts",
    "android/src/main/java/com/fantasma/reactnative/FantasmaReactNativeModule.kt",
    "android/src/main/java/com/fantasma/sdk/FantasmaClient.kt",
  ]) {
    if (!packedFiles.has(requiredFile)) {
      throw new Error(`npm pack is missing required Android file: ${requiredFile}`);
    }
  }

  const tempAndroidDir = path.join(tempDir, "android-project");
  await cp(path.join(packageDir, "android"), tempAndroidDir, { recursive: true });
  await writeFile(
    path.join(tempAndroidDir, "local.properties"),
    `sdk.dir=${androidSdkDir.replaceAll("\\", "\\\\")}\n`,
  );

  execFileSync(
    path.join(repoRoot, "sdks/android/gradlew"),
    [
      "-p",
      tempAndroidDir,
      `-PfantasmaReactNativeVersion=${reactNativeVersion}`,
      "compileDebugKotlin",
    ],
    {
      cwd: repoRoot,
      stdio: "inherit",
      env: {
        ...process.env,
        ANDROID_HOME: androidSdkDir,
        ANDROID_SDK_ROOT: androidSdkDir,
        GRADLE_USER_HOME: path.join(tempDir, "gradle-user-home"),
      },
    },
  );
} finally {
  await removeDirectoryWithRetries(tempDir);
}

console.log("Android bridge sources compile and the packed package includes the required files.");

async function removeDirectoryWithRetries(targetDir) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await rm(targetDir, { recursive: true, force: true });
      return;
    } catch (error) {
      if (
        error?.code !== "ENOTEMPTY" &&
        error?.code !== "EBUSY" &&
        error?.code !== "EPERM"
      ) {
        throw error;
      }
      await delay(200);
    }
  }

  await rm(targetDir, { recursive: true, force: true });
}
