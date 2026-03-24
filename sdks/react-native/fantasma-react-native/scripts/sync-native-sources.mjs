import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const packageDir = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = path.resolve(packageDir, "../../..");

const androidSourceDir = path.join(
  repoRoot,
  "sdks/android/fantasma-sdk/src/main/java/com/fantasma/sdk",
);
const androidDestinationDir = path.join(
  packageDir,
  "android/src/main/java/com/fantasma/sdk",
);
const iosSourceDir = path.join(
  repoRoot,
  "sdks/ios/FantasmaSDK/Sources/FantasmaSDK",
);
const iosDestinationDir = path.join(packageDir, "ios/vendor/FantasmaSDK");

await rm(androidDestinationDir, { recursive: true, force: true });
await mkdir(path.dirname(androidDestinationDir), { recursive: true });
await cp(androidSourceDir, androidDestinationDir, { recursive: true });

await rm(iosDestinationDir, { recursive: true, force: true });
await mkdir(path.dirname(iosDestinationDir), { recursive: true });
await cp(iosSourceDir, iosDestinationDir, { recursive: true });

const sqliteQueuePath = path.join(iosDestinationDir, "SQLiteQueue.swift");
const sqliteQueueSource = await readFile(sqliteQueuePath, "utf8");
await writeFile(
  sqliteQueuePath,
  sqliteQueueSource.replace("import CSQLite", "import SQLite3"),
);

console.log("Synced Fantasma native sources into the React Native package.");
