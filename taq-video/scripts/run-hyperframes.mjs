#!/usr/bin/env node

import { existsSync, readFileSync } from "node:fs";
import { delimiter, dirname, join, resolve } from "node:path";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const projectDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);
const cliPackagePath = join(projectDir, "node_modules", "hyperframes", "package.json");

if (!existsSync(cliPackagePath)) {
  console.error("HyperFrames is not installed in this video project. Run npm install first.");
  process.exit(1);
}

const cliPackage = JSON.parse(readFileSync(cliPackagePath, "utf8"));
const cliEntry = cliPackage.bin?.hyperframes;
if (typeof cliEntry !== "string") {
  console.error("The installed HyperFrames package does not expose its CLI entry point.");
  process.exit(1);
}

const ffmpegPath = require("ffmpeg-static");
const ffprobePath = join(
  projectDir,
  "node_modules",
  "@ffprobe-installer",
  "win32-x64",
  "ffprobe.exe",
);
if (!ffmpegPath || !existsSync(ffmpegPath) || !existsSync(ffprobePath)) {
  console.error("Local FFmpeg/FFprobe tools are missing. Run npm install first.");
  process.exit(1);
}

const localMediaPath = [dirname(ffmpegPath), dirname(ffprobePath)].join(delimiter);
const env = { ...process.env, PATH: `${localMediaPath}${delimiter}${process.env.PATH ?? ""}` };
const child = spawn(
  process.execPath,
  [join(projectDir, "node_modules", "hyperframes", cliEntry), ...process.argv.slice(2)],
  { cwd: projectDir, env, stdio: "inherit" },
);

child.on("error", (error) => {
  console.error(`Unable to start HyperFrames: ${error.message}`);
  process.exitCode = 1;
});
child.on("exit", (code, signal) => {
  if (signal) {
    console.error(`HyperFrames stopped by signal ${signal}.`);
    process.exitCode = 1;
  } else {
    process.exitCode = code ?? 1;
  }
});
