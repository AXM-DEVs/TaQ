#!/usr/bin/env node

import { readFileSync, writeFileSync } from "node:fs";
import { delimiter, dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const projectDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const skillsDir =
  process.env.HYPERFRAMES_SKILL_DIR ??
  join(process.env.USERPROFILE ?? "", ".agents", "skills", "product-launch-video", "scripts");
const assemblerPath = join(skillsDir, "assemble-index.mjs");

try {
  readFileSync(assemblerPath);
} catch (error) {
  console.error(`Product-launch assembler not found at ${assemblerPath}: ${error.message}`);
  process.exit(1);
}

const toolDirs = [
  dirname(resolve(projectDir, "node_modules", "ffmpeg-static", "ffmpeg.exe")),
  resolve(projectDir, "node_modules", "@ffprobe-installer", "win32-x64"),
];
const env = { ...process.env, PATH: `${toolDirs.join(delimiter)}${delimiter}${process.env.PATH ?? ""}` };
const result = spawnSync(
  process.execPath,
  [assemblerPath, "--hyperframes", projectDir, ...process.argv.slice(2)],
  { cwd: projectDir, env, stdio: "inherit" },
);

if (result.error) {
  console.error(`Unable to start product-launch assembler: ${result.error.message}`);
  process.exit(1);
}
if (result.status !== 0) process.exit(result.status ?? 1);

const indexPath = join(projectDir, "index.html");
const html = readFileSync(indexPath, "utf8");
const cdnGsap = "https://cdn.jsdelivr.net/npm/gsap@3.14.2/dist/gsap.min.js";
if (!html.includes(cdnGsap)) {
  console.error("Expected GSAP CDN script was not found in assembled index.html.");
  process.exit(1);
}
writeFileSync(indexPath, html.replace(cdnGsap, "node_modules/gsap/dist/gsap.min.js"), "utf8");
console.log("✓ GSAP is served from the project's local dependencies.");
