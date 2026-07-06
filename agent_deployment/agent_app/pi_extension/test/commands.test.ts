import test from "node:test";
import assert from "node:assert/strict";
import { parseDtSystemArgs } from "../datatailr-system-builder/commands.ts";

test("parseDtSystemArgs defaults to new when no args are provided", () => {
  assert.deepEqual(parseDtSystemArgs(""), { action: "new" });
});

test("parseDtSystemArgs extracts action and target", () => {
  assert.deepEqual(parseDtSystemArgs("deploy market_data"), { action: "deploy", target: "market_data" });
  assert.deepEqual(parseDtSystemArgs("logs market-data-app"), { action: "logs", target: "market-data-app" });
});
