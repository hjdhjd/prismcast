/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streamSettings.census.test.ts: Holds every read of a next-stream setting to the per-stream snapshot. A setting whose class is next-stream promises that a
 * save reaches the streams that start afterward and never one already running, which is true only while every stream reads such a setting from the snapshot
 * its registry entry carries rather than from the running configuration. The rows below keep that true as the code grows: no production module outside the
 * configuration layer reads a next-stream leaf, the snapshot's builder reads exactly the next-stream set, and production calls the builder once, from the
 * function that registers a stream's entry, with the running configuration.
 *
 * The census resolves reads with the TypeScript checker over the program the build itself compiles rather than matching text. A spelling census over
 * configuration chains needs escape heuristics and cannot see a read through a parameter typed as the configuration, an aliased group, a destructured binding,
 * or a spread; the checker resolves each of them to the property declaration it reads, and a read counts when that declaration is a next-stream leaf of the
 * configuration's own types. The program's root names are the files the build configuration parses to, so the census restates no list of what production
 * source is. The set it holds reads to is derived from the classification, so a setting flipped to next-stream is held to the snapshot the moment it flips.
 *
 * The census's stated boundary is the reads no static census can see: a dynamic path read through a computed key, and a configuration-layer function that
 * returned a next-stream leaf to a stream module. The configuration layer is exempt by role, as the owner of merging, normalizing and displaying every leaf.
 */
import { CONFIG_METADATA, getReactivityClass } from "./userConfig.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import ts from "typescript";

// The repository root, resolved from this file so the program does not depend on the directory the runner was started from.
const ROOT = path.join(import.meta.dirname, "..", "..");

// The configuration layer, whose modules are exempt from the offender row by role.
const CONFIG_LAYER = path.join(ROOT, "src", "config") + path.sep;

// The next-stream set, derived from the classification rather than restated, sorted.
const NEXT_STREAM_PATHS = Object.values(CONFIG_METADATA).flat().map((setting) => setting.path)
  .filter((settingPath) => getReactivityClass(settingPath) === "next-stream").toSorted();

// The in-memory fixture, served by the compiler host at a path under src/streaming/ that no file on disk occupies. The tree rows iterate the build's root
// names, which never include it, and the fixture row scans it alone.
const FIXTURE_PATH = path.join(ROOT, "src", "streaming", "streamSettingsCensus.fixture.ts");

// The fixture's lines. Each case sits on a line of its own, and the fixture row finds a case's line by the text it carries.
const FIXTURE_LINES = [
  "import { snapshotStreamSettings as copySettings } from \"../config/streamSettings.ts\";",
  "import { CONFIG } from \"../config/index.ts\";",
  "import type { Config } from \"../types/index.ts\";",
  "import { DEFAULTS } from \"../config/userConfig.ts\";",
  "import type { StreamSettings } from \"../config/streamSettings.ts\";",
  "type DurationOnly = Pick<Config[\"hls\"], \"segmentDuration\">;",
  "export function direct(): number { return CONFIG.hls.segmentDuration; }",
  "export function element(): number { return CONFIG.streaming[\"frameRate\"]; }",
  "export function aliased(): number { const group = CONFIG.playback; return group.monitorInterval; }",
  "export function destructured(): number { const { audioBitsPerSecond } = CONFIG.streaming; return audioBitsPerSecond; }",
  "export function parameter(config: Readonly<Config>): number { return config.streaming.videoBitsPerSecond; }",
  "export function spread(): object { return { ...CONFIG.hls }; }",
  "function snapshotStreamSettings(config: Config): number { return config.streaming.frameRate; }",
  "export function picked(value: DurationOnly): number { return value.segmentDuration; }",
  "export function member(settings: StreamSettings): number { return settings.segmentDuration; }",
  "// A comment naming segmentDuration, which no read resolves.",
  "export function text(): string { return \"segmentDuration\"; }",
  "export function other(): number { return CONFIG.hls.maxSegments; }",
  "export function calls(): StreamSettings[] { return [ copySettings(CONFIG), copySettings(DEFAULTS) ]; }",
  "export function local(): number { return snapshotStreamSettings(CONFIG); }"
];

// One read the collector found: the leaf's path, the source line, and the syntax it was read through.
interface LeafRead {

  readonly kind: "destructure" | "element" | "property" | "spread";
  readonly leaf: string;
  readonly line: number;
}

// One call the collector found to the builder: whether its argument is the running configuration, the function enclosing it, and the source line.
interface BuilderCall {

  readonly argumentIsConfig: boolean;
  readonly enclosing: ts.Node | undefined;
  readonly line: number;
}

// What the collector found under a node.
interface Collected {

  readonly calls: BuilderCall[];
  readonly reads: LeafRead[];
}

// What the census measures against: the checker, the build's root source files, the fixture, the leaf declarations keyed to their paths, and the builder,
// running-configuration and registration symbols.
interface Census {

  readonly builder: ts.Symbol;
  readonly checker: ts.TypeChecker;
  readonly config: ts.Symbol;
  readonly fixture: ts.SourceFile;
  readonly leaves: ReadonlyMap<ts.Declaration, string>;
  readonly registerStream: ts.Symbol;
  readonly roots: readonly ts.SourceFile[];
}

/**
 * Builds the program the build compiles, plus the in-memory fixture, and resolves the symbols the rows measure against.
 * @returns The census.
 */
function buildCensus(): Census {

  const parsed = ts.getParsedCommandLineOfConfigFile(path.join(ROOT, "tsconfig.build.json"), undefined, {

    ...ts.sys,
    onUnRecoverableConfigFileDiagnostic: (diagnostic: ts.Diagnostic): void => {

      throw new Error("The build configuration could not be read: " + ts.flattenDiagnosticMessageText(diagnostic.messageText, " ") + ".");
    }
  });

  assert.ok(parsed, "The build configuration parses.");

  // The compiler host serves the fixture from memory and every other file from disk, as the build's own host would.
  const fixtureSource = FIXTURE_LINES.join("\n") + "\n";
  const base = ts.createCompilerHost(parsed.options, true);
  const host: ts.CompilerHost = {

    ...base,
    fileExists: (fileName: string): boolean => (fileName === FIXTURE_PATH) || base.fileExists(fileName),
    getSourceFile: (fileName, languageVersionOrOptions, onError, shouldCreateNewSourceFile): ts.SourceFile | undefined => (fileName === FIXTURE_PATH) ?
      ts.createSourceFile(fileName, fixtureSource, languageVersionOrOptions, true) : base.getSourceFile(fileName, languageVersionOrOptions, onError,
        shouldCreateNewSourceFile),
    readFile: (fileName: string): string | undefined => (fileName === FIXTURE_PATH) ? fixtureSource : base.readFile(fileName)
  };

  const program = ts.createProgram({ host, options: parsed.options, rootNames: [ ...parsed.fileNames, FIXTURE_PATH ] });
  const checker = program.getTypeChecker();
  const sourceFile = (fileName: string): ts.SourceFile => {

    const file = program.getSourceFile(fileName);

    assert.ok(file, fileName + " is in the program.");

    return file;
  };
  const exported = (fileName: string, name: string): ts.Symbol => {

    const moduleSymbol = checker.getSymbolAtLocation(sourceFile(fileName));
    const symbol = moduleSymbol ? checker.getExportsOfModule(moduleSymbol).find((candidate) => candidate.name === name) : undefined;

    assert.ok(symbol, fileName + " exports " + name + ".");

    return symbol;
  };

  // The leaf declarations, resolved from the set down the configuration's own types: the declared type of the Config interface, then each path segment's
  // property and that property's type, so a leaf is its declaration and never its name.
  const leaves = new Map<ts.Declaration, string>();
  const configType = checker.getDeclaredTypeOfSymbol(exported(path.join(ROOT, "src", "types", "config.ts"), "Config"));

  for(const leafPath of NEXT_STREAM_PATHS) {

    let type: ts.Type = configType;
    let property: ts.Symbol | undefined;

    for(const segment of leafPath.split(".")) {

      property = type.getProperty(segment);

      assert.ok(property, leafPath + " resolves through the configuration's types.");
      type = checker.getTypeOfSymbol(property);
    }

    for(const declaration of property?.declarations ?? []) {

      leaves.set(declaration, leafPath);
    }
  }

  return {

    builder: exported(path.join(ROOT, "src", "config", "streamSettings.ts"), "snapshotStreamSettings"),
    checker,
    config: exported(path.join(ROOT, "src", "config", "index.ts"), "CONFIG"),
    fixture: sourceFile(FIXTURE_PATH),
    leaves,
    registerStream: exported(path.join(ROOT, "src", "streaming", "registry.ts"), "registerStream"),
    roots: parsed.fileNames.map(sourceFile)
  };
}

const census = buildCensus();

/**
 * Resolves the symbol at a node through any import alias to the declaration it names.
 * @param node - The node to resolve.
 * @returns The resolved symbol, or undefined when the node names none.
 */
function resolveSymbol(node: ts.Node): ts.Symbol | undefined {

  const symbol = census.checker.getSymbolAtLocation(node);

  return (symbol && (symbol.flags & ts.SymbolFlags.Alias)) ? census.checker.getAliasedSymbol(symbol) : symbol;
}

/**
 * Reports whether a resolved symbol names the same declaration as the symbol a row measures against.
 * @param symbol - The symbol a node resolved to.
 * @param target - The symbol the row measures against.
 * @returns True when the symbol carries the target's first declaration.
 */
function isSymbol(symbol: ts.Symbol | undefined, target: ts.Symbol): boolean {

  const declaration = symbol?.declarations?.[0];

  return (declaration !== undefined) && (declaration === target.declarations?.[0]);
}

/**
 * Returns the leaf a property symbol declares, or undefined when none of its declarations is a next-stream leaf.
 * @param property - The property symbol.
 * @returns The leaf's path.
 */
function leafOf(property: ts.Symbol | undefined): string | undefined {

  for(const declaration of property?.declarations ?? []) {

    const leaf = census.leaves.get(declaration);

    if(leaf !== undefined) {

      return leaf;
    }
  }

  return undefined;
}

/**
 * Returns the function-like node enclosing a node, or undefined at module scope.
 * @param node - The node.
 * @returns The enclosing function.
 */
function enclosingFunction(node: ts.Node): ts.Node | undefined {

  for(let current = node.parent as ts.Node | undefined; current !== undefined; current = current.parent) {

    if(ts.isFunctionLike(current)) {

      return current;
    }
  }

  return undefined;
}

/**
 * Collects every next-stream leaf read and every builder call under a node.
 * @param root - The node to walk.
 * @returns The reads and the calls, in source order.
 */
function collect(root: ts.Node): Collected {

  const sourceFile = root.getSourceFile();
  const lineOf = (node: ts.Node): number => sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile)).line + 1;
  const collected: Collected = { calls: [], reads: [] };
  const record = (kind: LeafRead["kind"], leaf: string | undefined, node: ts.Node): void => {

    if(leaf !== undefined) {

      collected.reads.push({ kind, leaf, line: lineOf(node) });
    }
  };

  const visit = (node: ts.Node): void => {

    if(ts.isPropertyAccessExpression(node)) {

      record("property", leafOf(census.checker.getSymbolAtLocation(node.name)), node);
    } else if(ts.isElementAccessExpression(node) && ts.isStringLiteralLike(node.argumentExpression)) {

      record("element", leafOf(census.checker.getPropertyOfType(census.checker.getTypeAtLocation(node.expression), node.argumentExpression.text)), node);
    } else if(ts.isBindingElement(node) && ts.isObjectBindingPattern(node.parent) && !node.dotDotDotToken) {

      const nameNode = node.propertyName ?? node.name;

      if(ts.isIdentifier(nameNode) || ts.isStringLiteralLike(nameNode)) {

        record("destructure", leafOf(census.checker.getPropertyOfType(census.checker.getTypeAtLocation(node.parent), nameNode.text)), node);
      }
    } else if(ts.isSpreadAssignment(node)) {

      for(const property of census.checker.getPropertiesOfType(census.checker.getTypeAtLocation(node.expression))) {

        record("spread", leafOf(property), node);
      }
    } else if(ts.isCallExpression(node)) {

      const callee = ts.isPropertyAccessExpression(node.expression) ? node.expression.name : node.expression;

      if(isSymbol(resolveSymbol(callee), census.builder)) {

        const [argument] = node.arguments;
        const argumentIsConfig = (argument !== undefined) && isSymbol(resolveSymbol(argument), census.config);

        collected.calls.push({ argumentIsConfig, enclosing: enclosingFunction(node), line: lineOf(node) });
      }
    }

    ts.forEachChild(node, visit);
  };

  visit(root);

  return collected;
}

/**
 * Renders a source file's path relative to the repository root.
 * @param sourceFile - The source file.
 * @returns The relative path.
 */
function relative(sourceFile: ts.SourceFile): string {

  return path.relative(ROOT, sourceFile.fileName);
}

/**
 * Returns the fixture line carrying a piece of text.
 * @param text - The text the line carries.
 * @returns The one-based line number.
 */
function fixtureLine(text: string): number {

  return FIXTURE_LINES.findIndex((line) => line.includes(text)) + 1;
}

describe("the next-stream read-site census", () => {

  test("the classification declares a next-stream set", () => {

    assert.ok(NEXT_STREAM_PATHS.length > 0, "The census holds reads to a set, and the classification declares no next-stream setting.");
  });

  test("no production module outside the configuration layer reads a next-stream setting", () => {

    const scanned = census.roots.filter((sourceFile) => !sourceFile.fileName.startsWith(CONFIG_LAYER));
    const offenders = scanned.flatMap((sourceFile) => collect(sourceFile).reads.map((read) => relative(sourceFile) + ":" + String(read.line) + " " + read.leaf));

    assert.ok(scanned.length > 0, "The build's program holds production modules outside the configuration layer to scan.");
    assert.deepEqual(offenders, [], "A production module outside the configuration layer reads a next-stream setting a stream must read from its snapshot.");
  });

  test("the snapshot's builder reads exactly the next-stream set", () => {

    const declaration = census.builder.declarations?.[0];

    assert.ok(declaration, "The builder has a declaration to read.");
    assert.deepEqual(collect(declaration).reads.map((read) => read.leaf).toSorted(), NEXT_STREAM_PATHS,
      "The builder reads a leaf outside the next-stream set, or misses one inside it.");
  });

  test("production calls the builder once, with the running configuration, from the function that registers a stream's entry", () => {

    // The registering function is found by role, as the one function holding a call that resolves to registerStream, so a refactor of the registration moves
    // the row's subject with it.
    const registering = new Set<ts.Node | undefined>();
    const calls: { call: BuilderCall; site: string }[] = [];

    for(const sourceFile of census.roots) {

      const visit = (node: ts.Node): void => {

        if(ts.isCallExpression(node) && isSymbol(resolveSymbol(node.expression), census.registerStream)) {

          registering.add(enclosingFunction(node));
        }

        ts.forEachChild(node, visit);
      };

      visit(sourceFile);
      calls.push(...collect(sourceFile).calls.map((call) => ({ call, site: relative(sourceFile) + ":" + String(call.line) })));
    }

    assert.equal(registering.size, 1, "Exactly one production function registers a stream's entry.");
    assert.equal(calls.length, 1, "Production calls the builder exactly once, and calls it at: " + calls.map(({ site }) => site).join(", ") + ".");

    const [only] = calls;

    assert.ok(only?.call.argumentIsConfig, "The builder's one call copies the running configuration.");
    assert.ok((only.call.enclosing !== undefined) && registering.has(only.call.enclosing),
      "The builder's one call sits in the function that registers a stream's entry.");
  });

  test("the collector resolves every read form and builder call in the fixture, and nothing else", () => {

    /* The fixture holds a read of each form the collector must see - direct, by string-literal element access, through an aliased group, by destructuring,
     * through a read-only configuration parameter, by spreading a group, inside a local function that shares the builder's name, and through a Pick-derived type -
     * beside forms it must not count: a snapshot member, a comment and a string naming a leaf, and a configuration leaf outside the set. Its calls are the
     * builder through an aliased import with the running configuration and with the defaults, and the local same-named function, which is not the builder. A
     * collector that resolves nothing, counts instead of matching, or exempts by name fails here rather than passing the empty-offender row.
     */
    const collected = collect(census.fixture);

    assert.deepEqual(collected.reads, [

      { kind: "property", leaf: "hls.segmentDuration", line: fixtureLine("function direct(") },
      { kind: "element", leaf: "streaming.frameRate", line: fixtureLine("function element(") },
      { kind: "property", leaf: "playback.monitorInterval", line: fixtureLine("function aliased(") },
      { kind: "destructure", leaf: "streaming.audioBitsPerSecond", line: fixtureLine("function destructured(") },
      { kind: "property", leaf: "streaming.videoBitsPerSecond", line: fixtureLine("function parameter(") },
      { kind: "spread", leaf: "hls.segmentDuration", line: fixtureLine("function spread(") },
      { kind: "property", leaf: "streaming.frameRate", line: fixtureLine("function snapshotStreamSettings(") },
      { kind: "property", leaf: "hls.segmentDuration", line: fixtureLine("function picked(") }
    ], "The collector finds exactly the fixture's leaf reads, each by its line, its form and its leaf.");
    assert.deepEqual(collected.calls.map(({ argumentIsConfig, line }) => ({ argumentIsConfig, line })), [

      { argumentIsConfig: true, line: fixtureLine("function calls(") },
      { argumentIsConfig: false, line: fixtureLine("function calls(") }
    ], "The collector finds exactly the fixture's aliased builder calls, each with its argument's verdict, and not the local same-named call.");
  });
});
