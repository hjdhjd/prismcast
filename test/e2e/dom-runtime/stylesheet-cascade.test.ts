/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * stylesheet-cascade.test.ts: Holds every declaration the served stylesheets ship to taking effect somewhere. A declaration that a later or more specific rule
 * always overrides is text that misleads its reader: the next person to change the look edits the line that says what they want, sees nothing move, and either
 * gives up or piles a third rule on top. The row below renders the landing and debug pages, reads their sheets through the CSSOM, and reports any declaration
 * that lost the cascade on every element it matched.
 *
 * Two limits are worth stating, because they bound what a green row means. Shorthands are compared as the longhands happy-dom expands them, so a `padding`
 * shorthand is judged as its four sides and a rule that restates one side of an inherited shorthand reads as four declarations rather than one. And a rule
 * that matches nothing the two pages render is not judged at all - it is unobserved, not alive - so a rule whose markup appears only behind an interaction
 * this tier does not drive falls outside the row's reach.
 */
import { CSSMediaRule, CSSStyleRule } from "happy-dom";
import type { CSSRule, Document, Element } from "happy-dom";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createDomTestContext } from "../../helpers/dom.helpers.ts";

// The pages the row renders. Between them they carry every stylesheet the server serves.
const PAGES = [ "/", "/debug" ];

/* Rules whose declarations lose on every element the pages render, and stay anyway. Each row names the selector and why the loss is expected rather than a
 * defect, so a new survivor cannot hide behind a blanket allowance.
 */
const EXEMPTIONS = [
  { reason: "Stream rows are built on the client from the status stream, so the server renders only the empty-state row. That row's own rule wins on its one " +
      "cell, which leaves the table's cell rule looking dead against a page that has no stream rows in it yet.", selector: ".streams-table td" }
];

// Pseudo-classes that describe a state the element is not in while the page sits still. A rule carrying one applies under a narrower condition than the same
// rule without it, so the walker records the state and matches against the stateless remainder.
const STATE_PATTERN = /:(hover|focus-visible|focus-within|focus|active|visited|target|checked|disabled|enabled|indeterminate|placeholder-shown)(?![\w-])/g;

// Pseudo-elements address a box the element generates rather than the element, so they are conditions in the same sense.
const PSEUDO_ELEMENT_PATTERN = /::?(before|after|placeholder|selection|-webkit-[\w-]+|-moz-[\w-]+|first-line|first-letter|marker|backdrop)(?![\w-])/g;

/** One declaration of one rule, carrying what the walker needs to judge it and what the failure message needs to name it. */
interface Declaration {

  /** The media and state conditions under which the declaration applies. A rule with a subset of these applies whenever this one does. */
  readonly conditions: string[];

  /** Element keys on which some applicable rule set the same property to a different value. */
  readonly deadFor: Set<string>;

  /** Whether the declaration carries !important, which outranks specificity. */
  readonly important: boolean;

  /** The rules that beat this declaration, as text, for the failure message. */
  readonly killers: Set<string>;

  /** The selector with its state conditions stripped, which is what elements are matched against. */
  readonly matchSelector: string;

  /** Element keys the declaration's selector matched. */
  readonly matched: Set<string>;

  /** Document order of the rule within its sheet, which breaks ties between equal specificities. */
  readonly order: number;

  /** The property name, compared exactly - a shorthand and its longhands are different properties here. */
  readonly property: string;

  /** The rule's full selector text, including every selector in a list, as written in the source. */
  readonly rule: string;

  /** The one selector of the list this declaration was judged under. */
  readonly selector: string;

  /** Specificity as id, class, and type counts. */
  readonly specificity: number[];

  /** The declared value. */
  readonly value: string;
}

/** The pruning keys taken from a selector's rightmost compound, used to skip rules that cannot match an element before the matcher runs. */
interface PruneKeys {

  readonly keyClass: string | null;
  readonly keyId: string | null;
  readonly keyTag: string | null;
}

/**
 * Counts a selector's specificity as ids, classes, and types. Attribute selectors and pseudo-classes count as classes; pseudo-elements count as types. The
 * functional forms are flattened to their contents first, which is close enough for the comparisons here because every selector in these sheets is simple.
 * @param selector - One selector, with its list already split.
 * @returns The three specificity counts, most significant first.
 */
function specificityOf(selector: string): number[] {

  const flattened = selector.replace(/\[[^\]]*\]/g, "[a]").replace(/:not\(|:is\(|:where\(/g, "(");
  const ids = (flattened.match(/#[\w-]+/g) ?? []).length;
  const classes = (flattened.match(/\.[\w-]+/g) ?? []).length + (flattened.match(/\[a\]/g) ?? []).length +
    (flattened.match(/(?<!:):(?!:)[\w-]+/g) ?? []).length;
  const types = (flattened.match(/(?:^|[\s>+~(,])[a-zA-Z][\w-]*/g) ?? []).length + (flattened.match(/::[\w-]+/g) ?? []).length;

  return [ ids, classes, types ];
}

/**
 * Compares two specificities.
 * @param left - The first specificity.
 * @param right - The second specificity.
 * @returns A positive number when left outranks right, negative when right outranks left, zero when they tie.
 */
function compareSpecificity(left: number[], right: number[]): number {

  for(const [ index, value ] of left.entries()) {

    if(value !== (right[index] ?? 0)) {

      return value - (right[index] ?? 0);
    }
  }

  return 0;
}

/**
 * Splits a selector into the part elements are matched against and the conditions under which it applies.
 * @param selector - One selector, with its list already split.
 * @returns The stateless selector to match with, and the conditions the stripped parts stand for.
 */
function analyzeSelector(selector: string): { conditions: string[]; matchSelector: string } {

  const conditions: string[] = [];
  const stateless = selector.replace(STATE_PATTERN, (_match, name: string): string => {

    conditions.push("state:" + name);

    return "";
  }).replace(PSEUDO_ELEMENT_PATTERN, (_match, name: string): string => {

    conditions.push("pseudo:" + name);

    return "";
  }).trim();

  return { conditions, matchSelector: (stateless.length > 0) ? stateless : "*" };
}

/**
 * Takes the rightmost compound of a complex selector, ignoring combinators that sit inside parentheses or brackets.
 * @param selector - One selector, with its list already split.
 * @returns The rightmost compound.
 */
function rightmostCompound(selector: string): string {

  let depth = 0;
  let start = 0;

  for(const [ index, character ] of Array.from(selector).entries()) {

    if((character === "(") || (character === "[")) {

      depth++;

      continue;
    }

    if((character === ")") || (character === "]")) {

      depth--;

      continue;
    }

    if((depth === 0) && [ " ", ">", "+", "~" ].includes(character)) {

      start = index + 1;
    }
  }

  return selector.slice(start).trim();
}

/**
 * Reads the class, id, and tag a selector's rightmost compound requires, so an element missing all of them can skip the matcher entirely.
 * @param matchSelector - The stateless selector.
 * @returns The pruning keys, each null when the compound does not constrain that dimension.
 */
function pruneKeysOf(matchSelector: string): PruneKeys {

  const compound = rightmostCompound(matchSelector).replace(/\[[^\]]*\]/g, "").replace(/:not\([^)]*\)/g, "");

  return {

    keyClass: (/\.([\w-]+)/).exec(compound)?.[1] ?? null,
    keyId: (/#([\w-]+)/).exec(compound)?.[1] ?? null,
    keyTag: (/^([a-zA-Z][\w-]*)/).exec(compound)?.[1]?.toLowerCase() ?? null
  };
}

/**
 * Builds a stable key for one rendered element: its own tag, id, and classes, prefixed by every ancestor's, with each step carrying its position among its
 * siblings. The position is what makes a structural pseudo-class exact, and the ancestor chain is what tells two same-looking cells in different tables apart.
 * @param element - The rendered element.
 * @param keys - The cache the walk fills as it climbs, so an ancestor chain is built once per element rather than once per descendant.
 * @returns The element's key.
 */
function elementKey(element: Element, keys: WeakMap<Element, string>): string {

  const cached = keys.get(element);

  if(cached !== undefined) {

    return cached;
  }

  const classes = element.getAttribute("class");
  const parent = element.parentElement;
  const siblings = parent ? Array.from(parent.children) : [element];
  const own = element.tagName.toLowerCase() + (element.id ? "#" + element.id : "") +
    (classes ? "." + classes.trim().split(/\s+/).toSorted().join(".") : "") + "@" + String(siblings.indexOf(element) + 1);
  const key = parent ? elementKey(parent, keys) + ">" + own : own;

  keys.set(element, key);

  return key;
}

/**
 * Collects every declaration of every rule in a document's <style> sheets, splitting selector lists and recursing into media rules.
 * @param document - The rendered document.
 * @returns One entry per declaration per selector.
 */
function collectDeclarations(document: Document): Declaration[] {

  const declarations: Declaration[] = [];

  let order = 0;

  const walk = (rules: CSSRule[], mediaConditions: string[]): void => {

    for(const rule of rules) {

      if(rule instanceof CSSMediaRule) {

        walk(Array.from(rule.cssRules), [ ...mediaConditions, "media:" + rule.conditionText ]);

        continue;
      }

      if(!(rule instanceof CSSStyleRule)) {

        continue;
      }

      order++;

      const style = rule.style;

      for(const selector of rule.selectorText.split(",").map((entry) => entry.trim()).filter((entry) => entry.length > 0)) {

        const { conditions, matchSelector } = analyzeSelector(selector);
        const specificity = specificityOf(selector);

        // The declaration list is index-addressed rather than iterable, so a counting loop is what reads it; index math is the exception the house style names.
        for(let index = 0; index < style.length; index++) {

          const property = style.item(index);

          // Custom properties are inherited values rather than rendered ones, and a later definition of one is a redefinition rather than an override.
          if(property.startsWith("--")) {

            continue;
          }

          declarations.push({

            conditions: [ ...mediaConditions, ...conditions ],
            deadFor: new Set<string>(),
            important: style.getPropertyPriority(property) === "important",
            killers: new Set<string>(),
            matchSelector,
            matched: new Set<string>(),
            order,
            property,
            rule: rule.selectorText,
            selector,
            specificity,
            value: style.getPropertyValue(property)
          });
        }
      }
    }
  };

  for(const styleElement of Array.from(document.querySelectorAll("style"))) {

    const sheet = (styleElement as unknown as { sheet?: { cssRules: CSSRule[] } }).sheet;

    if(sheet) {

      walk(Array.from(sheet.cssRules), []);
    }
  }

  return declarations;
}

/**
 * Matches every declaration against every element the document rendered and records, per element, whether some applicable rule set the same property to a
 * different value. Candidates are pruned by the rightmost compound's class, id, or tag first, because most rules cannot match most elements.
 * @param document - The rendered document.
 * @param declarations - The declarations collected from the document's sheets.
 * @param page - The page path, which prefixes the element keys so the two pages' elements stay distinct.
 */
function judgeAgainstElements(document: Document, declarations: Declaration[], page: string): void {

  const byClass = new Map<string, Declaration[]>();
  const byId = new Map<string, Declaration[]>();
  const byTag = new Map<string, Declaration[]>();
  const unpruned: Declaration[] = [];

  for(const declaration of declarations) {

    const { keyClass, keyId, keyTag } = pruneKeysOf(declaration.matchSelector);
    const index = keyClass ? byClass : (keyId ? byId : (keyTag ? byTag : null));
    const key = keyClass ?? keyId ?? keyTag;

    if(!index || (key === null)) {

      unpruned.push(declaration);

      continue;
    }

    index.set(key, [ ...(index.get(key) ?? []), declaration ]);
  }

  const keys = new WeakMap<Element, string>();

  for(const element of Array.from(document.querySelectorAll("*"))) {

    const key = page + " " + elementKey(element, keys);
    const classes = (element.getAttribute("class") ?? "").trim().split(/\s+/).filter((entry) => entry.length > 0);
    const candidates = [ ...unpruned, ...(byId.get(element.id) ?? []), ...(byTag.get(element.tagName.toLowerCase()) ?? []) ];

    for(const className of classes) {

      candidates.push(...(byClass.get(className) ?? []));
    }

    const byProperty = new Map<string, Declaration[]>();

    for(const declaration of candidates) {

      if(!element.matches(declaration.matchSelector)) {

        continue;
      }

      declaration.matched.add(key);
      byProperty.set(declaration.property, [ ...(byProperty.get(declaration.property) ?? []), declaration ]);
    }

    for(const competing of byProperty.values()) {

      for(const declaration of competing) {

        /* A rule beats this one when it sets a different value, applies whenever this one applies (its conditions are a subset), and outranks it - by
         * importance first, then specificity, then document order.
         */
        const killer = competing.find((candidate) => (candidate !== declaration) && (candidate.value !== declaration.value) &&
          candidate.conditions.every((condition) => declaration.conditions.includes(condition)) &&
          ((candidate.important && !declaration.important) || ((candidate.important === declaration.important) &&
            ((compareSpecificity(candidate.specificity, declaration.specificity) > 0) ||
              ((compareSpecificity(candidate.specificity, declaration.specificity) === 0) && (candidate.order > declaration.order))))));

        if(killer) {

          declaration.deadFor.add(key);
          declaration.killers.add(killer.selector + " { " + killer.property + ": " + killer.value + " }");
        }
      }
    }
  }
}

/**
 * Merges the per-page declaration records that describe one source declaration, so a rule alive for one of its selectors or on one page is alive.
 * @param declarations - Every declaration from every page.
 * @returns One record per source declaration, with the pages' matches and losses combined.
 */
function mergeByRule(declarations: Declaration[]): Declaration[] {

  const merged = new Map<string, Declaration>();

  for(const declaration of declarations) {

    const key = [ declaration.rule, declaration.property, declaration.value, String(declaration.important),
      declaration.conditions.filter((condition) => condition.startsWith("media:")).join(",") ].join("|");
    const existing = merged.get(key);

    if(!existing) {

      merged.set(key, declaration);

      continue;
    }

    for(const element of declaration.matched) {

      existing.matched.add(element);
    }

    for(const element of declaration.deadFor) {

      existing.deadFor.add(element);
    }

    for(const killer of declaration.killers) {

      existing.killers.add(killer);
    }
  }

  return Array.from(merged.values());
}

/**
 * Renders the pages and reports every declaration that matched at least one element and lost the cascade on all of them.
 * @returns The dead declarations, each rendered as a line naming the rule, the property, the value, and what beat it.
 */
async function findDeadDeclarations(): Promise<{ lines: string[]; selectors: string[] }> {

  const collected: Declaration[] = [];

  for(const page of PAGES) {

    // eslint-disable-next-line no-await-in-loop -- sequential by design: each page boots its own listener, and the binding disposes it before the next starts.
    await using ctx = await createDomTestContext({ path: page });

    const document = ctx.document;
    const declarations = collectDeclarations(document);

    judgeAgainstElements(document, declarations, page);
    collected.push(...declarations);
  }

  const dead = mergeByRule(collected).filter((declaration) => (declaration.matched.size > 0) && (declaration.deadFor.size === declaration.matched.size));

  return {

    lines: dead.map((declaration) => declaration.rule + " { " + declaration.property + ": " + declaration.value + " } lost to " +
      Array.from(declaration.killers).join(" and ")),
    selectors: dead.map((declaration) => declaration.rule)
  };
}

describe("the served stylesheets against the cascade", () => {

  test("ships no declaration that a rule applying whenever it applies always overrides", async () => {

    const { lines, selectors } = await findDeadDeclarations();
    const exempted = new Set(EXEMPTIONS.map((entry) => entry.selector));
    const survivors = lines.filter((_line, index) => !exempted.has(selectors[index] ?? ""));

    assert.deepEqual(survivors, [], "a declaration that never wins is text that misleads its reader; remove it, or add an exemption row saying why it stays");
  });

  test("reports a planted pair of same-specificity rules that disagree", async () => {

    /* The negative half of the row above. A walker that silently matched nothing - a broken selector split, a sheet it failed to read - would report an empty
     * dead set and read as a pass. Planting a rule that a later one of equal specificity overrides, on an element that exists, proves the row can still see.
     */
    await using ctx = await createDomTestContext({ path: "/" });

    const document = ctx.document;

    document.head.insertAdjacentHTML("beforeend",
      "<style>.planted-cascade-probe { color: rgb(1, 2, 3); } .planted-cascade-probe { color: rgb(4, 5, 6); }</style>");
    document.body.insertAdjacentHTML("beforeend", "<div class=\"planted-cascade-probe\">probe</div>");

    const declarations = collectDeclarations(document);

    judgeAgainstElements(document, declarations, "/");

    const dead = mergeByRule(declarations).filter((declaration) => (declaration.matched.size > 0) &&
      (declaration.deadFor.size === declaration.matched.size) && declaration.rule.includes("planted-cascade-probe"));

    assert.deepEqual(dead.map((declaration) => declaration.property + ": " + declaration.value), ["color: rgb(1, 2, 3)"],
      "the walker names the overridden declaration of the planted pair, and only that one");
  });
});
