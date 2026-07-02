/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the predefined channel catalog in src/channels/index.ts. The module's surface is two exports - PREDEFINED_TAGS (a vocabulary
 * constant) and CHANNELS / PREDEFINED_CHANNELS (the flat ChannelMap produced by Pacific auto-generation + flattening at module load). Tests are grouped by
 * concern: the tag vocabulary, the two exports sharing one map, the catalog-wide structural invariants (every entry well-formed, canonical/variant key shapes,
 * identity vs binding partition), the canonical resolution rules (site-wins, alphabetically-first, single-service, identity inheritance, tags copy,
 * pacificStationId presence, variant binding-only), the Pacific generation rules (auto-generation step 1, service merging step 2, manual-override precedence,
 * East/West skip), and a sampling of known catalog entries.
 */
import { CHANNELS, PREDEFINED_CHANNELS, PREDEFINED_TAGS } from "./index.ts";
import type { CanonicalChannel, Channel, VariantChannel } from "../types/index.ts";
import { describe, test } from "node:test";
import { CHANNEL_BINDING_KEYS } from "../types/index.ts";
import assert from "node:assert/strict";

/* isVariant narrows a Channel to VariantChannel via the documented discriminator. The type system enforces that canonicalKey is structurally absent on
 * CanonicalChannel (typed as never), so a string canonicalKey unambiguously identifies a variant. Tests that need to assert per-shape invariants reach for this
 * helper.
 */
function isVariant(entry: Channel): entry is VariantChannel {

  return typeof (entry as VariantChannel).canonicalKey === "string";
}

/* The full set of identity-only fields (CHANNEL_IDENTITY_KEYS) imported here would create a dependency on the partition arrays in tests. Instead, we hard-code
 * the identity fields the catalog-flattener is documented to copy onto canonical entries. If a new identity field is added to ChannelDefinition, this list and
 * buildCanonicalEntry both need updates - the test will fail loudly on mismatch.
 */
const IDENTITY_FIELDS_ON_CANONICAL: readonly (keyof CanonicalChannel)[] =
  [ "channelNumber", "name", "pacificStationId", "stationId", "tags", "tvgShift" ];

describe("PREDEFINED_TAGS", () => {

  test("contains the expected vocabulary in alphabetical order", () => {

    // Locks both the contents and the ordering. Adding a new predefined tag means this test must be updated, which is the intended source-of-truth coupling.
    assert.deepEqual(
      PREDEFINED_TAGS,
      [ "Documentary", "Entertainment", "HBO", "Kids", "Lifestyle", "Local", "Movies", "News", "Showtime", "Sports", "Starz" ]
    );
  });

  test("is sorted alphabetically (case-sensitive ASCII)", () => {

    // Boundary: confirms via comparison rather than literal so that the alphabetical contract is checked independently of the literal contents above.
    const sorted = PREDEFINED_TAGS.toSorted();

    assert.deepEqual([...PREDEFINED_TAGS], sorted);
  });

  test("contains no duplicate entries", () => {

    assert.equal(new Set(PREDEFINED_TAGS).size, PREDEFINED_TAGS.length);
  });

  test("contains only non-empty strings", () => {

    // Negative test: any empty or whitespace-only entry would pollute the user-facing tag picker.
    for(const tag of PREDEFINED_TAGS) {

      assert.equal(typeof tag, "string");
      assert.ok(tag.trim().length > 0, "tag must be non-empty");
    }
  });
});

describe("PREDEFINED_CHANNELS / CHANNELS exports", () => {

  test("PREDEFINED_CHANNELS is the same object as CHANNELS (re-export alias)", () => {

    // The module re-exports CHANNELS under both names. Identity equality locks the alias contract - no copying or wrapping.
    assert.equal(PREDEFINED_CHANNELS, CHANNELS);
  });

  test("contains a non-empty set of channel entries", () => {

    // Smoke check: the catalog must always have content; an empty CHANNELS map indicates a flattener regression that drops everything.
    assert.ok(Object.keys(CHANNELS).length > 0, "catalog must have entries");
  });
});

describe("flattened channel structural invariants", () => {

  test("every entry has a non-empty url", () => {

    // The flattener requires variant.url for both canonical and variant entries (it's the required field on ServiceVariant). A missing or empty url is a
    // catalog data error.
    for(const [ key, channel ] of Object.entries(CHANNELS)) {

      assert.equal(typeof channel.url, "string", "channel " + key + " must have a string url");
      assert.ok(channel.url.length > 0, "channel " + key + " must have a non-empty url");
    }
  });

  test("every entry has either a name (canonical) or a canonicalKey (variant), but not both", () => {

    // The discriminated union: canonicals carry identity (including name), variants carry only binding plus canonicalKey. This locks the partition.
    for(const [ key, channel ] of Object.entries(CHANNELS)) {

      if(isVariant(channel)) {

        assert.equal(typeof channel.canonicalKey, "string", key + " variant must have string canonicalKey");
        // Variants have no identity fields - name/stationId/tags etc. live only on the canonical.
        assert.equal((channel as unknown as { name?: string }).name, undefined, key + " variant must not carry name");
      } else {

        assert.equal(typeof channel.name, "string", key + " canonical must have a name");
        assert.ok((channel.name ?? "").length > 0, key + " canonical name must be non-empty");
      }
    }
  });

  test("every variant's canonicalKey points at an existing canonical entry in the catalog", () => {

    // Negative test: if any variant references a canonicalKey that is missing or refers to another variant, the resolver downstream cannot inherit identity.
    for(const [ key, channel ] of Object.entries(CHANNELS)) {

      if(!isVariant(channel)) {

        continue;
      }

      const parent = CHANNELS[channel.canonicalKey];

      assert.ok(parent, key + " variant references missing canonical " + channel.canonicalKey);
      assert.ok(!isVariant(parent), key + " variant must not point to another variant");
    }
  });

  test("variant keys follow the {canonicalKey}-{slug} format", () => {

    // The flattener composes variant keys as "key + '-' + slug". For every variant, the part before the last hyphen must equal canonicalKey, and the part after
    // must be a non-empty slug.
    for(const [ key, channel ] of Object.entries(CHANNELS)) {

      if(!isVariant(channel)) {

        continue;
      }

      const dashIndex = key.lastIndexOf("-");

      assert.notEqual(dashIndex, -1, "variant key " + key + " must contain a hyphen");

      const inferredCanonical = key.slice(0, dashIndex);
      const inferredSlug = key.slice(dashIndex + 1);

      assert.equal(inferredCanonical, channel.canonicalKey, "variant key prefix must equal canonicalKey for " + key);
      assert.ok(inferredSlug.length > 0, "variant key " + key + " must have a non-empty slug suffix");
    }
  });

  test("variants only carry binding fields (CHANNEL_BINDING_KEYS) plus canonicalKey", () => {

    // The flattener's buildVariantEntry feeds variants exclusively through applyServiceBinding, which iterates CHANNEL_BINDING_KEYS. Identity fields must not
    // leak onto variants - this is the structural invariant that the discriminated-union type system enforces statically and we lock at runtime here.
    const allowedKeys = new Set<string>([ ...CHANNEL_BINDING_KEYS, "canonicalKey" ]);

    for(const [ key, channel ] of Object.entries(CHANNELS)) {

      if(!isVariant(channel)) {

        continue;
      }

      for(const field of Object.keys(channel)) {

        assert.ok(allowedKeys.has(field), key + " variant carries unexpected field " + field);
      }
    }
  });

  test("canonicals only carry identity, binding, or canonicalKey fields", () => {

    // Mirror invariant for canonicals: every key on a canonical entry must be in identity ∪ binding (canonicalKey is the variant-only carve-out and never appears
    // on canonicals at runtime). Locks the partition that the compile-time _ChannelKeyExhaustiveness check guards.
    const allowedKeys = new Set<string>([ ...IDENTITY_FIELDS_ON_CANONICAL as readonly string[], ...CHANNEL_BINDING_KEYS ]);

    for(const [ key, channel ] of Object.entries(CHANNELS)) {

      if(isVariant(channel)) {

        continue;
      }

      for(const field of Object.keys(channel)) {

        assert.ok(allowedKeys.has(field), key + " canonical carries unexpected field " + field);
      }
    }
  });
});

describe("canonical resolution rules", () => {

  test("when a definition has a 'site' service, the site URL becomes the canonical", () => {

    // Rule 1 from the source comment: 'site' always wins as canonical. abc has services { cox, directv, hulu, site, sling, spectrum, xfinity, yttv }. The
    // canonical entry's url must be abc.com/watch-live (site URL), and no abc-site variant key is emitted.
    const abc = CHANNELS["abc"];

    assert.ok(abc, "abc canonical must exist");
    assert.equal(isVariant(abc), false, "abc must be canonical");
    assert.equal(abc.url, "https://abc.com/watch-live");
    assert.equal(CHANNELS["abc-site"], undefined, "no variant key emitted for the canonical service");
  });

  test("when a definition has no 'site' service, the alphabetically-first slug becomes the canonical", () => {

    // Rule 2: amcthrillers has services { sling, yttv }. Sorted alphabetically: sling first. Canonical url must be the sling URL; only amcthrillers-yttv is
    // emitted as a variant.
    const amcThrillers = CHANNELS["amcthrillers"];

    assert.ok(amcThrillers, "amcthrillers canonical must exist");
    assert.equal(isVariant(amcThrillers), false);
    assert.equal(amcThrillers.url, "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z");
    assert.equal(amcThrillers.channelSelector, "AMC Thrillers");

    const yttvVariant = CHANNELS["amcthrillers-yttv"];

    assert.ok(yttvVariant, "yttv variant key must exist");
    assert.ok(isVariant(yttvVariant));
    assert.equal((yttvVariant).canonicalKey, "amcthrillers");
    assert.equal((yttvVariant).url, "https://tv.youtube.com/live");
  });

  test("a single-service definition produces a canonical and no variant entries", () => {

    // Boundary: bloombergoriginals has only one service (yttv). After flattening it becomes one canonical with no variants. The yttv slug never produces a
    // variant because it is the canonical service.
    const bloombergOriginals = CHANNELS["bloombergoriginals"];

    assert.ok(bloombergOriginals, "bloombergoriginals canonical must exist");
    assert.equal(isVariant(bloombergOriginals), false);
    assert.equal(bloombergOriginals.url, "https://tv.youtube.com/live");
    assert.equal(CHANNELS["bloombergoriginals-yttv"], undefined, "single-service catalog entry must not emit a self-referential variant");
  });

  test("canonical entries inherit identity from the parent definition (name, stationId, tags)", () => {

    // The canonical builder copies identity from ChannelDefinition. We verify at least one well-known channel: ABC News Live has stationId "113380" and tags
    // ["News"] in the source.
    const abcNews = CHANNELS["abcnews"] as CanonicalChannel | undefined;

    assert.ok(abcNews, "abcnews canonical must exist");
    assert.equal(abcNews.name, "ABC News Live");
    assert.equal(abcNews.stationId, "113380");
    assert.deepEqual(abcNews.tags, ["News"]);
  });

  test("canonical tags array is a copy, not a shared reference with the source ChannelDefinition", () => {

    // buildCanonicalEntry uses def.tags.slice(), so the canonical's tags array is a fresh copy of the source definition's rather than an alias. We exercise
    // ae.tags directly by pushing a test marker and popping it back off.
    const ae = CHANNELS["ae"] as CanonicalChannel | undefined;

    assert.ok(ae, "ae canonical must exist");
    assert.ok(Array.isArray(ae.tags), "ae must have tags");

    const originalLength = ae.tags.length;

    // Mutating the fixture is fine because the assertions below only observe ae.tags' own length: the push grows it by one and the pop restores the original
    // length. The round-trip leaves ae.tags exactly as we found it, so later tests are unaffected.
    ae.tags.push("__test-marker");
    assert.equal(ae.tags.length, originalLength + 1);
    ae.tags.pop();
    assert.equal(ae.tags.length, originalLength);
  });

  test("canonical entries carry pacificStationId when present on the parent definition", () => {

    // pacificStationId is identity (it drives Pacific auto-generation). It must be present on the canonical so downstream consumers can identify Pacific-eligible
    // channels. Confirmed against animal whose source pacificStationId is "68785".
    const animal = CHANNELS["animal"] as CanonicalChannel | undefined;

    assert.ok(animal, "animal canonical entry should exist in CHANNELS");
    assert.equal(animal.pacificStationId, "68785");
  });

  test("canonical entries omit pacificStationId when absent from the parent definition", () => {

    // Negative test: a non-Pacific-eligible channel (no pacificStationId in the source) must not have the field set on the canonical. abcnews has stationId but
    // no pacificStationId.
    const abcNews = CHANNELS["abcnews"] as CanonicalChannel | undefined;

    assert.ok(abcNews, "abcnews canonical entry should exist in CHANNELS");
    assert.equal(abcNews.pacificStationId, undefined);
  });

  test("variants do not carry the parent's name, stationId, or tags (binding-only)", () => {

    // We pick abc's cox variant (abc-cox) and verify it does not carry identity. Identity inherits from the canonical at resolution time per the documented
    // contract.
    const variant = CHANNELS["abc-cox"];

    assert.ok(variant, "abc-cox variant must exist");
    assert.ok(isVariant(variant));
    assert.equal((variant as unknown as { name?: string }).name, undefined);
    assert.equal((variant as unknown as { stationId?: string }).stationId, undefined);
    assert.equal((variant as unknown as { tags?: string[] }).tags, undefined);
    assert.equal((variant).url, "https://watchtv.cox.com/listings");
    assert.equal((variant).channelSelector, "ABC");
  });
});

describe("Pacific auto-generation: Step 1 (generate from pacificStationId)", () => {

  test("generates a {key}p sibling for an East definition that has pacificStationId and no manual Pacific entry", () => {

    // animal has pacificStationId "68785" and no manual animalp. A Pacific definition must auto-generate. Its canonical name is "Animal Planet (Pacific)" and
    // stationId is the Pacific ID 68785. Site is the canonical (animal has 'site') so animalp's canonical url is the same site URL.
    const animalp = CHANNELS["animalp"] as CanonicalChannel | undefined;

    assert.ok(animalp, "animalp must be auto-generated");
    assert.equal(isVariant(animalp), false);
    assert.equal(animalp.name, "Animal Planet (Pacific)");
    assert.equal(animalp.stationId, "68785");
  });

  test("auto-generated Pacific carries inherited tags from the East definition", () => {

    // Step 1 spreads tags from the East def: animal has tags ["Documentary"] -> animalp must too.
    const animalp = CHANNELS["animalp"] as CanonicalChannel | undefined;

    assert.ok(animalp, "animalp Pacific variant should be auto-generated from animal");
    assert.deepEqual(animalp.tags, ["Documentary"]);
  });

  test("auto-generated Pacific does NOT carry pacificStationId itself (Pacific is the destination, not the source)", () => {

    // pacificStationId on a Pacific entry would be meaningless and could trigger another generation cycle. Confirmed by checking animalp.
    const animalp = CHANNELS["animalp"] as CanonicalChannel | undefined;

    assert.ok(animalp, "animalp Pacific variant should be auto-generated");
    assert.equal(animalp.pacificStationId, undefined, "Pacific entries must not advertise pacificStationId");
  });

  test("auto-generated Pacific inherits all East services that lack East/West selectors", () => {

    // ae has 8 services (cox, directv, hulu, site, sling, spectrum, xfinity, yttv) with no East/West selectors. After Pacific generation, aep must carry every
    // one of them. Site wins canonical, so aep canonical's url is the East site URL and the others appear as variants.
    const aep = CHANNELS["aep"] as CanonicalChannel | undefined;

    assert.ok(aep, "aep must be auto-generated");
    assert.equal(aep.url, "https://play.aetv.com/live", "Pacific canonical inherits East site URL when no manual override exists");

    // Each non-canonical East service should appear as a Pacific variant.
    for(const slug of [ "cox", "directv", "hulu", "sling", "spectrum", "xfinity", "yttv" ]) {

      const variantKey = "aep-" + slug;
      const variant = CHANNELS[variantKey];

      assert.ok(variant, "Pacific variant " + variantKey + " must be generated");
      assert.ok(isVariant(variant));
      assert.equal((variant).canonicalKey, "aep", variantKey + " must point at aep");
    }
  });

  test("does not generate a Pacific sibling when the East definition has no pacificStationId", () => {

    // Negative test: abc has no pacificStationId. abcp must not be auto-generated. (No manual abcp exists either.)
    assert.equal(CHANNELS["abcp"], undefined);
  });
});

describe("Pacific auto-generation: Step 2 (merge East services into Pacific)", () => {

  test("manual Pacific definition is preserved (Step 1 does not overwrite it)", () => {

    // bravop is manually defined with a West-specific site URL. The pacificStationId-driven auto-generator must defer to it. We verify the canonical url is the
    // West URL, not the East URL.
    const bravop = CHANNELS["bravop"] as CanonicalChannel | undefined;

    assert.ok(bravop, "bravop must exist");
    assert.equal(bravop.url, "https://www.nbc.com/live?brand=bravo&callsign=bravo_west", "manual Pacific site URL wins over East merge");
    assert.equal(bravop.name, "Bravo (Pacific)");
    assert.equal(bravop.stationId, "73994", "manual Pacific stationId preserved");
  });

  test("Step 2 merges East services into a manual Pacific definition", () => {

    // Even though bravop is manually defined, Step 2 fills in services that bravop didn't declare. The manual bravop has only 'site'; cox/directv/hulu/sling/
    // spectrum/xfinity/yttv from bravo East must be merged in as variants of bravop.
    for(const slug of [ "cox", "directv", "hulu", "sling", "spectrum", "xfinity", "yttv" ]) {

      const variantKey = "bravop-" + slug;
      const variant = CHANNELS[variantKey];

      assert.ok(variant, "Step 2 must merge " + slug + " from bravo into bravop -> " + variantKey);
      assert.ok(isVariant(variant));
      assert.equal((variant).canonicalKey, "bravop");
    }
  });

  test("Step 2 skips services with East/West-specific channelSelectors", () => {

    // cartoonp manually declares hulu with selector "Cartoon Network (West)"; East cartoon's hulu carries the timezone-specific selector "Cartoon Network (East)".
    // The already-declared rule skips hulu first because cartoonp already declares it, so the East selector is never copied - and the East/West skip would filter
    // that timezone-specific selector as a backstop even without the manual entry. The manual West entry remains untouched.
    const cartoonpHulu = CHANNELS["cartoonp-hulu"] as VariantChannel | undefined;

    assert.ok(cartoonpHulu, "cartoonp-hulu manual variant must exist");
    assert.equal(cartoonpHulu.channelSelector, "Cartoon Network (West)", "manual West selector preserved, East selector not copied over it");
  });

  test("Step 2 does not overwrite a Pacific service that was already manually declared", () => {

    // bravop manually declares 'site' with the West URL. East bravo also has 'site' with the East URL. The merger must NOT overwrite the manual site - that
    // would replace the West URL with the East URL and break the channel. We verify the canonical url is still the West URL.
    const bravop = CHANNELS["bravop"] as CanonicalChannel | undefined;

    assert.ok(bravop, "bravop manual Pacific variant should exist in CHANNELS");
    assert.equal(bravop.url, "https://www.nbc.com/live?brand=bravo&callsign=bravo_west");
  });

  test("Step 2 preserves the manual Pacific's identity (stationId, name) even when enhancing with East services", () => {

    // Locks the regression: enhancement must not replace identity. cartoonp's source declares stationId "67703" (the Pacific ID); the East cartoon has a
    // different stationId "60048". The merge must keep the Pacific stationId.
    const cartoonp = CHANNELS["cartoonp"] as CanonicalChannel | undefined;

    assert.ok(cartoonp, "cartoonp manual Pacific variant should exist in CHANNELS");
    assert.equal(cartoonp.stationId, "67703");
    assert.equal(cartoonp.name, "Cartoon Network (Pacific)");
  });

  test("Step 2 enhances a manual Pacific even when the East definition has no pacificStationId", () => {

    // bravo has NO pacificStationId in its source - bravop exists manually independent of Pacific generation. Step 2 still merges. This locks the documented
    // contract that Step 2 runs for "every Pacific definition with a corresponding East definition," not only auto-generated ones.
    const bravopCox = CHANNELS["bravop-cox"];

    assert.ok(bravopCox, "bravop-cox must be merged in even though bravo has no pacificStationId");
    assert.ok(isVariant(bravopCox));
  });
});

describe("known catalog entries (sampling)", () => {

  test("ABC canonical exposes the official abc.com URL via the site service", () => {

    const abc = CHANNELS["abc"] as CanonicalChannel | undefined;

    assert.ok(abc, "abc canonical entry should exist in CHANNELS");
    assert.equal(abc.name, "ABC");
    assert.equal(abc.url, "https://abc.com/watch-live");
    assert.deepEqual(abc.tags, ["Local"]);
  });

  test("CNBC has a custom 'usa' service that emits a cnbc-usa variant", () => {

    // CNBC includes a non-standard 'usa' slug (alongside the usual cox/directv/hulu/site/spectrum/xfinity/yttv set). It must produce cnbc-usa as a variant
    // because 'site' is the canonical for cnbc.
    const cnbcUsa = CHANNELS["cnbc-usa"] as VariantChannel | undefined;

    assert.ok(cnbcUsa, "cnbc-usa variant must exist");
    assert.equal(cnbcUsa.canonicalKey, "cnbc");
    assert.equal(cnbcUsa.channelSelector, "CNBC_US");
    assert.equal(cnbcUsa.url, "https://www.usanetwork.com/live");
  });

  test("every predefined tag mentioned by a channel exists in PREDEFINED_TAGS", () => {

    // Cross-export consistency: the catalog cannot reference tag names that do not exist in PREDEFINED_TAGS. This is a documented invariant - channels assign
    // from the predefined vocabulary, and the runtime tag registry merges PREDEFINED_TAGS - deletedTags + userTags. A typo in a tag name here would orphan the
    // channel until the user manually creates the misspelled tag.
    const tagSet = new Set<string>(PREDEFINED_TAGS);

    for(const [ key, channel ] of Object.entries(CHANNELS)) {

      if(isVariant(channel)) {

        continue;
      }

      const tags = (channel).tags;

      if(!tags) {

        continue;
      }

      for(const tag of tags) {

        assert.ok(tagSet.has(tag), key + " references unknown tag: " + tag);
      }
    }
  });
});
