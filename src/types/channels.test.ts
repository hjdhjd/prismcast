/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels.test.ts: Unit tests for the runtime constants and partition completeness machinery in channels.ts. The module's runtime exports are four readonly
 * arrays (CHANNEL_IDENTITY_KEYS, CHANNEL_BINDING_KEYS, DELTA_ELIGIBLE_IDENTITY_KEYS, DELTA_ELIGIBLE_BINDING_KEYS) that act as the single source of truth for
 * the identity/binding partition and the delta-eligible subset. The tests lock the membership, ordering, and disjointness contracts so a silent change to any
 * array surfaces as a failed assertion. Type-level tests pin the CanonicalChannel/VariantChannel tag: a value with a string canonicalKey is a
 * VariantChannel, anything else is a CanonicalChannel.
 */
import { CHANNEL_BINDING_KEYS, CHANNEL_IDENTITY_KEYS, DELTA_ELIGIBLE_BINDING_KEYS, DELTA_ELIGIBLE_IDENTITY_KEYS } from "./channels.ts";
import type { CanonicalChannel, Channel, ChannelDelta, ChannelIdentity, ChannelServiceBinding, CustomizableField, VariantChannel } from "./channels.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("CHANNEL_IDENTITY_KEYS", () => {

  test("contains the expected identity field names", () => {

    // The identity partition must include every property declared on ChannelIdentity. The compile-time _partitionCompleteness check enforces structural
    // coverage; this test pins the literal membership so an accidental rename or omission surfaces as a value-level failure too.
    assert.deepEqual(
      [...CHANNEL_IDENTITY_KEYS],
      [ "channelNumber", "guideTitle", "hdhrEnabled", "logoUrl", "name", "pacificStationId", "stationId", "tags", "tvgShift" ]
    );
  });

  test("has no duplicate entries", () => {

    // Duplicate keys would still compile but would produce confusing iteration order and weaken the partition contract. We assert distinctness explicitly.
    const set = new Set(CHANNEL_IDENTITY_KEYS);

    assert.equal(set.size, CHANNEL_IDENTITY_KEYS.length, "every identity key should be unique");
  });

  test("is sorted alphabetically", () => {

    // Alphabetical order is the convention across this file's arrays. A non-sorted array hints that a contributor added an entry in the wrong place.
    const sorted = CHANNEL_IDENTITY_KEYS.toSorted();

    assert.deepEqual([...CHANNEL_IDENTITY_KEYS], sorted, "identity keys should be in alphabetical order");
  });

  test("declares the expected number of identity fields", () => {

    // The `as const` assertion gives us a readonly tuple type whose length is fixed. We assert the expected count so that adding or removing a field surfaces
    // here as well as in the deepEqual membership test above - two failing assertions point a reviewer at the same root cause.
    assert.equal(CHANNEL_IDENTITY_KEYS.length, 9);
  });
});

describe("CHANNEL_BINDING_KEYS", () => {

  test("contains the expected service-binding field names", () => {

    // Binding partition mirrors ChannelServiceBinding. The compile-time check guarantees structural coverage; here we lock literal membership.
    assert.deepEqual(
      [...CHANNEL_BINDING_KEYS],
      [ "channelSelector", "dismissSelector", "profile", "scrollSelector", "scrollTarget", "scrollToBottom", "service", "url" ]
    );
  });

  test("has no duplicate entries", () => {

    const set = new Set(CHANNEL_BINDING_KEYS);

    assert.equal(set.size, CHANNEL_BINDING_KEYS.length, "every binding key should be unique");
  });

  test("is sorted alphabetically", () => {

    const sorted = CHANNEL_BINDING_KEYS.toSorted();

    assert.deepEqual([...CHANNEL_BINDING_KEYS], sorted, "binding keys should be in alphabetical order");
  });
});

describe("identity/binding partition disjointness", () => {

  test("CHANNEL_IDENTITY_KEYS and CHANNEL_BINDING_KEYS share no entries", () => {

    // The whole point of the partition is that a key belongs to exactly one of the two sets. An overlap would mean a field is classified as both, which would
    // break the canonical->variant inheritance logic.
    const identitySet = new Set<string>(CHANNEL_IDENTITY_KEYS);
    const overlap = CHANNEL_BINDING_KEYS.filter((k) => identitySet.has(k));

    assert.deepEqual(overlap, [], "identity and binding partitions must not overlap");
  });

  test("neither partition contains the canonicalKey discriminator", () => {

    // canonicalKey is the explicit "neither" carve-out per the compile-time exhaustiveness check. Including it in either array would corrupt the partition.
    const all: string[] = [ ...CHANNEL_IDENTITY_KEYS, ...CHANNEL_BINDING_KEYS ];

    assert.ok(!all.includes("canonicalKey"), "canonicalKey is the explicit neither-set carve-out");
  });
});

describe("DELTA_ELIGIBLE_IDENTITY_KEYS", () => {

  test("contains the expected user-overridable identity field names", () => {

    // Delta-eligible identity is a subset of CHANNEL_IDENTITY_KEYS that excludes catalog-driven structural fields (currently pacificStationId).
    assert.deepEqual(
      [...DELTA_ELIGIBLE_IDENTITY_KEYS],
      [ "channelNumber", "guideTitle", "hdhrEnabled", "logoUrl", "name", "stationId", "tags", "tvgShift" ]
    );
  });

  test("excludes pacificStationId (catalog-driven, not user-overridable)", () => {

    // The catalog-driven pacificStationId triggers Pacific auto-generation in the flattener. Allowing users to override it would create inconsistent state.
    const eligible: string[] = [...DELTA_ELIGIBLE_IDENTITY_KEYS];

    assert.ok(!eligible.includes("pacificStationId"), "pacificStationId must never appear in the user-facing delta surface");
  });

  test("every entry is also a member of CHANNEL_IDENTITY_KEYS", () => {

    // Subset relation is what the `satisfies` constraint enforces at compile time. We cross-check at runtime as a defense against accidental drift.
    const identitySet = new Set<string>(CHANNEL_IDENTITY_KEYS);

    for(const key of DELTA_ELIGIBLE_IDENTITY_KEYS) {

      assert.ok(identitySet.has(key), "delta-eligible identity key " + key + " must be in CHANNEL_IDENTITY_KEYS");
    }
  });

  test("has no duplicate entries", () => {

    const set = new Set(DELTA_ELIGIBLE_IDENTITY_KEYS);

    assert.equal(set.size, DELTA_ELIGIBLE_IDENTITY_KEYS.length, "delta-eligible identity keys should be unique");
  });
});

describe("DELTA_ELIGIBLE_BINDING_KEYS", () => {

  test("contains the expected user-overridable binding field names", () => {

    // Delta-eligible binding excludes internal DOM-hook fields (dismissSelector, scrollSelector, scrollTarget, scrollToBottom, service) that are owned by site
    // profiles or ServiceVariant catalog entries. Only channelSelector, profile, and url are exposed to user input.
    assert.deepEqual(
      [...DELTA_ELIGIBLE_BINDING_KEYS],
      [ "channelSelector", "profile", "url" ]
    );
  });

  test("excludes internal DOM-hook binding fields", () => {

    // Internal fields (dismissSelector, scrollSelector, scrollTarget, scrollToBottom, service) must remain non-user-overridable. Listing them as eligible would
    // expose them to the form, JSON import, and channels.json hand edits, breaking the layering between catalog/profile data and user input.
    const internalFields = [ "dismissSelector", "scrollSelector", "scrollTarget", "scrollToBottom", "service" ];
    const eligible: string[] = [...DELTA_ELIGIBLE_BINDING_KEYS];

    for(const field of internalFields) {

      assert.ok(!eligible.includes(field), field + " must not appear in the user-facing delta surface");
    }
  });

  test("every entry is also a member of CHANNEL_BINDING_KEYS", () => {

    const bindingSet = new Set<string>(CHANNEL_BINDING_KEYS);

    for(const key of DELTA_ELIGIBLE_BINDING_KEYS) {

      assert.ok(bindingSet.has(key), "delta-eligible binding key " + key + " must be in CHANNEL_BINDING_KEYS");
    }
  });

  test("has no duplicate entries", () => {

    const set = new Set(DELTA_ELIGIBLE_BINDING_KEYS);

    assert.equal(set.size, DELTA_ELIGIBLE_BINDING_KEYS.length, "delta-eligible binding keys should be unique");
  });
});

describe("delta-eligible partition disjointness", () => {

  test("DELTA_ELIGIBLE_IDENTITY_KEYS and DELTA_ELIGIBLE_BINDING_KEYS share no entries", () => {

    // CustomizableField is the union of these two arrays. If they shared a key, the union semantics would be unaffected (set union absorbs duplicates) but the
    // intent of the two arrays would be muddled. We assert disjointness to keep the partition crisp.
    const identitySet = new Set<string>(DELTA_ELIGIBLE_IDENTITY_KEYS);
    const overlap = DELTA_ELIGIBLE_BINDING_KEYS.filter((k) => identitySet.has(k));

    assert.deepEqual(overlap, [], "delta-eligible identity and binding partitions must not overlap");
  });
});

/* makeChannel returns a Channel value whose static type is the open union (CanonicalChannel | VariantChannel). The function signature widens what would
 * otherwise be a literal-narrowed type so the branches on the tag in the tests below exercise real narrowing instead of being constant-folded by the
 * compiler. The runtime value is whatever the caller passed.
 */
function makeChannel(value: Channel): Channel {

  return value;
}

describe("Channel discriminated union (type-level)", () => {

  test("a value with a string canonicalKey narrows to VariantChannel", () => {

    // The field that marks the kind: VariantChannel.canonicalKey is `string`; CanonicalChannel.canonicalKey is `never` (structurally absent). When a Channel value has a
    // non-undefined canonicalKey, TypeScript narrows it to VariantChannel - assignment to a binding-only type works.
    const variant: Channel = makeChannel({ canonicalKey: "parent-key", url: "https://example.com/live" });

    if(variant.canonicalKey !== undefined) {

      // Inside this branch, variant has been narrowed to VariantChannel. We can assign it to ChannelServiceBinding (variants extend exactly that interface).
      const binding: ChannelServiceBinding = variant;

      assert.equal(binding.url, "https://example.com/live", "narrowed variant retains its binding fields");
      assert.equal(variant.canonicalKey, "parent-key", "narrowed variant exposes canonicalKey as a string");
    } else {

      assert.fail("the test value declares canonicalKey, so this branch should not be reached");
    }
  });

  test("a value without canonicalKey narrows to CanonicalChannel", () => {

    // The other branch: when canonicalKey is absent (undefined), the union narrows to CanonicalChannel. Canonicals carry both identity and binding.
    const canonical: Channel = makeChannel({ name: "Example Channel", url: "https://example.com/live" });

    if(canonical.canonicalKey === undefined) {

      // Inside this branch, canonical has been narrowed to CanonicalChannel. Identity fields are reachable.
      const identity: ChannelIdentity = canonical;

      assert.equal(identity.name, "Example Channel", "narrowed canonical exposes identity fields");
    } else {

      assert.fail("the test value omits canonicalKey, so this branch should not be reached");
    }
  });

  test("CanonicalChannel structurally rejects a string canonicalKey", () => {

    // A literal CanonicalChannel cannot be assigned a string canonicalKey because the field is typed as `never` on that interface. The @ts-expect-error
    // directive proves this at the type level: removing the directive should produce a real error.
    // @ts-expect-error - canonicalKey is `never` on CanonicalChannel; assigning a string is structurally rejected.
    const bad: CanonicalChannel = { canonicalKey: "x", url: "https://example.com/live" };

    // The runtime value still exists - TypeScript's assertion is a compile-time check, not a runtime one. We touch the variable so it is not flagged unused.
    assert.equal(bad.url, "https://example.com/live", "the runtime value still exists");
  });

  test("VariantChannel requires a canonicalKey (type system rejects omission)", () => {

    // Without canonicalKey, the literal does not satisfy VariantChannel. The @ts-expect-error pins this contract.
    // @ts-expect-error - VariantChannel.canonicalKey is required; omitting it is a type error.
    const bad: VariantChannel = { url: "https://example.com/live" };

    assert.equal(bad.url, "https://example.com/live", "the runtime value still exists");
  });
});

describe("ChannelDelta / CustomizableField shape (type-level)", () => {

  test("CustomizableField permits the literal members of the delta-eligible partitions", () => {

    // Every literal in DELTA_ELIGIBLE_IDENTITY_KEYS and DELTA_ELIGIBLE_BINDING_KEYS must be assignable to CustomizableField. We exercise a representative
    // sample of each partition.
    const identityField: CustomizableField = "name";
    const bindingField: CustomizableField = "url";

    assert.equal(identityField, "name");
    assert.equal(bindingField, "url");
  });

  test("CustomizableField rejects fields outside the delta-eligible partition", () => {

    // pacificStationId is an identity field but is intentionally excluded from the delta surface; it must not be assignable to CustomizableField.
    // @ts-expect-error - pacificStationId is not delta-eligible.
    const bad: CustomizableField = "pacificStationId";

    assert.equal(bad, "pacificStationId", "the runtime string still exists");
  });

  test("ChannelDelta accepts any subset of the delta-eligible fields", () => {

    // The delta shape is fully optional; every CustomizableField may be present, all may be absent, or any subset may be set. We exercise the empty case and
    // a representative populated case.
    const empty: ChannelDelta = {};
    const populated: ChannelDelta = { channelNumber: 7, name: "Override", url: "https://override.example.com/live" };

    assert.equal(Object.keys(empty).length, 0, "empty delta has no keys");
    assert.equal(populated.channelNumber, 7);
    assert.equal(populated.name, "Override");
  });

  test("ChannelDelta accepts null (cleared) and a value (override) for nullable fields", () => {

    // Nullable<T> fields distinguish "user cleared this field" (null) from "inherit from predefined" (absent). Both shapes must type-check.
    const cleared: ChannelDelta = { channelNumber: null, name: null };
    const set: ChannelDelta = { channelNumber: 42, name: "Set" };

    assert.equal(cleared.channelNumber, null, "null clears the field");
    assert.equal(set.channelNumber, 42, "value overrides the field");
  });
});
