/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelForm.helpers.ts: Test helper for constructing ChannelFormValues fixtures. Co-located with channelForm.ts, the production module that owns the form
 * value shape. Consumed by the unit suite beside it and by the integration-tier form-domain suite, so both tiers reason about one set of defaults instead of
 * maintaining per-file variants whose defaults can drift out of agreement. Excluded from the build emit by the *.helpers.ts pattern in tsconfig.build.json.
 *
 * The defaults model an empty form submission: string fields post as "", channelNumber posts as undefined, and the two booleans carry opposite conventions
 * that are stated explicitly rather than left to a reader's inference. hdhrEnabled defaults true because its checkbox has a hidden-input partner supplying
 * "false" when the box is clear; forceCapture defaults false because it has no such partner, so an absent field simply means unchecked.
 *
 * No declareKeysOf/assertSameShape parity harness guards this factory, unlike the StreamRegistryEntry one in streaming/registry.helpers.ts. It does not need
 * one: every member of ChannelFormValues is required, so a field added to the interface fails to compile at exactly this literal and nowhere else. The type is
 * its own drift catch, and a second mechanism would restate what the compiler already guarantees.
 */
import type { ChannelFormValues } from "./channelForm.ts";

/**
 * Builds a ChannelFormValues literal carrying the empty-submission defaults. Callers override only the fields their scenario cares about.
 * @param overrides - Field overrides merged onto the defaults.
 * @returns A fully-shaped ChannelFormValues.
 */
export function makeForm(overrides: Partial<ChannelFormValues> = {}): ChannelFormValues {

  return {

    channelNumber: undefined,
    channelSelector: "",
    forceCapture: false,
    guideTitle: "",
    hdhrEnabled: true,
    logoUrl: "",
    name: "",
    profile: "",
    stationId: "",
    url: "",
    ...overrides
  };
}
