/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelForm.ts: Domain logic for channel form submissions (add, edit, inline edit, revert).
 *
 * The channel save/edit flow has to answer three questions that are logically independent of HTTP concerns:
 *
 * 1. Do the submitted form values match an existing channel definition? (Used to detect no-op saves and implicit reverts.)
 * 2. How does the submission differ from the predefined base? (Delta computation for storing user overrides.)
 * 3. Do the submitted values match any sibling service variant? (Used to implicit-revert a customized channel back to a variant definition.)
 *
 * These decisions are kept here - in a dedicated module that the route handlers delegate to - so the handlers read as thin adapters while the comparison logic
 * stays testable, typed, and discoverable. This file imports from both userChannels.ts and services.ts but is not imported by either, so adding new comparisons
 * here never creates a circular dependency.
 */
import type { ChannelDelta, ResolvedChannel } from "../types/index.ts";
import { PREDEFINED_SUFFIX, getServiceGroup, resolvePredefinedVariant } from "./services.ts";
import { getChannelEffectiveTags, getEffectiveHdhrEnabled, sortTags } from "./userChannels.ts";

/**
 * Case-insensitive order-independent comparison of two tag arrays. Delegates ordering to sortTags so the case-insensitive locale-aware sort policy lives in
 * exactly one place - this function is about equality, not ordering, and stays out of the "how do we sort tags" decision.
 * @param a - The first tag array.
 * @param b - The second tag array.
 * @returns True when the arrays contain the same tags under case-insensitive ordering.
 */
function tagsEqual(a: readonly string[], b: readonly string[]): boolean {

  if(a.length !== b.length) {

    return false;
  }

  return JSON.stringify(sortTags(a)) === JSON.stringify(sortTags(b));
}

/**
 * Normalized form values submitted for a channel save. String fields use "" for empty (matching HTML form inputs). channelNumber uses undefined for empty.
 * hdhrEnabled defaults to true when absent from the form (a checkbox is only submitted when checked, but our form includes a hidden input for the unchecked state).
 */
export interface ChannelFormValues {

  readonly channelNumber: number | undefined;
  readonly channelSelector: string;
  readonly guideTitle: string;
  readonly hdhrEnabled: boolean;
  readonly logoUrl: string;
  readonly name: string;
  readonly profile: string;
  readonly stationId: string;
  readonly url: string;
}

/* Scalar field list shared by channelMatches and computePredefinedDelta. Keeping these in one const ensures both functions iterate the same field set, so adding
 * a new scalar field to ChannelFormValues requires a single-line addition here and nothing else. The `satisfies` constraint validates every entry is a real key
 * on both Channel (for reads via channelScalar) and ChannelDelta (for writes in computePredefinedDelta), so renaming or removing a field surfaces here at compile
 * time rather than silently orphaning the iteration.
 */
const FORM_SCALAR_FIELDS = [

  "channelNumber", "channelSelector", "guideTitle", "hdhrEnabled", "logoUrl", "name", "profile", "stationId", "url"
] as const satisfies readonly (keyof ResolvedChannel & keyof ChannelDelta)[];

type FormScalarField = (typeof FORM_SCALAR_FIELDS)[number];

/**
 * Reads the comparable value for a scalar form field from a channel record. String fields default to "" when absent (matching the form's empty representation).
 * hdhrEnabled is derived as (value !== false), matching the form's default-true semantics. channelNumber stays number | undefined, matching the form's empty
 * representation. This normalization lets us compare form values against channel records with strict equality.
 * @param channel - The channel to read from.
 * @param field - The scalar field name.
 * @returns The comparable value as a boolean, number, string, or undefined.
 */
function channelScalar(channel: ResolvedChannel, field: FormScalarField): boolean | number | string | undefined {

  switch(field) {

    case "channelNumber": {

      return channel.channelNumber;
    }

    case "hdhrEnabled": {

      return getEffectiveHdhrEnabled(channel);
    }

    default: {

      // At this point the field is narrowed to a string-valued Channel key. Default to "" so the comparison matches the form's empty representation.
      return channel[field] ?? "";
    }
  }
}

/**
 * Checks whether submitted form values (scalars plus tags) match a channel's current definition. Uses channelScalar for field normalization and
 * getChannelEffectiveTags for tag comparison so only vocabulary-filtered tags participate - matching the edit form's pre-populated view. tagsEqual sorts both
 * sides defensively, so callers do not need to pre-sort.
 * @param channel - The channel to compare against.
 * @param formValues - The normalized form values.
 * @param tags - The tag array submitted with the form.
 * @returns True when every scalar field and the tag set match the channel's effective state.
 */
export function channelMatches(channel: ResolvedChannel, formValues: ChannelFormValues, tags: readonly string[]): boolean {

  for(const field of FORM_SCALAR_FIELDS) {

    if(formValues[field] !== channelScalar(channel, field)) {

      return false;
    }
  }

  // Tags comparison uses vocabulary-filtered effective tags so the comparison isn't thrown off by tags that exist on the channel but are hidden from the active
  // vocabulary (e.g., deleted predefined tags still stored on a predefined channel definition). Order-independent via tagsEqual.
  return tagsEqual(tags, getChannelEffectiveTags(channel));
}

/**
 * Result of computePredefinedDelta. `hasChanges` tells the caller whether the delta has any differences vs the predefined base (if false, the caller may treat
 * the submission as a no-op or implicit revert).
 */
export interface PredefinedDeltaResult {

  delta: ChannelDelta;
  hasChanges: boolean;
}

/**
 * Computes a ChannelDelta representing the difference between submitted form values and the predefined base. Fields that differ from the base are stored in the
 * delta; fields that are cleared (empty string or undefined) are stored as null so the normalizer preserves the explicit clear (absent field means inherit;
 * null means override to "no value"). Tags are compared separately against the base's effective tags and stored as null when cleared.
 * @param predefinedBase - The canonical predefined channel to diff against.
 * @param formValues - The normalized form values.
 * @param tags - The tag array submitted with the form. Internal comparison via tagsEqual sorts defensively; caller does not need to pre-sort.
 * @returns The computed delta and a flag indicating whether any field differs from the base.
 */
export function computePredefinedDelta(predefinedBase: ResolvedChannel, formValues: ChannelFormValues, tags: readonly string[]): PredefinedDeltaResult {

  const delta: ChannelDelta = {};
  let hasChanges = false;

  // Typed delta assignment helper. The delta interface has Nullable<T> on every field; this cast bridges the FORM_SCALAR_FIELDS string iteration to the strongly
  // typed delta shape.
  const deltaWrite = delta as Record<string, boolean | number | string | null | undefined>;

  for(const field of FORM_SCALAR_FIELDS) {

    if(formValues[field] === channelScalar(predefinedBase, field)) {

      continue;
    }

    const formVal = formValues[field];

    // Empty string or undefined means "clear this field" - store null so the normalizer preserves the explicit clear through the delta model.
    deltaWrite[field] = ((formVal === "") || (formVal === undefined)) ? null : formVal;
    hasChanges = true;
  }

  // Tags delta: compare against the base's effective (vocabulary-filtered) tags. Using effective tags prevents editing an unrelated field from baking a
  // vocabulary deletion into the stored delta. sortTags on the stored value mirrors PATCH and transformChannelTags, keeping the storage-order invariant that the
  // channels normalizer's JSON.stringify equality relies on.
  if(!tagsEqual(tags, getChannelEffectiveTags(predefinedBase))) {

    delta.tags = (tags.length > 0) ? sortTags(tags) : null;
    hasChanges = true;
  }

  return { delta, hasChanges };
}

/**
 * Searches a channel's service group for a variant whose pure predefined definition matches the submitted form values. When a match is found, the caller can
 * treat the submission as an implicit revert to that variant (removing the user override and switching the service selection) rather than storing a new custom
 * override. This handles the case where a user customizes a channel away from canonical, saves (creating an override), then edits again and reverts the change -
 * the form values no longer match canonical but do match a sibling variant.
 *
 * Variants are resolved against pure predefined data (not the user-overridden channelsRef) to avoid contamination from the current override. Synthetic
 * ":predefined" suffix entries (used for the override-vs-original UI) are skipped since they represent the same canonical data.
 * @param canonicalKey - The canonical channel key. Used to identify the service group and to skip the canonical entry itself.
 * @param formValues - The normalized form values to match against variants.
 * @param tags - The tag array submitted with the form. Forwarded to channelMatches, which sorts defensively via tagsEqual.
 * @returns The matching variant key, or undefined when no variant matches.
 */
export function findMatchingVariant(canonicalKey: string, formValues: ChannelFormValues, tags: readonly string[]): string | undefined {

  const serviceGroup = getServiceGroup(canonicalKey);

  if(!serviceGroup) {

    return undefined;
  }

  for(const variant of serviceGroup.variants) {

    // Skip the canonical entry (handled by the no-op-save check upstream) and synthetic :predefined entries (they represent canonical data, already covered).
    if((variant.key === canonicalKey) || variant.key.endsWith(PREDEFINED_SUFFIX)) {

      continue;
    }

    const resolvedVariant = resolvePredefinedVariant(variant.key);

    if(resolvedVariant && channelMatches(resolvedVariant, formValues, tags)) {

      return variant.key;
    }
  }

  return undefined;
}
