/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * vocabulary.ts: The words the settings page and its client script share.
 */
import type { Nullable, ReactivityClass } from "../../types/index.ts";
import type { BadgeVariant } from "../components.ts";

/* This module holds the words the settings page and its script share, so every sentence that names a badge or the Save button, or promises when a save takes
 * effect, is composed from one statement of them. Every import here is a type import and the module imports no value, so it is a leaf no module-initialization
 * cycle can reach, and every consumer imports it directly rather than through a barrel.
 */

// The badge a reactivity class carries on a settings field, its style the badge variant the table below pairs with the class.
interface ClassBadge<Variant extends BadgeVariant> {

  // The badge text.
  readonly label: string;

  // The tooltip: one sentence stating when a save of the setting takes effect.
  readonly title: string;

  // The badge style.
  readonly variant: Variant;
}

// The streams a next-stream setting's saved value reaches. Every sentence that says when a next-stream save takes effect composes from it, so those
// promises cannot drift apart.
export const NEXT_STREAM_SCOPE = "streams that start after the save";

// When a restart-class setting's saved value takes effect. Every sentence that says so composes from it, so those promises cannot drift apart.
export const RESTART_TIMING = "after PrismCast restarts";

// Each reactivity class's badge on a settings field. Keyed by the class union, so a class added later cannot compile until it decides its badge, and each
// entry's style must be the variant named for its own class, so no class badge can take another's look; live carries none, because a save already
// promises it and no badge style bears its name. Each title composes from its class's phrase above, and every message naming a badge reads its label, so
// the badges and those messages cannot drift apart.
export const REACTIVITY_BADGES = {

  live: null,
  "next-stream": { label: "Next stream", title: "Takes effect for " + NEXT_STREAM_SCOPE + ".", variant: "next-stream" },
  restart: { label: "Restart", title: "Takes effect " + RESTART_TIMING + ".", variant: "restart" }
} as const satisfies { readonly [Class in ReactivityClass]: Nullable<ClassBadge<Extract<BadgeVariant, Class>>> };

// The sentence that tells the user when the settings marked Restart take effect, composed from the restart badge's label and its timing phrase. Every message
// that says so reads this one sentence.
export const RESTART_SETTINGS_SENTENCE = "Settings marked " + REACTIVITY_BADGES.restart.label + " take effect " + RESTART_TIMING + ".";

// The attribute a pending-marker slot carries, naming its setting's path. Both sides of the slot, the server that renders it and the script that fills it, read
// the attribute's name from this one constant.
export const PENDING_PATH_ATTRIBUTE = "data-pending-path";

// The Save button's label, the same in every mode because each field's badge says when a save of it takes effect. The button and every client message that
// names it read this one constant, so a message cannot name a button the page does not show.
export const SAVE_SETTINGS_LABEL = "Save Settings";

/**
 * Reads a count of settings for a person, "1 setting" or "2 settings". Every count of settings the page or a save message states reads this one phrase.
 * @param count - The number of settings.
 * @returns The count with its noun agreeing in number.
 */
export function formatSettingCount(count: number): string {

  return String(count) + " setting" + ((count === 1) ? "" : "s");
}
