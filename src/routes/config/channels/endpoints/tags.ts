/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tags.ts: Tag vocabulary management endpoints.
 *
 * The tag vocabulary is the merge of predefined tags (minus user-deleted) plus user-created tags. Mutations cascade through transformChannelTags to keep channel
 * assignments consistent with the vocabulary. Every successful mutation response includes the full tag UI bundle so the filter dropdown and tag manager modal stay
 * in sync.
 */
import type { Express, Request, Response } from "express";
import { getActiveTagVocabulary, getPredefinedChannel, getTagRegistry, isInVocabulary, setTagRegistry, tagsMatch,
  transformChannelTags } from "../../../../config/userChannels.ts";
import { sendConflictError, sendError, sendNotFoundError, sendSuccess, sendValidationError } from "../../http/envelope.ts";
import { LOG } from "../../../../utils/index.ts";
import { PREDEFINED_TAGS } from "../../../../channels/index.ts";
import { route } from "../http/handler.ts";

// Pattern for valid tag names: must start and end with alphanumeric, may contain letters, numbers, spaces, and hyphens in between. Enforced by validateTagName,
// the single source of truth for tag-name rules.
const TAG_NAME_PATTERN = /^[a-zA-Z0-9]([a-zA-Z0-9 -]*[a-zA-Z0-9])?$/;

// Maximum tag name length, enforced by validateTagName, the single source of truth for tag-name rules. Keep this in sync with the maxlength attribute on the
// tag-manager input in table.ts, which duplicates this value as a literal.
const TAG_NAME_MAX_LENGTH = 30;

/**
 * Validates a submitted tag name against the project's tag-name rules (required, length cap, character pattern). Returns an error message on failure or
 * undefined when the name passes. Used by every endpoint that accepts a user-supplied tag name, so the rules live in exactly one place.
 * @param tag - The tag name to validate. Caller is expected to have already trimmed whitespace.
 * @returns The user-facing validation error, or undefined when valid.
 */
function validateTagName(tag: string): string | undefined {

  if(tag.length === 0) {

    return "Tag name is required.";
  }

  if(tag.length > TAG_NAME_MAX_LENGTH) {

    return "Tag name must be " + String(TAG_NAME_MAX_LENGTH) + " characters or less.";
  }

  if(!TAG_NAME_PATTERN.test(tag)) {

    return "Tag name must start and end with a letter or number, and contain only letters, numbers, spaces, and hyphens.";
  }

  return undefined;
}

/**
 * Registers the tag management endpoints on the Express application.
 * @param app - The Express application.
 */
export function registerTagRoutes(app: Express): void {

  // GET /config/tags - Returns the tag vocabulary and registry state.
  app.get("/config/tags", route("read tags", (_req: Request, res: Response): void => {

    res.json({

      active: getActiveTagVocabulary(),
      predefined: [...PREDEFINED_TAGS],
      registry: getTagRegistry(),
      success: true
    });
  }));

  // POST /config/tags - Create a new user tag. Validates the tag name and rejects duplicates against the active vocabulary or deleted predefined tags (which
  // should be restored, not re-created).
  app.post("/config/tags", route("create tag", async (req: Request, res: Response) => {

    const body = req.body as { tag?: string };
    const tag = typeof body.tag === "string" ? body.tag.trim() : "";

    const nameError = validateTagName(tag);

    if(nameError) {

      sendValidationError(res, nameError);

      return;
    }

    if(isInVocabulary(tag)) {

      sendConflictError(res, "Tag '" + tag + "' already exists.");

      return;
    }

    const registry = getTagRegistry();

    if(registry.deletedTags.some((t) => tagsMatch(t, tag))) {

      sendConflictError(res, "Tag '" + tag + "' is a deleted predefined tag. Use restore instead of creating a new one.");

      return;
    }

    registry.tags.push(tag);
    await setTagRegistry(registry);

    LOG.info("Created tag: %s.", tag);

    sendSuccess(res, { tags: true });
  }));

  // DELETE /config/tags/:tag - Delete a tag from the vocabulary and cascade to all channel assignments. For predefined tags, the tag is added to deletedTags. For
  // user-created tags, the tag is removed from the user registry.
  app.delete("/config/tags/:tag", route("delete tag", async (req: Request, res: Response) => {

    const tag = (req.params as { tag?: string }).tag?.trim() ?? "";

    if(tag.length === 0) {

      sendValidationError(res, "Tag name is required.");

      return;
    }

    const registry = getTagRegistry();
    const isPredefined = PREDEFINED_TAGS.some((t) => tagsMatch(t, tag));
    const isUserTag = registry.tags.some((t) => tagsMatch(t, tag));

    if(!isPredefined && !isUserTag) {

      sendNotFoundError(res, "Tag '" + tag + "' not found.");

      return;
    }

    if(isPredefined) {

      const canonicalTag = PREDEFINED_TAGS.find((t) => tagsMatch(t, tag)) ?? tag;

      // Already deleted - no-op, return current state without unnecessary I/O.
      if(registry.deletedTags.some((t) => tagsMatch(t, tag))) {

        sendSuccess(res, { tags: true });

        return;
      }

      registry.deletedTags.push(canonicalTag);
    } else {

      registry.tags = registry.tags.filter((t) => !tagsMatch(t, tag));
    }

    await setTagRegistry(registry);

    // Cascade: strip the deleted tag from every channel that has it. transformChannelTags handles loading, delta normalization, and persistence.
    const { affectedKeys, error } = await transformChannelTags(
      (entry) => entry.channel.tags?.some((t) => tagsMatch(t, tag)) === true,
      (tags) => tags.filter((t) => !tagsMatch(t, tag))
    );

    if(error) {

      sendError(res, 400, { error });

      return;
    }

    LOG.info("Deleted tag '%s' from vocabulary and %d channel assignments.", tag, affectedKeys.length);

    // The hint is included whenever the cascade touched a channel: tags render in the playlist, while a vocabulary-only change shows nothing there.
    sendSuccess(res, { affectedKeys, message: "Tag '" + tag + "' deleted.", playlistHint: affectedKeys.length > 0, tags: true });
  }));

  // POST /config/tags/restore - Restore a previously deleted predefined tag. Removes it from deletedTags so it reappears in the active vocabulary, then cascade-
  // restores the tag on predefined channels whose definition includes it.
  app.post("/config/tags/restore", route("restore tag", async (req: Request, res: Response) => {

    const body = req.body as { tag?: string };
    const tag = typeof body.tag === "string" ? body.tag.trim() : "";

    if(tag.length === 0) {

      sendValidationError(res, "Tag name is required.");

      return;
    }

    const registry = getTagRegistry();

    if(!registry.deletedTags.some((t) => tagsMatch(t, tag))) {

      sendNotFoundError(res, "Tag '" + tag + "' is not a deleted predefined tag.");

      return;
    }

    registry.deletedTags = registry.deletedTags.filter((t) => !tagsMatch(t, tag));
    await setTagRegistry(registry);

    // Resolve the canonical predefined tag name for the restored tag so channel data uses the predefined casing.
    const canonicalTag = PREDEFINED_TAGS.find((t) => tagsMatch(t, tag)) ?? tag;

    // Cascade-restore: add the tag back to predefined channels whose definition includes it but whose current resolved tags don't (stripped during cascade
    // delete). The normalizer strips the tags delta when the result matches the predefined definition, reverting the channel to its default state.
    const { affectedKeys, error } = await transformChannelTags(
      (entry) => {

        const predefined = getPredefinedChannel(entry.key);

        return (predefined?.tags?.some((t) => tagsMatch(t, tag)) === true) &&
          (entry.channel.tags?.some((t) => tagsMatch(t, tag)) !== true);
      },
      (tags) => [ ...tags, canonicalTag ]
    );

    if(error) {

      sendError(res, 400, { error });

      return;
    }

    LOG.info("Restored predefined tag '%s' on %d channels.", tag, affectedKeys.length);

    // The hint is included whenever the cascade touched a channel: tags render in the playlist, while a vocabulary-only change shows nothing there.
    sendSuccess(res, { affectedKeys, message: "Tag '" + canonicalTag + "' restored.", playlistHint: affectedKeys.length > 0, tags: true });
  }));

  // POST /config/tags/rename - Rename a tag across the vocabulary and all channel assignments via two ordered writes: the registry first, then a single batched
  // channel update through transformChannelTags. Case-only renames are allowed (the identity is shared).
  app.post("/config/tags/rename", route("rename tag", async (req: Request, res: Response) => {

    const body = req.body as { newTag?: string; oldTag?: string };
    const oldTag = typeof body.oldTag === "string" ? body.oldTag.trim() : "";
    const newTag = typeof body.newTag === "string" ? body.newTag.trim() : "";

    if(!oldTag || !newTag) {

      sendValidationError(res, "Both old and new tag names are required.");

      return;
    }

    if(oldTag === newTag) {

      sendValidationError(res, "New tag name must differ from the old name.");

      return;
    }

    const newTagError = validateTagName(newTag);

    if(newTagError) {

      sendValidationError(res, newTagError);

      return;
    }

    if(!isInVocabulary(oldTag)) {

      sendNotFoundError(res, "Tag '" + oldTag + "' not found.");

      return;
    }

    // Collision check: allow case-only renames of the same identity, reject collisions with a different existing tag.
    if(isInVocabulary(newTag) && !tagsMatch(oldTag, newTag)) {

      sendConflictError(res, "Tag '" + newTag + "' already exists.");

      return;
    }

    const registry = getTagRegistry();
    const oldIsPredefined = PREDEFINED_TAGS.some((t) => tagsMatch(t, oldTag));

    if(oldIsPredefined) {

      // Predefined tag: "delete" the old (add to deletedTags using the canonical predefined form) and create the new as a user tag.
      const canonicalOld = PREDEFINED_TAGS.find((t) => tagsMatch(t, oldTag)) ?? oldTag;

      if(!registry.deletedTags.some((t) => tagsMatch(t, oldTag))) {

        registry.deletedTags.push(canonicalOld);
      }

      registry.tags.push(newTag);
    } else {

      registry.tags = registry.tags.map((t) => tagsMatch(t, oldTag) ? newTag : t);
    }

    await setTagRegistry(registry);

    const { affectedKeys, error } = await transformChannelTags(
      (entry) => entry.channel.tags?.some((t) => tagsMatch(t, oldTag)) === true,
      (tags) => tags.map((t) => tagsMatch(t, oldTag) ? newTag : t)
    );

    if(error) {

      sendError(res, 400, { error });

      return;
    }

    LOG.info("Renamed tag '%s' to '%s' across %d channels.", oldTag, newTag, affectedKeys.length);

    // The hint is included whenever the cascade touched a channel: tags render in the playlist, while a vocabulary-only change shows nothing there.
    sendSuccess(res, { affectedKeys, message: "Tag '" + oldTag + "' renamed to '" + newTag + "'.", playlistHint: affectedKeys.length > 0, tags: true });
  }));
}
