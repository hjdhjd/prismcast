/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * predefined.ts: Predefined channel enable/disable endpoints.
 *
 * Toggles predefined channels without deleting their definitions. Disabled channels remain in the lineup but are hidden from streaming and the playlist. State
 * lives in user config's disabledPredefined list - a deliberate choice since the state belongs to "which predefineds are active" rather than to the channel
 * record itself (a predefined channel has no mutable record to toggle).
 */
import type { Express, Request, Response } from "express";
import { disablePredefinedChannels, enablePredefinedChannels, getEastWithPacificPredefinedKeys, getPacificPredefinedKeys, getPredefinedChannels,
  isPredefinedChannel } from "../../../../config/userChannels.js";
import { sendSuccess, sendValidationError } from "../http/envelope.js";
import { LOG } from "../../../../utils/index.js";
import { route } from "../http/handler.js";

type BulkScope = "all" | "east" | "pacific";

/**
 * Registers the predefined-toggle endpoints on the Express application.
 * @param app - The Express application.
 */
export function registerPredefinedRoutes(app: Express): void {

  // POST /config/channels/toggle-predefined - Toggle a single predefined channel's enabled/disabled state.
  app.post("/config/channels/toggle-predefined", route("toggle channel", async (req: Request, res: Response) => {

    const body = req.body as { enabled?: boolean; key?: string };
    const key = body.key?.trim();
    const enabled = body.enabled;

    if(!key) {

      sendValidationError(res, "Channel key is required.");

      return;
    }

    if(typeof enabled !== "boolean") {

      sendValidationError(res, "Enabled state (true/false) is required.");

      return;
    }

    if(!isPredefinedChannel(key)) {

      sendValidationError(res, "Channel '" + key + "' is not a predefined channel.");

      return;
    }

    if(enabled) {

      await enablePredefinedChannels([key]);
    } else {

      await disablePredefinedChannels([key]);
    }

    LOG.info("Predefined channel '%s' %s.", key, enabled ? "enabled" : "disabled");

    sendSuccess(res, { affectedKeys: [key] });
  }));

  // POST /config/channels/bulk-toggle-predefined - Toggle predefined channels by scope (all, pacific, east).
  app.post("/config/channels/bulk-toggle-predefined", route("toggle channels", async (req: Request, res: Response) => {

    const body = req.body as { enabled?: boolean; scope?: string };
    const enabled = body.enabled;
    const scope = body.scope as BulkScope | undefined;

    if(typeof enabled !== "boolean") {

      sendValidationError(res, "Enabled state (true/false) is required.");

      return;
    }

    if((scope !== "all") && (scope !== "pacific") && (scope !== "east")) {

      sendValidationError(res, "Scope must be 'all', 'pacific', or 'east'.");

      return;
    }

    // Compute the target key set based on scope.
    let targetKeys: string[];

    switch(scope) {

      case "pacific": {

        targetKeys = getPacificPredefinedKeys();

        break;
      }

      case "east": {

        targetKeys = getEastWithPacificPredefinedKeys();

        break;
      }

      default: {

        targetKeys = Object.keys(getPredefinedChannels());

        break;
      }
    }

    // Route through the shared enable/disable helpers so every path that mutates the disabled-predefined list goes through the same implementation. No separate
    // inline branches - the helpers handle the subtractive/additive set manipulation and the CONFIG sync.
    if(enabled) {

      await enablePredefinedChannels(targetKeys);
    } else {

      await disablePredefinedChannels(targetKeys);
    }

    const scopeLabel = (scope === "all") ? "All" : (scope === "pacific") ? "Pacific" : "East";

    LOG.info("%s predefined channels %s (%d affected).", scopeLabel, enabled ? "enabled" : "disabled", targetKeys.length);

    sendSuccess(res, { affectedKeys: targetKeys });
  }));
}
