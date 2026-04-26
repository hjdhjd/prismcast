/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * prefs.ts: Channel table display preferences and setup state endpoints.
 *
 * These endpoints are thin adapters over the display-prefs and setup-state helpers in config/userChannels. Validation happens here (HTTP boundary); the actual
 * state mutation and persistence lives in the config layer.
 */
import type { Express, Request, Response } from "express";
import { VALID_OPTIONAL_COLUMNS, buildChannelTableState } from "../table.js";
import { markSetupCompleted, saveChannelDisplayPrefs, setChannelDisplayPrefs } from "../../../../config/userChannels.js";
import { sendSuccess, sendValidationError } from "../http/envelope.js";
import type { ChannelSortField } from "../../../../types/index.js";
import { VALID_SORT_FIELDS } from "../../../../config/services.js";
import { route } from "../http/handler.js";

/**
 * Registers the display-preferences and setup-completed endpoints on the Express application.
 * @param app - The Express application.
 */
export function registerPrefsRoutes(app: Express): void {

  // POST /config/channels/display-prefs - Update channel table display preferences (visible columns, sort field, sort direction). Every field is optional;
  // only supplied fields are updated. The setChannelDisplayPrefs/saveChannelDisplayPrefs helpers own the runtime CONFIG update and the file persistence.
  app.post("/config/channels/display-prefs", route("update display preferences", async (req: Request, res: Response) => {

    const body = req.body as { sortDirection?: string; sortField?: string; visibleColumns?: string[] };

    if(body.visibleColumns !== undefined) {

      if(!Array.isArray(body.visibleColumns)) {

        sendValidationError(res, "visibleColumns must be an array.");

        return;
      }

      for(const col of body.visibleColumns) {

        if(!VALID_OPTIONAL_COLUMNS.has(col)) {

          sendValidationError(res, "Unknown column: " + col);

          return;
        }
      }
    }

    if((body.sortField !== undefined) && !VALID_SORT_FIELDS.has(body.sortField as ChannelSortField)) {

      sendValidationError(res, "Unknown sort field: " + body.sortField);

      return;
    }

    if((body.sortDirection !== undefined) && (body.sortDirection !== "asc") && (body.sortDirection !== "desc")) {

      sendValidationError(res, "sortDirection must be \"asc\" or \"desc\".");

      return;
    }

    setChannelDisplayPrefs({

      channelSortDirection: body.sortDirection,
      channelSortField: body.sortField as ChannelSortField | undefined,
      visibleColumns: body.visibleColumns
    });

    await saveChannelDisplayPrefs();

    sendSuccess(res);
  }));

  // POST /config/channels/setup-completed - Mark the Service Setup flow as completed. Called when the wizard finishes or the user explicitly skips. Returns a
  // counts-only patch so the client can refresh summary counters after the wizard's browse step may have added channels and changed the service filter.
  app.post("/config/channels/setup-completed", route("save setup state", async (_req: Request, res: Response) => {

    await markSetupCompleted();

    const { counts, scopeCounts } = buildChannelTableState();

    sendSuccess(res, { patch: { counts, rows: [], scopeCounts } });
  }));
}
