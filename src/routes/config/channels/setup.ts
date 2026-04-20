/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.ts: Aggregator that installs every channel-config endpoint group on the Express application.
 *
 * Keeping the registration list in one place means adding a new endpoint group requires exactly one line change here plus a new file under endpoints/. Every
 * registered route has a distinct verb+path combination, so the order of registration is not significant - we keep the alphabetical order stable for easier diff
 * review.
 */
import type { Express } from "express";
import { registerBrowseRoutes } from "./endpoints/browse.js";
import { registerBulkRoutes } from "./endpoints/bulk.js";
import { registerCrudRoutes } from "./endpoints/crud.js";
import { registerImportExportRoutes } from "./endpoints/importExport.js";
import { registerPredefinedRoutes } from "./endpoints/predefined.js";
import { registerPrefsRoutes } from "./endpoints/prefs.js";
import { registerServiceRoutes } from "./endpoints/service.js";
import { registerTagRoutes } from "./endpoints/tags.js";

/**
 * Installs all channel-configuration route handlers on the Express application.
 * @param app - The Express application.
 */
export function setupChannelRoutes(app: Express): void {

  registerBrowseRoutes(app);
  registerBulkRoutes(app);
  registerCrudRoutes(app);
  registerImportExportRoutes(app);
  registerPredefinedRoutes(app);
  registerPrefsRoutes(app);
  registerServiceRoutes(app);
  registerTagRoutes(app);
}
