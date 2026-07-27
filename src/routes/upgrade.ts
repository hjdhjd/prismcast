/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * upgrade.ts: Upgrade endpoints for PrismCast web UI.
 */
import type { Express, Request, Response } from "express";
import { LOG, fetchLatestVersion, getPackageVersion, isRunningAsService, isVersionLessThan, normalizeVersion } from "../utils/index.ts";
import { sendErrorResponse, sendSuccess, sendValidationError } from "./config/http/envelope.ts";
import { closeBrowser } from "../browser/index.ts";
import { detectInstallMethod } from "../upgrade/detection.ts";
import { exec as execCallback } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(execCallback);

/* These endpoints provide upgrade information and execution for the web UI. GET /upgrade/info returns the current install method and version status; POST /upgrade
 * executes the upgrade command and optionally triggers a service restart.
 */

/**
 * Configures upgrade-related HTTP endpoints.
 * @param app - The Express application.
 */
export function setupUpgradeEndpoint(app: Express): void {

  // GET /upgrade/info - Returns installation method, version information, and whether the installation is upgradeable.
  app.get("/upgrade/info", async (_req: Request, res: Response): Promise<void> => {

    try {

      const info = detectInstallMethod();
      const currentVersion = normalizeVersion(getPackageVersion());
      const latestVersion = await fetchLatestVersion();
      const updateAvailable = (latestVersion !== null) && isVersionLessThan(currentVersion, latestVersion);

      res.json({

        currentVersion,
        latestVersion,
        method: info.method,
        updateAvailable,
        upgradeCommand: info.upgradeCommand,
        upgradeable: info.upgradeable
      });
    } catch(error) {

      sendErrorResponse(res, error, "get upgrade info");
    }
  });

  // POST /upgrade - Executes the upgrade command for the detected installation method. Uses async exec so the event loop stays free during the upgrade command,
  // allowing Express to continue serving SSE updates and health checks.
  app.post("/upgrade", async (_req: Request, res: Response): Promise<void> => {

    try {

      const info = detectInstallMethod();

      if(!info.upgradeable) {

        sendValidationError(res, "This installation method does not support in-place upgrades.");

        return;
      }

      // We build the exec options for the upgrade command with a two-minute timeout, which bounds a slow package install so a stuck download is killed and surfaced as a
      // failed upgrade rather than hanging the request indefinitely. For a local npm install we also point the working directory at the package directory below.
      const options: { cwd?: string; timeout: number } = { timeout: 120000 };

      if((info.method === "npm-local") && info.packageDir) {

        options.cwd = info.packageDir;
      }

      LOG.info("Executing upgrade via web UI: %s.", info.upgradeCommand);

      await exec(info.upgradeCommand, options);

      const willRestart = isRunningAsService();

      sendSuccess(res, { data: { willRestart }, message: "Upgrade complete." });

      // If running as a service, delay briefly before exiting so the HTTP response from sendSuccess above has time to flush to the client. An immediate exit
      // could truncate the response and leave the caller without confirmation that the upgrade succeeded; the service manager then restarts us with the new version.
      if(willRestart) {

        setTimeout(() => {

          LOG.info("Exiting for service manager restart after upgrade.");

          // We exit either way, regardless of whether closeBrowser() succeeds, so the service manager restarts us on the new version in both cases. The exit
          // code distinguishes a clean shutdown (0) from an unclean one (1) for anyone reading service logs.
          void closeBrowser().then(() => { process.exit(0); }).catch(() => { process.exit(1); });
        }, 500);
      }
    } catch(error) {

      sendErrorResponse(res, error, "upgrade");
    }
  });
}
