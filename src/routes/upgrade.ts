/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * upgrade.ts: Upgrade endpoints for PrismCast web UI.
 */
import type { Express, Request, Response } from "express";
import { LOG, fetchLatestVersion, formatError, getPackageVersion, isRunningAsService, isVersionLessThan, normalizeVersion } from "../utils/index.js";
import { closeBrowser } from "../browser/index.js";
import { detectInstallMethod } from "../upgrade/detection.js";
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

      LOG.error("Failed to get upgrade info: %s.", formatError(error));
      res.status(500).json({ message: "Failed to get upgrade info: " + formatError(error), success: false });
    }
  });

  // POST /upgrade - Executes the upgrade command for the detected installation method. Uses async exec so the event loop stays free during the upgrade command,
  // allowing Express to continue serving SSE updates and health checks.
  app.post("/upgrade", async (_req: Request, res: Response): Promise<void> => {

    try {

      const info = detectInstallMethod();

      if(!info.upgradeable) {

        res.json({ message: "This installation method does not support in-place upgrades.", success: false, willRestart: false });

        return;
      }

      // Execute the upgrade command asynchronously to avoid blocking the event loop.
      const options: { cwd?: string; timeout: number } = { timeout: 120000 };

      if((info.method === "npm-local") && info.packageDir) {

        options.cwd = info.packageDir;
      }

      LOG.info("Executing upgrade via web UI: %s.", info.upgradeCommand);

      await exec(info.upgradeCommand, options);

      const willRestart = isRunningAsService();

      res.json({ message: "Upgrade complete.", success: true, willRestart });

      // If running as a service, exit after a short delay so the service manager restarts us with the new version.
      if(willRestart) {

        setTimeout(() => {

          LOG.info("Exiting for service manager restart after upgrade.");

          void closeBrowser().then(() => { process.exit(0); }).catch(() => { process.exit(1); });
        }, 500);
      }
    } catch(error) {

      LOG.error("Upgrade failed: %s.", formatError(error));
      res.status(500).json({ message: "Upgrade failed: " + formatError(error), success: false, willRestart: false });
    }
  });
}
