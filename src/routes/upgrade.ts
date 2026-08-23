/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * upgrade.ts: Upgrade endpoints for PrismCast web UI.
 */
import type { Express, Request, Response } from "express";
import { LOG, fetchLatestVersion, getPackageVersion, isRunningAsService, isVersionLessThan, normalizeVersion } from "../utils/index.ts";
import { sendErrorResponse, sendSuccess, sendValidationError } from "./config/http/envelope.ts";
import type { InstallInfo } from "../upgrade/detection.ts";
import type { UpgradeStep } from "../upgrade/lifecycle.ts";
import { closeBrowser } from "../browser/index.ts";
import { createDefaultLifecycleContext } from "../upgrade/lifecycle.context.ts";
import { detectInstallMethod } from "../upgrade/detection.ts";
import { performUpgrade } from "../upgrade/lifecycle.ts";

/* These endpoints provide upgrade information and execution for the web UI. GET /upgrade/info returns the current install method and version status; POST /upgrade
 * dispatches the upgrade to the platform-aware upgrade lifecycle and answers with what happened.
 *
 * The route declares WHAT was asked for - an upgrade, over HTTP - and the lifecycle port (upgrade/lifecycle.ts) owns HOW that happens on each OS. POSIX runs the
 * package manager in this process and waits; Windows spawns a detached helper and hands the job to it, because npm cannot rename an install directory that a
 * running node.exe still holds open. Reusing the port is what keeps that platform knowledge in one place rather than duplicated between the CLI and here.
 */

// How long the response is given to reach the client before the process exits. An immediate exit could truncate the response and leave the caller with no
// confirmation of what happened.
const RESPONSE_FLUSH_DELAY_MS = 500;

// The deadline on an upgrade command the web UI runs in this process. It bounds a slow package install so a stalled download is killed and surfaced as a failed
// upgrade rather than holding the request open with no end in sight. The CLI runs unbounded instead, because a user is watching the terminal and can interrupt.
const UPGRADE_COMMAND_TIMEOUT_MS = 120000;

/**
 * UpgradeDeps is the boundary the upgrade handlers act through: install detection, the platform-aware upgrade dispatch, the service-mode probe, and the
 * post-upgrade shutdown. It is injected as a default parameter so a test can drive every branch - both UpgradeStep kinds and the failed-command path - through
 * the real handler on a real Express server without running a package install or exiting the process, while production wires the real adapters through
 * defaultUpgradeDeps. This mirrors the HealthDeps pattern in health.ts: a typed interface, a module-const default, and a defaulted parameter.
 */
export interface UpgradeDeps {

  // Detects the install method, and with it whether an in-place upgrade is possible at all.
  readonly detect: typeof detectInstallMethod;

  // Whether the process runs under a service manager (launchd, systemd, Windows Task Scheduler). The in-process success branch exits only when it does, so the
  // service manager brings PrismCast back up on the new version; a manual install stays up on the old one until the user restarts it.
  readonly isRunningAsService: typeof isRunningAsService;

  // Performs the upgrade for one InstallInfo by dispatching to the platform-appropriate lifecycle strategy. The route holds this reference rather than building
  // a lifecycle context itself, so construction stays at the adapter and the route never learns which platform does what.
  readonly performUpgrade: (info: InstallInfo) => Promise<UpgradeStep>;

  // Shuts PrismCast down once the response has had time to flush. The Windows handoff helper is waiting on this exit to release the file locks npm must rename
  // through, and the POSIX service path uses it to hand the restart to the service manager.
  readonly scheduleShutdown: () => void;
}

/**
 * Schedules the post-upgrade shutdown. The delay gives the HTTP response time to reach the client before the process goes away; the browser is closed on the way
 * out so Chrome does not outlive us. We exit either way, regardless of whether closeBrowser() succeeds, because something downstream is waiting on this exit in
 * both cases that reach here - a service manager to restart us, or the Windows helper to take the file locks. The exit code distinguishes a clean shutdown (0)
 * from an unclean one (1) for anyone reading service logs.
 */
function scheduleShutdownAfterUpgrade(): void {

  setTimeout(() => {

    LOG.info("Exiting to complete the upgrade.");

    void closeBrowser().then(() => { process.exit(0); }).catch(() => { process.exit(1); });
  }, RESPONSE_FLUSH_DELAY_MS);
}

const defaultUpgradeDeps: UpgradeDeps = {

  detect: detectInstallMethod,
  isRunningAsService,
  performUpgrade: async (info: InstallInfo): Promise<UpgradeStep> => performUpgrade(createDefaultLifecycleContext({ commandTimeoutMs:
    UPGRADE_COMMAND_TIMEOUT_MS }), info),
  scheduleShutdown: scheduleShutdownAfterUpgrade
};

/**
 * Configures upgrade-related HTTP endpoints.
 * @param app - The Express application.
 * @param deps - The detection, dispatch, service-probe, and shutdown boundary; defaults to defaultUpgradeDeps, injectable so a test can drive every branch.
 */
export function setupUpgradeEndpoint(app: Express, deps: UpgradeDeps = defaultUpgradeDeps): void {

  // GET /upgrade/info - Returns installation method, version information, and whether the installation is upgradeable.
  app.get("/upgrade/info", async (_req: Request, res: Response): Promise<void> => {

    try {

      const info = deps.detect();
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

  // POST /upgrade - Executes the upgrade for the detected installation method by dispatching to the platform-aware upgrade lifecycle. The returned UpgradeStep
  // says whether the upgrade ran here or was handed to a detached helper, and each variant gets its own answer and shutdown behavior.
  app.post("/upgrade", async (_req: Request, res: Response): Promise<void> => {

    try {

      const info = deps.detect();

      if(!info.upgradeable) {

        sendValidationError(res, "This installation method does not support in-place upgrades.");

        return;
      }

      LOG.info("Executing upgrade via web UI: %s.", info.upgradeCommand);

      const step = await deps.performUpgrade(info);

      if(step.kind === "handed-off") {

        /* A detached helper owns the upgrade now and is waiting on this process to exit so the file locks on the install directory release. We exit
         * unconditionally for this outcome, service or not, because the helper cannot start until we are gone.
         *
         * The restart promise is deliberately two-clause. The helper restarts a registered service task itself, but a non-service install stays down until the
         * user starts it again, and we cannot tell which case applies without probing Task Scheduler on every upgrade. willRestart is reported false for the
         * same reason: the client polls for the server to come back when it is true, and this upgrade takes far longer than that poll is willing to wait.
         */
        sendSuccess(res, { data: { logPath: step.logPath, willRestart: false }, message: "The upgrade is running in the background. If PrismCast is " +
          "registered as a Windows service, it will restart when the upgrade completes; otherwise, restart PrismCast manually." });

        deps.scheduleShutdown();

        return;
      }

      // The command ran and failed. We answer on the same envelope and status a thrown failure produces, so the web UI's error path sees no difference between
      // an upgrade that failed and one that could not be attempted.
      if(!step.success) {

        sendErrorResponse(res, new Error("the upgrade command reported a failure; the package manager's output is in the PrismCast log"), "upgrade");

        return;
      }

      const willRestart = deps.isRunningAsService();

      sendSuccess(res, { data: { willRestart }, message: "Upgrade complete." });

      // A service-managed process comes back on the new version once we exit, so we exit after the response has flushed. A manual install keeps running the old
      // version until the user restarts it, which is what the response tells them.
      if(willRestart) {

        deps.scheduleShutdown();
      }
    } catch(error) {

      sendErrorResponse(res, error, "upgrade");
    }
  });
}
