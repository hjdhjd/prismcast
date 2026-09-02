/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * auth.ts: Authentication routes for PrismCast channel login.
 */
import type { Express, Request, Response } from "express";
import { endLoginMode, getLoginStatus, startLoginMode } from "../browser/index.ts";
import { getResolvedChannel, resolveServiceKey } from "../config/services.ts";
import { sendConflictError, sendNotFoundError, sendSuccess, sendValidationError } from "./config/http/envelope.ts";
import { getDomainConfig } from "../config/sites.ts";

/* These routes manage the login workflow for TV provider authentication. Many streaming channels require users to authenticate with their TV provider (cable,
 * satellite, or streaming service) before content can be accessed.
 *
 * The login flow works as follows:
 * 1. User clicks "Login" on a channel in the web UI.
 * 2. POST /auth/login opens a browser tab with the channel's URL.
 * 3. The browser window is kept on screen so the user can interact with it.
 * 4. User completes TV provider authentication.
 * 5. User clicks "Done" in the web UI (POST /auth/done) or closes the browser tab.
 * 6. The browser tab is closed and the window settles where the visibility policy puts it: on screen while any capture stream runs, minimized otherwise.
 *
 * During login mode, new stream requests are blocked to prevent interference.
 */

/**
 * Request body for POST /auth/login.
 */
interface LoginRequest {

  // The channel key to login to (e.g., "nbc", "cnn"). Either channel or url must be provided.
  channel?: string;

  // The URL to navigate to for login. Either channel or url must be provided.
  url?: string;
}

/**
 * Sets up authentication routes for channel login.
 * @param app - The Express application.
 */
export function setupAuthEndpoint(app: Express): void {

  /**
   * POST /auth/login - Start login mode for a channel or URL.
   *
   * Request body:
   * - channel: The channel key (e.g., "nbc") - will be resolved to URL
   * - url: Direct URL to navigate to
   *
   * One of channel or url must be provided.
   */
  app.post("/auth/login", async (req: Request, res: Response): Promise<void> => {

    const body = req.body as LoginRequest;

    // Resolve URL from channel or use provided URL.
    let url: string | undefined;

    if(body.channel) {

      // Resolve service selection and get the channel. This respects both user-defined channel overrides and service selections (e.g., Disney+ vs ESPN.com).
      const resolvedKey = resolveServiceKey(body.channel);
      const channel = getResolvedChannel(resolvedKey);

      if(!channel) {

        sendNotFoundError(res, "Channel not found.");

        return;
      }

      url = channel.url;
    } else if(body.url) {

      url = body.url;
    }

    if(!url) {

      sendValidationError(res, "Either channel or url must be provided.");

      return;
    }

    // Check if the domain has a login URL override. Some sites (e.g., Fox.com) show different authentication options on their homepage vs their player page.
    const domainConfig = getDomainConfig(url);

    if(domainConfig?.loginUrl) {

      url = domainConfig.loginUrl;
    }

    const result = await startLoginMode(url);

    if(result.success) {

      sendSuccess(res, { message: "Login mode started. Complete authentication in the browser window." });
    } else {

      sendConflictError(res, result.error ?? "Login mode could not be started.");
    }
  });

  /**
   * POST /auth/done - End login mode.
   *
   * Called when the user clicks "Done" in the web UI to indicate authentication is complete.
   */
  app.post("/auth/done", async (_req: Request, res: Response): Promise<void> => {

    await endLoginMode();

    sendSuccess(res, { message: "Login mode ended." });
  });

  /**
   * GET /auth/status - Get current login status.
   *
   * Returns whether login mode is active and details about the current login session.
   */
  app.get("/auth/status", (_req: Request, res: Response): void => {

    const status = getLoginStatus();

    res.json(status);
  });
}
