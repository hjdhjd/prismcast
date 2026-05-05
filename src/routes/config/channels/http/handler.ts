/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * handler.ts: Route wrapper that provides uniform error handling for channel config endpoints.
 *
 * Every endpoint handler in this subdirectory shares the same exception-handling contract - caught errors flow through sendErrorResponse with a human-readable
 * action label for the log message. Rather than repeating try/catch in every handler, endpoints declare their business logic as a plain async function and wrap
 * it with `route(action, handler)`. The wrapper catches exceptions and delegates to sendErrorResponse, so handlers can early-return on validation errors and let
 * the wrapper deal with unexpected failures.
 */
import type { Request, RequestHandler, Response } from "express";
import { sendErrorResponse } from "../../http/envelope.ts";

/**
 * Wraps an endpoint handler with uniform error handling. The returned RequestHandler awaits the handler's (possibly sync) completion and catches any exception,
 * routing it through sendErrorResponse using the provided action label. Accepting both sync and async handlers lets endpoints that don't need to await (e.g.,
 * reads that respond immediately) skip the async keyword without a lint warning.
 * @param action - Human-readable description of the action for log messages (e.g., "toggle channel", "save channel"). Appears in the error log and 500 response.
 * @param handler - The endpoint implementation. Receives the Express request and response. Should early-return after calling sendValidationError/sendSuccess.
 * @returns An Express RequestHandler suitable for registering with app.get/post/put/patch/delete.
 */
export function route(action: string, handler: (req: Request, res: Response) => Promise<void> | void): RequestHandler {

  return async (req, res) => {

    try {

      await handler(req, res);
    } catch(error) {

      sendErrorResponse(res, error, action);
    }
  };
}
