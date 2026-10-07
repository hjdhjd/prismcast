/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * httpErrors.ts: The request error handler every PrismCast Express application installs.
 */
import type { NextFunction, Request, Response } from "express";
import { LOG } from "./logger.ts";
import { formatError } from "./errors.ts";

/**
 * Handles an error a route threw or passed on, installed as the last middleware of the main application and of the HDHomeRun server, so a request failure in
 * any PrismCast application reaches the project logger rather than Express's console fallback, and a client never receives a stack trace. While the response has
 * not started, it answers 500 with a sentence. Once headers are sent the status line is already on the wire and no answer can replace it, so the error goes on
 * to Express's default handler, which closes the connection rather than leaving a half-written response open. Express recognizes an error handler by its four
 * parameters, which is why next stays in the signature on the path that does not call it.
 * @param error - The error the route raised.
 * @param req - The request that failed.
 * @param res - The response to answer on.
 * @param next - Express's next function, handed the error once headers are sent.
 */
export function handleRequestError(error: unknown, req: Request, res: Response, next: NextFunction): void {

  LOG.error("A request failed with an unhandled error.", { error: formatError(error), method: req.method, url: req.originalUrl });

  if(res.headersSent) {

    next(error);

    return;
  }

  res.status(500).send("Internal server error.");
}
