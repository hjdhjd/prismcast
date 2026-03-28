/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cliOutput.ts: CLI output helpers for PrismCast service and upgrade commands.
 */

/**
 * Prints a message to stdout. Used by CLI subcommands (service, upgrade) that run before the logger is initialized.
 * @param message - The message to print.
 */
// eslint-disable-next-line no-console
export const print = (message: string): void => { console.log(message); };

/**
 * Prints an error message to stderr. Used by CLI subcommands (service, upgrade) that run before the logger is initialized.
 * @param message - The error message to print.
 */
// eslint-disable-next-line no-console
export const printError = (message: string): void => { console.error(message); };
