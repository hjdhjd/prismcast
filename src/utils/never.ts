/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * never.ts: Exhaustiveness guard for switches over discriminated unions.
 */

/* A switch over a discriminated union is only as complete as the arms someone remembered to write, and a union grows. Calling this from the default arm is
 * what turns a forgotten arm into a compile error: once every member is handled, the value that reaches the default is narrowed to `never` and satisfies the
 * parameter, and a member added later widens it to a type the parameter rejects. The throw is the runtime half, for a value that entered from outside the type
 * system - parsed JSON, a wire payload, a hand-edited file - where the compile-time proof cannot reach.
 */

/**
 * Exhaustiveness guard for a switch over a union. A switch's default arm calls it with the narrowed value, which is what makes a member without an arm fail
 * to compile; the throw reports a value that reaches it at runtime rather than letting the switch fall through in silence.
 * @param value - The value the switch did not handle. Narrowed to never when every member of the union has an arm.
 * @returns Never returns.
 * @throws Always, naming the value that was not handled.
 */
export function assertNever(value: never): never {

  throw new Error("Unhandled value: " + JSON.stringify(value) + ".");
}
