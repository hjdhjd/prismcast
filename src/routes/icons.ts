/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * icons.ts: Shared SVG icon constants for the PrismCast web UI. All icons are 14x14 with stroke-based rendering using currentColor so they adapt to their
 * container's text color. Icons are organized into labeled sections (Action, Status) with entries listed alphabetically by name within each section.
 */

// Action Icons.

export const ICON_ADD = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M8 3v10M3 8h10\"/></svg>";

export const ICON_BOLT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M9 1.5L4 9h4l-1 5.5L12 7H8l1-5.5z\"/></svg>";

export const ICON_COPY = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><rect x=\"5\" y=\"5\" width=\"9\" height=\"9\" rx=\"1\"/><path d=\"M5 11H3a1 1 0 01-1-1V3a1 1 0 011-1h7a1 1 0 011 1v2\"/></svg>";

export const ICON_DELETE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M2 4h12\"/><path d=\"M5 4V2.5a.5.5 0 01.5-.5h5a.5.5 0 01.5.5V4\"/><path d=\"M12.5 4l-.5 9.5a1 1 0 01-1 .5H5a1 1 0 " +
  "01-1-.5L3.5 4\"/></svg>";

export const ICON_DISABLE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 5.5l5 5\"/></svg>";

export const ICON_EDIT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M11.5 1.5l3 3L5 14H2v-3L11.5 1.5z\"/></svg>";

export const ICON_ENABLE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 8l2 2 3.5-4\"/></svg>";

export const ICON_EXPORT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M8 10V2M5 5l3-3 3 3\"/><path d=\"M2 11v2a1 1 0 001 1h10a1 1 0 001-1v-2\"/></svg>";

export const ICON_FILTER = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M1 2h14l-5 6v5l-4 2V8z\"/></svg>";

export const ICON_IMPORT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M8 2v8M5 7l3 3 3-3\"/><path d=\"M2 11v2a1 1 0 001 1h10a1 1 0 001-1v-2\"/></svg>";

export const ICON_LINK = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\">" +
  "<path d=\"M6 10a3.5 3.5 0 0 1 0-5l2-2a3.5 3.5 0 0 1 5 5l-1 1\"/>" +
  "<path d=\"M10 6a3.5 3.5 0 0 1 0 5l-2 2a3.5 3.5 0 0 1-5-5l1-1\"/></svg>";

export const ICON_LOGIN = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M6.5 2H3.5a1 1 0 00-1 1v10a1 1 0 001 1h3\"/><path d=\"M10.5 11l3-3-3-3\"/><path d=\"M13.5 8H6.5\"/></svg>";

export const ICON_MANAGE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><rect x=\"1.5\" y=\"5\" width=\"13\" height=\"9.5\" rx=\"1.5\"/>" +
  "<path d=\"M7 4.5L3.5 1\"/><path d=\"M9 4.5L12.5 1\"/></svg>";

export const ICON_REVERT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M3 8a5 5 0 1 1 1.5 3.5\"/><path d=\"M3 4v4h4\"/></svg>";

export const ICON_TRANSFER = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M3 5h10M10 2l3 3-3 3\"/><path d=\"M13 11H3M6 8l-3 3 3 3\"/></svg>";

// Status Icons.

export const ICON_HEALTH = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><polyline points=\"1,9 4,9 6,4 8,12 10,7 12,9 15,9\"/></svg>";
