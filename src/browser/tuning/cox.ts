/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cox.ts: Cox Contour TV provider module. Thin wrapper over the shared Comcast Polymer SPA factory in comcastPolymer.ts.
 */
import type { ProviderModule } from "../../types/index.js";
import { createComcastPolymerProvider } from "./comcastPolymer.js";

export const coxProvider: ProviderModule = createComcastPolymerProvider({

  debugCategory: "tuning:cox",
  guideUrl: "https://watchtv.cox.com/listings",
  label: "Cox Contour TV",
  presetSuffix: "-cox",
  profileDescription: "Cox Contour TV with in-page SPA channel switching. Set Channel Selector to the channel callSign (e.g., CNNHDP, ESPND) " +
    "or network name (e.g., CNN, ESPN).",
  profileName: "coxStream",
  profileSummary: "Cox Contour TV (SPA tuning, needs selector)",
  slug: "cox",
  strategyName: "coxDirect"
});
