/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * xfinity.ts: Xfinity Stream provider module. Thin wrapper over the shared Comcast Polymer SPA factory in comcastPolymer.ts.
 */
import type { ProviderModule } from "../../types/index.js";
import { createComcastPolymerProvider } from "./comcastPolymer.js";

export const xfinityProvider: ProviderModule = createComcastPolymerProvider({

  debugCategory: "tuning:xfinity",
  guideUrl: "https://www.xfinity.com/stream/listings",
  label: "Xfinity Stream",
  presetSuffix: "-xfinity",
  profileDescription: "Xfinity Stream with in-page SPA channel switching. Set Channel Selector to the channel callSign (e.g., CNNHD, ESPND) " +
    "or network name (e.g., CNN, ESPN).",
  profileName: "xfinityStream",
  profileSummary: "Xfinity Stream (SPA tuning, needs selector)",
  slug: "xfinity",
  strategyName: "xfinityDirect"
});
