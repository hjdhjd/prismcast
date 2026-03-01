/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * directv.ts: DirecTV Stream channel selection via webpack injection for direct tuning, with logo click fallback.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.js";
import { LOG, delay, formatError } from "../../utils/index.js";
import { logAvailableChannels, normalizeChannelName } from "../channelSelection.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";

// Unified channel cache entry combining discovery metadata and tuning identifiers. Populated from the Redux store's channel lineup emitted via the
// [DIRECTV-CHANNELS] console signal during page load. The channelId and resourceId are the stable tuning artifacts used by the playConsumable action.
interface DirectvChannelEntry {

  callSign: string;
  channelId: string;
  displayName: string;
  resourceId: string;
}

// Unified channel cache for DirecTV Stream. Maps normalized channel names to their combined discovery and tuning data. Populated by the webpack interceptor's
// Redux store extraction, emitted as a [DIRECTV-CHANNELS] console signal and parsed by the console listener. Cleared on browser disconnect via clearDirectvCache().
const directvChannelCache = new Map<string, DirectvChannelEntry>();

// Set to true after a complete discovery confirms the channel cache contains the full channel lineup. Individual tunes only populate the cache from the Redux store
// extraction during that tune's page load, but the store contains the full lineup, so a single successful extraction fully enumerates. Without this flag,
// getCachedChannels() would return null before the first successful extraction.
let directvFullyDiscovered = false;

// Tracks which pages have console listeners registered to avoid duplicate registrations.
const directvPagesWithListeners = new WeakSet<Page>();

// Per-page tune state. Each entry holds the resolve function for the tune result promise and the promise itself. Keyed by Page so concurrent tunes on different
// pages are isolated — the console listener resolves the correct entry via the page reference it closes over, and the strategy function retrieves the promise for
// its page. Entries are created by resolveDirectvDirectUrl before navigation and consumed by directvGridStrategy after page load.
interface TuneState {

  promise: Promise<boolean>;
  resolve: (success: boolean) => void;
}
const pendingTunes = new WeakMap<Page, TuneState>();

// DirecTV guide URL. All tunes navigate here — the webpack interceptor handles channel selection during page load.
const DIRECTV_GUIDE_URL = "https://stream.directv.com/guide";

// Maximum time to wait for the webpack interceptor to emit a tune result. This covers the time from page navigation start through webpack chunk interception,
// React fiber tree traversal, Redux store discovery, channel matching, and playConsumable dispatch. Validated tunes complete in 2-5 seconds; eight seconds
// provides headroom for slow page loads while failing fast enough that logo click fallback isn't delayed excessively.
const TUNE_TIMEOUT = 8000;

// Maximum time to wait for the [DIRECTV-CHANNELS] console signal during discovery. The Redux store extraction polls for the React mount point with a 10-second
// overall timeout and a 5-second milestone timeout after DOM content appears. This Node-side timeout should exceed the browser-side timeout to avoid race
// conditions where the browser script succeeds but the console signal hasn't been processed yet.
const DISCOVERY_TIMEOUT = 15000;

// Network names that have local affiliates on DirecTV Stream, named as "{NETWORK}-{CALLSIGN}" (e.g., "ABC-WABC", "PBS-WNET"). Used for cache aliasing in
// processChannelLineup. The in-page interceptor and logo click fallback perform generic prefix matching independent of this set.
const DIRECTV_LOCAL_NETWORKS = new Set([ "abc", "cbs", "cw", "fox", "nbc", "pbs" ]);

/**
 * Clears all DirecTV state: the unified channel cache and the fully-discovered flag. Called by clearChannelSelectionCaches() in the coordinator when the browser
 * restarts, since cached state may be stale in a new browser session. The pendingTunes WeakMap is self-cleaning — entries are GC'd when their Page references are
 * released after page close.
 */
function clearDirectvCache(): void {

  directvChannelCache.clear();
  directvFullyDiscovered = false;
}

/**
 * Processes the channel lineup JSON emitted by the webpack interceptor's [DIRECTV-CHANNELS] console signal. Parses the JSON array and populates the unified
 * channel cache. Each entry contains channelId, channelName, callSign, and resourceId from the Redux store's channel lineup.
 * @param json - The JSON string emitted by the console signal.
 */
function processChannelLineup(json: string): void {

  let channels: { callSign?: string; channelId?: string; channelName?: string; resourceId?: string }[];

  try {

    channels = JSON.parse(json) as typeof channels;
  } catch {

    LOG.debug("tuning:directv", "Failed to parse [DIRECTV-CHANNELS] JSON.");

    return;
  }

  if(!Array.isArray(channels) || (channels.length === 0)) {

    return;
  }

  // Remove stale alias keys before repopulating. The population loop creates fresh objects for every channel, which would leave alias keys pointing to old objects
  // from the previous call. Deleting them first ensures the aliasing pass below always re-establishes aliases against the current objects.
  for(const network of DIRECTV_LOCAL_NETWORKS) {

    directvChannelCache.delete(network);
  }

  let count = 0;

  for(const ch of channels) {

    if(ch.channelName && ch.channelId && ch.resourceId) {

      const normalized = normalizeChannelName(ch.channelName);

      directvChannelCache.set(normalized, {

        callSign: ch.callSign ?? "",
        channelId: ch.channelId,
        displayName: ch.channelName,
        resourceId: ch.resourceId
      });

      count++;
    }
  }

  if(count > 0) {

    directvFullyDiscovered = true;

    LOG.debug("tuning:directv", "Channel lineup populated: %s channels.", count);

    // Cross-reference local affiliates so the short network name (e.g., "abc") resolves to the affiliate's cache entry (e.g., "abc-wabc"). This mirrors Hulu's
    // affiliate cache aliasing (hulu.ts:1089-1098) and ensures warm-cache diagnostic logging works for local channels.
    for(const network of DIRECTV_LOCAL_NETWORKS) {

      // Skip if the network name itself is already an exact cache key (unlikely on DirecTV, but defensive).
      if(directvChannelCache.has(network)) {

        continue;
      }

      // Collect all affiliate entries whose normalized name starts with "{network}-", then pick the first alphabetically. Sorting ensures deterministic alias
      // resolution when a network has multiple affiliates in the same DMA (e.g., PBS-WEDW, PBS-WNET, PBS-WNJN). For networks with a single affiliate this is a no-op.
      const affiliates: { entry: DirectvChannelEntry; key: string }[] = [];

      for(const [ key, entry ] of directvChannelCache) {

        if(key.startsWith(network + "-")) {

          affiliates.push({ entry, key });
        }
      }

      if(affiliates.length > 0) {

        affiliates.sort((a, b) => a.key.localeCompare(b.key));
        directvChannelCache.set(network, affiliates[0].entry);

        LOG.debug("tuning:directv", "Cross-referenced cache: %s -> %s.", network, affiliates[0].entry.displayName);
      }
    }
  }
}

/**
 * Sets up console listeners on the page to bridge in-page console signals from the evaluateOnNewDocument interceptor to the Node.js process. Handles four signal
 * types: [DIRECTV-CHANNELS] for channel lineup data, [DIRECTV-TUNE-OK] and [DIRECTV-TUNE-FAIL] for tune result signaling, and [DIRECTV-DIAG] for diagnostic
 * logging. Uses a WeakSet to prevent duplicate listener registration on the same page.
 * @param page - The Puppeteer page object.
 */
function setupConsoleListeners(page: Page): void {

  if(directvPagesWithListeners.has(page)) {

    return;
  }

  directvPagesWithListeners.add(page);

  page.on("console", (msg) => {

    const text = msg.text();

    // Channel lineup data emitted by the webpack interceptor's Redux store extraction.
    if(text.startsWith("[DIRECTV-CHANNELS] ")) {

      processChannelLineup(text.slice("[DIRECTV-CHANNELS] ".length));

      return;
    }

    // Tune success signal — the playConsumable dispatch settled successfully (synchronous action or async thunk resolution).
    if(text.startsWith("[DIRECTV-TUNE-OK]")) {

      pendingTunes.get(page)?.resolve(true);

      return;
    }

    // Tune failure signal — the playConsumable dispatch failed or the target channel was not found in the Redux store.
    if(text.startsWith("[DIRECTV-TUNE-FAIL]")) {

      LOG.debug("tuning:directv", "Tune failure signal: %s.", text);
      pendingTunes.get(page)?.resolve(false);

      return;
    }

    // Diagnostic messages bridged to the debug log for troubleshooting the webpack interceptor.
    if(text.startsWith("[DIRECTV-DIAG]")) {

      LOG.debug("tuning:directv", text);
    }
  });
}

/**
 * Installs the evaluateOnNewDocument script that intercepts webpack chunk loading to capture __webpack_require__, extracts the Redux store from the React fiber
 * tree, emits the channel lineup, and optionally dispatches playConsumable to tune to a specific channel. The script runs before any SPA JavaScript and is
 * entirely self-contained (no Node.js references).
 *
 * The interceptor has four phases:
 * 1. Main-frame guard (skip ad iframes)
 * 2. Wrap webpackChunk push to capture __webpack_require__ from natural chunk callbacks (defense-in-depth for future webpack builds)
 * 3. Poll for React fiber root, BFS for Redux store, extract channel lineup, emit [DIRECTV-CHANNELS]
 * 4. If not discoverOnly: capture __webpack_require__ via synthetic chunk push if needed, find playConsumable module, match channel, dispatch tune
 *
 * @param page - The Puppeteer page object.
 * @param channelName - The target channel name (normalized) for tuning. Ignored when discoverOnly is true.
 * @param discoverOnly - When true, skip the tune phase and only extract the channel lineup.
 */
async function installDirectTuneInterceptor(page: Page, channelName: string, discoverOnly: boolean): Promise<void> {

  await page.evaluateOnNewDocument((targetName: string, discoverOnlyFlag: boolean): void => {

    // Phase 1: Main-frame guard. The evaluateOnNewDocument script runs in every frame, including ad iframes. We only want to intercept webpack chunks in the main
    // frame where the DirecTV SPA loads.
    try {

      if(window.self !== window.top) {

        return;
      }
    } catch {

      return;
    }

    // eslint-disable-next-line no-console
    console.log("[DIRECTV-DIAG] Interceptor installed (target=" + targetName + ", discoverOnly=" + String(discoverOnlyFlag) + ").");

    // Captured __webpack_require__ function. Set by the chunk push wrapper when the first natural chunk callback fires. WreqType is defined inline because it
    // describes a runtime shape used in type casts — compile-time-only aliases like Nullable<T> work fine since TypeScript erases them. This webpack 5 build
    // does not expose a module cache property (.c) on __webpack_require__ — modules are loaded via wreq(moduleId) which uses an internal closure-based cache.
    interface WreqType { (moduleId: string): Record<string, unknown>; m: Record<string, unknown> }
    let wreq: WreqType | null = null;

    // Phase 2: Wrap webpackChunk push to capture __webpack_require__. This wrapper intercepts natural chunk pushes and captures __webpack_require__ from any chunk
    // that includes an entry point callback (the optional third element). DirecTV's current webpack build uses 2-element chunks without callbacks, so this wrapper
    // serves as defense-in-depth — the primary capture mechanism is the synthetic chunk push in Phase 4. If a future DirecTV deploy changes the chunk format, this
    // wrapper captures wreq early without needing a code change.
    const chunkArrayName = "webpackChunk_directv_web";

    // Ensure the chunk array exists before wrapping.
    const win = window as unknown as Record<string, unknown[]>;

    win[chunkArrayName] ??= [];

    const chunkArray = win[chunkArrayName];
    const originalPush = chunkArray.push.bind(chunkArray);

    chunkArray.push = function(...args: unknown[]): number {

      // Each chunk is an array: [ [chunkIds], { moduleId: moduleFactory }, executeCallback ]. The third element, when present, is a function that receives
      // __webpack_require__ as its argument. We intercept the first one that has a callback.
      for(const chunk of args) {

        if(!Array.isArray(chunk) || (chunk.length < 3)) {

          continue;
        }

        const callback = chunk[2] as Nullable<(wreqFn: unknown) => void>;

        if((typeof callback === "function") && !wreq) {

          // Replace the callback with our wrapper that captures __webpack_require__ before calling the original.
          chunk[2] = (wreqFn: unknown): void => {

            wreq = wreqFn as typeof wreq;

            // eslint-disable-next-line no-console
            console.log("[DIRECTV-DIAG] Captured __webpack_require__ (" + String(Object.keys((wreqFn as WreqType).m).length) + " modules).");

            callback(wreqFn);
          };
        }
      }

      return originalPush(...args);
    };

    // Phase 3: Poll for React fiber root, extract Redux store and channel lineup. React mounts asynchronously after webpack chunks load, so we poll the DOM for
    // the fiber root at 200ms intervals. Once found, BFS through the fiber tree to locate pendingProps.store (the Redux store), then extract the channel lineup
    // from the store's state.
    const POLL_INTERVAL = 200;
    const MAX_POLL_TIME = 10000;
    const pollStart = Date.now();

    // React property prefixes to search for on candidate mount elements. Covers React 18 createRoot (__reactContainer$), React 17-18 individual fibers
    // (__reactFiber$), and React 16 legacy (__reactInternalInstance$). Defined once and shared across poll iterations.
    const REACT_PREFIXES = [ "__reactContainer$", "__reactFiber$", "__reactInternalInstance$" ];

    // Finds the first React property key on an element, or undefined if no React properties exist.
    const findReactKey = (el: Element): string | undefined => Object.keys(el).find((k) => REACT_PREFIXES.some((p) => k.startsWith(p)));

    // One-shot diagnostic flags for the poll loop. Prevent flooding the console when the poll retries repeatedly.
    let mountPointLogged = false;
    let bfsExhaustedLogged = false;

    // Tracks when the page body first has children (DOM content loaded). Used for milestone-based early exit — if the page has rendered content but no React
    // mount point exists after MOUNT_SEARCH_TIMEOUT, the SPA structure is different from what we expect and continuing to poll is pointless.
    let pageContentTime = 0;
    const MOUNT_SEARCH_TIMEOUT = 5000;

    // StoreType is declared outside the poll callback so cachedStore can reference it across iterations. The store object is stable — Redux stores are
    // created once and never replaced — so holding a reference across poll ticks is safe.
    interface StoreType { dispatch: (action: unknown) => unknown; getState: () => Record<string, unknown> }
    let cachedStore: StoreType | null = null;

    // One-shot flag for the "channels not yet populated" diagnostic. The SPA fetches the channel lineup asynchronously after the Redux store is created, so
    // several poll iterations may pass before the data arrives.
    let channelsEmptyLogged = false;

    const pollTimer = setInterval((): void => {

      if((Date.now() - pollStart) > MAX_POLL_TIME) {

        clearInterval(pollTimer);

        // eslint-disable-next-line no-console
        console.log("[DIRECTV-DIAG] Overall poll timeout (" + String(MAX_POLL_TIME) + "ms) — " +
          (cachedStore ? "Redux store found but channel lineup never populated." : "Redux store not found."));

        if(!discoverOnlyFlag) {

          // eslint-disable-next-line no-console
          console.log("[DIRECTV-TUNE-FAIL] Timed out — " +
            (cachedStore ? "channel lineup never populated in Redux store." : "React mount point or Redux store not found."));
        }

        return;
      }

      // Phase 3a: Find the React mount point and Redux store (skipped once cached). The store is found early (~400ms) but the channel lineup is populated
      // asynchronously by the SPA's API calls. We cache the store reference and continue polling for channel data in Phase 3b below.
      if(!cachedStore) {

        // Find the React mount point. Try the known DirecTV ID first (fast path), then scan body's direct children for any element with React fiber
        // properties. This resilient approach survives element ID changes — if DirecTV renames their root container, the body scan finds it automatically.
        let mountEl: Element | null = document.getElementById("app-root");
        let reactKey: string | undefined = mountEl ? findReactKey(mountEl) : undefined;

        if(!reactKey) {

          mountEl = null;

          for(const child of Array.from(document.body.children)) {

            const key = findReactKey(child);

            if(key) {

              mountEl = child;
              reactKey = key;

              break;
            }
          }
        }

        if(!mountEl || !reactKey) {

          // Track when the page first has DOM content. Once content exists but no React mount is found, start the milestone countdown.
          if(!pageContentTime && (document.body.children.length > 0)) {

            pageContentTime = Date.now();
          }

          if(pageContentTime && ((Date.now() - pageContentTime) > MOUNT_SEARCH_TIMEOUT)) {

            clearInterval(pollTimer);

            // eslint-disable-next-line no-console
            console.log("[DIRECTV-DIAG] Page has DOM content but no React mount point found after " + String(MOUNT_SEARCH_TIMEOUT) + "ms.");

            if(!discoverOnlyFlag) {

              // eslint-disable-next-line no-console
              console.log("[DIRECTV-TUNE-FAIL] No React mount point found.");
            }
          }

          return;
        }

        // Log the discovered mount point once. The tag name and ID help diagnose future element ID changes without requiring code changes.
        if(!mountPointLogged) {

          mountPointLogged = true;

          // eslint-disable-next-line no-console
          console.log("[DIRECTV-DIAG] React mount: <" + mountEl.tagName.toLowerCase() + " id=\"" + (mountEl.id || "") + "\"> via " +
            reactKey.split("$")[0] + "$.");
        }

        // Get the fiber root from the React property. __reactContainer$ returns an internal root object whose .current property is the actual fiber node.
        // __reactFiber$ returns the fiber directly.
        const value = (mountEl as unknown as Record<string, unknown>)[reactKey] as Nullable<Record<string, unknown>>;
        const fiberRoot: Nullable<Record<string, unknown>> =
          (value && (typeof value.current === "object")) ? value.current as Record<string, unknown> : value ?? null;

        if(!fiberRoot) {

          return;
        }

        // BFS through the fiber tree to find the Redux store. The store is attached as pendingProps.store on a Provider fiber node.
        const queue: Record<string, unknown>[] = [fiberRoot];
        let store: StoreType | null = null;
        let visited = 0;
        const MAX_NODES = 5000;

        while((queue.length > 0) && (visited < MAX_NODES)) {

          const node = queue.shift();

          if(!node) {

            break;
          }

          visited++;

          // Check for Redux store on pendingProps. React-Redux v7 attaches directly as pendingProps.store. React-Redux v8 uses React context, placing the
          // store at pendingProps.value.store on the context Provider fiber node.
          const props = node.pendingProps as Record<string, unknown> | null;
          const candidate = (props?.store ?? (props?.value as Record<string, unknown> | null)?.store) as Record<string, unknown> | null;

          if(candidate && (typeof candidate.dispatch === "function") && (typeof candidate.getState === "function")) {

            store = candidate as unknown as StoreType;

            break;
          }

          // Traverse fiber tree: child and sibling links.
          if(node.child) {

            queue.push(node.child as Record<string, unknown>);
          }

          if(node.sibling) {

            queue.push(node.sibling as Record<string, unknown>);
          }
        }

        if(!store) {

          if(!bfsExhaustedLogged) {

            bfsExhaustedLogged = true;

            // eslint-disable-next-line no-console
            console.log("[DIRECTV-DIAG] Fiber root found but BFS visited " + String(visited) + " nodes without finding Redux store. Continuing to poll.");
          }

          return;
        }

        // Cache the store reference for subsequent poll iterations. The store object is stable — Redux stores are never recreated.
        cachedStore = store;

        // eslint-disable-next-line no-console
        console.log("[DIRECTV-DIAG] Found Redux store after " + String(Date.now() - pollStart) + "ms (" + String(visited) + " nodes visited).");
      }

      // Phase 3b: Extract the channel lineup from the Redux store state. The store is found early (~400ms after page load) but the SPA fetches channel data
      // asynchronously. We poll getState() each interval until the lineup appears. The validated path is state.channels.channelArrays (a 151-element array).
      // Fallback paths cover plausible restructurings in future app versions.
      const state = cachedStore.getState();
      let channels: { callSign?: string; ccid?: string; channelName?: string; resourceId?: string }[] = [];

      if(state.channels) {

        const channelsState = state.channels as Record<string, unknown>;

        // Primary path: channelArrays is the validated location for the channel lineup array.
        if(Array.isArray(channelsState.channelArrays)) {

          channels = channelsState.channelArrays as typeof channels;
        } else if(Array.isArray(channelsState.lineup)) {

          channels = channelsState.lineup as typeof channels;
        } else if(Array.isArray(channelsState.channels)) {

          channels = channelsState.channels as typeof channels;
        } else if(Array.isArray(channelsState.allChannels)) {

          channels = channelsState.allChannels as typeof channels;
        }
      }

      // Fallback: some app versions may use a dedicated channelLineup reducer.
      if((channels.length === 0) && state.channelLineup) {

        const lineup = state.channelLineup as Record<string, unknown>;

        if(Array.isArray(lineup.channelArrays)) {

          channels = lineup.channelArrays as typeof channels;
        } else if(Array.isArray(lineup.channels)) {

          channels = lineup.channels as typeof channels;
        }
      }

      if(channels.length === 0) {

        // The channel data hasn't arrived yet — the SPA is still fetching from DirecTV's API. Log once and continue polling.
        if(!channelsEmptyLogged) {

          channelsEmptyLogged = true;

          // eslint-disable-next-line no-console
          console.log("[DIRECTV-DIAG] Redux store found but channel lineup not yet populated. Waiting for async data fetch.");
        }

        return;
      }

      // Channel data found — stop polling and proceed with emission and (optionally) tuning.
      clearInterval(pollTimer);

      // eslint-disable-next-line no-console
      console.log("[DIRECTV-DIAG] Channel lineup populated: " + String(channels.length) + " channels (" + String(Date.now() - pollStart) + "ms total).");

      // Emit the channel lineup as a JSON array via console signal. The Node-side console listener parses this and populates the unified channel cache.
      const lineupData = channels.map((ch) => ({

        callSign: ch.callSign ?? "",
        channelId: ch.ccid ?? "",
        channelName: ch.channelName ?? "",
        resourceId: ch.resourceId ?? ""
      })).filter((ch) => ch.channelName && ch.channelId);

      // eslint-disable-next-line no-console
      console.log("[DIRECTV-CHANNELS] " + JSON.stringify(lineupData));

      // Phase 4: If not discovery-only, find and call playConsumable to tune to the target channel.
      if(discoverOnlyFlag) {

        return;
      }

      // If wreq wasn't captured naturally from chunk callbacks (DirecTV's current webpack build uses 2-element chunks without entry point callbacks), use a
      // synthetic chunk push to capture __webpack_require__. By this point webpack has fully initialized — it replaced the chunk array's push method with its own
      // webpackJsonpCallback handler during bootstrap. Pushing a synthetic chunk with an entry point callback (third element) triggers webpack to call it with
      // __webpack_require__ as the argument, giving us access to all loaded modules. The ref holder pattern isolates the synchronous callback mutation from
      // TypeScript's control flow narrowing — wreq is narrowed to null in this scope, so we capture into a holder and only assign after a null check.
      if(!wreq) {

        const wreqHolder: { ref: WreqType | null } = { ref: null };

        try {

          chunkArray.push([ ["__prismcast__"], {}, (wreqFn: unknown): void => {

            wreqHolder.ref = wreqFn as WreqType;
          } ]);
        } catch(err) {

          // eslint-disable-next-line no-console
          console.log("[DIRECTV-DIAG] Synthetic chunk push error: " + String(err));
        }

        if(wreqHolder.ref) {

          wreq = wreqHolder.ref;

          // eslint-disable-next-line no-console
          console.log("[DIRECTV-DIAG] Captured __webpack_require__ via synthetic chunk (" + String(Object.keys(wreq.m).length) + " modules).");
        } else {

          // eslint-disable-next-line no-console
          console.log("[DIRECTV-DIAG] Synthetic chunk push did not trigger callback — webpack handler may not be in place.");
        }
      }

      if(!wreq) {

        // eslint-disable-next-line no-console
        console.log("[DIRECTV-TUNE-FAIL] __webpack_require__ not captured (natural callback and synthetic chunk push both failed).");

        return;
      }

      // Normalize the target name for case-insensitive matching. Duplicated inline because evaluateOnNewDocument cannot reference Node.js functions.
      const normalize = (name: string): string => name.trim().replace(/\s+/g, " ").toLowerCase();
      const normalizedTarget = normalize(targetName);

      // Find the target channel in the lineup by matching normalized channel name. Falls back to prefix-with-hyphen matching for local affiliates (e.g., "PBS"
      // matches "PBS-WEDW"). When multiple affiliates match the prefix (e.g., PBS-WEDW, PBS-WNET, PBS-WNJN), we sort alphabetically and pick the first to ensure
      // deterministic selection consistent with the Node-side cache alias in processChannelLineup.
      const targetChannel = channels.find((ch) => normalize(ch.channelName ?? "") === normalizedTarget) ??
        channels.filter((ch) => normalize(ch.channelName ?? "").startsWith(normalizedTarget + "-"))
          .sort((a, b) => normalize(a.channelName ?? "").localeCompare(normalize(b.channelName ?? ""))).at(0);

      if(!targetChannel) {

        // eslint-disable-next-line no-console
        console.log("[DIRECTV-TUNE-FAIL] Channel \"" + targetName + "\" not found in lineup (" + String(channels.length) + " channels).");

        return;
      }

      // eslint-disable-next-line no-console
      console.log("[DIRECTV-DIAG] Target channel found: " + (targetChannel.channelName ?? "") + " (ccid=" + (targetChannel.ccid ?? "") +
        ", resourceId=" + (targetChannel.resourceId ?? "") + ").");

      // Find the playConsumable module by searching webpack module factories for the characteristic function signature. The factory source must contain both
      // "playConsumable" and "playAsset" to positively identify the module. Once found, load it via wreq(moduleId) which uses webpack's internal closure-based
      // cache and correctly resolves all closure dependencies. Manual factory execution (wreq.m[id](mod, exports, wreq)) does NOT work — it creates a fresh
      // closure scope where minified dependency variables are uninitialized, causing "a is not a function" at call time.
      let playConsumableFn: ((payload: Record<string, unknown>) => void) | null = null;

      for(const moduleId of Object.keys(wreq.m)) {

        // Webpack 5 may delete factories from wreq.m after loading. Skip non-function entries to avoid TypeError on .toString().
        if(typeof wreq.m[moduleId] !== "function") {

          continue;
        }

        let moduleSource: string;

        try {

          moduleSource = (wreq.m[moduleId] as { toString: () => string }).toString();
        } catch {

          continue;
        }

        if(moduleSource.includes("playConsumable") && moduleSource.includes("playAsset")) {

          // Load the module through webpack's own require function. This resolves all internal dependencies correctly via the closure-based module cache.
          try {

            const moduleExports = wreq(moduleId);

            if(typeof moduleExports.playConsumable === "function") {

              playConsumableFn = moduleExports.playConsumable as (payload: Record<string, unknown>) => void;
            } else {

              // Search all exports for a function whose source mentions playConsumable.
              for(const exportKey of Object.keys(moduleExports)) {

                const exportVal = moduleExports[exportKey];

                if((typeof exportVal === "function") && exportVal.toString().includes("playConsumable")) {

                  playConsumableFn = exportVal as (payload: Record<string, unknown>) => void;

                  break;
                }
              }
            }
          } catch(loadErr) {

            // eslint-disable-next-line no-console
            console.log("[DIRECTV-TUNE-FAIL] Found playConsumable factory (module " + moduleId + ") but failed to load: " + String(loadErr));

            return;
          }

          break;
        }
      }

      if(!playConsumableFn) {

        // eslint-disable-next-line no-console
        console.log("[DIRECTV-TUNE-FAIL] playConsumable module not found in webpack modules.");

        return;
      }

      // eslint-disable-next-line no-console
      console.log("[DIRECTV-DIAG] Dispatching playConsumable for " + (targetChannel.channelName ?? "") + ".");

      // Call playConsumable directly with the validated payload. The function is self-dispatching — it uses the dispatch and getState functions passed in the
      // payload to internally dispatch Redux actions for authorization, CDN token generation, manifest construction, and player initialization. This is NOT a
      // Redux action creator — do not wrap the call in store.dispatch().
      try {

        playConsumableFn({

          consumable: {

            augmentation: { constraints: { isPlayable: true } },
            badges: ["OnNow"],
            consumableType: "LINEAR",
            duration: 3600,
            programChannelId: targetChannel.resourceId ?? ""
          },
          consumableResourceId: targetChannel.resourceId ?? "",
          dispatch: cachedStore.dispatch,
          getState: cachedStore.getState,
          makeFullscreen: true,
          restart: false
        });

        // eslint-disable-next-line no-console
        console.log("[DIRECTV-TUNE-OK]");
      } catch(err) {

        // eslint-disable-next-line no-console
        console.log("[DIRECTV-TUNE-FAIL] Dispatch error: " + String(err));
      }
    }, POLL_INTERVAL);
  }, channelName, discoverOnly);
}

/**
 * Installs the DirecTV webpack interceptor before navigation. Called by the coordinator's resolveDirectUrl before page.goto fires. Always returns null so
 * channel selection runs directvGridStrategy, which awaits the interceptor result and falls back to logo click if it fails. The interceptor is installed
 * regardless of cache state — warm tunes dispatch playConsumable immediately, cold tunes discover the channel via Redux store extraction during page load.
 *
 * @param channelSelector - The channel selector string (e.g., "CNN", "ESPN").
 * @param page - The Puppeteer page for evaluateOnNewDocument installation and console listener setup.
 * @returns Always null — channel selection is never skipped.
 */
async function resolveDirectvDirectUrl(channelSelector: string, page: Page): Promise<Nullable<string>> {

  const normalizedName = normalizeChannelName(channelSelector);

  // Set up console listeners for all tunes. Must be registered before navigation so we capture signals emitted during page load.
  setupConsoleListeners(page);

  // Create a per-page tune state. The console listener resolves this page's entry when [DIRECTV-TUNE-OK] or [DIRECTV-TUNE-FAIL] is emitted. We build the
  // TuneState object first so the resolve function is captured before any await yields control.
  const tuneState = {} as TuneState;

  tuneState.promise = new Promise<boolean>((resolve) => {

    tuneState.resolve = resolve;
  });

  pendingTunes.set(page, tuneState);

  // Install the webpack interceptor. It runs before any page JavaScript and captures __webpack_require__ from the first webpack chunk push.
  try {

    await installDirectTuneInterceptor(page, channelSelector, false);
  } catch(error) {

    LOG.debug("tuning:directv", "Failed to install interceptor for %s: %s.", channelSelector, formatError(error));
    pendingTunes.delete(page);

    return null;
  }

  // Log cache state for diagnostics. Both warm and cold paths return null so channel selection always runs directvGridStrategy, which awaits the interceptor
  // result and falls back to logo click if it fails.
  if(directvChannelCache.has(normalizedName)) {

    LOG.debug("tuning:directv", "Warm cache for %s. Interceptor will dispatch playConsumable during page load.", channelSelector);
  } else {

    LOG.debug("tuning:directv", "Cold cache for %s. Interceptor will discover and tune during page load.", channelSelector);
  }

  return null;
}

/**
 * DirecTV grid strategy: waits for the webpack interceptor to complete the tune via playConsumable dispatch. The interceptor was already installed by
 * resolveDirectvDirectUrl before navigation. This function awaits the tune result promise with a timeout, falling back to the logo click strategy if the
 * interceptor fails or times out.
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector.
 * @returns Result object with success status and optional failure reason.
 */
async function directvGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;

  // If resolveDirectvDirectUrl set up a tune promise for this page, await it with a timeout. The interceptor fires during page load and emits [DIRECTV-TUNE-OK]
  // or [DIRECTV-TUNE-FAIL] when the playConsumable dispatch completes.
  const tuneState = pendingTunes.get(page);

  if(tuneState) {

    // Race the tune promise against a timeout. The promise maps to discriminated string results so we can distinguish "interceptor reported failure" from
    // "no signal arrived" — critical for debugging whether the interceptor ran at all or stalled silently.
    const result = await Promise.race([
      tuneState.promise.then((v) => v ? "success" : "failure"),
      delay(TUNE_TIMEOUT).then(() => "timeout")
    ]);

    // Clean up after resolution or timeout. In the success case the console listener already resolved the promise; in the timeout case this removes the
    // stale entry so a late-arriving console signal is a no-op.
    pendingTunes.delete(page);

    if(result === "success") {

      LOG.debug("tuning:directv", "Interceptor tune succeeded for %s.", channelName);

      return { directTune: true, success: true };
    }

    // Invalidate the cache entry so the next tune rediscovers the channel with fresh IDs from the Redux store rather than retrying with the same stale data.
    directvChannelCache.delete(normalizeChannelName(channelName));

    if(result === "failure") {

      LOG.debug("tuning:directv", "Interceptor reported tune failure for %s. Invalidated cache entry. Falling back to logo click.", channelName);
    } else {

      LOG.debug("tuning:directv", "Timed out waiting for interceptor signal for %s (%sms). Invalidated cache entry. Falling back to logo click.",
        channelName, TUNE_TIMEOUT);
    }
  }

  // Fallback: use the logo click strategy. The interceptor either failed or was not set up (should not happen in normal flow).
  return await directvLogoClickFallback(page, channelName);
}

/**
 * Logo click fallback for DirecTV Stream. The guide's logo column is NOT virtualized — all ~152 channel logos are always in the DOM with
 * aria-label="view {channelName}" attributes. This function:
 * 1. Finds the logo element by aria-label
 * 2. Scrolls it into view and clicks it (DOM click, not coordinate-based — an invisible overlay blocks coordinate clicks)
 * 3. Waits for the mini-guide play button to appear
 * 4. Clicks the inner Pressable to start playback
 *
 * @param page - The Puppeteer page object.
 * @param channelName - The channel selector value (display name as shown in the guide).
 * @returns Result object with success status and optional failure reason.
 */
async function directvLogoClickFallback(page: Page, channelName: string): Promise<ChannelSelectorResult> {

  LOG.debug("tuning:directv", "Logo click fallback for %s.", channelName);

  // Wait for the guide grid to render. The logo column loads progressively as React renders the channel list.
  try {

    await page.waitForSelector("[aria-label^=\"view \"]", { timeout: CONFIG.streaming.videoTimeout, visible: true });
  } catch {

    return { reason: "DirecTV guide grid did not load (no channel logos found).", success: false };
  }

  // Find and scroll to the target channel's logo. We use DOM element.click() rather than coordinate-based page.mouse.click because DirecTV has an invisible
  // overlay (React Native for Web Pressable) that intercepts coordinate-based clicks. DOM click dispatches directly to the element, bypassing coordinate
  // hit-testing. The find-and-scroll step tags the element with a data attribute so the subsequent click step can retrieve it without duplicating the search.
  const found = await page.evaluate((name: string): boolean => {

    // Try exact match first, then case-insensitive fallback, then prefix match for local affiliates.
    let logo = document.querySelector("[aria-label=\"view " + name + "\"]") as Nullable<HTMLElement>;

    if(!logo) {

      const lowerName = name.toLowerCase();

      const logos = Array.from(document.querySelectorAll("[aria-label^=\"view \"]")).map((el) => ({

        el: el as HTMLElement,
        label: (el.getAttribute("aria-label") ?? "").slice("view ".length).toLowerCase()
      }));

      // Tier 2: case-insensitive exact match.
      for(const { el, label } of logos) {

        if(label === lowerName) {

          logo = el;

          break;
        }
      }

      // Tier 3: prefix match for local affiliates (e.g., "PBS" matches "PBS-WEDW"). When multiple affiliates match (e.g., PBS-WEDW, PBS-WNET, PBS-WNJN), pick
      // the first alphabetically for deterministic selection consistent with the webpack interceptor and the Node-side cache alias in processChannelLineup.
      if(!logo) {

        const prefixMatches = logos.filter(({ label }) => label.startsWith(lowerName + "-"));

        if(prefixMatches.length > 0) {

          prefixMatches.sort((a, b) => a.label.localeCompare(b.label));
          logo = prefixMatches[0].el;
        }
      }
    }

    if(!logo) {

      return false;
    }

    // Tag the element so the click step can find it without repeating the search.
    logo.setAttribute("data-prismcast-target", "1");

    // Scroll the logo into view so the mini-guide overlay appears at a visible position.
    logo.scrollIntoView({ behavior: "instant", block: "center" });

    return true;
  }, channelName);

  if(!found) {

    // Log available channels from the logo column to help users identify the correct channelSelector value.
    const availableChannels = await page.evaluate((): string[] => {

      const logos = document.querySelectorAll("[aria-label^=\"view \"]");

      return Array.from(logos).map((el) => (el.getAttribute("aria-label") ?? "").slice("view ".length)).filter((name) => name.length > 0).sort();
    });

    logAvailableChannels({

      availableChannels,
      channelName,
      guideUrl: DIRECTV_GUIDE_URL,
      presetSuffix: "-directv",
      providerName: "DirecTV Stream"
    });

    return { reason: "Could not find channel " + channelName + " in DirecTV Stream guide.", success: false };
  }

  // Brief settle delay after scrolling for animations and lazy content to finish.
  await delay(300);

  // Click the previously-tagged logo element via DOM click.
  const logoClicked = await page.evaluate((): boolean => {

    const logo = document.querySelector("[data-prismcast-target=\"1\"]") as Nullable<HTMLElement>;

    if(!logo) {

      return false;
    }

    logo.removeAttribute("data-prismcast-target");
    logo.click();

    return true;
  });

  if(!logoClicked) {

    LOG.debug("tuning:directv", "Tagged logo element not found after scroll delay for %s.", channelName);

    return { reason: "DirecTV logo element disappeared after scrolling for " + channelName + ".", success: false };
  }

  // Wait for the mini-guide overlay to appear with the "on now" play button. DirecTV shows a mini-guide strip when a logo is clicked, with the currently-airing
  // program tile that starts playback when clicked.
  try {

    await page.waitForSelector("[aria-label^=\"on now,\"]", { timeout: CONFIG.streaming.videoTimeout, visible: true });
  } catch {

    return { reason: "DirecTV mini-guide play button did not appear for " + channelName + ".", success: false };
  }

  // Brief settle delay for the play button's React state to bind.
  await delay(200);

  // Click the inner Pressable element inside the on-now tile. The outer element has the aria-label but the clickable Pressable is the first focusable child.
  const clicked = await page.evaluate((): boolean => {

    const onNow = document.querySelector("[aria-label^=\"on now,\"]") as Nullable<HTMLElement>;

    if(!onNow) {

      return false;
    }

    // Find the inner clickable element. DirecTV uses tabindex="0" on Pressable components.
    const pressable = onNow.querySelector("[tabindex=\"0\"]") as Nullable<HTMLElement> ?? onNow;

    pressable.click();

    return true;
  });

  if(!clicked) {

    return { reason: "Failed to click DirecTV play button for " + channelName + ".", success: false };
  }

  LOG.info("DirecTV logo click fallback succeeded for %s.", channelName);

  return { success: true };
}

/**
 * Converts the unified channel cache to a sorted DiscoveredChannel array.
 * @returns Sorted array of discovered channels.
 */
function buildDirectvDiscoveredChannels(): DiscoveredChannel[] {

  const channels: DiscoveredChannel[] = [];
  const seen = new Set<string>();

  for(const entry of directvChannelCache.values()) {

    if(seen.has(entry.displayName)) {

      continue;
    }

    seen.add(entry.displayName);
    channels.push({ channelSelector: entry.displayName, name: entry.displayName });
  }

  channels.sort((a, b) => a.name.localeCompare(b.name));

  return channels;
}

/**
 * Discovers all channels from DirecTV Stream by installing the webpack interceptor with discoverOnly=true, navigating to the guide page, and waiting for the
 * Redux store extraction to emit the channel lineup. The Redux store contains the complete channel lineup, so a single page load fully enumerates all available
 * channels. Requires handlesOwnNavigation on the provider module because the interceptor must be installed before navigation.
 *
 * Side effect: populates the module-level directvChannelCache, warming the tuning cache for subsequent channel tunes.
 * @param page - The Puppeteer page object (fresh page, not yet navigated).
 * @returns Array of discovered channels.
 */
async function discoverDirectvChannels(page: Page): Promise<DiscoveredChannel[]> {

  // Return from the unified cache if a prior discovery or tune has fully enumerated the lineup.
  if(directvFullyDiscovered && (directvChannelCache.size > 0)) {

    return buildDirectvDiscoveredChannels();
  }

  // Set up console listeners before navigation so we capture the [DIRECTV-CHANNELS] signal.
  setupConsoleListeners(page);

  // Install the interceptor in discover-only mode — skip the playConsumable tune phase.
  try {

    await installDirectTuneInterceptor(page, "", true);
  } catch(error) {

    LOG.warn("Failed to install DirecTV discovery interceptor: %s.", formatError(error));

    return [];
  }

  await page.goto(DIRECTV_GUIDE_URL, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });

  // Wait for the channel lineup to be emitted by the interceptor. The Redux store extraction polls at 200ms intervals in the browser context.
  const discoveryStart = Date.now();

  while(!directvFullyDiscovered && ((Date.now() - discoveryStart) < DISCOVERY_TIMEOUT)) {

    // eslint-disable-next-line no-await-in-loop
    await delay(500);
  }

  if(!directvFullyDiscovered || (directvChannelCache.size === 0)) {

    LOG.debug("tuning:directv", "Discovery timed out or returned empty results after %sms.", Date.now() - discoveryStart);

    return [];
  }

  LOG.debug("tuning:directv", "Discovery completed: %s channels in %sms.", directvChannelCache.size, Date.now() - discoveryStart);

  return buildDirectvDiscoveredChannels();
}

/**
 * Returns cached discovered channels if the provider has fully enumerated its lineup, or null if no enumeration has occurred. The Redux store extraction
 * produces the complete lineup on a single page load, so the fully-discovered flag is reliable.
 * @returns Sorted array of discovered channels or null.
 */
function getDirectvCachedChannels(): Nullable<DiscoveredChannel[]> {

  if(!directvFullyDiscovered || (directvChannelCache.size === 0)) {

    return null;
  }

  return buildDirectvDiscoveredChannels();
}

export const directvProvider: ProviderModule = {

  discoverChannels: discoverDirectvChannels,
  getCachedChannels: getDirectvCachedChannels,
  guideUrl: DIRECTV_GUIDE_URL,
  handlesOwnNavigation: true,
  label: "DirecTV Stream",
  slug: "directv",
  strategy: {

    clearCache: clearDirectvCache,
    execute: directvGridStrategy,
    resolveDirectUrl: resolveDirectvDirectUrl
  },
  strategyName: "directvGrid"
};
