/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.ts: Service channel discovery route for PrismCast.
 */
import type { DiscoveredChannel, ProviderModule } from "../types/index.ts";
import type { Express, Request, Response } from "express";
import { LOG, raceWithTimeout } from "../utils/index.ts";
import { getChannelListing, getChannelLogo, isPredefinedChannel } from "../config/userChannels.ts";
import { getChannelServiceLabel, getResolvedChannel, getServiceGroup, getServiceTagForChannel, isServiceTagEnabled,
  resolveServiceKey } from "../config/services.ts";
import { getProviderBySlug, normalizeChannelName } from "../browser/channelSelection.ts";
import { recordDiscoveryOutcome, withProviderGuidePage } from "../browser/precaching.ts";
import { sendError, sendNotFoundError } from "./config/http/envelope.ts";

/* The services endpoint exposes channel discovery for each registered service. A GET request to /services/:slug/channels creates a temporary browser page,
 * navigates to the service's guide, runs the service's discoverChannels implementation, and returns a sorted JSON array of discovered channels. The temporary
 * page is always closed in a finally block to prevent resource leaks. Concurrent requests for the same service are coalesced - only one discovery walk runs at a
 * time, and subsequent requests piggyback on the in-flight result. A refresh=true request aborts any in-flight discovery and starts fresh.
 */

// Sentinel error used to identify aborted discoveries in the retry loop. Distinguishes abort rejections from genuine discovery failures so the loop only retries
// when the failure was caused by a refresh=true cancellation, not an unrelated error.
class DiscoveryAbortError extends Error {

  constructor() {

    super("Discovery aborted.");
    this.name = "DiscoveryAbortError";
  }
}

// In-flight discovery state. Tracks the running discovery promise and its associated abort controller for each service slug. When a discovery is in flight,
// subsequent requests await the existing promise instead of spawning redundant browser pages. The abort controller's signal is used to close the page when a
// refresh=true request needs to cancel an in-flight non-refresh discovery.
interface InflightEntry {

  controller: AbortController;
  promise: Promise<DiscoveredChannel[]>;
}

const inflight = new Map<string, InflightEntry>();

// The bound on how long a replacement discovery waits for its doomed predecessor's teardown to settle before proceeding. That teardown awaits CDP calls which
// Puppeteer bounds only at its 180-second default protocol timeout, so a wedged browser degrades the refresh to clearing while the doomed teardown is still
// running - the narrow race the sequencing below exists to close, confined to the pathological case - rather than chain-stalling every later refresh behind it.
const DISCOVERY_SETTLEMENT_TIMEOUT_MS = 10000;

/**
 * Logs a discovery failure and sends a 500 error response.
 * @param res - The Express response object.
 * @param label - The service's display label for log messages.
 * @param error - The error that caused the failure.
 */
function sendDiscoveryError(res: Response, label: string, error: unknown): void {

  const message = (error instanceof Error) ? error.message : String(error);

  // Channel discovery failures are warnings rather than errors because they typically reflect provider DOM drift, network blips, or transient site outages
  // (recoverable by retry) rather than server-side faults. The envelope helper normalizes the response shape.
  LOG.warn("Channel discovery failed for %s: %s.", label, message);
  sendError(res, 500, { error: "Channel discovery failed: " + message + "." });
}

/* ServiceDiscoveryDeps is the cross-module collaborator set the discovery route composes on: the provider-registry lookup that resolves a slug to its module, plus
 * the two precaching primitives the discovery walk delegates to - the guarded guide-page session and the discovery-outcome policy. It is injected as a default
 * parameter threaded from setupServicesEndpoint through the route handler into runDiscovery, so a test can substitute stubs at the same injection point - no loader mock
 * - while production uses the real defaultServiceDiscoveryDeps built from the functions this module already imports. getProviderBySlug earns its place here because the
 * provider registry is module-private with no registration hook, so injecting the lookup is the only way a test drives the route with a stub provider. This is the
 * collaborator-injection form of the Clock port (utils/clock.ts), matching VideoTuneDeps in browser/video.ts and PrecachingDeps in browser/precaching.ts.
 */
export interface ServiceDiscoveryDeps {

  readonly getProviderBySlug: typeof getProviderBySlug;
  readonly recordDiscoveryOutcome: typeof recordDiscoveryOutcome;
  readonly withProviderGuidePage: typeof withProviderGuidePage;
}

const defaultServiceDiscoveryDeps: ServiceDiscoveryDeps = { getProviderBySlug, recordDiscoveryOutcome, withProviderGuidePage };

/**
 * Runs service channel discovery through the shared guarded guide-page session, applying this endpoint's discovery-outcome policy. The helper owns the page
 * lifecycle, the consent-overlay poll, and the close-on-abort mechanics; this function contributes only the endpoint's policy: record the discovery outcome unless
 * the walk was aborted, sort the results, and translate any abort into a DiscoveryAbortError the retry loop understands.
 * @param provider - The provider module to discover channels for.
 * @param signal - Abort signal for cancellation by refresh requests.
 * @param deps - The injected discovery collaborators; the walk and the outcome recording run through deps.withProviderGuidePage and deps.recordDiscoveryOutcome.
 * @returns Sorted array of discovered channels.
 */
async function runDiscovery(provider: ProviderModule, signal: AbortSignal, deps: ServiceDiscoveryDeps): Promise<DiscoveredChannel[]> {

  try {

    const channels = await deps.withProviderGuidePage(provider, {

      afterWalk: async (page, discovered): Promise<void> => {

        // Record the domain auth consequences of this discovery while the page is still open - an empty result classifies the page it walked, and a non-empty result
        // supplies the success evidence that verifies the domain or clears a standing needs-sign-in flag. Skipped when the walk was aborted by a refresh=true
        // request, since an aborted walk says nothing about the provider.
        if(!signal.aborted) {

          await deps.recordDiscoveryOutcome(provider, discovered, page);
        }
      },
      signal
    });

    // Sort by name for consistent output. Discovery functions sort at cache time, but fresh (uncached) results from the first call may not be sorted yet.
    channels.sort((a, b) => a.name.localeCompare(b.name));

    return channels;
  } catch(error) {

    // Wrap Puppeteer errors caused by page closure during abort into a DiscoveryAbortError so the retry loop can distinguish aborts from genuine failures.
    // signal.aborted is the single source of truth for whether an abort occurred.
    if(signal.aborted) {

      throw new DiscoveryAbortError();
    }

    throw error;
  }
}

/**
 * Lineup state for a discovered channel that matches an existing channel in the user's lineup. Attached to each discovered channel by the annotation step
 * so the client can render three-state checkboxes without any matching logic of its own.
 */
interface LineupState {

  // The canonical channel key (e.g., "animal" for Animal Planet). Used by the client to send back to the server for service switch/remove operations.
  canonicalKey: string;

  // Human-readable label for the currently active service (e.g., "Hulu", "Spectrum"). Displayed in the browse modal's state label.
  currentService: string;

  // Service tag for the currently active service (e.g., "hulu", "spectrum"). Compared against the browsed service's slug to determine checked vs
  // indeterminate state.
  currentTag: string;

  // The canonical channel's display name from the predefined or user-defined definition (e.g., "Disney", "Disney (Pacific)", "A&E"). The client renders
  // this instead of the raw discovery name when present, ensuring the browse modal shows the same names users see in the channels table.
  displayName: string;

  // Whether the channel is currently enabled in the lineup.
  enabled: boolean;

  // Whether the channel has at least one other enabled service variant besides the browsed service. Used by the client to determine the visual state
  // when unchecking a "current" channel: indeterminate if alternatives exist (channel persists), empty if not (channel will be disabled).
  hasAlternatives: boolean;

  // Channel logo URL from the DVR logo cache. Used by the client to render logos alongside channel names via channelDisplayHtml.
  logoUrl?: string;

  // The stationId (Gracenote ID) for this canonical channel. Used to disambiguate when two canonicals (East and Pacific) share the same channelSelector
  // for a service - the discovery entry's stationId is matched against this value to assign the correct canonical.
  stationId?: string;

  // Whether the channel is predefined or user-defined.
  source: string;
}

/**
 * Annotated discovery result that extends DiscoveredChannel with optional lineup state. When a discovered channel matches an existing channel in the user's
 * lineup (by canonical key), the lineup field provides the current service state. When absent, the channel is new (not in the lineup).
 */
interface AnnotatedChannel extends DiscoveredChannel {

  lineup?: LineupState;
}

/**
 * Annotates discovered channels with lineup state by matching each channel's channelSelector against the service variants for the browsed service. This
 * uses the existing service group system to find which canonical channel each discovered channel corresponds to, avoiding fragile name-to-key normalization
 * (predefined keys are hand-crafted and may not match generateChannelKey output).
 *
 * For each canonical in the listing, we check for a variant matching the browsed service (via getServiceGroup) and extract its channelSelector via
 * getResolvedChannel. This builds a channelSelector -> lineup state map that the discovered channels are matched against.
 *
 * @param channels - The raw discovered channels from the service.
 * @param serviceSlug - The slug of the service being browsed (e.g., "spectrum", "hulu").
 * @returns Annotated channels with lineup state where applicable.
 */
function annotateWithLineupState(channels: DiscoveredChannel[], serviceSlug: string): AnnotatedChannel[] {

  // Build a channelSelector -> lineup state mapping for the browsed service. For each canonical entry in the listing, we find the variant that corresponds
  // to this service (if one exists) and index by that variant's channelSelector. This ensures matching works even when predefined keys don't match the
  // normalized channel name (e.g., "axstv" vs generateChannelKey("AXS TV") -> "axs-tv").
  //
  // When two canonicals share the same channelSelector for a service (East/Pacific pairs like "Disney Channel"), the map stores an array of states. The
  // annotation step then uses the discovery entry's stationId to disambiguate which canonical to assign.
  const listing = getChannelListing();
  const bySelector = new Map<string, LineupState[]>();

  // Appends a lineup state to the map under the normalized key. Normalization (lowercase, collapsed whitespace) ensures matching is resilient to casing
  // differences between predefined channelSelectors and service discovery output (e.g., "Cartoon Network (East)" vs "cartoon network (east)").
  function indexState(key: string, state: LineupState): void {

    const normalized = normalizeChannelName(key);
    const existing = bySelector.get(normalized);

    if(existing) {

      existing.push(state);
    } else {

      bySelector.set(normalized, [state]);
    }
  }

  for(const entry of listing) {

    const canonicalKey = entry.key;
    const resolvedKey = resolveServiceKey(canonicalKey);
    const currentTag = getServiceTagForChannel(resolvedKey);
    const currentService = getChannelServiceLabel(entry.channel);
    const displayName = entry.channel.name ?? canonicalKey;
    const source = isPredefinedChannel(canonicalKey) ? "predefined" : "user";

    // Check the service group for variants. Used both for channelSelector matching and for computing hasAlternatives.
    const group = getServiceGroup(canonicalKey);

    // Determine whether the channel has at least one enabled service besides the browsed service. Check the canonical's own tag first, then iterate
    // the group's variants. This uses the existing service tag and filter infrastructure.
    const canonicalTag = getServiceTagForChannel(canonicalKey);
    let hasAlternatives = (canonicalTag !== serviceSlug) && isServiceTagEnabled(canonicalTag);

    if(!hasAlternatives && group) {

      for(const variant of group.variants) {

        const variantTag = getServiceTagForChannel(variant.key);

        if((variantTag !== serviceSlug) && isServiceTagEnabled(variantTag)) {

          hasAlternatives = true;

          break;
        }
      }
    }

    const state: LineupState = {

      canonicalKey, currentService, currentTag, displayName, enabled: entry.enabled, hasAlternatives,
      logoUrl: getChannelLogo(canonicalKey),
      source, stationId: entry.channel.stationId
    };

    // Find the variant matching the browsed service and store by its channelSelector.
    if(group) {

      for(const variant of group.variants) {

        if(getServiceTagForChannel(variant.key) === serviceSlug) {

          const variantChannel = getResolvedChannel(variant.key);

          if(variantChannel?.channelSelector) {

            indexState(variantChannel.channelSelector, state);
          }

          break;
        }
      }
    }

    // Also match the canonical itself if its service tag matches (single-service channels or canonicals that point directly to this service).
    if((currentTag === serviceSlug) && entry.channel.channelSelector) {

      indexState(entry.channel.channelSelector, state);
    }

    // Index by display name when it differs from the channelSelector, so discovered channels whose names don't match any predefined channelSelector can still
    // be annotated via the name fallback. This connects Pacific timezone variants (e.g., discovered "A&E (Pacific)") to their predefined canonical (aep with
    // displayName "A&E (Pacific)") whose channelSelector ("A&E") wouldn't match the discovered name.
    if(entry.channel.channelSelector && (displayName !== entry.channel.channelSelector)) {

      indexState(displayName, state);
    }
  }

  // Annotate each discovered channel by matching its channelSelector against the service-specific lookup. When the channelSelector doesn't match (e.g.,
  // Xfinity uses callSigns like "ESPND" while predefined variants use display names like "ESPN"), the display name is tried as a fallback. When multiple
  // canonicals share the same key (East/Pacific pairs), the discovery entry's stationId disambiguates which canonical to assign.
  return channels.map((ch) => {

    const states = bySelector.get(normalizeChannelName(ch.channelSelector)) ?? bySelector.get(normalizeChannelName(ch.name));

    if(!states || (states.length === 0)) {

      return ch;
    }

    // Single match - no disambiguation needed.
    if(states.length === 1) {

      return { ...ch, lineup: states[0] };
    }

    // Multiple matches - use stationId to pick the right canonical. Fall back to the first match if stationId doesn't disambiguate.
    const match = states.find((s) => s.stationId === ch.stationId) ?? states[0];

    return { ...ch, lineup: match };
  });
}

/**
 * Creates the service channel discovery endpoint.
 * @param app - The Express application.
 * @param deps - The injected discovery collaborators; defaults to defaultServiceDiscoveryDeps. Threaded into the handler so a test drives the route with a stub
 * provider and a stubbed guide-page session without a loader mock.
 */
export function setupServicesEndpoint(app: Express, deps: ServiceDiscoveryDeps = defaultServiceDiscoveryDeps): void {

  app.get("/services/:slug/channels", async (req: Request, res: Response): Promise<void> => {

    const slug = req.params["slug"] as string;
    const provider = deps.getProviderBySlug(slug);

    if(!provider) {

      sendNotFoundError(res, "Unknown service: " + slug + ".");

      return;
    }

    // A refresh=true request rebuilds the service's caches (unified channel cache, row caches, fully-enumerated flags, etc.) so the discovery walk runs against
    // fresh data. Clearing them also resets warm tuning state (watch URLs, GUIDs), but the discovery walk repopulates the unified cache before returning - any
    // subsequent tune resolves from the freshly populated cache as normal.
    const lineup = req.query["lineup"] === "true";
    const refresh = req.query["refresh"] === "true";

    // The in-flight entry is read once and serves both the abort below and the replacement's reference to its predecessor. Nothing mutates the map between
    // those two uses in this synchronous turn, so one read is the whole truth.
    let entry = inflight.get(slug);

    // A refresh only SIGNALS the in-flight walk here. The doomed walk's page teardown settles asynchronously, and its interceptor callbacks keep writing into
    // the provider's caches until that page is actually closed, so clearing on this statement would let the dying walk's stragglers seed the very lineup the
    // refresh asked to rebuild. The clear is sequenced inside the replacement entry below, after the predecessor has settled; entry removal belongs to the
    // walk's own finally, which is the single removal path.
    if(refresh) {

      entry?.controller.abort();
    }

    // Check for cached discovery results before creating a browser page. When a prior tune or discovery call has already enumerated the service's lineup, the
    // cache is warm and we can return immediately without any browser interaction. Skipped when refresh=true, since the sequenced replacement below owns the
    // clear and a refresh must be answered from the rebuilt lineup.
    if(!refresh) {

      const cached = provider.getCachedChannels();

      if(cached) {

        res.json(lineup ? annotateWithLineupState(cached, slug) : cached);

        return;
      }
    }

    /* Coalesce concurrent requests. If a discovery is already in flight for this service, piggyback on the existing promise instead of spawning a redundant
     * browser page. If the in-flight discovery was aborted (by a refresh=true request that arrived after we checked above), the promise rejects with a
     * DiscoveryAbortError and we retry against whatever new entry replaced it in the map.
     *
     * A refresh always creates its own entry - it must never piggyback on the walk it just aborted - and creating that replacement in the same synchronous turn
     * as the abort is what guarantees a piggybacking requester always finds a successor. The inflight.set below overwrites the doomed entry, whose own finally
     * then sees a different controller in the map and correctly leaves it alone.
     */
    if(refresh || !entry) {

      const controller = new AbortController();
      const doomed = entry;

      const promise = (async (): Promise<DiscoveredChannel[]> => {

        /* The predecessor's settlement gates everything that follows: the guarded guide-page session awaits its page close before the walk promise settles, so
         * once this await returns no interceptor callback of that walk can write into the caches we are about to clear. How it settled is its own business -
         * the refresh cares only THAT it settled. The race bounds the wait: a teardown wedged inside a CDP call would otherwise hold every later refresh
         * hostage to Puppeteer's 180-second default protocol timeout, so past the bound we proceed and accept the clear-while-settling race for that
         * pathological case alone.
         */
        if(doomed) {

          const settlementTimeout = new Error("The previous discovery walk's teardown did not settle in time.");

          try {

            await raceWithTimeout(doomed.promise, DISCOVERY_SETTLEMENT_TIMEOUT_MS, settlementTimeout);
          } catch(error) {

            // An aborted walk rejects by design and stays quiet, and a genuine walk failure was already reported to that walk's own requesters. The timeout
            // sentinel is ours alone - reference identity tells it apart - and a wedged teardown is exactly the in-trouble signal an operator wants while it is
            // still happening.
            if(error === settlementTimeout) {

              LOG.warn("Clearing %s discovery caches while the previous walk's teardown is still settling.", provider.label);
            }
          }
        }

        // With no predecessor to await, this body runs synchronously to here inside the request's own turn, so a refresh with nothing in flight still clears
        // before anything else can observe the caches - and before the walk below starts reading them.
        if(refresh) {

          provider.strategy.clearCache?.();
        }

        return runDiscovery(provider, controller.signal, deps);
      })().finally(() => {

        // Only remove our own entry. A refresh=true request may have already replaced it with a new one.
        if(inflight.get(slug)?.controller === controller) {

          inflight.delete(slug);
        }
      });

      entry = { controller, promise };
      inflight.set(slug, entry);
    }

    // Await the in-flight discovery. If it was aborted by a refresh=true request, a new discovery should now be in the map - retry against that one. The caller
    // doesn't know or care about the abort; they just want channels. Only DiscoveryAbortError triggers a retry; genuine failures are reported immediately.
    for(;;) {

      try {

        // eslint-disable-next-line no-await-in-loop -- Intentional: each iteration awaits a different promise (the replacement after an abort).
        const channels = await entry.promise;

        res.json(lineup ? annotateWithLineupState(channels, slug) : channels);

        return;
      } catch(error) {

        // Only retry if this was an abort and a new discovery has replaced the aborted one.
        const retryEntry = inflight.get(slug);

        if((error instanceof DiscoveryAbortError) && retryEntry && (retryEntry !== entry)) {

          entry = retryEntry;

          continue;
        }

        // Genuine failure or no replacement entry after abort.
        sendDiscoveryError(res, provider.label, error);

        return;
      }
    }
  });
}
