/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consistencyProbe.ts: Cross-store consistency probe for PrismCast persistence.
 *
 * The probe runs once at startup after every initialize* function has loaded its store. It validates "foreign-key-style" invariants that span multiple stores
 * - things the per-store schema migrations cannot enforce because they only see one file at a time:
 *
 *   - Service selections (in channels.json) reference variants that exist in the rebuilt service group taxonomy.
 *   - Variant entries with a canonicalKey (in channels.json) reference a canonical that exists in PREDEFINED_CHANNELS or the user's stored channels.
 *   - User domain mappings (in profiles.json) reference profiles that exist as builtin or user-defined.
 *   - The service tag filter (in config.json's channels.enabledServices) contains only recognized service tags.
 *
 * Each issue is logged loudly and, when an auto-fix is provided, applied immediately. Auto-fixes that need to mutate persistent state go through the same
 * mutate* functions as user code so the integrity validators and snapshot machinery cover them too.
 *
 * Adding a new check is a single function returning ConsistencyIssue[]; collectConsistencyIssues fans out to every check in parallel.
 */
import { LOG, formatError } from "../utils/index.ts";
import { getAllServiceTags, mutateEnabledServices } from "./services.ts";
import { CONFIG } from "./index.ts";
import type { Channel } from "../types/index.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import { getBuiltinProfile } from "./sites.ts";
import { getStoredUserChannels } from "./userChannels.ts";
import { getUserDomains } from "./userProfiles.ts";

/**
 * A single consistency issue detected by the probe. Each carries enough metadata for the probe runner to log uniformly and apply auto-fixes when present.
 */
interface ConsistencyIssue {

  // Optional auto-fix function. Idempotent and safe to call from the probe runner. Issues without an autoFix are surfaced for operator action only.
  autoFix?: () => Promise<void>;

  // Stable category identifier. Used for log grouping and future filtering.
  category: string;

  // Human-readable description of the inconsistency.
  description: string;

  // "warning" issues run their autoFix automatically (when present). "error" issues require operator action.
  severity: "warning" | "error";
}

/**
 * Validates that every service tag in CONFIG.channels.enabledServices is recognized. Strips and persists unknown tags. Mirrors the runtime cleanup that
 * already happens in initializeUserChannels, but persists the cleanup so the on-disk file no longer carries stale tags between boots.
 */
function checkServiceTagFilter(): ConsistencyIssue[] {

  if(CONFIG.channels.enabledServices.length === 0) {

    return [];
  }

  const knownTags = new Set(getAllServiceTags().map((tag) => tag.tag));
  const enabledSet = new Set(CONFIG.channels.enabledServices);
  const invalid = [...enabledSet.difference(knownTags)];

  if(invalid.length === 0) {

    return [];
  }

  return [{

    autoFix: async (): Promise<void> => {

      // mutateEnabledServices persists the cleaned list to config.json and updates the in-memory cache atomically.
      await mutateEnabledServices([...enabledSet.intersection(knownTags)]);
    },
    category: "unknown-service-tag",
    description: "Service filter contains unrecognized tag(s): " + invalid.join(", "),
    severity: "warning"
  }];
}

/**
 * Validates that every variant entry's canonicalKey points at a channel that exists either in the predefined catalog or the user's stored channels. Dangling
 * canonical references usually mean the user deleted the canonical without cleaning the variants up; we surface for operator action rather than auto-fixing
 * because the right action depends on intent (delete the variants vs. restore the canonical).
 */
function checkVariantCanonicals(): ConsistencyIssue[] {

  const issues: ConsistencyIssue[] = [];
  const stored = getStoredUserChannels();

  for(const [ key, entry ] of Object.entries(stored)) {

    const canonicalKey = (entry as Channel).canonicalKey;

    if(!canonicalKey) {

      continue;
    }

    if(PREDEFINED_CHANNELS[canonicalKey] || stored[canonicalKey]) {

      continue;
    }

    issues.push({

      category: "dangling-variant-canonical",
      description: "Variant '" + key + "' references missing canonical '" + canonicalKey + "'.",
      severity: "warning"
    });
  }

  return issues;
}

/**
 * Validates that every user domain mapping references a profile that exists either as a builtin or user-defined profile.
 */
function checkDomainProfiles(): ConsistencyIssue[] {

  const issues: ConsistencyIssue[] = [];
  const userDomains = getUserDomains();

  for(const [ domain, config ] of Object.entries(userDomains)) {

    const profileKey = config.profile;

    if(!profileKey) {

      continue;
    }

    if(getBuiltinProfile(profileKey)) {

      continue;
    }

    issues.push({

      category: "dangling-domain-profile",
      description: "Domain '" + domain + "' references missing profile '" + profileKey + "'.",
      severity: "warning"
    });
  }

  return issues;
}

/**
 * Aggregates issues from every consistency check. New checks should be added here as additional collector calls.
 */
function collectConsistencyIssues(): ConsistencyIssue[] {

  return [

    ...checkServiceTagFilter(),
    ...checkVariantCanonicals(),
    ...checkDomainProfiles()
  ];
}

/**
 * Runs the consistency probe at startup. Logs every issue, runs auto-fixes for warnings, and surfaces errors for operator review. Errors do not block startup
 * - a consistency error is recoverable runtime state, not an unbootable system. Operators see them in the log and can act before the next restart.
 *
 * Auto-fix failures are themselves logged but do not propagate. The probe is best-effort hygiene; a failure here must not bring down the server.
 */
export async function runConsistencyProbeAtStartup(): Promise<void> {

  const issues = collectConsistencyIssues();

  if(issues.length === 0) {

    return;
  }

  for(const issue of issues) {

    const message = "Consistency probe (" + issue.category + ", " + issue.severity + "): " + issue.description;

    // Defensive: no current checker emits severity:"error" - all three checks return "warning" issues. The branch exists so a future check that needs operator
    // attention rather than auto-cleanup can mark itself error and surface accordingly. Pin via a future test seam if/when an error-severity check is added.
    if(issue.severity === "error") {

      LOG.error(message);
    } else {

      LOG.warn(message);
    }
  }

  // Run auto-fixes in parallel via Promise.allSettled - each fix is independent and a failure on one must not block the others. We iterate the eligible issues
  // and look up each settlement by index (length-paired by construction; Promise.allSettled preserves input order and arity), logging rejected reasons as
  // warnings while leaving successful fixes intact.
  const eligible = issues.filter((issue) => (issue.severity === "warning") && issue.autoFix);
  const results = await Promise.allSettled(eligible.map(async (issue) => issue.autoFix?.()));

  for(const [ index, issue ] of eligible.entries()) {

    const result = results[index];

    if(result?.status === "rejected") {

      LOG.warn("Consistency probe auto-fix failed for %s: %s.", issue.category, formatError(result.reason));
    }
  }

  // Defensive: paired with the per-issue severity dispatch above. No checker currently emits severity:"error", so this aggregate report is unreachable today;
  // it remains in place so a future error-severity check produces the operator-visible summary line without any further wiring.
  const errors = issues.filter((issue) => issue.severity === "error").length;

  if(errors > 0) {

    LOG.error("Consistency probe found %d error(s) requiring operator review.", errors);
  }
}
