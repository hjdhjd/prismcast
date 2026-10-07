/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consistencyProbe.ts: Cross-store consistency probe for PrismCast persistence.
 *
 * The probe runs once at startup after every initialize* function has loaded its store. It validates "foreign-key-style" constraints that span multiple stores
 * - things the per-store schema migrations cannot enforce because they only see one file at a time:
 *
 *   - Variant entries with a canonicalKey (in channels.json) reference a canonical that exists in PREDEFINED_CHANNELS or the user's stored channels.
 *   - User domain mappings (in profiles.json) reference profiles that exist as builtin or user-defined.
 *
 * The probe reports what an operator must act on and changes nothing: each issue is logged, because the right repair depends on what the operator intended. A
 * rule the running process can enforce on its own belongs where the process applies it - an unknown service tag never reaches the running filter because the
 * services module restricts the filter itself - rather than here.
 *
 * Adding a new check is a single function returning ConsistencyIssue[]; collectConsistencyIssues calls each registered check function and aggregates the
 * results into one array.
 */
import { getUserDomains, getUserProfiles } from "./userProfiles.ts";
import type { Channel } from "../types/index.ts";
import { LOG } from "../utils/index.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import { getBuiltinProfile } from "./sites.ts";
import { getStoredUserChannels } from "./userChannels.ts";

/**
 * A single consistency issue detected by the probe. Each carries enough metadata for the probe runner to log it uniformly.
 */
interface ConsistencyIssue {

  // Stable category identifier. Used for log grouping and future filtering.
  category: string;

  // Human-readable description of the inconsistency.
  description: string;

  // How loudly the runner logs the issue: a warning, or an error that needs the operator before the next restart.
  severity: "warning" | "error";
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
 * Validates that every user domain mapping references a profile that exists - either a builtin profile (getBuiltinProfile) or a user-defined one (getUserProfiles).
 */
function checkDomainProfiles(): ConsistencyIssue[] {

  const issues: ConsistencyIssue[] = [];
  const userDomains = getUserDomains();
  const userProfiles = getUserProfiles();

  for(const [ domain, config ] of Object.entries(userDomains)) {

    const profileKey = config.profile;

    if(!profileKey) {

      continue;
    }

    // A domain mapping may target either a builtin profile or a user-defined one - the save-path validator (validateDomain) accepts both - so we consult both
    // tables here. Checking only builtins would raise a false "missing profile" warning for a perfectly valid mapping onto a user-created profile.
    if(getBuiltinProfile(profileKey) || userProfiles[profileKey]) {

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
 * Aggregates issues from each registered check function. New checks are added here as an additional call in the same list.
 */
function collectConsistencyIssues(): ConsistencyIssue[] {

  return [

    ...checkVariantCanonicals(),
    ...checkDomainProfiles()
  ];
}

/**
 * Runs the consistency probe at startup. Logs every issue at its severity and changes nothing, so an operator sees in the log what needs their action before the
 * next restart. Errors do not block startup - a consistency error is recoverable runtime state, not an unbootable system.
 */
export async function runConsistencyProbeAtStartup(): Promise<void> {

  const issues = collectConsistencyIssues();

  if(issues.length === 0) {

    return;
  }

  for(const issue of issues) {

    const message = "Consistency probe (" + issue.category + ", " + issue.severity + "): " + issue.description;

    // Defensive: no current checker emits severity:"error" - every check returns "warning" issues. The branch exists so a future check whose issue needs the
    // operator before the next restart can mark itself error and surface accordingly. Add a test to assert this behavior once an error-severity check exists.
    if(issue.severity === "error") {

      LOG.error(message);
    } else {

      LOG.warn(message);
    }
  }

  // Defensive: paired with the per-issue severity dispatch above. No checker currently emits severity:"error", so this aggregate report is unreachable today;
  // it remains in place so a future error-severity check produces the operator-visible summary line without any further wiring.
  const errors = issues.filter((issue) => issue.severity === "error").length;

  if(errors > 0) {

    LOG.error("Consistency probe found %d error(s) requiring operator review.", errors);
  }
}
