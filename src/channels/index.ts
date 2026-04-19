/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Channel definitions for PrismCast.
 */
import type { Channel, ChannelDefinition, ChannelMap, ServiceVariant } from "../types/index.js";

// Channel identity fields — metadata that describes the channel itself, independent of which service serves it. This is the single source of truth for the
// identity/service-specific field separation used by: (1) applyVariantInheritance in services.ts (identity fields always come from the canonical),
// (2) startup migration in userChannels.ts (strips legacy identity fields from variant entries), and (3) DELTA_ALLOWED_FIELDS derivation (identity +
// service-specific editable fields). Adding a new identity field here automatically propagates to all three consumers.
export const CHANNEL_IDENTITY_FIELDS = new Set([ "channelNumber", "guideTitle", "hdhrEnabled", "logoUrl", "name", "stationId", "tags", "tvgShift" ] as const);

// Predefined tag vocabulary. These tags ship with PrismCast and are assigned to predefined channel definitions below. Users can delete predefined tags from
// their registry (they're tracked in channels.json deletedTags) and create their own tags. The runtime vocabulary is: (PREDEFINED_TAGS - deletedTags) + userTags.
export const PREDEFINED_TAGS: readonly string[] =
  [ "Documentary", "Entertainment", "HBO", "Kids", "Lifestyle", "Local", "Movies", "News", "Showtime", "Sports", "Starz" ];

// Site service key. When a channel has its own streaming website, this key is used in the services map. The site service always wins as canonical.
const SITE_KEY = "site";

/* Nested channel definitions. Each entry maps a channel key to a ChannelDefinition with identity fields (name, stationId) and a services map keyed by service
 * slug. The "site" key represents the channel's own streaming website. All other keys are multi-service platform slugs (cox, directv, hulu, sling, spectrum,
 * xfinity, yttv, foxone, paramountplus).
 *
 * At module load, the flattener compiles these nested definitions into the flat ChannelMap consumed by the rest of the codebase. Each variant entry gets
 * canonicalKey set to its parent definition's key, which buildServiceGroups in services.ts uses to assemble service groups.
 *
 * Canonical resolution rules:
 * 1. If "site" exists in services, the canonical always gets the site URL.
 * 2. Otherwise, the service whose key sorts first alphabetically (computed, not source-order) becomes canonical.
 *
 * Adding a new service never changes canonicals unless a "site" entry is introduced.
 *
 * Pacific timezone support:
 * - pacificStationId on an East ChannelDefinition triggers auto-generation of a Pacific sibling.
 * - Manual Pacific definitions (keys ending in "p") can pre-declare overrides (e.g., West-specific channelSelectors).
 * - Pacific generation merges inherited services from the East definition into the Pacific definition, skipping services whose channelSelector contains
 *   "East" or "West" and never overwriting existing Pacific services. See generatePacificDefinitions() for full rules.
 *
 * FAST channels: This list contains only traditional linear TV networks and public broadcasters - no FAST (Free Ad-Supported Streaming Television) channels.
 * FAST channels from platforms like Pluto TV or Tubi should not be added here. Users who want FAST content can add them as user-defined channels through the
 * web UI or user channels file, or preferably use dedicated high-quality integrations such as Plex Channels, Pluto for Channels, or Tubi for Channels.
 */
/* eslint-disable @hjdhjd/blank-line-after-open-brace, @stylistic/comma-dangle, sort-keys */
const BASE_CHANNEL_DEFINITIONS: Record<string, ChannelDefinition> = {

  abc: {
    name: "ABC",
    tags: ["Local"],
    services: {
      cox: { channelSelector: "ABC", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "ABC", url: "https://stream.directv.com" },
      hulu: { channelSelector: "ABC", url: "https://www.hulu.com/live" },
      site: { url: "https://abc.com/watch-live" },
      sling: { channelSelector: "ABC", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "ABC", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "ABC", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ABC", url: "https://tv.youtube.com/live" },
    },
  },

  abcnews: {
    name: "ABC News Live",
    tags: ["News"],
    stationId: "113380",
    services: {
      cox: { channelSelector: "ABCNL", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "ABC News Live", url: "https://stream.directv.com" },
      hulu: { channelSelector: "ABC News Live", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "ABC News Live", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "ABCNL", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ABC News Live", url: "https://tv.youtube.com/live" },
    },
  },

  ae: {
    name: "A&E",
    tags: [ "Documentary", "Entertainment" ],
    pacificStationId: "57439",
    stationId: "51529",
    services: {
      cox: { channelSelector: "A&E", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "A&E", url: "https://stream.directv.com" },
      hulu: { channelSelector: "A&E", url: "https://www.hulu.com/live" },
      site: { url: "https://play.aetv.com/live" },
      sling: { channelSelector: "A&E", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "A&E", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "A&E", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "A&E", url: "https://tv.youtube.com/live" },
    },
  },

  ahc: {
    name: "American Heroes",
    tags: ["Documentary"],
    stationId: "78808",
    services: {
      cox: { channelSelector: "American Heroes", url: "https://watchtv.cox.com/listings" },
      site: { url: "https://watch.foodnetwork.com/channel/ahc" },
      spectrum: { channelSelector: "American Heroes Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "American Heroes", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  amc: {
    name: "AMC",
    tags: [ "Entertainment", "Movies" ],
    pacificStationId: "78836",
    stationId: "59337",
    services: {
      cox: { channelSelector: "AMC", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "AMC", url: "https://stream.directv.com" },
      sling: { channelSelector: "AMC", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "AMC", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "AMC", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "AMC", url: "https://tv.youtube.com/live" },
    },
  },

  amcthrillers: {
    name: "AMC Thrillers",
    tags: [ "Entertainment", "Movies" ],
    stationId: "115678",
    services: {
      sling: { channelSelector: "AMC Thrillers", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      yttv: { channelSelector: "AMC Thrillers", url: "https://tv.youtube.com/live" },
    },
  },

  animal: {
    name: "Animal Planet",
    tags: ["Documentary"],
    pacificStationId: "68785",
    stationId: "57394",
    services: {
      cox: { channelSelector: "Animal Planet", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Animal Planet", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Animal Planet", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/animal-planet" },
      spectrum: { channelSelector: "Animal Planet", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Animal Planet", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Animal Planet", url: "https://tv.youtube.com/live" },
    },
  },

  axstv: {
    name: "AXS TV",
    tags: [ "Entertainment", "Sports" ],
    stationId: "28506",
    services: {
      cox: { channelSelector: "AXS TV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "AXS TV", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "AXS TV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "AXS TV", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  bbcamerica: {
    name: "BBC America",
    tags: ["Entertainment"],
    pacificStationId: "76739",
    stationId: "64492",
    services: {
      cox: { channelSelector: "BBC America", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "BBC America", url: "https://stream.directv.com" },
      sling: { channelSelector: "BBC America", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "BBC America", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "BBC America", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "BBC America", url: "https://tv.youtube.com/live" },
    },
  },

  bbcnews: {
    name: "BBC News (North America)",
    tags: ["News"],
    stationId: "101449",
    services: {
      cox: { channelSelector: "BBC News", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "BBC News", url: "https://stream.directv.com" },
      sling: { channelSelector: "BBC News", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "BBC World News", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "BBC News", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "BBC News", url: "https://tv.youtube.com/live" },
    },
  },

  bet: {
    name: "BET",
    tags: ["Entertainment"],
    pacificStationId: "64673",
    stationId: "63236",
    services: {
      cox: { channelSelector: "BET", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "BET", url: "https://stream.directv.com" },
      hulu: { channelSelector: "BET", url: "https://www.hulu.com/live" },
      site: { url: "https://www.bet.com/live-tv" },
      sling: { channelSelector: "BET", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "BET", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "BET", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "BET", url: "https://tv.youtube.com/live" },
    },
  },

  bether: {
    name: "BET Her",
    tags: ["Entertainment"],
    pacificStationId: "97360",
    stationId: "63220",
    services: {
      cox: { channelSelector: "BET Her", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "BET Her", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "BET Her", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "BET Her", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "BET Her", url: "https://tv.youtube.com/live" },
    },
  },

  bigten: {
    name: "Big 10",
    tags: ["Sports"],
    stationId: "58321",
    services: {
      cox: { channelSelector: "Big Ten Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Big Ten", url: "https://stream.directv.com" },
      foxone: { channelSelector: "BTN", url: "https://www.fox.com/live/channels" },
      hulu: { channelSelector: "Big Ten Network", url: "https://www.hulu.com/live" },
      site: { url: "https://www.foxsports.com/live/btn" },
      spectrum: { channelSelector: "Big Ten Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Big Ten Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "BTN", url: "https://tv.youtube.com/live" },
    },
  },

  bloomberg: {
    name: "Bloomberg Television",
    tags: ["News"],
    stationId: "71799",
    services: {
      cox: { channelSelector: "Bloomberg", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Bloomberg TV", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Bloomberg Television", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Bloomberg TV+", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Bloomberg TV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Bloomberg", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Bloomberg TV+", url: "https://tv.youtube.com/live" },
    },
  },

  bloombergoriginals: {
    name: "Bloomberg Originals",
    tags: ["News"],
    stationId: "175656",
    services: {
      yttv: { channelSelector: "Bloomberg Originals", url: "https://tv.youtube.com/live" },
    },
  },

  bravo: {
    name: "Bravo",
    tags: ["Entertainment"],
    stationId: "58625",
    services: {
      cox: { channelSelector: "Bravo", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Bravo", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Bravo", url: "https://www.hulu.com/live" },
      site: { url: "https://www.nbc.com/live?brand=bravo&callsign=bravo_east" },
      sling: { channelSelector: "Bravo", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Bravo", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Bravo", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Bravo", url: "https://tv.youtube.com/live" },
    },
  },

  bravop: {
    name: "Bravo (Pacific)",
    tags: ["Entertainment"],
    stationId: "73994",
    services: {
      site: { url: "https://www.nbc.com/live?brand=bravo&callsign=bravo_west" },
    },
  },

  cartoon: {
    name: "Cartoon Network",
    tags: ["Kids"],
    pacificStationId: "67703",
    stationId: "60048",
    services: {
      cox: { channelSelector: "Cartoon Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Cartoon Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Cartoon Network (East)", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "Cartoon Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Cartoon Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Cartoon Network", url: "https://tv.youtube.com/live" },
    },
  },

  cartoonp: {
    name: "Cartoon Network (Pacific)",
    tags: ["Kids"],
    stationId: "67703",
    services: {
      hulu: { channelSelector: "Cartoon Network (West)", url: "https://www.hulu.com/live" },
    },
  },

  cbs: {
    name: "CBS",
    tags: ["Local"],
    services: {
      cox: { channelSelector: "CBS", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "CBS", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CBS", url: "https://www.hulu.com/live" },
      paramountplus: { url: "https://www.paramountplus.com/live-tv/" },
      site: { url: "https://www.cbs.com/live-tv/stream" },
      spectrum: { channelSelector: "CBS", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CBS", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CBS", url: "https://tv.youtube.com/live" },
    },
  },

  cbsnews: {
    name: "CBS News 24/7",
    tags: ["News"],
    stationId: "104846",
    services: {
      hulu: { channelSelector: "CBS News 24/7", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "CBS News 24/7", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  cbssports: {
    name: "CBS Sports Network",
    tags: ["Sports"],
    stationId: "59250",
    services: {
      cox: { channelSelector: "CBS Sports Network", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "CBS Sports Network", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "CBS Sports Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CBS Sports Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CBS Sports Network", url: "https://tv.youtube.com/live" },
    },
  },

  cmt: {
    name: "CMT",
    tags: ["Entertainment"],
    pacificStationId: "64610",
    stationId: "59440",
    services: {
      cox: { channelSelector: "CMT", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "CMT", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CMT", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "CMT", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CMT", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CMT", url: "https://tv.youtube.com/live" },
    },
  },

  cnbc: {
    name: "CNBC",
    tags: ["News"],
    stationId: "58780",
    services: {
      cox: { channelSelector: "CNBC", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "CNBC", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CNBC", url: "https://www.hulu.com/live" },
      site: { url: "https://www.cnbc.com/live-tv" },
      spectrum: { channelSelector: "CNBC", url: "https://watch.spectrum.net/guide" },
      usa: { channelSelector: "CNBC_US", url: "https://www.usanetwork.com/live" },
      xfinity: { channelSelector: "CNBC", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CNBC", url: "https://tv.youtube.com/live" },
    },
  },

  cnbcworld: {
    name: "CNBC World",
    tags: ["News"],
    stationId: "26849",
    services: {
      directv: { channelSelector: "CNBC World", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "CNBC World", url: "https://watch.spectrum.net/guide" },
    },
  },

  cnn: {
    name: "CNN",
    tags: ["News"],
    stationId: "58646",
    services: {
      cox: { channelSelector: "CNN", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "CNN", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CNN", url: "https://www.hulu.com/live" },
      site: { url: "https://www.cnn.com/videos/cnn" },
      sling: { channelSelector: "CNN", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "CNN", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CNN", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CNN", url: "https://tv.youtube.com/live" },
    },
  },

  cnni: {
    name: "CNN International",
    tags: ["News"],
    stationId: "83110",
    services: {
      directv: { channelSelector: "CNNi HD East", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CNN International", url: "https://www.hulu.com/live" },
      site: { url: "https://www.cnn.com/videos/cnn-i" },
      yttv: { channelSelector: "CNN International", url: "https://tv.youtube.com/live" },
    },
  },

  comedycentral: {
    name: "Comedy Central",
    tags: ["Entertainment"],
    pacificStationId: "64599",
    stationId: "62420",
    services: {
      cox: { channelSelector: "Comedy Central", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Comedy Central", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Comedy Central", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Comedy Central", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Comedy Central", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Comedy Central", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Comedy Central", url: "https://tv.youtube.com/live" },
    },
  },

  cooking: {
    name: "Cooking",
    tags: ["Lifestyle"],
    stationId: "68065",
    services: {
      cox: { channelSelector: "Cooking Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Cooking Channel", url: "https://stream.directv.com" },
      site: { url: "https://watch.foodnetwork.com/channel/cooking-channel" },
      spectrum: { channelSelector: "Cooking Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Cooking Channel", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  cspan: {
    name: "C-SPAN",
    tags: ["News"],
    stationId: "68344",
    services: {
      cox: { channelSelector: "C-SPAN", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "C-SPAN", url: "https://stream.directv.com" },
      hulu: { channelSelector: "C-SPAN", url: "https://www.hulu.com/live" },
      site: { url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span" },
      spectrum: { channelSelector: "C-SPAN", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "C-SPAN", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "C-SPAN", url: "https://tv.youtube.com/live" },
    },
  },

  cspan2: {
    name: "C-SPAN 2",
    tags: ["News"],
    stationId: "68334",
    services: {
      cox: { channelSelector: "CPN2D", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "C-SPAN2", url: "https://stream.directv.com" },
      hulu: { channelSelector: "C-SPAN2", url: "https://www.hulu.com/live" },
      site: { url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span-2" },
      spectrum: { channelSelector: "C-SPAN 2", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CPN2D", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "C-SPAN2", url: "https://tv.youtube.com/live" },
    },
  },

  cspan3: {
    name: "C-SPAN 3",
    tags: ["News"],
    stationId: "68332",
    services: {
      cox: { channelSelector: "CPN3H", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "C-SPAN3", url: "https://www.hulu.com/live" },
      site: { url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span-3" },
      spectrum: { channelSelector: "C-SPAN 3", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CPN3H", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "C-SPAN3", url: "https://tv.youtube.com/live" },
    },
  },

  cw: {
    name: "CW",
    tags: ["Local"],
    services: {
      cox: { channelSelector: "CW TV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "CW", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CW", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "CW", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CW TV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CW", url: "https://tv.youtube.com/live" },
    },
  },

  destinationamerica: {
    name: "Destination America",
    tags: ["Documentary"],
    stationId: "60468",
    services: {
      cox: { channelSelector: "Destination America", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Destination America HD", url: "https://stream.directv.com" },
      site: { url: "https://watch.foodnetwork.com/channel/destination-america" },
      spectrum: { channelSelector: "Destination America", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Destination America", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  discovery: {
    name: "Discovery",
    tags: ["Documentary"],
    pacificStationId: "80399",
    stationId: "56905",
    services: {
      cox: { channelSelector: "Discovery Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Discovery", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Discovery", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/discovery" },
      sling: { channelSelector: "Discovery", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Discovery Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Discovery Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Discovery Channel", url: "https://tv.youtube.com/live" },
    },
  },

  discoverylife: {
    name: "Discovery Life",
    tags: ["Documentary"],
    stationId: "92204",
    services: {
      cox: { channelSelector: "Discovery Life", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Discovery Life", url: "https://stream.directv.com" },
      site: { url: "https://watch.foodnetwork.com/channel/discovery-life" },
      spectrum: { channelSelector: "Discovery Life", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Discovery Life", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  discoveryturbo: {
    name: "Discovery Turbo",
    tags: ["Documentary"],
    stationId: "31046",
    services: {
      cox: { channelSelector: "Discovery Turbo", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Discovery Turbo", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Discovery Turbo", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/motortrend" },
      sling: { channelSelector: "Discovery Turbo", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Discovery Turbo", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Discovery Turbo", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Discovery Turbo", url: "https://tv.youtube.com/live" },
    },
  },

  disney: {
    name: "Disney",
    tags: ["Kids"],
    pacificStationId: "63320",
    stationId: "59684",
    services: {
      cox: { channelSelector: "Disney Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Disney Channel", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Disney Channel", url: "https://www.hulu.com/live" },
      site: { url: "https://disneynow.com/watch-live?brand=004" },
      sling: { channelSelector: "Disney Channel", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Disney Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Disney Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Disney Channel", url: "https://tv.youtube.com/live" },
    },
  },

  disneyjr: {
    name: "Disney Jr.",
    tags: ["Kids"],
    pacificStationId: "75004",
    stationId: "74885",
    services: {
      cox: { channelSelector: "Disney Jr.", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Disney Junior", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Disney Junior", url: "https://www.hulu.com/live" },
      site: { url: "https://disneynow.com/watch-live?brand=008" },
      spectrum: { channelSelector: "Disney Junior", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Disney Jr.", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Disney Junior", url: "https://tv.youtube.com/live" },
    },
  },

  disneyxd: {
    name: "Disney XD",
    tags: ["Kids"],
    pacificStationId: "63322",
    stationId: "60006",
    services: {
      cox: { channelSelector: "Disney XD", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "Disney XD", url: "https://www.hulu.com/live" },
      site: { url: "https://disneynow.com/watch-live?brand=009" },
      spectrum: { channelSelector: "Disney XD", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Disney XD", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Disney XD", url: "https://tv.youtube.com/live" },
    },
  },

  e: {
    name: "E!",
    tags: ["Entertainment"],
    stationId: "61812",
    services: {
      cox: { channelSelector: "E!", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "E!", url: "https://stream.directv.com" },
      hulu: { channelSelector: "E!", url: "https://www.hulu.com/live" },
      site: { channelSelector: "E-_East", url: "https://www.usanetwork.com/live" },
      sling: { channelSelector: "E!", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "E!", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "E!", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "E!", url: "https://tv.youtube.com/live" },
    },
  },

  ep: {
    name: "E! (Pacific)",
    tags: ["Entertainment"],
    stationId: "91579",
    services: {
      site: { channelSelector: "E-_West", url: "https://www.usanetwork.com/live" },
    },
  },

  espn: {
    name: "ESPN",
    tags: ["Sports"],
    stationId: "32645",
    services: {
      cox: { channelSelector: "ESPN", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "ESPN", url: "https://stream.directv.com" },
      disneyplus: { channelSelector: "poster_linear_espn_none", url: "https://www.disneyplus.com/browse/live" },
      hulu: { channelSelector: "ESPN", url: "https://www.hulu.com/live" },
      site: { url: "https://www.espn.com/watch/player?network=espn" },
      sling: { channelSelector: "ESPN", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "ESPN", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "ESPN", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ESPN", url: "https://tv.youtube.com/live" },
    },
  },

  espn2: {
    name: "ESPN2",
    tags: ["Sports"],
    stationId: "45507",
    services: {
      cox: { channelSelector: "ES2HD", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "ESPN2", url: "https://stream.directv.com" },
      disneyplus: { channelSelector: "poster_linear_espn2_none", url: "https://www.disneyplus.com/browse/live" },
      hulu: { channelSelector: "ESPN2", url: "https://www.hulu.com/live" },
      site: { url: "https://www.espn.com/watch/player?network=espn2" },
      sling: { channelSelector: "ESPN2", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "ESPN2", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "ES2HD", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ESPN2", url: "https://tv.youtube.com/live" },
    },
  },

  espnacc: {
    name: "ACC Network",
    tags: ["Sports"],
    stationId: "111871",
    services: {
      cox: { channelSelector: "ACC Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "ACC Network", url: "https://stream.directv.com" },
      disneyplus: { channelSelector: "poster_linear_acc-network_none", url: "https://www.disneyplus.com/browse/live" },
      hulu: { channelSelector: "ACC Network", url: "https://www.hulu.com/live" },
      site: { url: "https://www.espn.com/watch/player?network=acc" },
      spectrum: { channelSelector: "ACC Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "ACC Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ACC Network", url: "https://tv.youtube.com/live" },
    },
  },

  espndeportes: {
    name: "ESPN Deportes",
    tags: ["Sports"],
    stationId: "71914",
    services: {
      cox: { channelSelector: "ESPN Deportes", url: "https://watchtv.cox.com/listings" },
      disneyplus: { channelSelector: "poster_linear_espn-deportes_none", url: "https://www.disneyplus.com/browse/live" },
      site: { url: "https://www.espn.com/watch/player?network=espndeportes" },
      spectrum: { channelSelector: "ESPN Deportes", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "ESPN Deportes", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ESPN Deportes", url: "https://tv.youtube.com/live" },
    },
  },

  espnews: {
    name: "ESPNews",
    tags: ["Sports"],
    stationId: "59976",
    services: {
      cox: { channelSelector: "ESWHD", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "ESPNews", url: "https://stream.directv.com" },
      disneyplus: { channelSelector: "poster_linear_espnews_none", url: "https://www.disneyplus.com/browse/live" },
      hulu: { channelSelector: "ESPNEWS", url: "https://www.hulu.com/live" },
      site: { url: "https://www.espn.com/watch/player?network=espnews" },
      spectrum: { channelSelector: "ESPNews", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "ESWHD", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ESPNEWS", url: "https://tv.youtube.com/live" },
    },
  },

  espnsec: {
    name: "SEC Network",
    tags: ["Sports"],
    stationId: "89714",
    services: {
      cox: { channelSelector: "SEC", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "SEC Network", url: "https://stream.directv.com" },
      disneyplus: { channelSelector: "poster_linear_sec-network_none", url: "https://www.disneyplus.com/browse/live" },
      hulu: { channelSelector: "SEC Network", url: "https://www.hulu.com/live" },
      site: { url: "https://www.espn.com/watch/player?network=sec" },
      spectrum: { channelSelector: "SEC Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "SEC", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "SEC Network", url: "https://tv.youtube.com/live" },
    },
  },

  espnu: {
    name: "ESPNU",
    tags: ["Sports"],
    stationId: "60696",
    services: {
      cox: { channelSelector: "ESPNU", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "ESPNU", url: "https://stream.directv.com" },
      disneyplus: { channelSelector: "poster_linear_espnu_none", url: "https://www.disneyplus.com/browse/live" },
      hulu: { channelSelector: "ESPNU", url: "https://www.hulu.com/live" },
      site: { url: "https://www.espn.com/watch/player?network=espnu" },
      spectrum: { channelSelector: "ESPNU", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "ESPNU", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ESPNU", url: "https://tv.youtube.com/live" },
    },
  },

  fbc: {
    name: "Fox Business",
    tags: ["News"],
    stationId: "58718",
    services: {
      cox: { channelSelector: "Fox Business", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Fox Business Network", url: "https://stream.directv.com" },
      foxone: { channelSelector: "FBN", url: "https://www.fox.com/live/channels" },
      hulu: { channelSelector: "Fox Business", url: "https://www.hulu.com/live" },
      site: { url: "https://www.foxbusiness.com/video/5640669329001" },
      spectrum: { channelSelector: "Fox Business Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Fox Business", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Fox Business", url: "https://tv.youtube.com/live" },
    },
  },

  fnc: {
    name: "Fox News",
    tags: ["News"],
    stationId: "60179",
    services: {
      cox: { channelSelector: "Fox News", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Fox News Channel", url: "https://stream.directv.com" },
      foxone: { channelSelector: "FNC", url: "https://www.fox.com/live/channels" },
      hulu: { channelSelector: "Fox News", url: "https://www.hulu.com/live" },
      site: { url: "https://www.foxnews.com/video/5614615980001" },
      sling: { channelSelector: "Fox News", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Fox News Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Fox News", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Fox News", url: "https://tv.youtube.com/live" },
    },
  },

  food: {
    name: "Food Network",
    tags: ["Lifestyle"],
    pacificStationId: "82119",
    stationId: "50747",
    services: {
      cox: { channelSelector: "Food Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Food Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Food Network", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/food-network" },
      sling: { channelSelector: "Food Network", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Food Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Food Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Food Network", url: "https://tv.youtube.com/live" },
    },
  },

  fox: {
    name: "Fox",
    tags: ["Local"],
    services: {
      cox: { channelSelector: "Fox", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FOX", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Fox", url: "https://www.hulu.com/live" },
      site: { channelSelector: "FOXD2C", url: "https://www.fox.com/live/channels" },
      sling: { channelSelector: "FOX", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "FOX", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Fox", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "FOX", url: "https://tv.youtube.com/live" },
    },
  },

  foxdeportes: {
    name: "Fox Deportes",
    tags: ["Sports"],
    stationId: "72189",
    services: {
      cox: { channelSelector: "Fox Deportes", url: "https://watchtv.cox.com/listings" },
      foxone: { channelSelector: "FOXD", url: "https://www.fox.com/live/channels" },
      site: { url: "https://www.foxsports.com/live/foxdep" },
      spectrum: { channelSelector: "FOX Deportes", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Fox Deportes", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Fox Deportes", url: "https://tv.youtube.com/live" },
    },
  },

  foxsoccerplus: {
    name: "Fox Soccer Plus",
    tags: ["Sports"],
    stationId: "66879",
    services: {
      site: { url: "https://www.foxsports.com/live/fsp" },
      yttv: { channelSelector: "FOX Soccer Plus", url: "https://tv.youtube.com/live" },
    },
  },

  france24: {
    name: "France 24",
    tags: ["News"],
    stationId: "60961",
    services: {
      site: { url: "https://www.france24.com/en/live" },
      sling: { channelSelector: "France 24 (English)", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  france24fr: {
    name: "France 24 (French)",
    tags: ["News"],
    stationId: "58685",
    services: {
      site: { url: "https://www.france24.com/fr/direct" },
      sling: { channelSelector: "France 24", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  freeform: {
    name: "Freeform",
    tags: ["Entertainment"],
    stationId: "59615",
    services: {
      cox: { channelSelector: "Freeform", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Freeform HD", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Freeform", url: "https://www.hulu.com/live" },
      site: { url: "https://www.freeform.com/watch-live/885c669e-fa9a-4039-b42e-6c85c90cc86d" },
      spectrum: { channelSelector: "Freeform", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Freeform", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Freeform", url: "https://tv.youtube.com/live" },
    },
  },

  freeformp: {
    name: "Freeform (Pacific)",
    tags: ["Entertainment"],
    stationId: "63324",
    services: {
      site: { url: "https://www.freeform.com/watch-live/3507c750-e86a-4c0f-8ff4-dd23c4859009" },
    },
  },

  fs1: {
    name: "FS1",
    tags: ["Sports"],
    stationId: "82547",
    services: {
      cox: { channelSelector: "FOX Sports 1", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FOX Sports 1", url: "https://stream.directv.com" },
      foxone: { channelSelector: "FS1", url: "https://www.fox.com/live/channels" },
      hulu: { channelSelector: "FS1", url: "https://www.hulu.com/live" },
      site: { url: "https://www.foxsports.com/live/fs1" },
      sling: { channelSelector: "FOX Sports 1", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "FS1", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "FOX Sports 1", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "FS1", url: "https://tv.youtube.com/live" },
    },
  },

  fs2: {
    name: "FS2",
    tags: ["Sports"],
    stationId: "59305",
    services: {
      cox: { channelSelector: "FOX Sports 2", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FOX Sports 2", url: "https://stream.directv.com" },
      foxone: { channelSelector: "FS2", url: "https://www.fox.com/live/channels" },
      hulu: { channelSelector: "FS2", url: "https://www.hulu.com/live" },
      site: { url: "https://www.foxsports.com/live/fs2" },
      spectrum: { channelSelector: "Fox Sports 2", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "FOX Sports 2", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "FS2", url: "https://tv.youtube.com/live" },
    },
  },

  fx: {
    name: "FX",
    tags: ["Entertainment"],
    stationId: "58574",
    services: {
      cox: { channelSelector: "FX", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FX", url: "https://stream.directv.com" },
      hulu: { channelSelector: "FX", url: "https://www.hulu.com/live" },
      site: { url: "https://abc.com/watch-live/93256af4-5e80-4558-aa2e-2bdfffa119a0" },
      sling: { channelSelector: "FX", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "FX", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "FX", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "FX", url: "https://tv.youtube.com/live" },
    },
  },

  fxm: {
    name: "FXM",
    tags: ["Movies"],
    pacificStationId: "98488",
    stationId: "70253",
    services: {
      cox: { channelSelector: "FXM", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FX Movie Channel", url: "https://stream.directv.com" },
      hulu: { channelSelector: "FXM", url: "https://www.hulu.com/live" },
      site: { url: "https://abc.com/watch-live/d298ab7e-c6b1-4efa-ac6e-a52dceed92ee" },
      spectrum: { channelSelector: "FXM", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "FXM", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "FXM", url: "https://tv.youtube.com/live" },
    },
  },

  fxp: {
    name: "FX (Pacific)",
    tags: ["Entertainment"],
    stationId: "59814",
    services: {
      site: { url: "https://abc.com/watch-live/2cee3401-f63b-42d0-b32e-962fef610b9e" },
    },
  },

  fxx: {
    name: "FXX",
    tags: ["Entertainment"],
    stationId: "66379",
    services: {
      cox: { channelSelector: "FXX", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FXX", url: "https://stream.directv.com" },
      hulu: { channelSelector: "FXX", url: "https://www.hulu.com/live" },
      site: { url: "https://abc.com/watch-live/49f4a471-8d36-4728-8457-ea65cbbc84ea" },
      spectrum: { channelSelector: "FXX", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "FXX", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "FXX", url: "https://tv.youtube.com/live" },
    },
  },

  fxxp: {
    name: "FXX (Pacific)",
    tags: ["Entertainment"],
    stationId: "82571",
    services: {
      site: { url: "https://abc.com/watch-live/e4c83395-62ed-4a49-829a-c55ab3c33e7d" },
    },
  },

  fyi: {
    name: "FYI",
    tags: ["Entertainment"],
    pacificStationId: "92372",
    stationId: "58988",
    services: {
      cox: { channelSelector: "FYI", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FYI", url: "https://stream.directv.com" },
      hulu: { channelSelector: "FYI", url: "https://www.hulu.com/live" },
      site: { url: "https://play.fyi.tv/live" },
      spectrum: { channelSelector: "FYI", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "FYI", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  gameshow: {
    name: "Game Show Network",
    tags: ["Entertainment"],
    pacificStationId: "90210",
    stationId: "68827",
    services: {
      cox: { channelSelector: "Game Show Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "GSN HD", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Game Show Network", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "Game Show Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Game Show Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Game Show Network", url: "https://tv.youtube.com/live" },
    },
  },

  golf: {
    name: "Golf",
    tags: ["Sports"],
    stationId: "61854",
    services: {
      cox: { channelSelector: "Golf Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Golf Channel", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Golf Channel", url: "https://www.hulu.com/live" },
      site: { url: "https://www.golfchannel.com/watch/live" },
      spectrum: { channelSelector: "Golf Channel", url: "https://watch.spectrum.net/guide" },
      usa: { channelSelector: "gc", url: "https://www.usanetwork.com/live" },
      xfinity: { channelSelector: "Golf Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Golf Channel", url: "https://tv.youtube.com/live" },
    },
  },

  hallmark: {
    name: "Hallmark",
    tags: ["Entertainment"],
    pacificStationId: "66415",
    stationId: "66268",
    services: {
      cox: { channelSelector: "Hallmark Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Hallmark Channel", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Hallmark Channel", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "Hallmark Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Hallmark Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Hallmark Channel", url: "https://tv.youtube.com/live" },
    },
  },

  hallmarkfamily: {
    name: "Hallmark Family",
    tags: ["Entertainment"],
    stationId: "105723",
    services: {
      cox: { channelSelector: "HFM", url: "https://watchtv.cox.com/listings" },
      spectrum: { channelSelector: "Hallmark Family", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "HFM", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Hallmark Family", url: "https://tv.youtube.com/live" },
    },
  },

  hallmarkmystery: {
    name: "Hallmark Mystery",
    tags: ["Entertainment"],
    pacificStationId: "66412",
    stationId: "46710",
    services: {
      cox: { channelSelector: "HMYS", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "Hallmark Mystery", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "Hallmark Mystery", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "HMYS", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Hallmark Mystery", url: "https://tv.youtube.com/live" },
    },
  },

  hbo: {
    name: "HBO",
    tags: [ "HBO", "Movies" ],
    stationId: "19548",
    services: {
      cox: { channelSelector: "HBO", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBO", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO East", url: "https://tv.youtube.com/live" },
    },
  },

  hbocomedy: {
    name: "HBO Comedy",
    tags: [ "HBO", "Movies" ],
    stationId: "59839",
    services: {
      cox: { channelSelector: "HBOCH", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Comedy", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBOCH", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Comedy East", url: "https://tv.youtube.com/live" },
    },
  },

  hbodrama: {
    name: "HBO Drama",
    tags: [ "HBO", "Movies" ],
    stationId: "59363",
    services: {
      cox: { channelSelector: "HBOSH", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Drama", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBOSH", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Drama East", url: "https://tv.youtube.com/live" },
    },
  },

  hbohits: {
    name: "HBO Hits",
    tags: [ "HBO", "Movies" ],
    stationId: "59368",
    services: {
      cox: { channelSelector: "HBO2H", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Hits", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBO2H", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Hits East", url: "https://tv.youtube.com/live" },
    },
  },

  hbomovies: {
    name: "HBO Movies",
    tags: [ "HBO", "Movies" ],
    stationId: "59845",
    services: {
      cox: { channelSelector: "HBOZH", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Movies", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBOZH", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Movies East", url: "https://tv.youtube.com/live" },
    },
  },

  hgtv: {
    name: "HGTV",
    tags: ["Lifestyle"],
    pacificStationId: "87317",
    stationId: "49788",
    services: {
      cox: { channelSelector: "HGTV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "HGTV", url: "https://stream.directv.com" },
      hulu: { channelSelector: "HGTV", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/hgtv" },
      sling: { channelSelector: "HGTV", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "HGTV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "HGTV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HGTV", url: "https://tv.youtube.com/live" },
    },
  },

  history: {
    name: "History",
    tags: ["Documentary"],
    pacificStationId: "88545",
    stationId: "57708",
    services: {
      cox: { channelSelector: "History", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "HISTORY", url: "https://stream.directv.com" },
      hulu: { channelSelector: "The HISTORY Channel", url: "https://www.hulu.com/live" },
      site: { url: "https://play.history.com/live" },
      sling: { channelSelector: "History", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "History", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "History", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  hln: {
    name: "HLN",
    tags: ["News"],
    stationId: "64549",
    services: {
      cox: { channelSelector: "HLN", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "HLN", url: "https://stream.directv.com" },
      hulu: { channelSelector: "HLN", url: "https://www.hulu.com/live" },
      site: { url: "https://www.cnn.com/videos/hln" },
      sling: { channelSelector: "HLN", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "HLN", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "HLN", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HLN", url: "https://tv.youtube.com/live" },
    },
  },

  id: {
    name: "Investigation Discovery",
    tags: ["Documentary"],
    pacificStationId: "80309",
    stationId: "65342",
    services: {
      cox: { channelSelector: "Investigation Discovery", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Investigation Discovery", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Investigation Discovery", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/investigation-discovery" },
      sling: { channelSelector: "Investigation Discovery", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Investigation Discovery", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Investigation Discovery", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "ID", url: "https://tv.youtube.com/live" },
    },
  },

  ifc: {
    name: "IFC",
    tags: [ "Entertainment", "Movies" ],
    pacificStationId: "109735",
    stationId: "59444",
    services: {
      cox: { channelSelector: "IFC", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "IFC", url: "https://stream.directv.com" },
      sling: { channelSelector: "IFC", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "IFC", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "IFC", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "IFC", url: "https://tv.youtube.com/live" },
    },
  },

  indieplex: {
    name: "IndiePlex",
    tags: [ "Movies", "Starz" ],
    stationId: "65795",
    services: {
      hulu: { channelSelector: "IndiePlex (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "IndiePlex", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "IndiePlex", url: "https://watch.spectrum.net/guide" },
    },
  },

  indieplexp: {
    name: "IndiePlex (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "65796",
    services: {
      hulu: { channelSelector: "IndiePlex (West)", url: "https://www.hulu.com/live" },
    },
  },

  lifetime: {
    name: "Lifetime",
    tags: ["Entertainment"],
    pacificStationId: "60250",
    stationId: "60150",
    services: {
      cox: { channelSelector: "Lifetime", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Lifetime", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Lifetime", url: "https://www.hulu.com/live" },
      site: { url: "https://play.mylifetime.com/live" },
      sling: { channelSelector: "Lifetime", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Lifetime", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Lifetime", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  lmn: {
    name: "Lifetime Movie Network",
    tags: [ "Entertainment", "Movies" ],
    pacificStationId: "92373",
    stationId: "55887",
    services: {
      cox: { channelSelector: "Lifetime Movies", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Lifetime Movie Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "LMN", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "LMN", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Lifetime Movies", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  magnolia: {
    name: "Magnolia Network",
    tags: ["Lifestyle"],
    pacificStationId: "122081",
    stationId: "67375",
    services: {
      cox: { channelSelector: "Magnolia Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Magnolia Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Magnolia Network", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/magnolia-network-preview-atve-us" },
      spectrum: { channelSelector: "Magnolia Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Magnolia Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Magnolia Network", url: "https://tv.youtube.com/live" },
    },
  },

  mgmplus: {
    name: "MGM+",
    tags: ["Movies"],
    pacificStationId: "95927",
    stationId: "65687",
    services: {
      cox: { channelSelector: "MGM+", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MGM+", url: "https://stream.directv.com" },
      xfinity: { channelSelector: "MGM+", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  mlb: {
    name: "MLB Network",
    tags: ["Sports"],
    stationId: "62081",
    services: {
      cox: { channelSelector: "MLB Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MLB Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "MLB Network", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "MLB Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "MLB Network", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  movieplex: {
    name: "MoviePlex",
    tags: [ "Movies", "Starz" ],
    stationId: "83075",
    services: {
      hulu: { channelSelector: "MoviePlex (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "MoviePlex", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Movieplex", url: "https://watch.spectrum.net/guide" },
    },
  },

  movieplexp: {
    name: "MoviePlex (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "105963",
    services: {
      hulu: { channelSelector: "MoviePlex (West)", url: "https://www.hulu.com/live" },
    },
  },

  msg: {
    name: "MSG",
    tags: ["Sports"],
    stationId: "35402",
    services: {
      directv: { channelSelector: "MSG", url: "https://stream.directv.com" },
    },
  },

  msgsn: {
    name: "MSG Sportsnet",
    tags: ["Sports"],
    stationId: "35383",
    services: {
      directv: { channelSelector: "MSG Sportsnet HD 635", url: "https://stream.directv.com" },
    },
  },

  msnow: {
    name: "MS NOW",
    tags: ["News"],
    stationId: "64241",
    services: {
      cox: { channelSelector: "MS NOW", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MS Now", url: "https://stream.directv.com" },
      hulu: { channelSelector: "MS NOW", url: "https://www.hulu.com/live" },
      site: { url: "https://www.ms.now/live" },
      sling: { channelSelector: "MS NOW", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "MS NOW", url: "https://watch.spectrum.net/guide" },
      usa: { channelSelector: "image-23", url: "https://www.usanetwork.com/live" },
      xfinity: { channelSelector: "MS NOW", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "MS NOW", url: "https://tv.youtube.com/live" },
    },
  },

  mtv: {
    name: "MTV",
    tags: ["Entertainment"],
    pacificStationId: "64630",
    stationId: "60964",
    services: {
      cox: { channelSelector: "MTV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MTV", url: "https://stream.directv.com" },
      hulu: { channelSelector: "MTV", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "MTV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "MTV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "MTV", url: "https://tv.youtube.com/live" },
    },
  },

  mtv2: {
    name: "MTV2",
    tags: ["Entertainment"],
    pacificStationId: "75506",
    stationId: "75077",
    services: {
      cox: { channelSelector: "MTV2", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MTV2", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "MTV2", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "MTV2", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  mtvclassic: {
    name: "MTV Classic",
    tags: ["Entertainment"],
    stationId: "92240",
    services: {
      cox: { channelSelector: "MTV Classic", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MTV Classic", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "MTV Classic", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "MTV Classic", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  natgeo: {
    name: "National Geographic",
    tags: ["Documentary"],
    stationId: "49438",
    services: {
      cox: { channelSelector: "Nat Geo", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "National Geographic Channel", url: "https://stream.directv.com" },
      hulu: { channelSelector: "National Geographic", url: "https://www.hulu.com/live" },
      site: { url: "https://www.nationalgeographic.com/tv/watch-live/0826a9a3-3384-4bb5-8841-91f01cb0e3a7" },
      sling: { channelSelector: "National Geographic", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "National Geographic", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Nat Geo", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Nat Geo", url: "https://tv.youtube.com/live" },
    },
  },

  natgeop: {
    name: "National Geographic (Pacific)",
    tags: ["Documentary"],
    stationId: "71601",
    services: {
      site: { url: "https://www.nationalgeographic.com/tv/watch-live/91456580-f32f-417c-8e1a-9f82640832a7" },
    },
  },

  natgeowild: {
    name: "Nat Geo Wild",
    tags: ["Documentary"],
    stationId: "67331",
    services: {
      cox: { channelSelector: "National Geographic WILD", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Nat Geo WILD", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Nat Geo WILD", url: "https://www.hulu.com/live" },
      site: { url: "https://www.nationalgeographic.com/tv/watch-live/239b9590-583f-4955-a499-22e9eefff9cf" },
      spectrum: { channelSelector: "NatGeo Wild", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "National Geographic WILD", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Nat Geo WILD", url: "https://tv.youtube.com/live" },
    },
  },

  nba: {
    name: "NBA TV",
    tags: ["Sports"],
    stationId: "45526",
    services: {
      cox: { channelSelector: "NBA TV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "NBA TV", url: "https://stream.directv.com" },
      site: { url: "https://www.nba.com/watch/nba-tv" },
      spectrum: { channelSelector: "NBA TV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "NBA TV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "NBA TV", url: "https://tv.youtube.com/live" },
    },
  },

  nbc: {
    name: "NBC",
    tags: ["Local"],
    services: {
      cox: { channelSelector: "NBC", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "NBC", url: "https://stream.directv.com" },
      hulu: { channelSelector: "NBC", url: "https://www.hulu.com/live" },
      site: { url: "https://www.nbc.com/live?brand=nbc&callsign=nbc" },
      sling: { channelSelector: "NBC", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "NBC", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "NBC", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "NBC", url: "https://tv.youtube.com/live" },
    },
  },

  nbcnews: {
    name: "NBC News Now",
    tags: ["News"],
    stationId: "114174",
    services: {
      cox: { channelSelector: "NBC News NOW", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "NBC News Now", url: "https://stream.directv.com" },
      hulu: { channelSelector: "NBC News NOW", url: "https://www.hulu.com/live" },
      site: { url: "https://www.nbc.com/live?brand=nbc-news&callsign=nbcnews" },
      sling: { channelSelector: "NBC News Now", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "NBC News NOW", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "NBC News NOW", url: "https://tv.youtube.com/live" },
    },
  },

  nbcsbayarea: {
    name: "NBC Sports Bay Area",
    tags: ["Sports"],
    stationId: "63138",
    services: {
      site: { url: "https://www.nbc.com/live?brand=rsn-bay-area&callsign=nbcsbayarea" },
    },
  },

  nbcsboston: {
    name: "NBC Sports Boston",
    tags: ["Sports"],
    stationId: "49198",
    services: {
      site: { url: "https://www.nbc.com/live?brand=rsn-boston&callsign=nbcsboston" },
    },
  },

  nbcscalifornia: {
    name: "NBC Sports California",
    tags: ["Sports"],
    stationId: "45540",
    services: {
      site: { url: "https://www.nbc.com/live?brand=rsn-california&callsign=nbcscalifornia" },
    },
  },

  nbcsn: {
    name: "NBC Sports Network",
    tags: ["Sports"],
    stationId: "194412",
    services: {
      yttv: { channelSelector: "NBC Sports Network", url: "https://tv.youtube.com/live" },
    },
  },

  nbcsphiladelphia: {
    name: "NBC Sports Philadelphia",
    tags: ["Sports"],
    stationId: "32571",
    services: {
      site: { url: "https://www.nbc.com/live?brand=rsn-philadelphia&callsign=nbcsphiladelphia" },
    },
  },

  necn: {
    name: "NECN",
    tags: ["News"],
    stationId: "66278",
    services: {
      site: { url: "https://www.nbc.com/live?brand=necn&callsign=necn" },
    },
  },

  nfl: {
    name: "NFL Network",
    tags: ["Sports"],
    stationId: "45399",
    services: {
      cox: { channelSelector: "NFL Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "NFL Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "NFL Network", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "NFL Network", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "NFL Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "NFL Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "NFL Network", url: "https://tv.youtube.com/live" },
    },
  },

  nhl: {
    name: "NHL Network",
    tags: ["Sports"],
    stationId: "58690",
    services: {
      cox: { channelSelector: "NHL Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "NHL Network HD", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "NHL Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "NHL Network", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  own: {
    name: "OWN",
    tags: ["Lifestyle"],
    stationId: "70388",
    services: {
      cox: { channelSelector: "OWN", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "OWN", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Oprah Winfrey Network", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/own" },
      spectrum: { channelSelector: "OWN", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "OWN", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "OWN", url: "https://tv.youtube.com/live" },
    },
  },

  oxygen: {
    name: "Oxygen",
    tags: ["Entertainment"],
    stationId: "70522",
    services: {
      cox: { channelSelector: "Oxygen True Crime", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Oxygen True Crime", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Oxygen True Crime", url: "https://www.hulu.com/live" },
      site: { channelSelector: "Oxygen_East", url: "https://www.usanetwork.com/live" },
      spectrum: { channelSelector: "Oxygen", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Oxygen True Crime", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Oxygen True Crime", url: "https://tv.youtube.com/live" },
    },
  },

  oxygenp: {
    name: "Oxygen (Pacific)",
    tags: ["Entertainment"],
    stationId: "74032",
    services: {
      site: { channelSelector: "Oxygen_West", url: "https://www.usanetwork.com/live" },
    },
  },

  paramount: {
    name: "Paramount Network",
    tags: ["Entertainment"],
    stationId: "59186",
    services: {
      cox: { channelSelector: "Paramount Network", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "Paramount Network", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "Paramount Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Paramount Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Paramount", url: "https://tv.youtube.com/live" },
    },
  },

  paramountp: {
    name: "Paramount (Pacific)",
    tags: ["Entertainment"],
    stationId: "64593",
    services: {
      yttv: { channelSelector: "Paramount Network", url: "https://tv.youtube.com/live" },
    },
  },

  pbs: {
    name: "PBS",
    tags: ["Local"],
    services: {
      cox: { channelSelector: "PBS", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "PBS", url: "https://stream.directv.com" },
      hulu: { channelSelector: "PBS", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "PBS", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "PBS", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "PBS", url: "https://tv.youtube.com/live" },
    },
  },

  pbschicago: {
    name: "PBS Chicago (WTTW)",
    tags: ["Local"],
    stationId: "30415",
    services: {
      hulu: { channelSelector: "PBS", url: "https://www.hulu.com/live" },
      site: { url: "https://www.wttw.com/wttw-live-stream" },
    },
  },

  pbslakeshore: {
    name: "PBS Lakeshore (WYIN)",
    tags: ["Local"],
    stationId: "49237",
    services: {
      hulu: { channelSelector: "Lakeshore PBS", url: "https://www.hulu.com/live" },
      site: { url: "https://video.lakeshorepbs.org/livestream" },
    },
  },

  retroplex: {
    name: "RetroPlex",
    tags: [ "Movies", "Starz" ],
    stationId: "65791",
    services: {
      hulu: { channelSelector: "RetroPlex (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "RetroPlex", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "RetroPlex", url: "https://watch.spectrum.net/guide" },
    },
  },

  retroplexp: {
    name: "RetroPlex (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "65793",
    services: {
      hulu: { channelSelector: "RetroPlex (West)", url: "https://www.hulu.com/live" },
    },
  },

  science: {
    name: "Science",
    tags: ["Documentary"],
    stationId: "57390",
    services: {
      cox: { channelSelector: "Science Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Science", url: "https://stream.directv.com" },
      site: { url: "https://watch.foodnetwork.com/channel/science" },
      spectrum: { channelSelector: "Science", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Science Channel", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  showtime: {
    name: "Showtime",
    tags: [ "Movies", "Showtime" ],
    stationId: "91620",
    services: {
      cox: { channelSelector: "PSHOh", url: "https://watchtv.cox.com/listings" },
      paramountplus: { url: "https://www.paramountplus.com/live-tv/stream/showtime-east" },
      xfinity: { channelSelector: "PSHOh", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Showtime East", url: "https://tv.youtube.com/live" },
    },
  },

  showtimep: {
    name: "Showtime (Pacific)",
    tags: [ "Movies", "Showtime" ],
    stationId: "91621",
    services: {
      paramountplus: { url: "https://www.paramountplus.com/live-tv/stream/showtime-west" },
    },
  },

  smithsonian: {
    name: "Smithsonian Channel",
    tags: ["Documentary"],
    pacificStationId: "82695",
    stationId: "58532",
    services: {
      cox: { channelSelector: "Smithsonian", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Smithsonian Channel HD", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Smithsonian Channel", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "Smithsonian Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Smithsonian", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Smithsonian Channel", url: "https://tv.youtube.com/live" },
    },
  },

  sny: {
    name: "SportsNet New York",
    tags: ["Sports"],
    stationId: "50038",
    services: {
      directv: { channelSelector: "SportsNet New York HD 639", url: "https://stream.directv.com" },
    },
  },

  starz: {
    name: "Starz",
    tags: [ "Movies", "Starz" ],
    stationId: "34941",
    services: {
      cox: { channelSelector: "STARZ", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ (East)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.starz.com/us/en/play/2" },
      sling: { channelSelector: "STARZ", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STARZ", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzcinema: {
    name: "Starz Cinema",
    tags: [ "Movies", "Starz" ],
    stationId: "67236",
    services: {
      hulu: { channelSelector: "STARZ Cinema (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Cinema", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzcinemap: {
    name: "Starz Cinema (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "67365",
    services: {
      hulu: { channelSelector: "STARZ Cinema (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzcomedy: {
    name: "Starz Comedy",
    tags: [ "Movies", "Starz" ],
    stationId: "57569",
    services: {
      hulu: { channelSelector: "STARZ Comedy (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Comedy", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzcomedyp: {
    name: "Starz Comedy (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "57575",
    services: {
      hulu: { channelSelector: "STARZ Comedy (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzedge: {
    name: "Starz Edge",
    tags: [ "Movies", "Starz" ],
    stationId: "57573",
    services: {
      hulu: { channelSelector: "STARZ Edge (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Edge", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzedgep: {
    name: "Starz Edge (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "57578",
    services: {
      hulu: { channelSelector: "STARZ Edge (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencore: {
    name: "Starz Encore",
    tags: [ "Movies", "Starz" ],
    stationId: "36225",
    services: {
      cox: { channelSelector: "STZEH", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ Encore (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STZEH", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzencoreaction: {
    name: "Starz Encore Action",
    tags: [ "Movies", "Starz" ],
    stationId: "72015",
    services: {
      cox: { channelSelector: "STZAH", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ Encore Action (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Action", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STZAH", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzencoreactionp: {
    name: "Starz Encore Action (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "103833",
    services: {
      hulu: { channelSelector: "STARZ Encore Action (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencoreblack: {
    name: "Starz Encore Black",
    tags: [ "Movies", "Starz" ],
    stationId: "72014",
    services: {
      cox: { channelSelector: "STZBH", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ Encore Black (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Black", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STZBH", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzencoreblackp: {
    name: "Starz Encore Black (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "103834",
    services: {
      hulu: { channelSelector: "STARZ Encore Black (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencoreclassic: {
    name: "Starz Encore Classic",
    tags: [ "Movies", "Starz" ],
    stationId: "83404",
    services: {
      hulu: { channelSelector: "STARZ Encore Classic (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Classic", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoreclassicp: {
    name: "Starz Encore Classic (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "97233",
    services: {
      hulu: { channelSelector: "STARZ Encore Classic (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencoreespanol: {
    name: "Starz Encore Español",
    tags: [ "Movies", "Starz" ],
    stationId: "72016",
    services: {
      hulu: { channelSelector: "STARZ Encore Español (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Español", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoreespanolp: {
    name: "Starz Encore Español (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "104730",
    services: {
      hulu: { channelSelector: "STARZ Encore Español (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencorefamily: {
    name: "Starz Encore Family",
    tags: [ "Kids", "Movies", "Starz" ],
    stationId: "14886",
    services: {
      hulu: { channelSelector: "STARZ Encore Family (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Family", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencorefamilyp: {
    name: "Starz Encore Family (Pacific)",
    tags: [ "Kids", "Movies", "Starz" ],
    stationId: "103829",
    services: {
      hulu: { channelSelector: "STARZ Encore Family (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencorep: {
    name: "Starz Encore (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "67237",
    services: {
      hulu: { channelSelector: "STARZ Encore (West)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore West", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoresuspense: {
    name: "Starz Encore Suspense",
    tags: [ "Movies", "Starz" ],
    stationId: "83076",
    services: {
      hulu: { channelSelector: "STARZ Encore Suspense (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Suspense", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoresuspensep: {
    name: "Starz Encore Suspense (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "103836",
    services: {
      hulu: { channelSelector: "STARZ Encore Suspense (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencorewesterns: {
    name: "Starz Encore Westerns",
    tags: [ "Movies", "Starz" ],
    stationId: "14765",
    services: {
      hulu: { channelSelector: "STARZ Encore Westerns (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Westerns", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencorewesternsp: {
    name: "Starz Encore Westerns (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "103856",
    services: {
      hulu: { channelSelector: "STARZ Encore Westerns (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzinblack: {
    name: "Starz in Black",
    tags: [ "Movies", "Starz" ],
    stationId: "67235",
    services: {
      hulu: { channelSelector: "STARZ in Black (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz in Black", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzinblackp: {
    name: "Starz in Black (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "67367",
    services: {
      hulu: { channelSelector: "STARZ in Black (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzkids: {
    name: "Starz Kids",
    tags: [ "Kids", "Starz" ],
    stationId: "57581",
    services: {
      hulu: { channelSelector: "STARZ Kids (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Kids", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzkidsp: {
    name: "Starz Kids (Pacific)",
    tags: [ "Kids", "Starz" ],
    stationId: "57583",
    services: {
      hulu: { channelSelector: "STARZ Kids (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzp: {
    name: "Starz (Pacific)",
    tags: [ "Movies", "Starz" ],
    stationId: "34949",
    services: {
      hulu: { channelSelector: "STARZ (West)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz West", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  sundancetv: {
    name: "SundanceTV",
    tags: ["Movies"],
    pacificStationId: "78806",
    stationId: "71280",
    services: {
      cox: { channelSelector: "SundanceTV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Sundance TV", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "SundanceTV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "SundanceTV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "SundanceTV", url: "https://tv.youtube.com/live" },
    },
  },

  syfy: {
    name: "Syfy",
    tags: ["Entertainment"],
    stationId: "58623",
    services: {
      cox: { channelSelector: "SYFY", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Syfy", url: "https://stream.directv.com" },
      hulu: { channelSelector: "SYFY", url: "https://www.hulu.com/live" },
      site: { channelSelector: "Syfy_East", url: "https://www.usanetwork.com/live" },
      sling: { channelSelector: "SYFY", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Syfy", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "SYFY", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "SYFY", url: "https://tv.youtube.com/live" },
    },
  },

  syfyp: {
    name: "Syfy (Pacific)",
    tags: ["Entertainment"],
    stationId: "65626",
    services: {
      site: { channelSelector: "Syfy_West", url: "https://www.usanetwork.com/live" },
    },
  },

  tbs: {
    name: "TBS",
    tags: ["Entertainment"],
    stationId: "58515",
    services: {
      cox: { channelSelector: "TBS", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "TBS", url: "https://stream.directv.com" },
      hulu: { channelSelector: "TBS (East)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.tbs.com/watchtbs/east" },
      sling: { channelSelector: "TBS", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "TBS", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "TBS", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "TBS", url: "https://tv.youtube.com/live" },
    },
  },

  tbsp: {
    name: "TBS (Pacific)",
    tags: ["Entertainment"],
    stationId: "67890",
    services: {
      hulu: { channelSelector: "TBS (West)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.tbs.com/watchtbs/west" },
    },
  },

  tcm: {
    name: "TCM",
    tags: ["Movies"],
    stationId: "64312",
    services: {
      cox: { channelSelector: "TCM", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "TCM", url: "https://stream.directv.com" },
      hulu: { channelSelector: "TCM (East)", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "TCM", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "TCM", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Turner Classic Movies", url: "https://tv.youtube.com/live" },
    },
  },

  tcmp: {
    name: "TCM (Pacific)",
    tags: ["Movies"],
    stationId: "64312",
    tvgShift: 3,
    services: {
      hulu: { channelSelector: "TCM (West)", url: "https://www.hulu.com/live" },
    },
  },

  tennis: {
    name: "Tennis Channel",
    tags: ["Sports"],
    stationId: "60316",
    services: {
      cox: { channelSelector: "Tennis Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Tennis Channel HD", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "Tennis Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Tennis Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Tennis Channel", url: "https://tv.youtube.com/live" },
    },
  },

  tennis2: {
    name: "Tennis Channel 2",
    tags: ["Sports"],
    stationId: "137752",
    services: {
      cox: { channelSelector: "T2", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "Tennis Channel 2", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "T2", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "T2", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "T2", url: "https://tv.youtube.com/live" },
    },
  },

  tlc: {
    name: "TLC",
    tags: ["Lifestyle"],
    pacificStationId: "79911",
    stationId: "57391",
    services: {
      cox: { channelSelector: "TLC", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "TLC", url: "https://stream.directv.com" },
      hulu: { channelSelector: "TLC", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/tlc" },
      sling: { channelSelector: "TLC", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "TLC", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "TLC", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "TLC", url: "https://tv.youtube.com/live" },
    },
  },

  tnt: {
    name: "TNT",
    tags: ["Entertainment"],
    stationId: "42642",
    services: {
      cox: { channelSelector: "TNT", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "TNT", url: "https://stream.directv.com" },
      hulu: { channelSelector: "TNT (East)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.tntdrama.com/watchtnt/east" },
      sling: { channelSelector: "TNT", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "TNT", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "TNT", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "TNT", url: "https://tv.youtube.com/live" },
    },
  },

  tntp: {
    name: "TNT (Pacific)",
    tags: ["Entertainment"],
    stationId: "61340",
    services: {
      hulu: { channelSelector: "TNT (West)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.tntdrama.com/watchtnt/west" },
    },
  },

  travel: {
    name: "Travel",
    tags: ["Lifestyle"],
    pacificStationId: "64525",
    stationId: "59303",
    services: {
      cox: { channelSelector: "Travel Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Travel Channel", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Travel Channel", url: "https://www.hulu.com/live" },
      site: { url: "https://watch.foodnetwork.com/channel/travel-channel" },
      sling: { channelSelector: "Travel Channel", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Travel Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Travel Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Travel Channel", url: "https://tv.youtube.com/live" },
    },
  },

  trutv: {
    name: "truTV",
    tags: ["Entertainment"],
    pacificStationId: "65717",
    stationId: "64490",
    services: {
      cox: { channelSelector: "truTV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "truTV", url: "https://stream.directv.com" },
      hulu: { channelSelector: "truTV (East)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.trutv.com/watchtrutv/east" },
      sling: { channelSelector: "truTV", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "truTV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "truTV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "truTV", url: "https://tv.youtube.com/live" },
    },
  },

  trutvp: {
    name: "truTV (Pacific)",
    tags: ["Entertainment"],
    stationId: "65717",
    services: {
      hulu: { channelSelector: "truTV (West)", url: "https://www.hulu.com/live" },
    },
  },

  tvland: {
    name: "TV Land",
    tags: ["Entertainment"],
    pacificStationId: "74134",
    stationId: "73541",
    services: {
      cox: { channelSelector: "TV Land", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "TV Land", url: "https://stream.directv.com" },
      hulu: { channelSelector: "TV Land", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "TV Land", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "TV Land", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "TV Land", url: "https://tv.youtube.com/live" },
    },
  },

  usa: {
    name: "USA Network",
    tags: ["Entertainment"],
    stationId: "58452",
    services: {
      cox: { channelSelector: "USA", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "USA Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "USA", url: "https://www.hulu.com/live" },
      site: { channelSelector: "USA_East", url: "https://www.usanetwork.com/live" },
      sling: { channelSelector: "USA", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "USA", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "USA", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "USA", url: "https://tv.youtube.com/live" },
    },
  },

  usap: {
    name: "USA Network (Pacific)",
    tags: ["Entertainment"],
    stationId: "74030",
    services: {
      site: { channelSelector: "USA_West", url: "https://www.usanetwork.com/live" },
    },
  },

  vh1: {
    name: "VH1",
    tags: ["Entertainment"],
    pacificStationId: "64634",
    stationId: "60046",
    services: {
      cox: { channelSelector: "VH1", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "VH1", url: "https://stream.directv.com" },
      hulu: { channelSelector: "VH1", url: "https://www.hulu.com/live" },
      site: { url: "https://www.vh1.com/live-tv" },
      spectrum: { channelSelector: "VH1", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "VH1", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "VH1", url: "https://tv.youtube.com/live" },
    },
  },

  vice: {
    name: "Vice",
    tags: ["Entertainment"],
    pacificStationId: "92375",
    stationId: "65732",
    services: {
      cox: { channelSelector: "VICE", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "VICE", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Vice", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "VICE", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Vice", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "VICE", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  weather: {
    name: "The Weather Channel",
    tags: ["News"],
    stationId: "58812",
    services: {
      cox: { channelSelector: "Weather Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "The Weather Channel HD", url: "https://stream.directv.com" },
      hulu: { channelSelector: "The Weather Channel", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "The Weather Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Weather Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "The Weather Channel", url: "https://tv.youtube.com/live" },
    },
  },

  wetv: {
    name: "WE tv",
    tags: ["Entertainment"],
    pacificStationId: "108192",
    stationId: "59296",
    services: {
      cox: { channelSelector: "WE tv", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "WE TV", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "WE tv", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "WE tv", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "WE tv", url: "https://tv.youtube.com/live" },
    },
  },

  yes: {
    name: "YES Network",
    tags: ["Sports"],
    stationId: "63558",
    services: {
      directv: { channelSelector: "Yes Network HD", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "YES Network", url: "https://watch.spectrum.net/guide" },
    },
  },
};
/* eslint-enable @hjdhjd/blank-line-after-open-brace, @stylistic/comma-dangle, sort-keys */

// Pacific channel auto-generation.

/* PrismCast automatically generates Pacific timezone channel definitions to reduce manual maintenance. Generation runs at module load, producing definitions
 * that are functionally identical to hand-written ones. Generated definitions never override manually-defined ones - if a key already exists in
 * BASE_CHANNEL_DEFINITIONS, the manual definition takes precedence.
 *
 * Step 1 - Generate Pacific ChannelDefinitions from East entries with pacificStationId:
 *
 *   When an East definition has a pacificStationId field and no "{key}p" entry exists in BASE_CHANNEL_DEFINITIONS, a new Pacific ChannelDefinition is created
 *   with the Pacific station ID, " (Pacific)" appended to the name, and an empty services map (to be filled by Step 2).
 *
 *   Example - adding pacificStationId to the East definition:
 *     animal: { name: "Animal Planet", pacificStationId: "68785", stationId: "57394", services: { ... } }
 *   Auto-generates:
 *     animalp: { name: "Animal Planet (Pacific)", stationId: "68785", services: {} }
 *
 * Step 2 - Merge East services into Pacific definitions:
 *
 *   For every Pacific definition "{key}p" (manual or generated), if a corresponding East definition "{key}" exists, its services are merged into the Pacific
 *   definition. Services are skipped if: (a) they already exist in the Pacific definition, or (b) their channelSelector contains "East" or "West" (these have
 *   timezone-specific entries and need manual Pacific definitions with the correct West selector).
 *
 *   Example - East services merged into auto-generated Pacific:
 *     animal's cox service: { channelSelector: "Animal Planet", url: "https://watchtv.cox.com/listings" }
 *   Merges into animalp as:
 *     animalp's cox service: { channelSelector: "Animal Planet", url: "https://watchtv.cox.com/listings" }
 *
 * Adding a new channel with Pacific support:
 *
 *   1. Look up both East and Pacific HD station IDs in Gracenote (http://localhost:8089/tms/stations/<search>).
 *   2. Add the East definition with both IDs: mychannel: { name: "...", pacificStationId: "PACIFIC_ID", stationId: "EAST_ID", services: { ... } }
 *   3. The system auto-generates mychannelp (Pacific definition) and merges all compatible services.
 *   4. If the Pacific version needs a different URL or channelSelector for specific services, define mychannelp manually with those overrides.
 */
function generatePacificDefinitions(definitions: Record<string, ChannelDefinition>): Record<string, ChannelDefinition> {

  const eastWestPattern = /east|west/i;
  const generated: Record<string, ChannelDefinition> = {};

  // Step 1: Generate Pacific ChannelDefinitions from East entries with pacificStationId.
  for(const [ key, def ] of Object.entries(definitions)) {

    if(!def.pacificStationId) {

      continue;
    }

    const pacificKey = key + "p";

    // Manual Pacific definition takes precedence — don't generate.
    if(pacificKey in definitions) {

      continue;
    }

    generated[pacificKey] = {

      name: def.name + " (Pacific)",
      services: {},
      stationId: def.pacificStationId,
      ...(def.tags ? { tags: def.tags } : {})
    };
  }

  // Merge base and generated for Pacific lookup in Step 2.
  const allDefinitions: Record<string, ChannelDefinition> = { ...definitions, ...generated };

  // Step 2: Merge East services into Pacific definitions (both manual and generated). For manually-defined Pacific entries, we create a copy with a fresh
  // services map to avoid mutating the input definitions.
  for(const [ pacKey, originalPacDef ] of Object.entries(allDefinitions)) {

    // Only process Pacific keys (ending in "p" with a corresponding East key).
    if(!pacKey.endsWith("p")) {

      continue;
    }

    const eastKey = pacKey.slice(0, -1);
    const eastDef = definitions[eastKey];

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(!eastDef) {

      continue;
    }

    // Create a copy with a fresh services map so we never mutate the input definitions (which are the module-level BASE_CHANNEL_DEFINITIONS).
    const pacDef: ChannelDefinition = { ...originalPacDef, services: { ...originalPacDef.services } };

    // Merge each East service into the Pacific definition, skipping existing and East/West-specific selectors.
    for(const [ slug, variant ] of Object.entries(eastDef.services)) {

      // Don't overwrite existing Pacific services (manual overrides take precedence).
      if(slug in pacDef.services) {

        continue;
      }

      // Skip services with timezone-specific channelSelectors - these need manual Pacific entries.
      if(variant.channelSelector && eastWestPattern.test(variant.channelSelector)) {

        continue;
      }

      // Copy the service variant. We spread to create a new object so the East and Pacific definitions don't share references.
      pacDef.services[slug] = { ...variant };
    }

    generated[pacKey] = pacDef;
  }

  return generated;
}

// Channel definition flattener.

/* The flattener compiles nested ChannelDefinition entries into the flat ChannelMap consumed by the rest of the codebase. For each definition, it produces a
 * canonical flat entry (keyed by the definition key) and variant flat entries (keyed as "{definitionKey}-{serviceSlug}").
 *
 * Canonical resolution:
 * 1. If "site" exists in services, the site service populates the canonical entry. No "{key}-site" variant is emitted.
 * 2. Otherwise, services are sorted alphabetically by key. The first service populates the canonical entry. No variant key is emitted for it.
 * 3. All remaining services produce variant entries keyed as "{key}-{slug}".
 *
 * Identity fields (name, stationId, channelNumber, tvgShift) are set on the canonical entry and inherited by all variants. Service-specific fields
 * (channelSelector, url, profile, etc.) come from each ServiceVariant. ServiceVariant fields override ChannelDefinition fields when both are set.
 */
function flattenChannelDefinitions(definitions: Record<string, ChannelDefinition>): ChannelMap {

  const channels: ChannelMap = {};

  for(const [ key, def ] of Object.entries(definitions)) {

    const slugs = Object.keys(def.services).sort();

    if(slugs.length === 0) {

      continue;
    }

    // Determine the canonical service: "site" wins, otherwise first alphabetically.
    const canonicalSlug = slugs.includes(SITE_KEY) ? SITE_KEY : slugs[0];
    const canonicalVariant = def.services[canonicalSlug];

    // Build the canonical flat entry with identity fields + canonical service's fields. pacificStationId is set only on the canonical entry - it drives Pacific
    // auto-generation and is not meaningful on service variants.
    const canonicalEntry = buildFlatEntry(def, canonicalVariant);

    if(def.pacificStationId) {

      canonicalEntry.pacificStationId = def.pacificStationId;
    }

    channels[key] = canonicalEntry;

    // Build variant entries for all non-canonical services. Each variant gets canonicalKey set to the definition key so that buildServiceGroups can group
    // channels by a single mechanism - scanning canonicalKey - regardless of whether the channel is predefined or user-defined.
    for(const slug of slugs) {

      if(slug === canonicalSlug) {

        continue;
      }

      const variantKey = key + "-" + slug;
      const variant = def.services[slug];
      const variantEntry = buildFlatEntry(def, variant);

      variantEntry.canonicalKey = key;
      channels[variantKey] = variantEntry;
    }
  }

  return channels;
}

/**
 * Builds a flat Channel entry by merging a ChannelDefinition's identity fields with a ServiceVariant's service-specific fields. ServiceVariant fields
 * override ChannelDefinition fields when both are set.
 * @param def - The parent ChannelDefinition with identity fields.
 * @param variant - The ServiceVariant with service-specific fields.
 * @returns A flat Channel entry.
 */
function buildFlatEntry(def: ChannelDefinition, variant: ServiceVariant): Channel {

  const entry: Channel = {

    name: def.name,
    url: variant.url
  };

  // Identity fields from ChannelDefinition. pacificStationId is intentionally omitted here — it is set only on the canonical entry by the flattener.
  if(def.stationId) {

    entry.stationId = def.stationId;
  }

  // channelNumber: variant override wins, then definition default.
  const channelNumber = variant.channelNumber ?? def.channelNumber;

  if(channelNumber !== undefined) {

    entry.channelNumber = channelNumber;
  }

  // Tags from definition only (identity field, inherited by all variants).
  if(def.tags) {

    entry.tags = def.tags.slice();
  }

  // tvgShift from definition only (not overridable per-variant).
  if(def.tvgShift !== undefined) {

    entry.tvgShift = def.tvgShift;
  }

  // Service-specific fields from ServiceVariant.
  const variantKeys = [ "channelSelector", "dismissSelector", "profile", "scrollSelector", "scrollTarget", "scrollToBottom", "service" ] as const;

  for(const key of variantKeys) {

    if(variant[key] !== undefined) {

      (entry as unknown as Record<string, unknown>)[key] = variant[key];
    }
  }

  return entry;
}

// Compile the nested definitions into flat output.
// Pacific-generated entries take precedence over base entries because Step 2 of Pacific generation produces enhanced copies of manual Pacific definitions
// (e.g., cartoonp with merged East services). Base entries for non-Pacific keys are unaffected since they don't appear in the generated map.
const allDefinitions = { ...BASE_CHANNEL_DEFINITIONS, ...generatePacificDefinitions(BASE_CHANNEL_DEFINITIONS) };
const CHANNELS = flattenChannelDefinitions(allDefinitions);

export { CHANNELS, CHANNELS as PREDEFINED_CHANNELS };
