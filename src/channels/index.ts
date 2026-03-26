/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Channel definitions for PrismCast.
 */
import type { Channel, ChannelDefinition, ChannelMap, ProviderVariant } from "../types/index.js";

// Site provider key. When a channel has its own streaming website, this key is used in the providers map. The site provider always wins as canonical.
const SITE_KEY = "site";

/* Nested channel definitions. Each entry maps a channel key to a ChannelDefinition with identity fields (name, stationId) and a providers map keyed by provider
 * slug. The "site" key represents the channel's own streaming website. All other keys are multi-provider platform slugs (cox, directv, hulu, sling, spectrum,
 * xfinity, yttv, foxone, paramountplus).
 *
 * At module load, the flattener compiles these nested definitions into the flat ChannelMap consumed by the rest of the codebase. Each variant entry gets
 * canonicalKey set to its parent definition's key, which buildProviderGroups in providers.ts uses to assemble provider groups.
 *
 * Canonical resolution rules:
 * 1. If "site" exists in providers, the canonical always gets the site URL.
 * 2. Otherwise, the provider whose key sorts first alphabetically (computed, not source-order) becomes canonical.
 *
 * Adding a new provider never changes canonicals unless a "site" entry is introduced.
 *
 * Pacific timezone support:
 * - pacificStationId on an East ChannelDefinition triggers auto-generation of a Pacific sibling.
 * - Manual Pacific definitions (keys ending in "p") can pre-declare overrides (e.g., West-specific channelSelectors).
 * - Pacific generation merges inherited providers from the East definition into the Pacific definition, skipping providers whose channelSelector contains
 *   "East" or "West" and never overwriting existing Pacific providers. See generatePacificDefinitions() for full rules.
 *
 * FAST channels: This list contains only traditional linear TV networks and public broadcasters — no FAST (Free Ad-Supported Streaming Television) channels.
 * FAST channels from platforms like Pluto TV or Tubi should not be added here. Users who want FAST content can add them as user-defined channels through the
 * web UI or user channels file, or preferably use dedicated high-quality integrations such as Plex Channels, Pluto for Channels, or Tubi for Channels.
 */
/* eslint-disable @hjdhjd/blank-line-after-open-brace, @stylistic/comma-dangle, sort-keys */
const BASE_CHANNEL_DEFINITIONS: Record<string, ChannelDefinition> = {

  abc: {
    name: "ABC",
    providers: {
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
    stationId: "113380",
    providers: {
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
    pacificStationId: "57439",
    stationId: "51529",
    providers: {
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
    stationId: "78808",
    providers: {
      cox: { channelSelector: "American Heroes", url: "https://watchtv.cox.com/listings" },
      site: { url: "https://watch.foodnetwork.com/channel/ahc" },
      spectrum: { channelSelector: "American Heroes Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "American Heroes", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  amc: {
    name: "AMC",
    pacificStationId: "78836",
    stationId: "59337",
    providers: {
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
    stationId: "115678",
    providers: {
      sling: { channelSelector: "AMC Thrillers", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      yttv: { channelSelector: "AMC Thrillers", url: "https://tv.youtube.com/live" },
    },
  },

  animal: {
    name: "Animal Planet",
    pacificStationId: "68785",
    stationId: "57394",
    providers: {
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
    stationId: "28506",
    providers: {
      cox: { channelSelector: "AXS TV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "AXS TV", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "AXS TV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "AXS TV", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  bbcamerica: {
    name: "BBC America",
    pacificStationId: "76739",
    stationId: "64492",
    providers: {
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
    stationId: "101449",
    providers: {
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
    pacificStationId: "64673",
    stationId: "63236",
    providers: {
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
    pacificStationId: "97360",
    stationId: "63220",
    providers: {
      cox: { channelSelector: "BET Her", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "BET Her", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "BET Her", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "BET Her", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "BET Her", url: "https://tv.youtube.com/live" },
    },
  },

  bigten: {
    name: "Big 10",
    stationId: "58321",
    providers: {
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
    stationId: "71799",
    providers: {
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
    stationId: "175656",
    providers: {
      yttv: { channelSelector: "Bloomberg Originals", url: "https://tv.youtube.com/live" },
    },
  },

  bravo: {
    name: "Bravo",
    stationId: "58625",
    providers: {
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
    stationId: "73994",
    providers: {
      site: { url: "https://www.nbc.com/live?brand=bravo&callsign=bravo_west" },
    },
  },

  cartoon: {
    name: "Cartoon Network",
    pacificStationId: "67703",
    stationId: "60048",
    providers: {
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
    stationId: "67703",
    providers: {
      hulu: { channelSelector: "Cartoon Network (West)", url: "https://www.hulu.com/live" },
    },
  },

  cbs: {
    name: "CBS",
    providers: {
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
    stationId: "104846",
    providers: {
      hulu: { channelSelector: "CBS News 24/7", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "CBS News 24/7", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  cbssports: {
    name: "CBS Sports Network",
    stationId: "59250",
    providers: {
      cox: { channelSelector: "CBS Sports Network", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "CBS Sports Network", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "CBS Sports Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CBS Sports Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CBS Sports Network", url: "https://tv.youtube.com/live" },
    },
  },

  cmt: {
    name: "CMT",
    pacificStationId: "64610",
    stationId: "59440",
    providers: {
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
    stationId: "58780",
    providers: {
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
    stationId: "26849",
    providers: {
      directv: { channelSelector: "CNBC World", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "CNBC World", url: "https://watch.spectrum.net/guide" },
    },
  },

  cnn: {
    name: "CNN",
    stationId: "58646",
    providers: {
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
    stationId: "83110",
    providers: {
      directv: { channelSelector: "CNNi HD East", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CNN International", url: "https://www.hulu.com/live" },
      site: { url: "https://www.cnn.com/videos/cnn-i" },
      yttv: { channelSelector: "CNN International", url: "https://tv.youtube.com/live" },
    },
  },

  comedycentral: {
    name: "Comedy Central",
    pacificStationId: "64599",
    stationId: "62420",
    providers: {
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
    stationId: "68065",
    providers: {
      cox: { channelSelector: "Cooking Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Cooking Channel", url: "https://stream.directv.com" },
      site: { url: "https://watch.foodnetwork.com/channel/cooking-channel" },
      spectrum: { channelSelector: "Cooking Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Cooking Channel", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  cspan: {
    name: "C-SPAN",
    stationId: "68344",
    providers: {
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
    stationId: "68334",
    providers: {
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
    stationId: "68332",
    providers: {
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
    providers: {
      cox: { channelSelector: "CW TV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "CW", url: "https://stream.directv.com" },
      hulu: { channelSelector: "CW", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "CW", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "CW TV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "CW", url: "https://tv.youtube.com/live" },
    },
  },

  discovery: {
    name: "Discovery",
    pacificStationId: "80399",
    stationId: "56905",
    providers: {
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
    stationId: "92204",
    providers: {
      cox: { channelSelector: "Discovery Life", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Discovery Life", url: "https://stream.directv.com" },
      site: { url: "https://watch.foodnetwork.com/channel/discovery-life" },
      spectrum: { channelSelector: "Discovery Life", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Discovery Life", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  discoveryturbo: {
    name: "Discovery Turbo",
    stationId: "31046",
    providers: {
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
    pacificStationId: "63320",
    stationId: "59684",
    providers: {
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
    pacificStationId: "75004",
    stationId: "74885",
    providers: {
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
    pacificStationId: "63322",
    stationId: "60006",
    providers: {
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
    stationId: "61812",
    providers: {
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
    stationId: "91579",
    providers: {
      site: { channelSelector: "E-_West", url: "https://www.usanetwork.com/live" },
    },
  },

  espn: {
    name: "ESPN",
    stationId: "32645",
    providers: {
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
    stationId: "45507",
    providers: {
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
    stationId: "111871",
    providers: {
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
    stationId: "71914",
    providers: {
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
    stationId: "59976",
    providers: {
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
    stationId: "89714",
    providers: {
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
    stationId: "60696",
    providers: {
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
    stationId: "58718",
    providers: {
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
    stationId: "60179",
    providers: {
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
    pacificStationId: "82119",
    stationId: "50747",
    providers: {
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
    providers: {
      cox: { channelSelector: "Fox", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "FOX", url: "https://stream.directv.com" },
      foxone: { channelSelector: "FOXD2C", url: "https://www.fox.com/live/channels" },
      hulu: { channelSelector: "Fox", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "FOX", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "FOX", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Fox", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "FOX", url: "https://tv.youtube.com/live" },
    },
  },

  foxdeportes: {
    name: "Fox Deportes",
    stationId: "72189",
    providers: {
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
    stationId: "66879",
    providers: {
      site: { url: "https://www.foxsports.com/live/fsp" },
      yttv: { channelSelector: "FOX Soccer Plus", url: "https://tv.youtube.com/live" },
    },
  },

  france24: {
    name: "France 24",
    stationId: "60961",
    providers: {
      site: { url: "https://www.france24.com/en/live" },
      sling: { channelSelector: "France 24 (English)", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  france24fr: {
    name: "France 24 (French)",
    stationId: "58685",
    providers: {
      site: { url: "https://www.france24.com/fr/direct" },
      sling: { channelSelector: "France 24", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  freeform: {
    name: "Freeform",
    stationId: "59615",
    providers: {
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
    stationId: "63324",
    providers: {
      site: { url: "https://www.freeform.com/watch-live/3507c750-e86a-4c0f-8ff4-dd23c4859009" },
    },
  },

  fs1: {
    name: "FS1",
    stationId: "82547",
    providers: {
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
    stationId: "59305",
    providers: {
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
    stationId: "58574",
    providers: {
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
    pacificStationId: "98488",
    stationId: "70253",
    providers: {
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
    stationId: "59814",
    providers: {
      site: { url: "https://abc.com/watch-live/2cee3401-f63b-42d0-b32e-962fef610b9e" },
    },
  },

  fxx: {
    name: "FXX",
    stationId: "66379",
    providers: {
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
    stationId: "82571",
    providers: {
      site: { url: "https://abc.com/watch-live/e4c83395-62ed-4a49-829a-c55ab3c33e7d" },
    },
  },

  fyi: {
    name: "FYI",
    pacificStationId: "92372",
    stationId: "58988",
    providers: {
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
    pacificStationId: "90210",
    stationId: "68827",
    providers: {
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
    stationId: "61854",
    providers: {
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
    pacificStationId: "66415",
    stationId: "66268",
    providers: {
      cox: { channelSelector: "Hallmark Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Hallmark Channel", url: "https://stream.directv.com" },
      hulu: { channelSelector: "Hallmark Channel", url: "https://www.hulu.com/live" },
      site: { url: "https://www.watchhallmarktv.com/playback/item/live" },
      spectrum: { channelSelector: "Hallmark Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Hallmark Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Hallmark Channel", url: "https://tv.youtube.com/live" },
    },
  },

  hallmarkfamily: {
    name: "Hallmark Family",
    stationId: "105723",
    providers: {
      cox: { channelSelector: "HFM", url: "https://watchtv.cox.com/listings" },
      site: { url: "https://www.watchhallmarktv.com/playback/item/hdlive" },
      spectrum: { channelSelector: "Hallmark Family", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "HFM", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Hallmark Family", url: "https://tv.youtube.com/live" },
    },
  },

  hallmarkmystery: {
    name: "Hallmark Mystery",
    pacificStationId: "66412",
    stationId: "46710",
    providers: {
      cox: { channelSelector: "HMYS", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "Hallmark Mystery", url: "https://www.hulu.com/live" },
      site: { url: "https://www.watchhallmarktv.com/playback/item/hmmlive" },
      spectrum: { channelSelector: "Hallmark Mystery", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "HMYS", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Hallmark Mystery", url: "https://tv.youtube.com/live" },
    },
  },

  hbo: {
    name: "HBO",
    stationId: "19548",
    providers: {
      cox: { channelSelector: "HBO", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBO", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO East", url: "https://tv.youtube.com/live" },
    },
  },

  hbocomedy: {
    name: "HBO Comedy",
    stationId: "59839",
    providers: {
      cox: { channelSelector: "HBOCH", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Comedy", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBOCH", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Comedy East", url: "https://tv.youtube.com/live" },
    },
  },

  hbodrama: {
    name: "HBO Drama",
    stationId: "59363",
    providers: {
      cox: { channelSelector: "HBOSH", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Drama", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBOSH", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Drama East", url: "https://tv.youtube.com/live" },
    },
  },

  hbohits: {
    name: "HBO Hits",
    stationId: "59368",
    providers: {
      cox: { channelSelector: "HBO2H", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Hits", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBO2H", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Hits East", url: "https://tv.youtube.com/live" },
    },
  },

  hbomovies: {
    name: "HBO Movies",
    stationId: "59845",
    providers: {
      cox: { channelSelector: "HBOZH", url: "https://watchtv.cox.com/listings" },
      site: { channelSelector: "HBO Movies", url: "https://play.hbomax.com" },
      xfinity: { channelSelector: "HBOZH", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "HBO Movies East", url: "https://tv.youtube.com/live" },
    },
  },

  hgtv: {
    name: "HGTV",
    pacificStationId: "87317",
    stationId: "49788",
    providers: {
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
    pacificStationId: "88545",
    stationId: "57708",
    providers: {
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
    stationId: "64549",
    providers: {
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
    pacificStationId: "80309",
    stationId: "65342",
    providers: {
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
    pacificStationId: "109735",
    stationId: "59444",
    providers: {
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
    stationId: "65795",
    providers: {
      hulu: { channelSelector: "IndiePlex (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "IndiePlex", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "IndiePlex", url: "https://watch.spectrum.net/guide" },
    },
  },

  indieplexp: {
    name: "IndiePlex (Pacific)",
    stationId: "65796",
    providers: {
      hulu: { channelSelector: "IndiePlex (West)", url: "https://www.hulu.com/live" },
    },
  },

  lifetime: {
    name: "Lifetime",
    pacificStationId: "60250",
    stationId: "60150",
    providers: {
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
    pacificStationId: "92373",
    stationId: "55887",
    providers: {
      cox: { channelSelector: "Lifetime Movies", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Lifetime Movie Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "LMN", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "LMN", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Lifetime Movies", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  magnolia: {
    name: "Magnolia Network",
    pacificStationId: "122081",
    stationId: "67375",
    providers: {
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
    pacificStationId: "95927",
    stationId: "65687",
    providers: {
      cox: { channelSelector: "MGM+", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MGM+", url: "https://stream.directv.com" },
      xfinity: { channelSelector: "MGM+", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  mlb: {
    name: "MLB Network",
    stationId: "62081",
    providers: {
      cox: { channelSelector: "MLB Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MLB Network", url: "https://stream.directv.com" },
      hulu: { channelSelector: "MLB Network", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "MLB Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "MLB Network", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  movieplex: {
    name: "MoviePlex",
    stationId: "83075",
    providers: {
      hulu: { channelSelector: "MoviePlex (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "MoviePlex", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "Movieplex", url: "https://watch.spectrum.net/guide" },
    },
  },

  movieplexp: {
    name: "MoviePlex (Pacific)",
    stationId: "105963",
    providers: {
      hulu: { channelSelector: "MoviePlex (West)", url: "https://www.hulu.com/live" },
    },
  },

  msg: {
    name: "MSG",
    stationId: "35402",
    providers: {
      directv: { channelSelector: "MSG", url: "https://stream.directv.com" },
    },
  },

  msgsn: {
    name: "MSG Sportsnet",
    stationId: "35383",
    providers: {
      directv: { channelSelector: "MSG Sportsnet HD 635", url: "https://stream.directv.com" },
    },
  },

  msnow: {
    name: "MS NOW",
    stationId: "64241",
    providers: {
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
    pacificStationId: "64630",
    stationId: "60964",
    providers: {
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
    pacificStationId: "75506",
    stationId: "75077",
    providers: {
      cox: { channelSelector: "MTV2", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MTV2", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "MTV2", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "MTV2", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  mtvclassic: {
    name: "MTV Classic",
    stationId: "92240",
    providers: {
      cox: { channelSelector: "MTV Classic", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "MTV Classic", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "MTV Classic", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "MTV Classic", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  natgeo: {
    name: "National Geographic",
    stationId: "49438",
    providers: {
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
    stationId: "71601",
    providers: {
      site: { url: "https://www.nationalgeographic.com/tv/watch-live/91456580-f32f-417c-8e1a-9f82640832a7" },
    },
  },

  natgeowild: {
    name: "Nat Geo Wild",
    stationId: "67331",
    providers: {
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
    stationId: "45526",
    providers: {
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
    providers: {
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
    stationId: "114174",
    providers: {
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
    stationId: "63138",
    providers: {
      site: { url: "https://www.nbc.com/live?brand=rsn-bay-area&callsign=nbcsbayarea" },
    },
  },

  nbcsboston: {
    name: "NBC Sports Boston",
    stationId: "49198",
    providers: {
      site: { url: "https://www.nbc.com/live?brand=rsn-boston&callsign=nbcsboston" },
    },
  },

  nbcscalifornia: {
    name: "NBC Sports California",
    stationId: "45540",
    providers: {
      site: { url: "https://www.nbc.com/live?brand=rsn-california&callsign=nbcscalifornia" },
    },
  },

  nbcsn: {
    name: "NBC Sports Network",
    stationId: "194412",
    providers: {
      yttv: { channelSelector: "NBC Sports Network", url: "https://tv.youtube.com/live" },
    },
  },

  nbcsphiladelphia: {
    name: "NBC Sports Philadelphia",
    stationId: "32571",
    providers: {
      site: { url: "https://www.nbc.com/live?brand=rsn-philadelphia&callsign=nbcsphiladelphia" },
    },
  },

  necn: {
    name: "NECN",
    stationId: "66278",
    providers: {
      site: { url: "https://www.nbc.com/live?brand=necn&callsign=necn" },
    },
  },

  nfl: {
    name: "NFL Network",
    stationId: "45399",
    providers: {
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
    stationId: "58690",
    providers: {
      cox: { channelSelector: "NHL Network", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "NHL Network HD", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "NHL Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "NHL Network", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  own: {
    name: "OWN",
    stationId: "70388",
    providers: {
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
    stationId: "70522",
    providers: {
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
    stationId: "74032",
    providers: {
      site: { channelSelector: "Oxygen_West", url: "https://www.usanetwork.com/live" },
    },
  },

  paramount: {
    name: "Paramount Network",
    stationId: "59186",
    providers: {
      cox: { channelSelector: "Paramount Network", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "Paramount Network", url: "https://www.hulu.com/live" },
      spectrum: { channelSelector: "Paramount Network", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Paramount Network", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Paramount", url: "https://tv.youtube.com/live" },
    },
  },

  paramountp: {
    name: "Paramount (Pacific)",
    stationId: "64593",
    providers: {
      yttv: { channelSelector: "Paramount Network", url: "https://tv.youtube.com/live" },
    },
  },

  pbs: {
    name: "PBS",
    providers: {
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
    stationId: "30415",
    providers: {
      hulu: { channelSelector: "PBS", url: "https://www.hulu.com/live" },
      site: { url: "https://www.wttw.com/wttw-live-stream" },
    },
  },

  pbslakeshore: {
    name: "PBS Lakeshore (WYIN)",
    stationId: "49237",
    providers: {
      hulu: { channelSelector: "Lakeshore PBS", url: "https://www.hulu.com/live" },
      site: { url: "https://video.lakeshorepbs.org/livestream" },
    },
  },

  retroplex: {
    name: "RetroPlex",
    stationId: "65791",
    providers: {
      hulu: { channelSelector: "RetroPlex (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "RetroPlex", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      spectrum: { channelSelector: "RetroPlex", url: "https://watch.spectrum.net/guide" },
    },
  },

  retroplexp: {
    name: "RetroPlex (Pacific)",
    stationId: "65793",
    providers: {
      hulu: { channelSelector: "RetroPlex (West)", url: "https://www.hulu.com/live" },
    },
  },

  science: {
    name: "Science",
    stationId: "57390",
    providers: {
      cox: { channelSelector: "Science Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Science", url: "https://stream.directv.com" },
      site: { url: "https://watch.foodnetwork.com/channel/science" },
      spectrum: { channelSelector: "Science", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Science Channel", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  showtime: {
    name: "Showtime",
    stationId: "91620",
    providers: {
      cox: { channelSelector: "PSHOh", url: "https://watchtv.cox.com/listings" },
      paramountplus: { url: "https://www.paramountplus.com/live-tv/stream/showtime-east" },
      xfinity: { channelSelector: "PSHOh", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Showtime East", url: "https://tv.youtube.com/live" },
    },
  },

  showtimep: {
    name: "Showtime (Pacific)",
    stationId: "91621",
    providers: {
      paramountplus: { url: "https://www.paramountplus.com/live-tv/stream/showtime-west" },
    },
  },

  smithsonian: {
    name: "Smithsonian Channel",
    pacificStationId: "82695",
    stationId: "58532",
    providers: {
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
    stationId: "50038",
    providers: {
      directv: { channelSelector: "SportsNet New York HD 639", url: "https://stream.directv.com" },
    },
  },

  starz: {
    name: "Starz",
    stationId: "34941",
    providers: {
      cox: { channelSelector: "STARZ", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ (East)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.starz.com/us/en/play/2" },
      sling: { channelSelector: "STARZ", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STARZ", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzcinema: {
    name: "Starz Cinema",
    stationId: "67236",
    providers: {
      hulu: { channelSelector: "STARZ Cinema (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Cinema", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzcinemap: {
    name: "Starz Cinema (Pacific)",
    stationId: "67365",
    providers: {
      hulu: { channelSelector: "STARZ Cinema (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzcomedy: {
    name: "Starz Comedy",
    stationId: "57569",
    providers: {
      hulu: { channelSelector: "STARZ Comedy (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Comedy", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzcomedyp: {
    name: "Starz Comedy (Pacific)",
    stationId: "57575",
    providers: {
      hulu: { channelSelector: "STARZ Comedy (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzedge: {
    name: "Starz Edge",
    stationId: "57573",
    providers: {
      hulu: { channelSelector: "STARZ Edge (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Edge", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzedgep: {
    name: "Starz Edge (Pacific)",
    stationId: "57578",
    providers: {
      hulu: { channelSelector: "STARZ Edge (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencore: {
    name: "Starz Encore",
    stationId: "36225",
    providers: {
      cox: { channelSelector: "STZEH", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ Encore (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STZEH", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzencoreaction: {
    name: "Starz Encore Action",
    stationId: "72015",
    providers: {
      cox: { channelSelector: "STZAH", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ Encore Action (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Action", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STZAH", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzencoreactionp: {
    name: "Starz Encore Action (Pacific)",
    stationId: "103833",
    providers: {
      hulu: { channelSelector: "STARZ Encore Action (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencoreblack: {
    name: "Starz Encore Black",
    stationId: "72014",
    providers: {
      cox: { channelSelector: "STZBH", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "STARZ Encore Black (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Black", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "STZBH", url: "https://www.xfinity.com/stream/listings" },
    },
  },

  starzencoreblackp: {
    name: "Starz Encore Black (Pacific)",
    stationId: "103834",
    providers: {
      hulu: { channelSelector: "STARZ Encore Black (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencoreclassic: {
    name: "Starz Encore Classic",
    stationId: "83404",
    providers: {
      hulu: { channelSelector: "STARZ Encore Classic (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Classic", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoreclassicp: {
    name: "Starz Encore Classic (Pacific)",
    stationId: "97233",
    providers: {
      hulu: { channelSelector: "STARZ Encore Classic (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencoreespanol: {
    name: "Starz Encore Español",
    stationId: "72016",
    providers: {
      hulu: { channelSelector: "STARZ Encore Español (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Español", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoreespanolp: {
    name: "Starz Encore Español (Pacific)",
    stationId: "104730",
    providers: {
      hulu: { channelSelector: "STARZ Encore Español (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencorefamily: {
    name: "Starz Encore Family",
    stationId: "14886",
    providers: {
      hulu: { channelSelector: "STARZ Encore Family (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Family", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencorefamilyp: {
    name: "Starz Encore Family (Pacific)",
    stationId: "103829",
    providers: {
      hulu: { channelSelector: "STARZ Encore Family (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencorep: {
    name: "Starz Encore (Pacific)",
    stationId: "67237",
    providers: {
      hulu: { channelSelector: "STARZ Encore (West)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore West", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoresuspense: {
    name: "Starz Encore Suspense",
    stationId: "83076",
    providers: {
      hulu: { channelSelector: "STARZ Encore Suspense (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Suspense", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencoresuspensep: {
    name: "Starz Encore Suspense (Pacific)",
    stationId: "103836",
    providers: {
      hulu: { channelSelector: "STARZ Encore Suspense (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzencorewesterns: {
    name: "Starz Encore Westerns",
    stationId: "14765",
    providers: {
      hulu: { channelSelector: "STARZ Encore Westerns (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Encore Westerns", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzencorewesternsp: {
    name: "Starz Encore Westerns (Pacific)",
    stationId: "103856",
    providers: {
      hulu: { channelSelector: "STARZ Encore Westerns (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzinblack: {
    name: "Starz in Black",
    stationId: "67235",
    providers: {
      hulu: { channelSelector: "STARZ in Black (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz in Black", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzinblackp: {
    name: "Starz in Black (Pacific)",
    stationId: "67367",
    providers: {
      hulu: { channelSelector: "STARZ in Black (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzkids: {
    name: "Starz Kids",
    stationId: "57581",
    providers: {
      hulu: { channelSelector: "STARZ Kids (East)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz Kids", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  starzkidsp: {
    name: "Starz Kids (Pacific)",
    stationId: "57583",
    providers: {
      hulu: { channelSelector: "STARZ Kids (West)", url: "https://www.hulu.com/live" },
    },
  },

  starzp: {
    name: "Starz (Pacific)",
    stationId: "34949",
    providers: {
      hulu: { channelSelector: "STARZ (West)", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "Starz West", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
    },
  },

  sundancetv: {
    name: "SundanceTV",
    pacificStationId: "78806",
    stationId: "71280",
    providers: {
      cox: { channelSelector: "SundanceTV", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Sundance TV", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "SundanceTV", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "SundanceTV", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "SundanceTV", url: "https://tv.youtube.com/live" },
    },
  },

  syfy: {
    name: "Syfy",
    stationId: "58623",
    providers: {
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
    stationId: "65626",
    providers: {
      site: { channelSelector: "Syfy_West", url: "https://www.usanetwork.com/live" },
    },
  },

  tbs: {
    name: "TBS",
    stationId: "58515",
    providers: {
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
    stationId: "67890",
    providers: {
      hulu: { channelSelector: "TBS (West)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.tbs.com/watchtbs/west" },
    },
  },

  tcm: {
    name: "TCM",
    stationId: "64312",
    providers: {
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
    stationId: "64312",
    tvgShift: 3,
    providers: {
      hulu: { channelSelector: "TCM (West)", url: "https://www.hulu.com/live" },
    },
  },

  tennis: {
    name: "Tennis Channel",
    stationId: "60316",
    providers: {
      cox: { channelSelector: "Tennis Channel", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "Tennis Channel HD", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "Tennis Channel", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "Tennis Channel", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "Tennis Channel", url: "https://tv.youtube.com/live" },
    },
  },

  tennis2: {
    name: "Tennis Channel 2",
    stationId: "137752",
    providers: {
      cox: { channelSelector: "T2", url: "https://watchtv.cox.com/listings" },
      hulu: { channelSelector: "Tennis Channel 2", url: "https://www.hulu.com/live" },
      sling: { channelSelector: "T2", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      xfinity: { channelSelector: "T2", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "T2", url: "https://tv.youtube.com/live" },
    },
  },

  tlc: {
    name: "TLC",
    pacificStationId: "79911",
    stationId: "57391",
    providers: {
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
    stationId: "42642",
    providers: {
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
    stationId: "61340",
    providers: {
      hulu: { channelSelector: "TNT (West)", url: "https://www.hulu.com/live" },
      site: { url: "https://www.tntdrama.com/watchtnt/west" },
    },
  },

  travel: {
    name: "Travel",
    pacificStationId: "64525",
    stationId: "59303",
    providers: {
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
    pacificStationId: "65717",
    stationId: "64490",
    providers: {
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
    stationId: "65717",
    providers: {
      hulu: { channelSelector: "truTV (West)", url: "https://www.hulu.com/live" },
    },
  },

  tvland: {
    name: "TV Land",
    pacificStationId: "74134",
    stationId: "73541",
    providers: {
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
    stationId: "58452",
    providers: {
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
    stationId: "74030",
    providers: {
      site: { channelSelector: "USA_West", url: "https://www.usanetwork.com/live" },
    },
  },

  vh1: {
    name: "VH1",
    pacificStationId: "64634",
    stationId: "60046",
    providers: {
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
    pacificStationId: "92375",
    stationId: "65732",
    providers: {
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
    stationId: "58812",
    providers: {
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
    pacificStationId: "108192",
    stationId: "59296",
    providers: {
      cox: { channelSelector: "WE tv", url: "https://watchtv.cox.com/listings" },
      directv: { channelSelector: "WE TV", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "WE tv", url: "https://watch.spectrum.net/guide" },
      xfinity: { channelSelector: "WE tv", url: "https://www.xfinity.com/stream/listings" },
      yttv: { channelSelector: "WE tv", url: "https://tv.youtube.com/live" },
    },
  },

  yes: {
    name: "YES Network",
    stationId: "63558",
    providers: {
      directv: { channelSelector: "Yes Network HD", url: "https://stream.directv.com" },
      spectrum: { channelSelector: "YES Network", url: "https://watch.spectrum.net/guide" },
    },
  },
};
/* eslint-enable @hjdhjd/blank-line-after-open-brace, @stylistic/comma-dangle, sort-keys */

// Pacific channel auto-generation.

/* PrismCast automatically generates Pacific timezone channel definitions to reduce manual maintenance. Generation runs at module load, producing definitions
 * that are functionally identical to hand-written ones. Generated definitions never override manually-defined ones — if a key already exists in
 * BASE_CHANNEL_DEFINITIONS, the manual definition takes precedence.
 *
 * Step 1 — Generate Pacific ChannelDefinitions from East entries with pacificStationId:
 *
 *   When an East definition has a pacificStationId field and no "{key}p" entry exists in BASE_CHANNEL_DEFINITIONS, a new Pacific ChannelDefinition is created
 *   with the Pacific station ID, " (Pacific)" appended to the name, and an empty providers map (to be filled by Step 2).
 *
 *   Example — adding pacificStationId to the East definition:
 *     animal: { name: "Animal Planet", pacificStationId: "68785", stationId: "57394", providers: { ... } }
 *   Auto-generates:
 *     animalp: { name: "Animal Planet (Pacific)", stationId: "68785", providers: {} }
 *
 * Step 2 — Merge East providers into Pacific definitions:
 *
 *   For every Pacific definition "{key}p" (manual or generated), if a corresponding East definition "{key}" exists, its providers are merged into the Pacific
 *   definition. Providers are skipped if: (a) they already exist in the Pacific definition, or (b) their channelSelector contains "East" or "West" (these have
 *   timezone-specific entries and need manual Pacific definitions with the correct West selector).
 *
 *   Example — East providers merged into auto-generated Pacific:
 *     animal's cox provider: { channelSelector: "Animal Planet", url: "https://watchtv.cox.com/listings" }
 *   Merges into animalp as:
 *     animalp's cox provider: { channelSelector: "Animal Planet", url: "https://watchtv.cox.com/listings" }
 *
 * Adding a new channel with Pacific support:
 *
 *   1. Look up both East and Pacific HD station IDs in Gracenote (http://localhost:8089/tms/stations/<search>).
 *   2. Add the East definition with both IDs: mychannel: { name: "...", pacificStationId: "PACIFIC_ID", stationId: "EAST_ID", providers: { ... } }
 *   3. The system auto-generates mychannelp (Pacific definition) and merges all compatible providers.
 *   4. If the Pacific version needs a different URL or channelSelector for specific providers, define mychannelp manually with those overrides.
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
      providers: {},
      stationId: def.pacificStationId
    };
  }

  // Merge base and generated for Pacific lookup in Step 2.
  const allDefinitions: Record<string, ChannelDefinition> = { ...definitions, ...generated };

  // Step 2: Merge East providers into Pacific definitions (both manual and generated). For manually-defined Pacific entries, we create a copy with a fresh
  // providers map to avoid mutating the input definitions.
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

    // Create a copy with a fresh providers map so we never mutate the input definitions (which are the module-level BASE_CHANNEL_DEFINITIONS).
    const pacDef: ChannelDefinition = { ...originalPacDef, providers: { ...originalPacDef.providers } };

    // Merge each East provider into the Pacific definition, skipping existing and East/West-specific selectors.
    for(const [ slug, variant ] of Object.entries(eastDef.providers)) {

      // Don't overwrite existing Pacific providers (manual overrides take precedence).
      if(slug in pacDef.providers) {

        continue;
      }

      // Skip providers with timezone-specific channelSelectors — these need manual Pacific entries.
      if(variant.channelSelector && eastWestPattern.test(variant.channelSelector)) {

        continue;
      }

      // Copy the provider variant. We spread to create a new object so the East and Pacific definitions don't share references.
      pacDef.providers[slug] = { ...variant };
    }

    generated[pacKey] = pacDef;
  }

  return generated;
}

// Channel definition flattener.

/* The flattener compiles nested ChannelDefinition entries into the flat ChannelMap consumed by the rest of the codebase. For each definition, it produces a
 * canonical flat entry (keyed by the definition key) and variant flat entries (keyed as "{definitionKey}-{providerSlug}").
 *
 * Canonical resolution:
 * 1. If "site" exists in providers, the site provider populates the canonical entry. No "{key}-site" variant is emitted.
 * 2. Otherwise, providers are sorted alphabetically by key. The first provider populates the canonical entry. No variant key is emitted for it.
 * 3. All remaining providers produce variant entries keyed as "{key}-{slug}".
 *
 * Identity fields (name, stationId, channelNumber, tvgShift) are set on the canonical entry and inherited by all variants. Provider-specific fields
 * (channelSelector, url, profile, etc.) come from each ProviderVariant. ProviderVariant fields override ChannelDefinition fields when both are set.
 */
function flattenChannelDefinitions(definitions: Record<string, ChannelDefinition>): ChannelMap {

  const channels: ChannelMap = {};

  for(const [ key, def ] of Object.entries(definitions)) {

    const slugs = Object.keys(def.providers).sort();

    if(slugs.length === 0) {

      continue;
    }

    // Determine the canonical provider: "site" wins, otherwise first alphabetically.
    const canonicalSlug = slugs.includes(SITE_KEY) ? SITE_KEY : slugs[0];
    const canonicalVariant = def.providers[canonicalSlug];

    // Build the canonical flat entry with identity fields + canonical provider's fields. pacificStationId is set only on the canonical entry — it drives Pacific
    // auto-generation and is not meaningful on provider variants.
    const canonicalEntry = buildFlatEntry(def, canonicalVariant);

    if(def.pacificStationId) {

      canonicalEntry.pacificStationId = def.pacificStationId;
    }

    channels[key] = canonicalEntry;

    // Build variant entries for all non-canonical providers. Each variant gets canonicalKey set to the definition key so that buildProviderGroups can group
    // channels by a single mechanism — scanning canonicalKey — regardless of whether the channel is predefined or user-defined.
    for(const slug of slugs) {

      if(slug === canonicalSlug) {

        continue;
      }

      const variantKey = key + "-" + slug;
      const variant = def.providers[slug];
      const variantEntry = buildFlatEntry(def, variant);

      variantEntry.canonicalKey = key;
      channels[variantKey] = variantEntry;
    }
  }

  return channels;
}

/**
 * Builds a flat Channel entry by merging a ChannelDefinition's identity fields with a ProviderVariant's provider-specific fields. ProviderVariant fields
 * override ChannelDefinition fields when both are set.
 * @param def - The parent ChannelDefinition with identity fields.
 * @param variant - The ProviderVariant with provider-specific fields.
 * @returns A flat Channel entry.
 */
function buildFlatEntry(def: ChannelDefinition, variant: ProviderVariant): Channel {

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

  // tvgShift from definition only (not overridable per-variant).
  if(def.tvgShift !== undefined) {

    entry.tvgShift = def.tvgShift;
  }

  // Provider-specific fields from ProviderVariant.
  if(variant.channelSelector) {

    entry.channelSelector = variant.channelSelector;
  }

  if(variant.dismissSelector) {

    entry.dismissSelector = variant.dismissSelector;
  }

  if(variant.profile) {

    entry.profile = variant.profile;
  }

  if(variant.provider) {

    entry.provider = variant.provider;
  }

  if(variant.scrollSelector) {

    entry.scrollSelector = variant.scrollSelector;
  }

  if(variant.scrollTarget) {

    entry.scrollTarget = variant.scrollTarget;
  }

  if(variant.scrollToBottom !== undefined) {

    entry.scrollToBottom = variant.scrollToBottom;
  }

  return entry;
}

// Compile the nested definitions into flat output.
// Pacific-generated entries take precedence over base entries because Step 2 of Pacific generation produces enhanced copies of manual Pacific definitions
// (e.g., cartoonp with merged East providers). Base entries for non-Pacific keys are unaffected since they don't appear in the generated map.
const allDefinitions = { ...BASE_CHANNEL_DEFINITIONS, ...generatePacificDefinitions(BASE_CHANNEL_DEFINITIONS) };
const CHANNELS = flattenChannelDefinitions(allDefinitions);

export { CHANNELS, CHANNELS as PREDEFINED_CHANNELS };
