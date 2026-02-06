/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Channel definitions for PrismCast.
 */
import type { ChannelMap } from "../types/index.js";

/*
 * Map short channel names to streaming configurations. Users request streams via /stream/nbc instead of full URLs.
 *
 * Channel properties:
 * - name: Display name shown in Channels DVR (required for canonical channels, inherited by variants).
 * - url: Streaming site URL.
 * - profile: Site behavior profile (optional). Use "auto" or omit to auto-detect from URL domain. See config/profiles.ts for available profiles.
 * - stationId: Gracenote station ID for guide data (optional). Local affiliates (ABC, CBS, NBC) vary by region.
 * - channelSelector: Image URL slug for multi-channel pages (required for apiMultiVideo and keyboardDynamicMultiVideo profiles).
 * - provider: UI label for provider selection dropdown (optional). Used when multiple providers offer the same content.
 *
 * Provider variants: Channels are grouped by key prefix convention — a key like "espn-disneyplus" is a variant of "espn" because it starts with "espn-" and
 * "espn" exists as a channel. Variants inherit `name` and `stationId` from the canonical entry. See config/providers.ts for grouping details.
 *
 * IMPORTANT: Avoid hyphenated keys that would unintentionally match an existing channel. If "foo" exists, "foo-bar" becomes its variant. Use non-hyphenated keys
 * for independent channels (e.g., "cnni" instead of "cnn-international").
 */
export const CHANNELS: ChannelMap = {

  abc: { name: "ABC", url: "https://abc.com/watch-live/b2f23a6e-a2a4-4d63-bd3b-e330921b0942" },
  ae: { name: "A&E", stationId: "51529", url: "https://play.aetv.com/live" },
  ahc: { name: "American Heroes", stationId: "78808", url: "https://watch.foodnetwork.com/channel/ahc" },
  animal: { name: "Animal Planet", stationId: "57394", url: "https://watch.foodnetwork.com/channel/animal-planet" },
  bigten: { name: "Big 10", stationId: "58321", url: "https://www.foxsports.com/live/btn" },
  bravo: { name: "Bravo", stationId: "58625", url: "https://www.nbc.com/live?brand=bravo&callsign=bravo_east" },
  bravop: { name: "Bravo (Pacific)", stationId: "73994", url: "https://www.nbc.com/live?brand=bravo&callsign=bravo_west" },
  cbs: { name: "CBS", provider: "CBS.com", url: "https://www.cbs.com/live-tv/stream" },
  "cbs-paramountplus": { name: "CBS", provider: "Paramount+", url: "https://www.paramountplus.com/live-tv/" },
  cnbc: { name: "CNBC", provider: "CNBC.com", stationId: "58780", url: "https://www.cnbc.com/live-tv" },
  "cnbc-usa": { channelSelector: "CNBC_US", provider: "USA Network", url: "https://www.usanetwork.com/live" },
  cnn: { name: "CNN", stationId: "58646", url: "https://www.cnn.com/videos/cnn" },
  cnni: { name: "CNN International", stationId: "83110", url: "https://www.cnn.com/videos/cnn-i" },
  cooking: { name: "Cooking", stationId: "68065", url: "https://watch.foodnetwork.com/channel/cooking-channel" },
  cspan: { name: "C-SPAN", stationId: "68344", url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span" },
  cspan2: { name: "C-SPAN 2", stationId: "68334", url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span-2" },
  cspan3: { name: "C-SPAN 3", stationId: "68332", url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span-3" },
  discovery: { name: "Discovery", stationId: "56905", url: "https://watch.foodnetwork.com/channel/discovery" },
  discoverylife: { name: "Discovery Life", stationId: "92204", url: "https://watch.foodnetwork.com/channel/discovery-life" },
  discoveryturbo: { name: "Discovery Turbo", stationId: "31046", url: "https://watch.foodnetwork.com/channel/motortrend" },
  disney: { name: "Disney", stationId: "59684", url: "https://disneynow.com/watch-live?brand=004" },
  disneyjr: { name: "Disney Jr.", stationId: "74885", url: "https://disneynow.com/watch-live?brand=008" },
  disneyxd: { name: "Disney XD", stationId: "60006", url: "https://disneynow.com/watch-live?brand=009" },
  e: { channelSelector: "E-_East", name: "E!", stationId: "61812", url: "https://www.usanetwork.com/live" },
  ep: { channelSelector: "E-_West", name: "E! (Pacific)", stationId: "91579", url: "https://www.usanetwork.com/live" },
  espn: { name: "ESPN", provider: "ESPN.com", stationId: "32645", url: "https://www.espn.com/watch/player?network=espn" },
  "espn-disneyplus": { channelSelector: "poster_linear_espn_none", provider: "Disney+", url: "https://www.disneyplus.com/browse/live" },
  espn2: { name: "ESPN2", provider: "ESPN.com", stationId: "45507", url: "https://www.espn.com/watch/player?network=espn2" },
  "espn2-disneyplus": { channelSelector: "poster_linear_espn2_none", provider: "Disney+", url: "https://www.disneyplus.com/browse/live" },
  espnacc: { name: "ACC Network", provider: "ESPN.com", stationId: "111871", url: "https://www.espn.com/watch/player?network=acc" },
  "espnacc-disneyplus": { channelSelector: "poster_linear_acc-network_none", provider: "Disney+", url: "https://www.disneyplus.com/browse/live" },
  espndeportes: { name: "ESPN Deportes", provider: "ESPN.com", stationId: "71914", url: "https://www.espn.com/watch/player?network=espndeportes" },
  "espndeportes-disneyplus": { channelSelector: "poster_linear_espn-deportes_none", provider: "Disney+", url: "https://www.disneyplus.com/browse/live" },
  espnews: { name: "ESPNews", provider: "ESPN.com", stationId: "59976", url: "https://www.espn.com/watch/player?network=espnews" },
  "espnews-disneyplus": { channelSelector: "poster_linear_espnews_none", provider: "Disney+", url: "https://www.disneyplus.com/browse/live" },
  espnsec: { name: "SEC Network", provider: "ESPN.com", stationId: "89714", url: "https://www.espn.com/watch/player?network=sec" },
  "espnsec-disneyplus": { channelSelector: "poster_linear_sec-network_none", provider: "Disney+", url: "https://www.disneyplus.com/browse/live" },
  espnu: { name: "ESPNU", provider: "ESPN.com", stationId: "60696", url: "https://www.espn.com/watch/player?network=espnu" },
  "espnu-disneyplus": { channelSelector: "poster_linear_espnu_none", provider: "Disney+", url: "https://www.disneyplus.com/browse/live" },
  fbc: { name: "Fox Business", stationId: "58718", url: "https://www.foxbusiness.com/video/5640669329001" },
  fnc: { name: "Fox News", stationId: "60179", url: "https://www.foxnews.com/video/5614615980001" },
  food: { name: "Food Network", stationId: "50747", url: "https://watch.foodnetwork.com/channel/food-network" },
  foxdeportes: { name: "Fox Deportes", stationId: "72189", url: "https://www.foxsports.com/live/foxdep" },
  foxsoccerplus: { name: "Fox Soccer Plus", stationId: "66879", url: "https://www.foxsports.com/live/fsp" },
  france24: { name: "France 24", stationId: "60961", url: "https://www.france24.com/en/live" },
  france24fr: { name: "France 24 (French)", stationId: "58685", url: "https://www.france24.com/fr/direct" },
  fs1: { name: "FS1", stationId: "82547", url: "https://www.foxsports.com/live/fs1" },
  fs2: { name: "FS2", stationId: "59305", url: "https://www.foxsports.com/live/fs2" },
  fx: { name: "FX", stationId: "58574", url: "https://abc.com/watch-live/93256af4-5e80-4558-aa2e-2bdfffa119a0" },
  fxm: { name: "FXM", stationId: "70253", url: "https://abc.com/watch-live/d298ab7e-c6b1-4efa-ac6e-a52dceed92ee" },
  fxp: { name: "FX (Pacific)", stationId: "59814", url: "https://abc.com/watch-live/2cee3401-f63b-42d0-b32e-962fef610b9e" },
  fxx: { name: "FXX", stationId: "66379", url: "https://abc.com/watch-live/49f4a471-8d36-4728-8457-ea65cbbc84ea" },
  fxxp: { name: "FXX (Pacific)", stationId: "82571", url: "https://abc.com/watch-live/e4c83395-62ed-4a49-829a-c55ab3c33e7d" },
  fyi: { name: "FYI", stationId: "58988", url: "https://play.fyi.tv/live" },
  golf: { name: "Golf", profile: "fullscreenApi", provider: "Golf Channel", stationId: "61854", url: "https://www.golfchannel.com/watch/live" },
  "golf-usa": { channelSelector: "gc", provider: "USA Network", url: "https://www.usanetwork.com/live" },
  hgtv: { name: "HGTV", stationId: "49788", url: "https://watch.foodnetwork.com/channel/hgtv" },
  history: { name: "History", stationId: "14771", url: "https://play.history.com/live" },
  hln: { name: "HLN", stationId: "64549", url: "https://www.cnn.com/videos/hln" },
  id: { name: "Investigation Discovery", stationId: "65342", url: "https://watch.foodnetwork.com/channel/investigation-discovery" },
  lifetime: { name: "Lifetime", stationId: "60150", url: "https://play.mylifetime.com/live" },
  magnolia: { name: "Magnolia Network", stationId: "67375", url: "https://watch.foodnetwork.com/channel/magnolia-network-preview-atve-us" },
  msnow: { name: "MSNOW", provider: "MSNOW", stationId: "64241", url: "https://www.ms.now/live" },
  "msnow-usa": { channelSelector: "image-23", provider: "USA Network", url: "https://www.usanetwork.com/live" },
  natgeo: { name: "National Geographic", stationId: "49438", url: "https://www.nationalgeographic.com/tv/watch-live/0826a9a3-3384-4bb5-8841-91f01cb0e3a7" },
  natgeop: { name: "National Geographic (Pacific)", stationId: "71601", url: "https://www.nationalgeographic.com/tv/watch-live/91456580-f32f-417c-8e1a-9f82640832a7" },
  natgeowild: { name: "Nat Geo Wild", stationId: "67331", url: "https://www.nationalgeographic.com/tv/watch-live/239b9590-583f-4955-a499-22e9eefff9cf" },
  nbc: { name: "NBC", url: "https://www.nbc.com/live?brand=nbc&callsign=nbc" },
  nbcnews: { name: "NBC News Now", stationId: "114174", url: "https://www.nbc.com/live?brand=nbc-news&callsign=nbcnews" },
  nbcsbayarea: { name: "NBC Sports Bay Area", stationId: "63138", url: "https://www.nbc.com/live?brand=rsn-bay-area&callsign=nbcsbayarea" },
  nbcsboston: { name: "NBC Sports Boston", stationId: "49198", url: "https://www.nbc.com/live?brand=rsn-boston&callsign=nbcsboston" },
  nbcscalifornia: { name: "NBC Sports California", stationId: "45540", url: "https://www.nbc.com/live?brand=rsn-california&callsign=nbcscalifornia" },
  nbcschicago: { name: "NBC Sports Chicago", stationId: "44905", url: "https://www.nbc.com/live?brand=rsn-chicago&callsign=nbcschicago" },
  nbcsphiladelphia: { name: "NBC Sports Philadelphia", stationId: "32571", url: "https://www.nbc.com/live?brand=rsn-philadelphia&callsign=nbcsphiladelphia" },
  necn: { name: "NECN", stationId: "66278", url: "https://www.nbc.com/live?brand=necn&callsign=necn" },
  own: { name: "OWN", stationId: "70388", url: "https://watch.foodnetwork.com/channel/own" },
  oxygen: { channelSelector: "Oxygen_East", name: "Oxygen", stationId: "70522", url: "https://www.usanetwork.com/live" },
  oxygenp: { channelSelector: "Oxygen_West", name: "Oxygen (Pacific)", stationId: "74032", url: "https://www.usanetwork.com/live" },
  pbschicago: { name: "PBS Chicago (WTTW)", stationId: "30415", url: "https://www.wttw.com/wttw-live-stream" },
  pbslakeshore: { name: "PBS Lakeshore (WYIN)", profile: "embeddedPlayer", stationId: "49237", url: "https://video.lakeshorepbs.org/livestream" },
  science: { name: "Science", stationId: "57390", url: "https://watch.foodnetwork.com/channel/science" },
  showtime: { name: "Showtime", stationId: "91620", url: "https://www.paramountplus.com/live-tv/stream/showtime-east" },
  showtimep: { name: "Showtime (Pacific)", stationId: "91621", url: "https://www.paramountplus.com/live-tv/stream/showtime-west" },
  syfy: { channelSelector: "Syfy_East", name: "Syfy", stationId: "58623", url: "https://www.usanetwork.com/live" },
  syfyp: { channelSelector: "Syfy_West", name: "Syfy (Pacific)", stationId: "65626", url: "https://www.usanetwork.com/live" },
  tbs: { name: "TBS", stationId: "58515", url: "https://www.tbs.com/watchtbs/east" },
  tbsp: { name: "TBS (Pacific)", stationId: "67890", url: "https://www.tbs.com/watchtbs/west" },
  tlc: { name: "TLC", stationId: "57391", url: "https://watch.foodnetwork.com/channel/tlc" },
  tnt: { name: "TNT", stationId: "42642", url: "https://www.tntdrama.com/watchtnt/east" },
  tntp: { name: "TNT (Pacific)", stationId: "61340", url: "https://www.tntdrama.com/watchtnt/west" },
  travel: { name: "Travel", stationId: "59303", url: "https://watch.foodnetwork.com/channel/travel-channel" },
  trutv: { name: "truTV", profile: "fullscreenApi", stationId: "64490", url: "https://www.trutv.com/watchtrutv/east" },
  usa: { channelSelector: "USA_East", name: "USA Network", stationId: "58452", url: "https://www.usanetwork.com/live" },
  usap: { channelSelector: "USA_West", name: "USA Network (Pacific)", stationId: "74030", url: "https://www.usanetwork.com/live" },
  vh1: { name: "VH1", stationId: "60046", url: "https://www.vh1.com/live-tv" }
};

// Re-export CHANNELS as PREDEFINED_CHANNELS for use in userChannels.ts where the distinction between predefined and user channels is important.
export { CHANNELS as PREDEFINED_CHANNELS };
