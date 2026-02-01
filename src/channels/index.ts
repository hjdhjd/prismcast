/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Channel definitions for PrismCast.
 */
import type { Channel, ChannelMap } from "../types/index.js";

/*
 * CHANNEL DEFINITIONS
 *
 * The channels object maps short channel names to their streaming URLs and optional profile hints. Users can request streams using simple names like /stream/nbc
 * instead of remembering full URLs. The optional profile property allows channels to explicitly declare which site profile to use, which is useful when the URL
 * domain does not match the expected behavior. Channels without an explicit profile use URL-based profile detection.
 */

export const CHANNELS: ChannelMap = {

  // Sites with multiple video elements requiring ready-state selection. ABC station ID varies by local affiliate.
  abc: { name: "ABC", profile: "keyboardMultiVideo", url: "https://abc.com/watch-live/b2f23a6e-a2a4-4d63-bd3b-e330921b0942" },

  // A&E family channels use standard players that work with the default profile.
  ae: { name: "A&E", stationId: "51529", url: "https://play.aetv.com/live" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  ahc: { name: "American Heroes", profile: "fullscreenApi", stationId: "78808", url: "https://watch.foodnetwork.com/channel/ahc" },
  animal: { name: "Animal Planet", profile: "fullscreenApi", stationId: "57394", url: "https://watch.foodnetwork.com/channel/animal-planet" },

  // Keyboard fullscreen sites with dynamic content loading requiring network idle wait.
  bravo: { name: "Bravo", profile: "keyboardDynamic", stationId: "58625", url: "https://www.nbc.com/live?brand=bravo&callsign=bravo_east" },
  bravop: { name: "Bravo (Pacific)", profile: "keyboardDynamic", stationId: "73994", url: "https://www.nbc.com/live?brand=bravo&callsign=bravo_west" },

  // Keyboard fullscreen sites with iframe-embedded players. CBS station ID varies by local affiliate.
  cbs: { name: "CBS", profile: "keyboardIframe", url: "https://www.cbs.com/live-tv/stream" },

  // CNBC and CNN sites use standard players.
  cnbc: { name: "CNBC", stationId: "58780", url: "https://www.cnbc.com/live-tv" },
  // cnbc: { channelSelector: "CNBC_US", name: "CNBC", profile: "keyboardDynamicMultiVideo", stationId: "58780", url: "https://www.usanetwork.com/live" },
  cnn: { name: "CNN", stationId: "58646", url: "https://www.cnn.com/videos/cnn" },
  cnni: { name: "CNN International", stationId: "83110", url: "https://www.cnn.com/videos/cnn-i" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  cooking: { name: "Cooking", profile: "fullscreenApi", stationId: "68065", url: "https://watch.foodnetwork.com/channel/cooking-channel" },

  // C-SPAN channels use the Brightcove player profile.
  cspan: { name: "C-SPAN", profile: "brightcove", stationId: "68344", url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span" },
  cspan2: { name: "C-SPAN 2", profile: "brightcove", stationId: "68334", url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span-2" },
  cspan3: { name: "C-SPAN 3", profile: "brightcove", stationId: "68332", url: "https://www.c-span.org/networks/?autoplay=true&channel=c-span-3" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  discovery: { name: "Discovery", profile: "fullscreenApi", stationId: "56905", url: "https://watch.foodnetwork.com/channel/discovery" },
  discoverylife: { name: "Discovery Life", profile: "fullscreenApi", stationId: "92204", url: "https://watch.foodnetwork.com/channel/discovery-life" },
  discoveryturbo: { name: "Discovery Turbo", profile: "fullscreenApi", stationId: "31046", url: "https://watch.foodnetwork.com/channel/motortrend" },

  // Multi-channel keyboard players requiring channel selection from the UI.
  e: { channelSelector: "E-_East", name: "E!", profile: "keyboardDynamicMultiVideo", stationId: "61812", url: "https://www.usanetwork.com/live" },
  ep: { channelSelector: "E-_West", name: "E! (Pacific)", profile: "keyboardDynamicMultiVideo", stationId: "91579", url: "https://www.usanetwork.com/live" },

  // Disney+ live channels using tile-based channel selection from the shared live TV page.
  espn: {

    channelSelector: "poster_linear_espn_none", name: "ESPN", profile: "apiMultiVideo", stationId: "32645",
    url: "https://www.disneyplus.com/browse/live"
  },
  espn2: {

    channelSelector: "poster_linear_espn2_none", name: "ESPN2", profile: "apiMultiVideo", stationId: "45507",
    url: "https://www.disneyplus.com/browse/live"
  },
  espnews: {

    channelSelector: "poster_linear_espnews_none", name: "ESPNews", profile: "apiMultiVideo", stationId: "59976",
    url: "https://www.disneyplus.com/browse/live"
  },

  // Iframe-embedded players with multiple video elements requiring network idle wait.
  fbc: { name: "Fox Business", profile: "embeddedDynamicMultiVideo", stationId: "58718", url: "https://www.foxbusiness.com/video/5640669329001" },
  fnc: { name: "Fox News", profile: "embeddedDynamicMultiVideo", stationId: "60179", url: "https://www.foxnews.com/video/5614615980001" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  food: { name: "Food Network", profile: "fullscreenApi", stationId: "50747", url: "https://watch.foodnetwork.com/channel/food-network" },

  // France24 channels require volume locking to prevent auto-muting.
  france24: { name: "France 24", profile: "embeddedVolumeLock", stationId: "60961", url: "https://www.france24.com/en/live" },
  france24fr: { name: "France 24 (French)", profile: "embeddedVolumeLock", stationId: "58685", url: "https://www.france24.com/fr/direct" },

  // Keyboard fullscreen sites with multiple video elements requiring ready-state selection.
  fx: { name: "FX", profile: "keyboardMultiVideo", stationId: "58574", url: "https://abc.com/watch-live/93256af4-5e80-4558-aa2e-2bdfffa119a0" },
  fxm: { name: "FXM", profile: "keyboardMultiVideo", stationId: "70253", url: "https://abc.com/watch-live/d298ab7e-c6b1-4efa-ac6e-a52dceed92ee" },
  fxp: { name: "FX (Pacific)", profile: "keyboardMultiVideo", stationId: "59814", url: "https://abc.com/watch-live/2cee3401-f63b-42d0-b32e-962fef610b9e" },
  fxx: { name: "FXX", profile: "keyboardMultiVideo", stationId: "66379", url: "https://abc.com/watch-live/49f4a471-8d36-4728-8457-ea65cbbc84ea" },
  fxxp: { name: "FXX (Pacific)", profile: "keyboardMultiVideo", stationId: "82571", url: "https://abc.com/watch-live/e4c83395-62ed-4a49-829a-c55ab3c33e7d" },

  // A&E family channels use standard players that work with the default profile.
  fyi: { name: "FYI", stationId: "58988", url: "https://play.fyi.tv/live" },

  // Golf Channel has its own standalone site using the fullscreen API profile.
  golf: { name: "Golf", profile: "fullscreenApi", stationId: "61854", url: "https://www.golfchannel.com/watch/live" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  hgtv: { name: "HGTV", profile: "fullscreenApi", stationId: "49788", url: "https://watch.foodnetwork.com/channel/hgtv" },

  // A&E family channels use standard players that work with the default profile.
  history: { name: "History", stationId: "14771", url: "https://play.history.com/live" },

  // CNBC and CNN sites use standard players.
  hln: { name: "HLN", stationId: "64549", url: "https://www.cnn.com/videos/hln" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  id: { name: "Investigation Discovery", profile: "fullscreenApi", stationId: "65342", url: "https://watch.foodnetwork.com/channel/investigation-discovery" },

  // A&E family channels use standard players that work with the default profile.
  lifetime: { name: "Lifetime", stationId: "60150", url: "https://play.mylifetime.com/live" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  magnolia: { name: "Magnolia Network", profile: "fullscreenApi", stationId: "67375", url: "https://watch.foodnetwork.com/channel/magnolia-network-preview-atve-us" },

  // Keyboard fullscreen sites with dynamic content loading requiring network idle wait.
  msnow: { name: "MSNOW", profile: "keyboardDynamic", stationId: "64241", url: "https://www.ms.now/live" },
  // msnow: { channelSelector: "image-23", name: "MSNOW", profile: "keyboardDynamicMultiVideo", stationId: "64241", url: "https://www.usanetwork.com/live" },

  // National Geographic channels use keyboard fullscreen with dynamic content and multiple video elements.
  natgeo: {

    name: "National Geographic", profile: "keyboardDynamicMultiVideo", stationId: "49438",
    url: "https://www.nationalgeographic.com/tv/watch-live/0826a9a3-3384-4bb5-8841-91f01cb0e3a7"
  },
  natgeop: {

    name: "National Geographic (Pacific)", profile: "keyboardDynamicMultiVideo", stationId: "71601",
    url: "https://www.nationalgeographic.com/tv/watch-live/91456580-f32f-417c-8e1a-9f82640832a7"
  },
  natgeowild: {

    name: "Nat Geo Wild", profile: "keyboardDynamicMultiVideo", stationId: "67331",
    url: "https://www.nationalgeographic.com/tv/watch-live/239b9590-583f-4955-a499-22e9eefff9cf"
  },

  // Keyboard fullscreen sites with dynamic content loading. NBC station ID varies by local affiliate.
  nbc: { name: "NBC", profile: "keyboardDynamic", url: "https://www.nbc.com/live?brand=nbc&callsign=nbc" },
  nbcnews: {

    name: "NBC News Now", profile: "keyboardDynamic", stationId: "114174",
    url: "https://www.nbc.com/live?brand=nbc-news&callsign=nbcnews"
  },
  nbcsbayarea: {

    name: "NBC Sports Bay Area", profile: "keyboardDynamic", stationId: "63138",
    url: "https://www.nbc.com/live?brand=rsn-bay-area&callsign=nbcsbayarea"
  },
  nbcsboston: {

    name: "NBC Sports Boston", profile: "keyboardDynamic", stationId: "49198",
    url: "https://www.nbc.com/live?brand=rsn-boston&callsign=nbcsboston"
  },
  nbcscalifornia: {

    name: "NBC Sports California", profile: "keyboardDynamic", stationId: "45540",
    url: "https://www.nbc.com/live?brand=rsn-california&callsign=nbcscalifornia"
  },
  nbcschicago: {

    name: "NBC Sports Chicago", profile: "keyboardDynamic", stationId: "44905",
    url: "https://www.nbc.com/live?brand=rsn-chicago&callsign=nbcschicago"
  },
  nbcsphiladelphia: {

    name: "NBC Sports Philadelphia", profile: "keyboardDynamic", stationId: "32571",
    url: "https://www.nbc.com/live?brand=rsn-philadelphia&callsign=nbcsphiladelphia"
  },
  necn: { name: "NECN", profile: "keyboardDynamic", stationId: "66278", url: "https://www.nbc.com/live?brand=necn&callsign=necn" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  own: { name: "OWN", profile: "fullscreenApi", stationId: "70388", url: "https://watch.foodnetwork.com/channel/own" },

  // Multi-channel keyboard players requiring channel selection from the UI.
  oxygen: { channelSelector: "Oxygen_East", name: "Oxygen", profile: "keyboardDynamicMultiVideo", stationId: "70522", url: "https://www.usanetwork.com/live" },
  oxygenp: { channelSelector: "Oxygen_West", name: "Oxygen (Pacific)", profile: "keyboardDynamicMultiVideo", stationId: "74032", url: "https://www.usanetwork.com/live" },

  // WTTW PBS uses a standard player.
  pbschicago: { name: "PBS Chicago (WTTW)", stationId: "30415", url: "https://www.wttw.com/wttw-live-stream" },

  // PBS station Lakeshore uses an embedded player in iframe.
  pbslakeshore: { name: "PBS Lakeshore (WYIN)", profile: "embeddedPlayer", stationId: "49237", url: "https://video.lakeshorepbs.org/livestream" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  science: { name: "Science", profile: "fullscreenApi", stationId: "57390", url: "https://watch.foodnetwork.com/channel/science" },

  // Paramount Plus / Showtime uses a standard player.
  showtime: { name: "Showtime", stationId: "91620", url: "https://www.paramountplus.com/live-tv/stream/showtime-east" },
  showtimep: { name: "Showtime (Pacific)", stationId: "91621", url: "https://www.paramountplus.com/live-tv/stream/showtime-west" },

  // Multi-channel keyboard players requiring channel selection from the UI.
  syfy: { channelSelector: "Syfy_East", name: "Syfy", profile: "keyboardDynamicMultiVideo", stationId: "58623", url: "https://www.usanetwork.com/live" },
  syfyp: { channelSelector: "Syfy_West", name: "Syfy (Pacific)", profile: "keyboardDynamicMultiVideo", stationId: "65626", url: "https://www.usanetwork.com/live" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  tbs: { name: "TBS", profile: "fullscreenApi", stationId: "58515", url: "https://www.tbs.com/watchtbs/east" },
  tbsp: { name: "TBS (Pacific)", profile: "fullscreenApi", stationId: "67890", url: "https://www.tbs.com/watchtbs/west" },
  tlc: { name: "TLC", profile: "fullscreenApi", stationId: "57391", url: "https://watch.foodnetwork.com/channel/tlc" },
  tnt: { name: "TNT", profile: "fullscreenApi", stationId: "42642", url: "https://www.tntdrama.com/watchtnt/east" },
  tntp: { name: "TNT (Pacific)", profile: "fullscreenApi", stationId: "61340", url: "https://www.tntdrama.com/watchtnt/west" },
  travel: { name: "Travel", profile: "fullscreenApi", stationId: "59303", url: "https://watch.foodnetwork.com/channel/travel-channel" },
  trutv: { name: "truTV", profile: "fullscreenApi", stationId: "64490", url: "https://www.trutv.com/watchtrutv/east" },

  // Multi-channel keyboard players requiring channel selection from the UI.
  usa: { channelSelector: "USA_East", name: "USA Network", profile: "keyboardDynamicMultiVideo", stationId: "58452", url: "https://www.usanetwork.com/live" },
  usap: { channelSelector: "USA_West", name: "USA Network (Pacific)", profile: "keyboardDynamicMultiVideo", stationId: "74030", url: "https://www.usanetwork.com/live" },

  // Discovery and Warner Brothers Discovery family channels use the fullscreen API profile.
  vh1: { name: "VH1", profile: "fullscreenApi", stationId: "60046", url: "https://www.vh1.com/live-tv" }
};

/**
 * Gets a channel by name.
 * @param name - The channel key name.
 * @returns The channel configuration or undefined if not found.
 */
export function getChannel(name: string): Channel | undefined {

  return CHANNELS[name];
}

/**
 * Returns the total number of configured channels.
 * @returns The channel count.
 */
export function getChannelCount(): number {

  return Object.keys(CHANNELS).length;
}

/**
 * Returns all channel names sorted alphabetically.
 * @returns Array of channel key names.
 */
export function getChannelNames(): string[] {

  return Object.keys(CHANNELS).sort();
}

// Re-export CHANNELS as PREDEFINED_CHANNELS for use in userChannels.ts where the distinction between predefined and user channels is important.
export { CHANNELS as PREDEFINED_CHANNELS };
