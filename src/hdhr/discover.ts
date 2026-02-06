/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * discover.ts: HDHomeRun discovery and lineup endpoints for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { CONFIG } from "../config/index.js";
import { buildChannelMap } from "./channelMap.js";
import { getAllStreams } from "../streaming/registry.js";
import { getPackageVersion } from "../utils/index.js";

/* These endpoints implement the HDHomeRun HTTP API that Plex and other clients use to identify, configure, and monitor tuners. Plex does not auto-detect emulated
 * tuners on non-standard ports — users must manually enter the address (IP:port) in Plex's DVR setup. The core discovery endpoints are device.xml (UPnP device
 * description), discover.json (device identity), lineup.json (channel lineup), and lineup_status.json (scan status). Additional endpoints include lineup.post
 * (scan control acknowledgement) and status.json (real-time tuner activity for monitoring dashboards).
 *
 * The stream URLs in lineup.json point to PrismCast's MPEG-TS streaming endpoint on the main HTTP server, not to this HDHR server. Plex requests the MPEG-TS stream
 * directly from the main server, which remuxes fMP4 segments to MPEG-TS with codec copy.
 */

/**
 * Tuner status entry for the /status.json endpoint. Active tuners include channel info and signal stats; idle tuners have only Resource.
 */
interface TunerStatusEntry {

  // RF tuning frequency in Hz. Always 0 for IP-based tuners.
  Frequency?: number;

  // Tuner identifier (e.g., "tuner0", "tuner1").
  Resource: string;

  // Signal quality percentage (0-100). Always 100 for network streams.
  SignalQualityPercent?: number;

  // Signal strength percentage (0-100). Always 100 for network streams.
  SignalStrengthPercent?: number;

  // Symbol quality percentage (0-100). Always 100 for network streams.
  SymbolQualityPercent?: number;

  // Client IP address receiving the stream.
  TargetIP?: string;

  // Channel display name (e.g., "CNN International").
  VctName?: string;

  // Numeric channel number as string (e.g., "7009").
  VctNumber?: string;
}

/**
 * Resolves the hostname from an incoming request for use in generated URLs. We extract the hostname from the request (which Plex already connected to) and
 * combine it with the appropriate port. This ensures URLs work from Plex's network perspective.
 * @param req - The Express request object.
 * @returns The hostname (without port) that the client used to connect.
 */
function resolveHostname(req: Request): string {

  // Check X-Forwarded-Host first (reverse proxy scenarios), then fall back to the Host header.
  const forwardedHost = req.get("x-forwarded-host");
  const hostHeader = forwardedHost ? forwardedHost.split(",")[0].trim() : req.get("host");

  if(hostHeader) {

    // Strip port from host header if present (e.g., "192.168.1.100:5004" -> "192.168.1.100").
    const bracketIndex = hostHeader.indexOf("]");

    // Handle IPv6 addresses in brackets (e.g., "[::1]:5004").
    if(bracketIndex !== -1) {

      return hostHeader.substring(0, bracketIndex + 1);
    }

    const colonIndex = hostHeader.lastIndexOf(":");

    return (colonIndex !== -1) ? hostHeader.substring(0, colonIndex) : hostHeader;
  }

  // Fallback to configured server host.
  return CONFIG.server.host;
}

/**
 * Sets up the HDHomeRun discovery and lineup endpoints on the given Express app.
 * @param app - The Express application for the HDHR server.
 */
export function setupHdhrEndpoints(app: Express): void {

  // GET /device.xml - UPnP device description. Plex fetches this during tuner discovery before querying discover.json. Without a valid device.xml response,
  // Plex may silently abort the discovery process.
  app.get("/device.xml", (req: Request, res: Response): void => {

    const hostname = resolveHostname(req);
    const baseUrl = "http://" + hostname + ":" + String(CONFIG.hdhr.port);
    const deviceId = CONFIG.hdhr.deviceId.toUpperCase();

    const xml = [
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
      "<root xmlns=\"urn:schemas-upnp-org:device-1-0\">",
      "  <specVersion>",
      "    <major>1</major>",
      "    <minor>0</minor>",
      "  </specVersion>",
      "  <URLBase>" + baseUrl + "</URLBase>",
      "  <device>",
      "    <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>",
      "    <friendlyName>" + CONFIG.hdhr.friendlyName + "</friendlyName>",
      "    <manufacturer>PrismCast</manufacturer>",
      "    <modelName>HDTC-2US</modelName>",
      "    <modelNumber>HDTC-2US</modelNumber>",
      "    <serialNumber>" + deviceId + "</serialNumber>",
      "    <UDN>uuid:" + deviceId + "</UDN>",
      "  </device>",
      "</root>"
    ].join("\n");

    res.set("Content-Type", "text/xml");
    res.send(xml);
  });

  // GET /discover.json - Device identity and capabilities. Plex uses this to identify the tuner model, determine concurrent stream capacity, and locate the
  // lineup endpoint.
  app.get("/discover.json", (req: Request, res: Response): void => {

    const hostname = resolveHostname(req);
    const baseUrl = "http://" + hostname + ":" + String(CONFIG.hdhr.port);

    // The response follows the HDHomeRun HTTP API format that Plex expects. DeviceAuth must be non-empty; we use the DeviceID since there's no DRM context.
    // ModelNumber "HDTC-2US" is the HDHomeRun CONNECT DUO, a widely-supported model that Plex recognizes.
    res.json({

      BaseURL: baseUrl,
      DeviceAuth: CONFIG.hdhr.deviceId.toUpperCase(),
      DeviceID: CONFIG.hdhr.deviceId.toUpperCase(),
      FirmwareName: "hdhomeruntc_atsc",
      FirmwareVersion: getPackageVersion(),
      FriendlyName: CONFIG.hdhr.friendlyName,
      LineupURL: baseUrl + "/lineup.json",
      Manufacturer: "PrismCast",
      ModelNumber: "HDTC-2US",
      TunerCount: CONFIG.streaming.maxConcurrentStreams
    });
  });

  // GET /lineup.json - Channel lineup with stream URLs. Each entry maps a numeric channel number to an MPEG-TS stream URL on the main PrismCast server. Plex
  // requests the MPEG-TS stream directly from the main server when tuning a channel.
  app.get("/lineup.json", (req: Request, res: Response): void => {

    const hostname = resolveHostname(req);
    const mainBaseUrl = "http://" + hostname + ":" + String(CONFIG.server.port);
    const channelMap = buildChannelMap();

    const lineup = channelMap.map((entry) => ({

      AudioCodec: "AAC",
      GuideName: entry.name,
      GuideNumber: String(entry.number),
      HD: 1,
      URL: mainBaseUrl + "/stream/" + entry.key,
      VideoCodec: "H264"
    }));

    res.json(lineup);
  });

  // GET /lineup_status.json - Channel scan status. Plex checks this during tuner setup. We return a static response indicating scan is complete since PrismCast's
  // channels are configured, not scanned.
  app.get("/lineup_status.json", (_req: Request, res: Response): void => {

    res.json({

      ScanInProgress: 0,
      ScanPossible: 1,
      Source: "Cable",
      SourceList: ["Cable"]
    });
  });

  // POST /lineup.post - Channel scan control. Some clients (Plex during initial setup) POST scan=start to this endpoint. We return 200 OK since PrismCast's channels
  // are statically configured and scanning is not applicable.
  app.post("/lineup.post", (_req: Request, res: Response): void => {

    res.sendStatus(200);
  });

  // GET /status.json - Tuner status. Returns a JSON array with one entry per tuner slot. Active tuners include channel info and signal stats; idle tuners have only
  // Resource. Monitoring dashboards (Home Assistant, Homepage) poll this endpoint to display real-time tuner activity.
  app.get("/status.json", (_req: Request, res: Response): void => {

    const channelMap = buildChannelMap();
    const channelByKey = new Map(channelMap.map((entry) => [ entry.key, entry ]));
    const streams = getAllStreams().sort((a, b) => a.id - b.id);
    const tunerCount = CONFIG.streaming.maxConcurrentStreams;
    const tuners: TunerStatusEntry[] = [];

    // Build active tuner entries from currently running streams.
    for(let i = 0; i < streams.length; i++) {

      const stream = streams[i];
      const channelEntry = channelByKey.get(stream.info.storeKey);

      // Build the tuner entry. Active tuners include channel info and signal stats. Signal values are hardcoded at 100 since PrismCast streams are network-based and
      // either working or not — there is no analog signal quality to report.
      const tuner: TunerStatusEntry = {

        Frequency: 0,
        Resource: "tuner" + String(i),
        SignalQualityPercent: 100,
        SignalStrengthPercent: 100,
        SymbolQualityPercent: 100
      };

      // Add channel info if available. Prefer the channel map for VctNumber (numeric channel) and VctName (display name). Fall back to stream.channelName for VctName
      // if the channel was removed from the map after the stream started.
      if(channelEntry) {

        tuner.VctName = channelEntry.name;
        tuner.VctNumber = String(channelEntry.number);
      } else if(stream.channelName) {

        tuner.VctName = stream.channelName;
      }

      // Add client address if known. Normalize IPv6-mapped IPv4 addresses (::ffff:192.168.1.1 → 192.168.1.1).
      if(stream.clientAddress) {

        tuner.TargetIP = stream.clientAddress.startsWith("::ffff:") ? stream.clientAddress.slice(7) : stream.clientAddress;
      }

      tuners.push(tuner);
    }

    // Fill remaining slots with idle tuner entries.
    for(let i = streams.length; i < tunerCount; i++) {

      tuners.push({ Resource: "tuner" + String(i) });
    }

    res.json(tuners);
  });
}
