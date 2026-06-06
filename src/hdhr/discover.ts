/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * discover.ts: HDHomeRun discovery and lineup endpoints for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { HDHR_FIRMWARE_NAME, HDHR_MANUFACTURER, HDHR_MODEL_NUMBER } from "./identity.ts";
import { CONFIG } from "../config/index.ts";
import { buildChannelMap } from "./channelMap.ts";
import { getPackageVersion } from "../utils/index.ts";
import { getTunerStates } from "./tunerState.ts";

/* These endpoints implement the HDHomeRun HTTP API that Plex uses to identify, configure, and monitor tuners. Plex auto-detects PrismCast on the LAN through
 * the UDP responder in udp.ts (when CONFIG.hdhr.discoveryEnabled is on) or accepts a manual IP:port via its DVR setup screen. Channels DVR and other HDHR-
 * aware clients may also reach these endpoints; their compatibility is incidental rather than supported, and Channels DVR specifically expects the HTTP
 * control plane on port 80 (see hdhr/index.ts for the discovery-flow rationale). The core endpoints are device.xml (UPnP device description), discover.json
 * (device identity), lineup.json (channel lineup), and lineup_status.json (scan status). Additional endpoints include lineup.post (scan control
 * acknowledgement) and status.json (real-time tuner activity for monitoring dashboards).
 *
 * The stream URLs in lineup.json point to PrismCast's MPEG-TS streaming endpoint on the main HTTP server, not to this HDHR server. The client requests the
 * MPEG-TS stream directly from the main server, which remuxes fMP4 segments to MPEG-TS with codec copy.
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
 * Resolves the hostname from an incoming request for use in generated URLs. We extract the hostname from the request (which the client already connected
 * to) and combine it with the appropriate port. This ensures URLs work from the client's network perspective.
 * @param req - The Express request object.
 * @returns The hostname (without port) that the client used to connect.
 */
function resolveHostname(req: Request): string {

  // Check X-Forwarded-Host first (reverse proxy scenarios), then fall back to the Host header.
  const forwardedHost = req.get("x-forwarded-host");
  const hostHeader = forwardedHost ? (forwardedHost.split(",")[0] ?? "").trim() : req.get("host");

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

  // GET /device.xml - UPnP device description. HDHR-aware clients (Plex in particular) fetch this during tuner discovery before querying discover.json.
  // Without a valid device.xml response the discovery process may abort silently on stricter clients.
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
      "    <manufacturer>" + HDHR_MANUFACTURER + "</manufacturer>",
      "    <modelName>" + HDHR_MODEL_NUMBER + "</modelName>",
      "    <modelNumber>" + HDHR_MODEL_NUMBER + "</modelNumber>",
      "    <serialNumber>" + deviceId + "</serialNumber>",
      "    <UDN>uuid:" + deviceId + "</UDN>",
      "  </device>",
      "</root>"
    ].join("\n");

    res.set("Content-Type", "text/xml");
    res.send(xml);
  });

  // GET /discover.json - Device identity and capabilities. Clients use this to identify the tuner model, determine concurrent stream capacity, and locate the
  // lineup endpoint.
  app.get("/discover.json", (req: Request, res: Response): void => {

    const hostname = resolveHostname(req);
    const baseUrl = "http://" + hostname + ":" + String(CONFIG.hdhr.port);

    // The response follows the HDHomeRun HTTP API format that Plex expects. DeviceAuth must be non-empty; we use the DeviceID since there is no DRM context.
    // Identity strings come from hdhr/identity.ts so a future model swap is a single-file edit.
    res.json({

      BaseURL: baseUrl,
      DeviceAuth: CONFIG.hdhr.deviceId.toUpperCase(),
      DeviceID: CONFIG.hdhr.deviceId.toUpperCase(),
      FirmwareName: HDHR_FIRMWARE_NAME,
      FirmwareVersion: getPackageVersion(),
      FriendlyName: CONFIG.hdhr.friendlyName,
      LineupURL: baseUrl + "/lineup.json",
      Manufacturer: HDHR_MANUFACTURER,
      ModelNumber: HDHR_MODEL_NUMBER,
      TunerCount: CONFIG.streaming.maxConcurrentStreams
    });
  });

  // GET /lineup.json - Channel lineup with stream URLs. Each entry maps a numeric channel number to an MPEG-TS stream URL on the main PrismCast server. The
  // client requests the MPEG-TS stream directly from the main server when tuning a channel.
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

  // GET /lineup_status.json - Channel scan status. Clients check this during tuner setup. We return a static response indicating scan is complete since PrismCast's
  // channels are configured, not scanned.
  app.get("/lineup_status.json", (_req: Request, res: Response): void => {

    res.json({

      ScanInProgress: 0,
      ScanPossible: 1,
      Source: "Cable",
      SourceList: ["Cable"]
    });
  });

  // POST /lineup.post - Channel scan control. Some clients (Plex during initial setup is the common case) POST scan=start here. We return 200 OK since
  // PrismCast's channels are statically configured and scanning is not applicable.
  app.post("/lineup.post", (_req: Request, res: Response): void => {

    res.sendStatus(200);
  });

  // GET /status.json - Tuner status. Returns a JSON array with one entry per tuner slot. Active tuners include channel info and signal stats; idle tuners have only
  // Resource. Monitoring dashboards (Home Assistant, Homepage) poll this endpoint to display real-time tuner activity. The slot-indexed projection comes from
  // getTunerStates - the same SSOT the UDP Get handlers use - so any change to channel-merge or fallback logic lands in one place.
  app.get("/status.json", (_req: Request, res: Response): void => {

    const tuners: TunerStatusEntry[] = getTunerStates().map((state) => {

      // Idle slots carry only the resource name; active slots include signal stats hardcoded at 100 (network streams have no analog signal quality), plus
      // channel and client information whenever the underlying TunerState provides it.
      if(!state.active) {

        return { Resource: state.resource };
      }

      return {

        Frequency: 0,
        Resource: state.resource,
        SignalQualityPercent: 100,
        SignalStrengthPercent: 100,
        SymbolQualityPercent: 100,
        ...((state.channelNumber !== null) ? { VctNumber: String(state.channelNumber) } : {}),
        ...(state.channelName ? { VctName: state.channelName } : {}),
        ...(state.clientAddress ? { TargetIP: state.clientAddress } : {})
      };
    });

    res.json(tuners);
  });
}
