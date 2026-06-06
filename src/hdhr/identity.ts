/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * identity.ts: HDHomeRun device-identity constants for PrismCast.
 *
 * Real HDHomeRun devices identify themselves via a small set of model strings that clients use to gate behavior - "ModelNumber" appears in /discover.json,
 * "/sys/model" and "/sys/hwmodel" come back over the UDP control plane on port 65001, and "/sys/copyright" is what hdhomerun_config and similar tools print
 * when probing a device. PrismCast emulates the HDHomeRun CONNECT DUO (an HTTP-streaming model) because the HTTP-streaming family is what every modern HDHR-
 * aware client expects, and because that model has no DRM context to fake. Three call sites consume these strings: the HTTP discovery endpoints (device.xml,
 * discover.json), the UDP Get-request responder (/sys/* keys), and the UPnP UDN. This module is the single source of truth so a future model change is a
 * one-line edit, not a sweep across every consumer.
 *
 * The hwmodel/model strings ("hdhomerun3_atsc") assert "ATSC tuner" which PrismCast manifestly is not - but every HDTC-2US in the wild returns this same value
 * and many clients filter on it. Cohesion with the existing ModelNumber wins over literal accuracy. If a future client gains a "browser tuner" device type the
 * strings here are the only place that needs to change.
 */

/**
 * Display name shown in client tuner pickers when probing PrismCast's HTTP discovery. Identifies the *manufacturer* of the emulated device, separate from
 * PrismCast's product identity used in CONFIG.hdhr.friendlyName (which the operator can customize).
 */
export const HDHR_MANUFACTURER = "PrismCast";

/**
 * Hardware model string returned by the UDP control plane for /sys/hwmodel queries. Matches the real HDTC-2US value so clients that filter device lists by
 * hwmodel ("show only ATSC tuners", "show only Connect-family devices") accept PrismCast.
 */
export const HDHR_HW_MODEL = "hdhomerun3_atsc";

/**
 * Software model string returned by the UDP control plane for /sys/model queries. Identical to the hardware model for the HDTC-2US generation; real Connect
 * devices report the same string for both keys.
 */
export const HDHR_MODEL = HDHR_HW_MODEL;

/**
 * HDHomeRun CONNECT DUO model identifier returned in /discover.json's ModelNumber field. The CONNECT DUO is an HTTP-streaming model - clients that see this
 * model expect to tune by HTTP fetching the lineup URL's stream entries, which is exactly how PrismCast serves video.
 */
export const HDHR_MODEL_NUMBER = "HDTC-2US";

/**
 * Firmware family identifier returned in /discover.json's FirmwareName field. The "atsc" suffix matches the hwmodel/model strings; the version is reported
 * separately as PrismCast's package version so operators can correlate the running PrismCast build with HDHR-aware client tuner pages.
 */
export const HDHR_FIRMWARE_NAME = "hdhomeruntc_atsc";

/**
 * Copyright string returned by the UDP control plane for /sys/copyright queries and matching the prose tools like hdhomerun_config print when probing a
 * device. Sentence form so the value reads cleanly when surfaced in a status display.
 */
export const HDHR_COPYRIGHT = "Copyright(C) HJD - PrismCast HDHomeRun emulation.";

/**
 * UDP discovery protocol uses a 4-byte device-type tag to classify the device. The single tuner-class value is 0x00000001; PrismCast advertises itself as a
 * tuner so HDHR-aware clients route to their tuner-handling code paths rather than to repeater or storage paths used by other SiliconDust products.
 */
export const HDHR_DEVICE_TYPE_TUNER = 0x00000001;
