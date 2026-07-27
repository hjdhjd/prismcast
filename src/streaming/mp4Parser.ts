/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mp4Parser.ts: Low-level MP4 box parsing for PrismCast.
 */
import type { Nullable } from "../types/index.ts";

/* MP4 files consist of a sequence of "boxes" (also called "atoms"). Each box has a simple structure:
 *
 * - 4 bytes: size (big-endian uint32) - total box size including header
 * - 4 bytes: type (4 ASCII characters, e.g., 'ftyp', 'moov', 'moof', 'mdat')
 * - (size - 8) bytes: payload
 *
 * Special case: when size == 1, the next 8 bytes contain a 64-bit extended size.
 *
 * This parser handles streaming input - data arrives in chunks, and we buffer incomplete boxes until we have enough data to emit a complete box.
 *
 * Container boxes like moof and traf contain child boxes in their payload. The iterateChildBoxes() function walks these children, and detectMoofKeyframe() uses it to
 * parse traf -> tfhd/trun structures for keyframe detection. This supports the fMP4 segmenter's ability to track keyframe frequency and verify that segments start with
 * sync samples. The offsetMoofTimestamps() function applies a constant per-track offset to Chrome's original timestamps, preserving Chrome's wall-clock-based
 * inter-track synchronization while bridging PTS discontinuities at tab replacement boundaries.
 */

// Types.

/**
 * Represents a complete MP4 box with its type and data.
 */
export interface MP4Box {

  // The complete box data including header.
  data: Buffer;

  // The box size in bytes.
  size: number;

  // The 4-character box type (e.g., 'ftyp', 'moov', 'moof', 'mdat').
  type: string;
}

/**
 * Callback invoked when a complete box is parsed.
 */
export type MP4BoxCallback = (box: MP4Box) => void;

/**
 * MP4 box parser that handles streaming input.
 */
export interface MP4BoxParser {

  // Flush any remaining buffered data (for cleanup).
  flush: () => void;

  // Push a chunk of data into the parser.
  push: (chunk: Buffer) => void;
}

// Constants.

// Minimum header size: 4 bytes size + 4 bytes type.
const MIN_HEADER_SIZE = 8;

// Extended header size: 4 bytes size (==1) + 4 bytes type + 8 bytes extended size.
const EXTENDED_HEADER_SIZE = 16;

/* Sane ceiling on a single declared box size, and by extension on the pending (un-emitted) buffer. fMP4 fragments captured from Chrome's MediaRecorder are
 * kilobytes to low-megabytes (a moof + mdat for a couple of seconds of 1080p video), so a box claiming to be larger than this is corrupt framing rather than
 * legitimate media. This ceiling is the single mechanism that bounds memory: processBuffer() only parks bytes while it waits for the rest of an in-progress box,
 * and it refuses to wait for any box whose declared size exceeds the ceiling - so the resident buffer can never exceed this ceiling plus one inbound chunk.
 * Resyncing (the malformed-size paths below) only ever shrinks the buffer, so there is no second accumulation path to guard against. Without this ceiling a single
 * malformed size field (e.g., a bit-flip that turns a 0x0000XXXX size into 0x40000000) would make the parser buffer incoming chunks toward a box that never
 * completes, leaking memory for the lifetime of the stream. When a declared size exceeds the ceiling we treat the framing as lost and resync one byte at a time,
 * exactly as we do for the other malformed-size cases below.
 */
const MAX_BOX_SIZE = 64 * 1024 * 1024;

// Streaming Parser.

/**
 * Creates an MP4 box parser that processes streaming input. The parser buffers incomplete boxes and invokes the callback when a complete box is available.
 * @param onBox - Callback invoked for each complete box.
 * @returns The parser interface with push and flush methods.
 */
export function createMP4BoxParser(onBox: MP4BoxCallback): MP4BoxParser {

  // Buffer for accumulating incomplete data.
  let buffer = Buffer.alloc(0);

  /**
   * Attempts to parse and emit complete boxes from the buffer.
   */
  function processBuffer(): void {

    // Keep parsing while we have enough data for at least a header.
    while(buffer.length >= MIN_HEADER_SIZE) {

      // Read the size field (first 4 bytes).
      const sizeField = buffer.readUInt32BE(0);

      // Determine actual box size.
      let boxSize: number;
      let headerSize: number;

      if(sizeField === 1) {

        // Extended size: need 16 bytes for full header.
        if(buffer.length < EXTENDED_HEADER_SIZE) {

          // Not enough data yet for extended header.
          return;
        }

        // Read 64-bit extended size. For practical purposes, we only use the lower 32 bits since JavaScript numbers safely handle up to 2^53, and we're unlikely to
        // encounter boxes larger than 4GB in streaming scenarios.
        const extendedSizeHigh = buffer.readUInt32BE(8);
        const extendedSizeLow = buffer.readUInt32BE(12);

        // Sanity check: reject impossibly large boxes.
        if(extendedSizeHigh > 0) {

          // Box claims to be > 4GB, which is unrealistic for streaming. Skip this box by advancing 1 byte and trying again.
          buffer = buffer.subarray(1);

          continue;
        }

        boxSize = extendedSizeLow;
        headerSize = EXTENDED_HEADER_SIZE;
      } else if(sizeField === 0) {

        // Size 0 means "extends to end of file" - not applicable for streaming. Skip this byte and try again.
        buffer = buffer.subarray(1);

        continue;
      } else {

        boxSize = sizeField;
        headerSize = MIN_HEADER_SIZE;
      }

      // Sanity check: box size must be at least the header size.
      if(boxSize < headerSize) {

        // Invalid box, skip one byte and try to resync.
        buffer = buffer.subarray(1);

        continue;
      }

      // Sanity check: a box larger than MAX_BOX_SIZE is corrupt framing, not legitimate media. Without this guard the parser would buffer incoming chunks toward
      // the declared size indefinitely, waiting for a box that never completes. Treat the framing as lost and resync one byte at a time.
      if(boxSize > MAX_BOX_SIZE) {

        buffer = buffer.subarray(1);

        continue;
      }

      // Check if we have the complete box.
      if(buffer.length < boxSize) {

        // Not enough data yet.
        return;
      }

      // Extract the complete box.
      const boxData = buffer.subarray(0, boxSize);
      const boxType = buffer.toString("ascii", 4, 8);

      // Emit the box.
      onBox({

        data: Buffer.from(boxData),
        size: boxSize,
        type: boxType
      });

      // Advance the buffer past this box.
      buffer = buffer.subarray(boxSize);
    }
  }

  return {

    flush: (): void => {

      // Clear the buffer. Any remaining data is an incomplete box that we discard.
      buffer = Buffer.alloc(0);
    },

    push: (chunk: Buffer): void => {

      // Append the new chunk to our buffer.
      buffer = Buffer.concat([ buffer, chunk ]);

      // Try to parse complete boxes. The pending buffer is bounded by MAX_BOX_SIZE: processBuffer() never parks bytes toward a box larger than that ceiling, so the
      // resident buffer stays within MAX_BOX_SIZE plus this single chunk regardless of how long a source streams without completing a box.
      processBuffer();
    }
  };
}

// Nested Box Parsing.

/**
 * Iterates over the immediate child boxes within a container box's payload. Container boxes in ISO 14496-12 (moof, traf, etc.) contain a sequence of child boxes
 * starting immediately after the parent's 8-byte header. This function parses each child box header and invokes the callback with the child's type, the parent buffer,
 * and the byte offset/size of the child box within that buffer. The callback receives offsets rather than sub-buffers to avoid memory allocation in the hot path.
 * @param data - The complete parent box buffer including its own 8-byte header.
 * @param callback - Called for each child box with (type, data, offset, size). The offset and size describe the child box's position within data.
 */
export function iterateChildBoxes(data: Buffer, callback: (type: string, data: Buffer, offset: number, size: number) => void): void {

  let pos = MIN_HEADER_SIZE;

  while((pos + MIN_HEADER_SIZE) <= data.length) {

    const sizeField = data.readUInt32BE(pos);

    let boxSize: number;

    if(sizeField === 1) {

      // Extended size box. Need 16 bytes for the full header.
      if((pos + EXTENDED_HEADER_SIZE) > data.length) {

        return;
      }

      // Reject impossibly large boxes (>4GB).
      if(data.readUInt32BE(pos + 8) > 0) {

        return;
      }

      boxSize = data.readUInt32BE(pos + 12);

      // Lower-bound guard, symmetric with the non-extended branch below. An extended-size box must be at least its own 16-byte header; a declared size smaller
      // than that (most dangerously zero) is malformed. Without this check a zero size would pass the fit check below and advance pos by zero, spinning this
      // loop forever on the main thread and hanging the process. A truncated or corrupted fMP4 fragment can carry exactly these bytes, so we stop iterating.
      if(boxSize < EXTENDED_HEADER_SIZE) {

        return;
      }
    } else if((sizeField < MIN_HEADER_SIZE) || (sizeField === 0)) {

      // Invalid size or "extends to end of file" - stop iterating.
      return;
    } else {

      boxSize = sizeField;
    }

    // Ensure the child box fits within the parent.
    if((pos + boxSize) > data.length) {

      return;
    }

    const boxType = data.toString("ascii", pos + 4, pos + 8);

    callback(boxType, data, pos, boxSize);

    pos += boxSize;
  }
}

// Keyframe Detection.

/**
 * Evaluates ISO 14496-12 sample flags to determine whether a sample is a sync sample (keyframe). The sample_depends_on field (bits 25-24) is the primary indicator,
 * with sample_is_non_sync_sample (bit 16) as a secondary check.
 *
 * Sample flags layout (32 bits):
 * - Bits 31-28: reserved
 * - Bits 27-26: is_leading
 * - Bits 25-24: sample_depends_on (0=unknown, 1=dependent/not keyframe, 2=independent/keyframe)
 * - Bits 23-22: sample_is_depended_on
 * - Bits 21-20: sample_has_redundancy
 * - Bits 19-17: sample_padding_value
 * - Bit 16: sample_is_non_sync_sample (0=may be sync, 1=not sync)
 * - Bits 15-0: sample_degradation_priority
 *
 * @param flags - The 32-bit sample flags value.
 * @returns true if keyframe, false if not keyframe.
 */
function evaluateSampleFlags(flags: number): boolean {

  const sampleDependsOn = (flags >>> 24) & 0x03;
  const isNonSync = (flags >>> 16) & 0x01;

  // sample_depends_on === 1: depends on other samples. This is not independently decodable (not a keyframe).
  if(sampleDependsOn === 1) {

    return false;
  }

  // sample_depends_on === 2: does not depend on other samples. This is an independently decodable frame (keyframe).
  if(sampleDependsOn === 2) {

    return true;
  }

  // sample_is_non_sync_sample === 1: explicitly marked as not a sync sample.
  if(isNonSync === 1) {

    return false;
  }

  // sample_depends_on is unknown (0) and no non-sync marker. Per ISO 14496-12 defaults, treat as a sync sample.
  return true;
}

/**
 * Parsed fields from a tfhd (track fragment header) box. Used by keyframe detection (defaultSampleFlags) and timestamp rewriting (defaultSampleDuration, trackId).
 */
interface TfhdInfo {

  // The default duration for each sample in this fragment, in timescale units. Zero when tfhd flags bit 0x000008 is not set. Used as a fallback when trun entries
  // don't carry per-sample durations (trun flags bit 0x100 not set).
  defaultSampleDuration: number;

  // The default sample flags that apply to all samples when individual sample flags are not present in the trun. Null when tfhd flags bit 0x000020 is not set.
  defaultSampleFlags: Nullable<number>;

  // The track ID from the tfhd box. Used to index into the per-track timestamp counter map.
  trackId: number;
}

/**
 * Parses a tfhd (track fragment header) box and extracts the track ID, default sample duration, and default sample flags. This is the single source of truth for
 * tfhd field extraction, used by both detectMoofKeyframe() (for sample flags) and offsetMoofTimestamps() (for track ID and duration).
 *
 * tfhd layout (FullBox):
 * - [0-3] size, [4-7] "tfhd", [8] version, [9-11] flags, [12-15] track_ID
 * - Optional fields (in order, each present only if its flag bit is set):
 *   0x000001: base_data_offset (8 bytes)
 *   0x000002: sample_description_index (4 bytes)
 *   0x000008: default_sample_duration (4 bytes)
 *   0x000010: default_sample_size (4 bytes)
 *   0x000020: default_sample_flags (4 bytes)
 *
 * @param data - The buffer containing the tfhd box.
 * @param offset - The byte offset of the tfhd box within the buffer.
 * @param size - The total size of the tfhd box.
 * @returns The parsed tfhd fields, or null if the box is malformed.
 */
function parseTfhd(data: Buffer, offset: number, size: number): Nullable<TfhdInfo> {

  // Need at least the FullBox header (12 bytes) plus track_ID (4 bytes) = 16 bytes.
  if(size < 16) {

    return null;
  }

  const tfhdFlags = data.readUInt32BE(offset + 8) & 0x00FFFFFF;
  const trackId = data.readUInt32BE(offset + 12);

  // Walk past optional fields in order. Each field is present only if its corresponding flag bit is set.
  let pos = offset + 16;

  if(tfhdFlags & 0x000001) {

    pos += 8;
  }

  if(tfhdFlags & 0x000002) {

    pos += 4;
  }

  // Extract default_sample_duration if present.
  let defaultSampleDuration = 0;

  if(tfhdFlags & 0x000008) {

    if((pos + 4) > (offset + size)) {

      return null;
    }

    defaultSampleDuration = data.readUInt32BE(pos);
    pos += 4;
  }

  // Skip default_sample_size.
  if(tfhdFlags & 0x000010) {

    pos += 4;
  }

  // Extract default_sample_flags if present.
  let defaultSampleFlags: Nullable<number> = null;

  if(tfhdFlags & 0x000020) {

    if((pos + 4) > (offset + size)) {

      return null;
    }

    defaultSampleFlags = data.readUInt32BE(pos);
  }

  return { defaultSampleDuration, defaultSampleFlags, trackId };
}

/**
 * Extracts the sample flags for the first sample in a trun (track fragment run) box. The flags are resolved from three sources in priority order:
 *
 * 1. first_sample_flags field in the trun (trun flags bit 0x004) - explicitly overrides the first sample's flags.
 * 2. Per-sample flags from the first sample entry (trun flags bit 0x400) - individual sample flags are present in each entry.
 * 3. default_sample_flags from the parent tfhd - applies when neither first_sample_flags nor per-sample flags are available.
 *
 * trun layout (FullBox):
 * - [0-3] size, [4-7] "trun", [8] version, [9-11] flags, [12-15] sample_count
 * - Optional fields after sample_count:
 *   0x001: data_offset (4 bytes)
 *   0x004: first_sample_flags (4 bytes)
 * - Per-sample entries (each containing optional fields based on flags):
 *   0x100: sample_duration (4 bytes)
 *   0x200: sample_size (4 bytes)
 *   0x400: sample_flags (4 bytes)
 *   0x800: sample_composition_time_offset (4 bytes)
 *
 * @param data - The buffer containing the trun box.
 * @param offset - The byte offset of the trun box within the buffer.
 * @param size - The total size of the trun box.
 * @param defaultSampleFlags - The default_sample_flags from the parent tfhd, or null if not present.
 * @returns The resolved sample flags for the first sample, or null if no source is available.
 */
function extractFirstSampleFlags(data: Buffer, offset: number, size: number, defaultSampleFlags: Nullable<number>): Nullable<number> {

  // Need at least the FullBox header (12 bytes) plus sample_count (4 bytes) = 16 bytes.
  if(size < 16) {

    return null;
  }

  const trunFlags = data.readUInt32BE(offset + 8) & 0x00FFFFFF;
  const sampleCount = data.readUInt32BE(offset + 12);

  // No samples means no flags to extract.
  if(sampleCount === 0) {

    return null;
  }

  let pos = offset + 16;

  // Skip optional data_offset field.
  if(trunFlags & 0x001) {

    pos += 4;
  }

  // Primary source: first_sample_flags field overrides the first sample's flags when present.
  if(trunFlags & 0x004) {

    if((pos + 4) > (offset + size)) {

      return null;
    }

    return data.readUInt32BE(pos);
  }

  // Secondary source: per-sample flags from the first sample entry. The per-sample entry fields appear in order: duration (0x100), size (0x200), flags (0x400),
  // composition_time_offset (0x800). We skip duration and size to reach the flags field of the first entry.
  if(trunFlags & 0x400) {

    // Skip the per-sample duration field (0x100) when present, to reach the flags field of the first entry.
    if(trunFlags & 0x100) {

      pos += 4;
    }

    if(trunFlags & 0x200) {

      pos += 4;
    }

    if((pos + 4) > (offset + size)) {

      return null;
    }

    return data.readUInt32BE(pos);
  }

  // Tertiary source: default_sample_flags from the parent tfhd.
  return defaultSampleFlags;
}

/**
 * Detects whether a moof box starts with a keyframe (sync sample) by examining the sample flags of the first sample in each trun box. The detection inspects all traf
 * boxes within the moof to handle multi-track containers (e.g., separate audio and video tracks). A non-keyframe signal from any traf (sample_depends_on === 1) takes
 * precedence because audio tracks are always independently decodable - the only source of sample_depends_on === 1 is a non-keyframe video track. This avoids needing
 * to map track IDs back to the moov box's codec metadata.
 *
 * The function checks three flag sources in priority order per the ISO 14496-12 spec: trun first_sample_flags (0x004), trun per-sample flags (0x400), and tfhd
 * default_sample_flags (0x020).
 *
 * @param moofData - The complete moof box buffer including its 8-byte header.
 * @returns true if the moof starts with a keyframe, false if it starts with a non-keyframe, or null if the flags could not be determined.
 */
export function detectMoofKeyframe(moofData: Buffer): Nullable<boolean> {

  let hasExplicitKeyframe = false;
  let hasExplicitNonKeyframe = false;

  // Walk the moof's child boxes looking for traf (track fragment) boxes.
  iterateChildBoxes(moofData, (type, data, offset, size) => {

    if(type !== "traf") {

      return;
    }

    // Create a subarray for this traf so we can iterate its child boxes. Buffer.subarray() shares memory with the parent, so this is O(1) with no data copying.
    const trafData = data.subarray(offset, offset + size);

    let defaultSampleFlags: Nullable<number> = null;

    // Walk the traf's child boxes. We need tfhd for default_sample_flags (fallback) and trun for the actual first-sample flags. tfhd always precedes trun in the
    // spec-mandated box ordering, so defaultSampleFlags will be populated before any trun is processed.
    iterateChildBoxes(trafData, (childType, childData, childOffset, childSize) => {

      if(childType === "tfhd") {

        defaultSampleFlags = parseTfhd(childData, childOffset, childSize)?.defaultSampleFlags ?? null;
      } else if(childType === "trun") {

        const sampleFlags = extractFirstSampleFlags(childData, childOffset, childSize, defaultSampleFlags);

        if(sampleFlags !== null) {

          const isKeyframe = evaluateSampleFlags(sampleFlags);

          if(isKeyframe) {

            hasExplicitKeyframe = true;
          } else {

            hasExplicitNonKeyframe = true;
          }
        }
      }
    });
  });

  // A non-keyframe traf (video track with sample_depends_on === 1) overrides keyframe trafs. Audio tracks are always sync (sample_depends_on === 2, or 0 for unknown),
  // so the presence of any non-keyframe signal is the definitive indicator that this fragment does not start with a video keyframe. TypeScript's control flow analysis
  // cannot track mutations made inside the iterateChildBoxes callback, so these variables appear "always falsy" to the linter despite being set to true at runtime.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(hasExplicitNonKeyframe) {

    return false;
  }

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(hasExplicitKeyframe) {

    return true;
  }

  // No definitive signal from any traf.
  return null;
}

// Timestamp Rewriting.

/**
 * Computes the total duration of all samples in a trun (track fragment run) box. The duration is the sum of per-sample durations when present (trun flags bit 0x100),
 * or sampleCount * defaultSampleDuration as a fallback. The result is returned as a BigInt for safe accumulation into the 64-bit timestamp counter.
 *
 * trun layout (FullBox):
 * - [0-3] size, [4-7] "trun", [8] version, [9-11] flags, [12-15] sample_count
 * - Optional fields after sample_count:
 *   0x001: data_offset (4 bytes)
 *   0x004: first_sample_flags (4 bytes)
 * - Per-sample entries (each containing optional fields based on flags):
 *   0x100: sample_duration (4 bytes)
 *   0x200: sample_size (4 bytes)
 *   0x400: sample_flags (4 bytes)
 *   0x800: sample_composition_time_offset (4 bytes)
 *
 * @param data - The buffer containing the trun box.
 * @param offset - The byte offset of the trun box within the buffer.
 * @param size - The total size of the trun box.
 * @param defaultSampleDuration - The default sample duration from the parent tfhd, used when per-sample durations are absent.
 * @returns The total duration of all samples as a BigInt, or 0n if the box is malformed or empty.
 */
function extractTrunTotalDuration(data: Buffer, offset: number, size: number, defaultSampleDuration: number): bigint {

  // Need at least the FullBox header (12 bytes) plus sample_count (4 bytes) = 16 bytes.
  if(size < 16) {

    return 0n;
  }

  const trunFlags = data.readUInt32BE(offset + 8) & 0x00FFFFFF;
  const sampleCount = data.readUInt32BE(offset + 12);

  if(sampleCount === 0) {

    return 0n;
  }

  // If per-sample durations are not present, use defaultSampleDuration * sampleCount.
  if(!(trunFlags & 0x100)) {

    return BigInt(defaultSampleDuration) * BigInt(sampleCount);
  }

  // Compute the byte size of each per-sample entry from the trun flags. Each optional field adds 4 bytes to the entry. The duration field (0x100) is always present
  // here because we returned early above when it was not set.
  let entrySize = 4;

  if(trunFlags & 0x200) {

    entrySize += 4;
  }

  if(trunFlags & 0x400) {

    entrySize += 4;
  }

  if(trunFlags & 0x800) {

    entrySize += 4;
  }

  // Walk past the optional header fields to reach the sample entries.
  let pos = offset + 16;

  if(trunFlags & 0x001) {

    pos += 4;
  }

  if(trunFlags & 0x004) {

    pos += 4;
  }

  // Sum per-sample durations. The duration field is the first field in each entry (when present), since entry fields appear in order: duration, size, flags,
  // composition_time_offset.
  let totalDuration = 0n;
  const endPos = offset + size;

  for(let i = 0; i < sampleCount; i++) {

    if((pos + 4) > endPos) {

      break;
    }

    totalDuration += BigInt(data.readUInt32BE(pos));
    pos += entrySize;
  }

  return totalDuration;
}

// Offset-Based Timestamp Rewriting.

/**
 * Per-track result from offset-based timestamp rewriting. The caller uses these values to initialize offsets lazily and to track the "next expected" timestamp for
 * future tab replacement handoff.
 */
export interface OffsetTrackResult {

  // Total duration of all samples in this track's trun(s), in timescale units. Used for EXTINF computation and "next expected" tracking.
  duration: bigint;

  // Chrome's original baseMediaDecodeTime read from the tfdt before the offset was applied. Used by the caller for lazy offset initialization on the first moof per
  // track: offset = initialTrackTimestamp - originalTfdt.
  originalTfdt: bigint;
}

/**
 * Applies a constant per-track offset to Chrome's original tfdt.baseMediaDecodeTime values. Reads Chrome's original tfdt, adds the per-track offset, and writes back.
 * During normal playback the offset is 0 (pure pass-through of Chrome's wall-clock-based timestamps). At tab replacement boundaries the offset bridges the PTS
 * discontinuity - it is computed once per track from the difference between the previous segmenter's "next expected" value and Chrome's new starting tfdt.
 *
 * This approach preserves Chrome's inter-track synchronization. Chrome uses wall-clock-based timestamps that keep audio and video aligned regardless of frame drops.
 *
 * The rewrite is done in-place on the moof buffer. This is safe because the buffer is an owned copy created by the MP4 box parser (Buffer.from() in
 * createMP4BoxParser).
 *
 * @param moofData - The complete moof box buffer including its 8-byte header. Modified in place.
 * @param trackOffsets - Map from track_ID to the constant offset (in timescale units) to add to Chrome's original tfdt. Entries may be absent for tracks whose
 * offsets have not been initialized yet - the caller initializes them lazily from the returned originalTfdt values.
 * @returns Map from track_ID to { originalTfdt, duration }. The caller uses originalTfdt for lazy offset initialization and duration for EXTINF and "next expected"
 * tracking. Each entry corresponds to a traf box whose tfhd was successfully parsed; a traf with a malformed or missing tfhd produces no entry.
 */
export function offsetMoofTimestamps(moofData: Buffer, trackOffsets: Map<number, bigint>): Map<number, OffsetTrackResult> {

  const results = new Map<number, OffsetTrackResult>();

  // Walk the moof's child boxes looking for traf (track fragment) boxes.
  iterateChildBoxes(moofData, (type, data, offset, size) => {

    if(type !== "traf") {

      return;
    }

    // Create a subarray for this traf so we can iterate its child boxes. Buffer.subarray() shares memory with the parent, so this is O(1) with no data copying.
    const trafData = data.subarray(offset, offset + size);

    let tfhdInfo: Nullable<TfhdInfo> = null;
    let originalTfdt = 0n;
    let totalDuration = 0n;

    // Walk the traf's child boxes. The spec mandates tfhd before tfdt before trun, so we process them in order. For each traf we: (1) parse tfhd for trackId and
    // defaultSampleDuration, (2) read the original tfdt and write the offset version back, (3) extract trun durations for EXTINF tracking.
    iterateChildBoxes(trafData, (childType, childData, childOffset, childSize) => {

      if(childType === "tfhd") {

        tfhdInfo = parseTfhd(childData, childOffset, childSize);
      } else if(childType === "tfdt") {

        // Read Chrome's original baseMediaDecodeTime, then write the offset version back. The tfhd must precede tfdt per the ISO 14496-12 box ordering, so
        // tfhdInfo is available here.
        if(!tfhdInfo) {

          return;
        }

        // tfdt layout (FullBox): [0-3] size, [4-7] "tfdt", [8] version, [9-11] flags, [12+] baseMediaDecodeTime.
        // Version 0: 32-bit baseMediaDecodeTime at offset 12. Version 1: 64-bit baseMediaDecodeTime at offset 12.
        if(childSize < 16) {

          return;
        }

        const version = childData.readUInt8(childOffset + 8);

        // Read the original tfdt value.
        if(version === 0) {

          originalTfdt = BigInt(childData.readUInt32BE(childOffset + 12));
        } else {

          if(childSize < 20) {

            return;
          }

          const high = BigInt(childData.readUInt32BE(childOffset + 12));
          const low = BigInt(childData.readUInt32BE(childOffset + 16));

          originalTfdt = (high << 32n) | low;
        }

        // Compute the new tfdt by adding the per-track offset. If the offset hasn't been initialized for this track yet, the caller will initialize it lazily
        // from the returned originalTfdt - for now, treat it as 0 (pure pass-through).
        const trackOffset = trackOffsets.get(tfhdInfo.trackId) ?? 0n;
        const newTfdt = originalTfdt + trackOffset;

        // Write the new tfdt back into the buffer.
        if(version === 0) {

          childData.writeUInt32BE(Number(newTfdt & 0xFFFFFFFFn), childOffset + 12);
        } else {

          const high = Number((newTfdt >> 32n) & 0xFFFFFFFFn);
          const low = Number(newTfdt & 0xFFFFFFFFn);

          childData.writeUInt32BE(high, childOffset + 12);
          childData.writeUInt32BE(low, childOffset + 16);
        }
      } else if(childType === "trun") {

        // Accumulate sample durations from each trun. A traf can contain multiple trun boxes (though rare in practice). The tfhd must precede trun per spec
        // ordering, so tfhdInfo and its defaultSampleDuration are available here.
        if(tfhdInfo) {

          totalDuration += extractTrunTotalDuration(childData, childOffset, childSize, tfhdInfo.defaultSampleDuration);
        }
      }
    });

    // Record the result for this track. TypeScript's control flow analysis cannot track mutations made inside the iterateChildBoxes callback, so tfhdInfo appears
    // "always null" to the linter despite being set at runtime.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(tfhdInfo) {

      const info = tfhdInfo as TfhdInfo;

      results.set(info.trackId, { duration: totalDuration, originalTfdt });
    }
  });

  return results;
}

// Moov Track Info Extraction.

/**
 * Per-track metadata extracted from a moov box. Combines timescale (from mdhd) and handler type (from hdlr) into a single structure, avoiding the need to walk the
 * moov twice.
 */
export interface MoovTrackInfo {

  // The handler type from the hdlr box: "vide" for video, "soun" for audio, "hint" for hint tracks, etc. This is the definitive way to identify which trackId
  // corresponds to video vs audio, as timescale-based heuristics (e.g., 90000 = video) are fragile.
  handlerType: string;

  // The timescale from the mdhd box. Converts sample durations (in timescale units) to real seconds: seconds = duration / timescale.
  timescale: number;
}

/**
 * Extracts per-track metadata from a moov (movie header) box in a single pass. Each track's tkhd provides the track_ID, while the mdia container holds both the
 * mdhd (timescale) and hdlr (handler type). Combining these into one walk avoids parsing the moov structure twice.
 *
 * Parsing path: moov > trak > { tkhd (track_ID), mdia > { mdhd (timescale), hdlr (handler_type) } }
 *
 * Box layouts referenced:
 * - tkhd (FullBox): [0-3] size, [4-7] "tkhd", [8] version, [9-11] flags. Version 0: track_ID at offset 20. Version 1: track_ID at offset 28.
 * - mdhd (FullBox): [0-3] size, [4-7] "mdhd", [8] version, [9-11] flags. Version 0: timescale at offset 20. Version 1: timescale at offset 28.
 * - hdlr (FullBox): [0-3] size, [4-7] "hdlr", [8] version, [9-11] flags, [12-15] pre_defined, [16-19] handler_type (4 ASCII chars).
 *
 * @param moovData - The complete moov box buffer including its 8-byte header.
 * @returns Map from track_ID to track info. Only includes tracks where the track ID, timescale, and handler type were all successfully extracted.
 */
export function parseMoovTrackInfo(moovData: Buffer): Map<number, MoovTrackInfo> {

  const result = new Map<number, MoovTrackInfo>();

  // Walk the moov's child boxes looking for trak (track) boxes.
  iterateChildBoxes(moovData, (type, data, offset, size) => {

    if(type !== "trak") {

      return;
    }

    const trakData = data.subarray(offset, offset + size);

    let trackId: Nullable<number> = null;
    let timescale: Nullable<number> = null;
    let handlerType: Nullable<string> = null;

    // Walk the trak's child boxes to find tkhd (track header) and mdia (media container). The spec mandates tkhd before mdia, so trackId is available before we
    // need it, but we don't depend on ordering - each field is extracted independently and combined after iteration.
    iterateChildBoxes(trakData, (childType, childData, childOffset, childSize) => {

      if(childType === "tkhd") {

        if(childSize < 16) {

          return;
        }

        const version = childData.readUInt8(childOffset + 8);

        if((version === 0) && (childSize >= 24)) {

          trackId = childData.readUInt32BE(childOffset + 20);
        } else if((version === 1) && (childSize >= 32)) {

          trackId = childData.readUInt32BE(childOffset + 28);
        }
      } else if(childType === "mdia") {

        // Walk the mdia's child boxes to find mdhd (timescale) and hdlr (handler type).
        const mdiaData = childData.subarray(childOffset, childOffset + childSize);

        iterateChildBoxes(mdiaData, (mdiaChildType, mdiaChildData, mdiaChildOffset, mdiaChildSize) => {

          if(mdiaChildType === "mdhd") {

            if(mdiaChildSize < 16) {

              return;
            }

            const mdhdVersion = mdiaChildData.readUInt8(mdiaChildOffset + 8);

            if((mdhdVersion === 0) && (mdiaChildSize >= 24)) {

              timescale = mdiaChildData.readUInt32BE(mdiaChildOffset + 20);
            } else if((mdhdVersion === 1) && (mdiaChildSize >= 32)) {

              timescale = mdiaChildData.readUInt32BE(mdiaChildOffset + 28);
            }
          } else if(mdiaChildType === "hdlr") {

            // hdlr needs at least 20 bytes: 8 header + 4 version/flags + 4 pre_defined + 4 handler_type.
            if(mdiaChildSize < 20) {

              return;
            }

            handlerType = mdiaChildData.toString("ascii", mdiaChildOffset + 16, mdiaChildOffset + 20);
          }
        });
      }
    });

    // Store the track only if every extracted field was successfully found. TypeScript's control flow analysis cannot track mutations made inside the iterateChildBoxes
    // callbacks, so these variables appear "always null" to the linter despite being set at runtime.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if((trackId !== null) && (timescale !== null) && (timescale > 0) && (handlerType !== null)) {

      result.set(trackId, { handlerType, timescale });
    }
  });

  return result;
}

// Codec Configuration Extraction.

/**
 * Video codec configuration extracted from the avcC box inside the moov. These are the parameters Chrome's MediaRecorder used to encode the video, needed to match
 * the preroll's FFmpeg encoding so CDVR's transcoder sees consistent codec parameters across the preroll-to-real DISCONTINUITY boundary.
 */
export interface VideoCodecConfig {

  // AVC level indication (e.g., 30 = level 3.0, 31 = level 3.1, 40 = level 4.0).
  level: number;

  // AVC profile indication (e.g., 66 = Baseline, 77 = Main, 100 = High).
  profile: number;

  // Profile compatibility flags byte.
  profileCompatibility: number;
}

/**
 * Audio codec configuration extracted from the esds box inside the moov. Contains the AAC object type and sample rate index from the AudioSpecificConfig.
 */
export interface AudioCodecConfig {

  // AAC object type / AudioObjectType (2 = AAC-LC, 5 = HE-AAC (SBR), 1 = AAC Main, etc.).
  objectType: number;

  // Sample rate index from the AudioSpecificConfig frequency table (e.g., 3 = 48000 Hz, 4 = 44100 Hz).
  sampleRateIndex: number;
}

/**
 * Combined codec configuration from a moov box.
 */
export interface MoovCodecConfig {

  // Audio codec configuration, or null if no mp4a/esds found.
  audio: Nullable<AudioCodecConfig>;

  // Video codec configuration, or null if no avc1/avcC found.
  video: Nullable<VideoCodecConfig>;
}

/**
 * Reads an MPEG-4 ES_Descriptor variable-length size field at the given offset. The encoding is continuation-bit based: each byte contributes 7 bits of size
 * value, and a high bit (0x80) means another byte follows. Returns the accumulated length and the offset where the descriptor's payload starts (one past the
 * last size byte). When the encoding runs off the end of the buffer (malformed input), returns length: 0 and payloadStart at the cursor's last position so
 * callers naturally produce an empty payload.
 * @param buffer - The buffer containing the descriptor.
 * @param offset - The offset of the first size byte (i.e., one past the tag byte).
 * @returns The decoded length and the payload start offset.
 */
export function readDescriptorSize(buffer: Buffer, offset: number): { length: number; payloadStart: number } {

  let length = 0;
  let cursor = offset;

  // Continuation-bit walk: at most a few iterations in practice (sizes rarely need more than 1-2 bytes), but the loop is bounded by buffer.length so a
  // malformed input cannot loop forever.
  while(cursor < buffer.length) {

    const byte = buffer[cursor] ?? 0;

    cursor++;
    length = (length << 7) | (byte & 0x7F);

    if((byte & 0x80) === 0) {

      return { length, payloadStart: cursor };
    }
  }

  // Ran past the buffer end without a terminating byte. Treat as malformed and return zero length so the caller sees an empty payload.
  return { length: 0, payloadStart: cursor };
}

/**
 * Walks an MPEG-4 ES_Descriptor tree to find the first descriptor with the given tag, returning its payload as a Buffer view. The walker recurses INTO known
 * container descriptors (ES_Descriptor 0x03, DecoderConfigDescriptor 0x04) by skipping their tag-specific fixed-size header fields and continuing the search
 * inside the remaining payload; for non-container or unknown tags it skips past the entire payload to advance to the next sibling. This is the structural
 * counterpart to a naive byte-scan-for-tag, which would produce false matches when a target tag value appears inside another descriptor's payload bytes (e.g.,
 * inside DecoderConfigDescriptor's bitrate fields, or as a length-encoding byte).
 *
 * Container header sizes:
 * - ES_Descriptor (0x03): 2 bytes ES_ID + 1 byte flags + (optional dependsOn_ES_ID, URL string, OCR_ES_Id depending on flag bits in byte 2).
 * - DecoderConfigDescriptor (0x04): 1 byte objectTypeIndication + 1 byte stream/up/reserved + 3 bytes bufferSizeDB + 4 bytes maxBitrate + 4 bytes avgBitrate
 *   = 13 bytes.
 *
 * @param buffer - The descriptor tree to search (e.g., the esds payload after the box header and version/flags).
 * @param targetTag - The descriptor tag to find (e.g., 0x05 for DecoderSpecificInfo).
 * @returns The matched descriptor's payload as a Buffer view, or null if not found.
 */
export function findDescriptor(buffer: Buffer, targetTag: number): Nullable<Buffer> {

  let offset = 0;

  while(offset < buffer.length) {

    const tag = buffer[offset];

    if(tag === undefined) {

      return null;
    }

    const { length, payloadStart } = readDescriptorSize(buffer, offset + 1);
    const payload = buffer.subarray(payloadStart, payloadStart + length);

    if(tag === targetTag) {

      return payload;
    }

    // ES_Descriptor (0x03) is a container. Compute the offset of nested descriptors by skipping the fixed header (ES_ID + flags) and any optional fields the
    // flag byte signals. The flag byte is at payload offset 2; bits 7/6/5 are streamDependenceFlag, URL_Flag, OCRstreamFlag.
    if(tag === 0x03) {

      let innerOffset = 3;
      const flags = payload[2] ?? 0;

      if((flags & 0x80) !== 0) {

        // streamDependenceFlag: a 16-bit dependsOn_ES_ID follows the flag byte.
        innerOffset += 2;
      }

      if((flags & 0x40) !== 0) {

        // URL_Flag: a 1-byte URLlength followed by URLlength bytes of URL text.
        const urlLength = payload[innerOffset] ?? 0;

        innerOffset += 1 + urlLength;
      }

      if((flags & 0x20) !== 0) {

        // OCRstreamFlag: a 16-bit OCR_ES_Id.
        innerOffset += 2;
      }

      const found = findDescriptor(payload.subarray(innerOffset), targetTag);

      if(found) {

        return found;
      }
    } else if(tag === 0x04) {

      // DecoderConfigDescriptor (0x04) is a container. Skip the 13-byte fixed header (objectTypeIndication + stream/up/reserved + bufferSizeDB + maxBitrate +
      // avgBitrate) and recurse into the remaining payload.
      const found = findDescriptor(payload.subarray(13), targetTag);

      if(found) {

        return found;
      }
    }

    // Advance past this descriptor (its tag + its size encoding + its full payload).
    offset = payloadStart + length;
  }

  return null;
}

/**
 * Extracts video and audio codec configuration from a moov box. Walks the box tree to find the avcC box (inside moov > trak > mdia > minf > stbl > stsd > avc1) for
 * video and the esds box (inside moov > trak > mdia > minf > stbl > stsd > mp4a) for audio.
 *
 * avcC layout: [0-3] size, [4-7] "avcC", [8] configurationVersion, [9] AVCProfileIndication, [10] profile_compatibility, [11] AVCLevelIndication, ...
 * esds layout: [0-3] size, [4-7] "esds", [8] version, [9-11] flags, then descriptor tags. AudioSpecificConfig is inside the DecoderSpecificInfo descriptor.
 *
 * @param moovData - The complete moov box buffer including its 8-byte header.
 * @returns The extracted codec configuration.
 */
export function parseMoovCodecConfig(moovData: Buffer): MoovCodecConfig {

  let video: Nullable<VideoCodecConfig> = null;
  let audio: Nullable<AudioCodecConfig> = null;

  // Walk moov > trak.
  iterateChildBoxes(moovData, (type, data, offset, size) => {

    if(type !== "trak") {

      return;
    }

    const trakData = data.subarray(offset, offset + size);

    // Walk trak > mdia.
    iterateChildBoxes(trakData, (childType, childData, childOffset, childSize) => {

      if(childType !== "mdia") {

        return;
      }

      const mdiaData = childData.subarray(childOffset, childOffset + childSize);

      // Walk mdia > minf.
      iterateChildBoxes(mdiaData, (minfType, minfData, minfOffset, minfSize) => {

        if(minfType !== "minf") {

          return;
        }

        const minfBox = minfData.subarray(minfOffset, minfOffset + minfSize);

        // Walk minf > stbl.
        iterateChildBoxes(minfBox, (stblType, stblData, stblOffset, stblSize) => {

          if(stblType !== "stbl") {

            return;
          }

          const stblBox = stblData.subarray(stblOffset, stblOffset + stblSize);

          // Walk stbl > stsd. The stsd box is a FullBox with a 4-byte entry count after the version/flags.
          iterateChildBoxes(stblBox, (stsdType, stsdData, stsdOffset, stsdSize) => {

            if(stsdType !== "stsd") {

              return;
            }

            // stsd is a FullBox: 8 header + 4 version/flags + 4 entry_count = 16 bytes before entries. Entries are child boxes starting at offset 16.
            if(stsdSize < 16) {

              return;
            }

            const stsdBox = stsdData.subarray(stsdOffset, stsdOffset + stsdSize);
            const stsdEntries = Buffer.alloc(stsdSize - 16 + 8);

            // Construct a synthetic container so iterateChildBoxes can parse the entries. Copy the entry data with a fake 8-byte box header.
            stsdEntries.writeUInt32BE(stsdSize - 16 + 8, 0);
            stsdEntries.write("stsd", 4);
            stsdBox.copy(stsdEntries, 8, 16);

            iterateChildBoxes(stsdEntries, (entryType, entryData, entryOffset, entrySize) => {

              if(entryType === "avc1") {

                // avc1 is a sample entry: 8 header + 6 reserved + 2 data_ref_index + 16 pre_defined/reserved + 2 width + 2 height + ... = 86 bytes minimum before
                // child boxes. Child boxes (including avcC) start at offset 86.
                if(entrySize < 86) {

                  return;
                }

                const avc1Box = entryData.subarray(entryOffset, entryOffset + entrySize);
                const avc1Children = Buffer.alloc(entrySize - 86 + 8);

                avc1Children.writeUInt32BE(entrySize - 86 + 8, 0);
                avc1Children.write("avc1", 4);
                avc1Box.copy(avc1Children, 8, 86);

                iterateChildBoxes(avc1Children, (avcType, avcData, avcOffset, avcSize) => {

                  if((avcType === "avcC") && (avcSize >= 12)) {

                    video = {

                      level: avcData.readUInt8(avcOffset + 11),
                      profile: avcData.readUInt8(avcOffset + 9),
                      profileCompatibility: avcData.readUInt8(avcOffset + 10)
                    };
                  }
                });
              } else if(entryType === "mp4a") {

                // mp4a is a sample entry: 8 header + 6 reserved + 2 data_ref_index + 8 reserved + 2 channelcount + 2 samplesize + 2 pre_defined + 2 reserved +
                // 4 samplerate = 36 bytes before child boxes.
                if(entrySize < 36) {

                  return;
                }

                const mp4aBox = entryData.subarray(entryOffset, entryOffset + entrySize);
                const mp4aChildren = Buffer.alloc(entrySize - 36 + 8);

                mp4aChildren.writeUInt32BE(entrySize - 36 + 8, 0);
                mp4aChildren.write("mp4a", 4);
                mp4aBox.copy(mp4aChildren, 8, 36);

                iterateChildBoxes(mp4aChildren, (esdsType, esdsData, esdsOffset, esdsSize) => {

                  if((esdsType === "esds") && (esdsSize >= 12)) {

                    // Parse the esds descriptor chain to find AudioSpecificConfig. The esds box after the FullBox header (version + flags = 4 bytes) contains an
                    // ES_Descriptor (tag 0x03) whose nested DecoderConfigDescriptor (tag 0x04) wraps the DecoderSpecificInfo (tag 0x05). findDescriptor walks the
                    // ES_Descriptor tree structurally - it does NOT byte-scan for 0x05, which would produce false matches when 0x05 appears inside another
                    // descriptor's payload (e.g., as a length byte or inside DecoderConfigDescriptor's bitrate fields). The DecoderSpecificInfo's payload is the
                    // AudioSpecificConfig: byte0's high 5 bits are objectType, byte0's low 3 bits plus byte1's high 1 bit are sampleRateIndex.
                    const esdsPayload = esdsData.subarray(esdsOffset + 12, esdsOffset + esdsSize);
                    const dsi = findDescriptor(esdsPayload, 0x05);

                    if(dsi && (dsi.length >= 2)) {

                      const byte0 = dsi[0];
                      const byte1 = dsi[1];

                      if((byte0 !== undefined) && (byte1 !== undefined)) {

                        audio = {

                          objectType: (byte0 >> 3) & 0x1F,
                          sampleRateIndex: ((byte0 & 0x07) << 1) | ((byte1 >> 7) & 0x01)
                        };
                      }
                    }
                  }
                });
              }
            });
          });
        });
      });
    });
  });

  return { audio, video };
}
