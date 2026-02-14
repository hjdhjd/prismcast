/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mp4Parser.ts: Low-level MP4 box parsing for PrismCast.
 */
import type { Nullable } from "../types/index.js";
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
 * sync samples. The rewriteMoofTimestamps() function rewrites tfdt.baseMediaDecodeTime values in each traf to produce monotonically increasing timestamps, eliminating
 * discontinuities caused by capture restarts (tab replacement, source reload recovery).
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

      // Try to parse complete boxes.
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
    } else if((sizeField < MIN_HEADER_SIZE) || (sizeField === 0)) {

      // Invalid size or "extends to end of file" — stop iterating.
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
 * Parsed fields from a tfhd (track fragment header) box. Used by both keyframe detection (defaultSampleFlags) and timestamp rewriting (defaultSampleDuration, trackId).
 * Consolidates the field-walking logic that was previously duplicated across two separate extraction functions.
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
 * tfhd field extraction, used by both detectMoofKeyframe() (for sample flags) and rewriteMoofTimestamps() (for track ID and duration).
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
 * 1. first_sample_flags field in the trun (trun flags bit 0x004) — explicitly overrides the first sample's flags.
 * 2. Per-sample flags from the first sample entry (trun flags bit 0x400) — individual sample flags are present in each entry.
 * 3. default_sample_flags from the parent tfhd — applies when neither first_sample_flags nor per-sample flags are available.
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

    // Skip first_sample_flags field position (it's not present since we checked 0x004 above, but the pos is already past it).
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
 * boxes within the moof to handle multi-track containers (e.g., separate audio and video tracks). A non-keyframe signal from any traf (sample_depends_on === 2) takes
 * precedence because audio tracks are always independently decodable — the only source of sample_depends_on === 2 is a non-keyframe video track. This avoids needing
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

  // A non-keyframe traf (video track with sample_depends_on === 2) overrides keyframe trafs. Audio tracks are always sync (sample_depends_on 0 or 1), so the presence
  // of any non-keyframe signal is the definitive indicator that this fragment does not start with a video keyframe. TypeScript's control flow analysis cannot track
  // mutations made inside the iterateChildBoxes callback, so these variables appear "always falsy" to the linter despite being set to true at runtime.
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

/**
 * Rewrites the baseMediaDecodeTime in every tfdt box within a moof to produce monotonically increasing timestamps. For each traf, the function reads the track ID from
 * tfhd, writes the current per-track timestamp into tfdt, then advances the counter by the total sample duration extracted from trun. This eliminates timestamp
 * discontinuities caused by capture restarts (tab replacement, source reload recovery) because Chrome's original timestamps are completely overwritten.
 *
 * The rewrite is done in-place on the moof buffer. This is safe because the buffer is an owned copy created by the MP4 box parser (Buffer.from() in createMP4BoxParser).
 *
 * Per-track state is necessary because FFmpeg outputs separate moof+mdat pairs for audio and video, and each track may have a different timescale (e.g., 90000 for
 * video, 48000 for audio). A single counter would produce incorrect timestamps for one track.
 *
 * @param moofData - The complete moof box buffer including its 8-byte header. Modified in place.
 * @param trackTimestamps - Map from track_ID to the next baseMediaDecodeTime value. Updated in place with the advanced timestamps after rewriting.
 * @returns Map from track_ID to the total duration (in timescale units) computed from trun sample durations. The caller uses this for diagnostics (zero durations
 * indicate trun parsing issues) and sanity checking (abnormal durations from corrupt trun data). Each entry corresponds to one traf box in the moof.
 */
export function rewriteMoofTimestamps(moofData: Buffer, trackTimestamps: Map<number, bigint>): Map<number, bigint> {

  const trafDurations = new Map<number, bigint>();

  // Walk the moof's child boxes looking for traf (track fragment) boxes.
  iterateChildBoxes(moofData, (type, data, offset, size) => {

    if(type !== "traf") {

      return;
    }

    // Create a subarray for this traf so we can iterate its child boxes. Buffer.subarray() shares memory with the parent, so this is O(1) with no data copying.
    const trafData = data.subarray(offset, offset + size);

    let tfhdInfo: Nullable<TfhdInfo> = null;
    let totalDuration = 0n;

    // Walk the traf's child boxes. The spec mandates tfhd before tfdt before trun, so we process them in order. The first pass extracts tfhd info and rewrites
    // tfdt. The trun duration extraction happens in the same pass since tfhd is already available by the time trun is encountered.
    iterateChildBoxes(trafData, (childType, childData, childOffset, childSize) => {

      if(childType === "tfhd") {

        tfhdInfo = parseTfhd(childData, childOffset, childSize);
      } else if(childType === "tfdt") {

        // Rewrite baseMediaDecodeTime with the current per-track timestamp. The tfhd must precede tfdt per the ISO 14496-12 box ordering, so tfhdInfo is
        // available here.
        if(!tfhdInfo) {

          return;
        }

        const currentTimestamp = trackTimestamps.get(tfhdInfo.trackId) ?? 0n;

        // tfdt layout (FullBox): [0-3] size, [4-7] "tfdt", [8] version, [9-11] flags, [12+] baseMediaDecodeTime.
        // Version 0: 32-bit baseMediaDecodeTime at offset 12. Version 1: 64-bit baseMediaDecodeTime at offset 12.
        if(childSize < 16) {

          return;
        }

        const version = childData.readUInt8(childOffset + 8);

        if(version === 0) {

          // Version 0: write as uint32 (truncate to 32 bits). Overflows at ~13 hours for 90000 Hz timescale, but version 0 tfdt boxes are inherently 32-bit.
          childData.writeUInt32BE(Number(currentTimestamp & 0xFFFFFFFFn), childOffset + 12);
        } else {

          // Version 1: write as uint64 (high 32 bits + low 32 bits). Requires at least 20 bytes in the tfdt box.
          if(childSize < 20) {

            return;
          }

          const high = Number((currentTimestamp >> 32n) & 0xFFFFFFFFn);
          const low = Number(currentTimestamp & 0xFFFFFFFFn);

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

    // Advance the per-track timestamp counter by the total sample duration from this traf. A zero duration with a valid tfhd indicates a trun parsing issue —
    // the caller should log a warning. We still advance (by zero) to avoid corrupting subsequent timestamps. TypeScript's control flow analysis cannot track
    // mutations made inside the iterateChildBoxes callback, so tfhdInfo appears "always null" to the linter despite being set at runtime.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(tfhdInfo) {

      const info = tfhdInfo as TfhdInfo;

      trafDurations.set(info.trackId, totalDuration);

      const currentTimestamp = trackTimestamps.get(info.trackId) ?? 0n;

      trackTimestamps.set(info.trackId, currentTimestamp + totalDuration);
    }
  });

  return trafDurations;
}

// Moov Timescale Extraction.

/**
 * Extracts per-track timescale values from a moov (movie header) box. Each track in the moov contains a tkhd box with the track_ID and an mdia > mdhd box with the
 * timescale. The timescale converts sample durations (in timescale units) to real seconds: seconds = duration / timescale. For example, a timescale of 16000 means
 * each unit is 1/16000 of a second.
 *
 * Parsing path: moov > trak > { tkhd (track_ID), mdia > mdhd (timescale) }
 *
 * @param moovData - The complete moov box buffer including its 8-byte header.
 * @returns Map from track_ID to timescale. Empty if no valid tracks are found.
 */
export function parseMoovTimescales(moovData: Buffer): Map<number, number> {

  const result = new Map<number, number>();

  // Walk the moov's child boxes looking for trak (track) boxes.
  iterateChildBoxes(moovData, (type, data, offset, size) => {

    if(type !== "trak") {

      return;
    }

    const trakData = data.subarray(offset, offset + size);

    let trackId: Nullable<number> = null;
    let timescale: Nullable<number> = null;

    // Walk the trak's child boxes to find tkhd (track header) and mdia (media container). The spec mandates tkhd before mdia, so trackId is available before we
    // need it, but we don't depend on ordering — both are extracted independently and combined after iteration.
    iterateChildBoxes(trakData, (childType, childData, childOffset, childSize) => {

      if(childType === "tkhd") {

        // tkhd layout (FullBox): [0-3] size, [4-7] "tkhd", [8] version, [9-11] flags.
        // Version 0: [12-15] creation_time, [16-19] modification_time, [20-23] track_ID.
        // Version 1: [12-19] creation_time, [20-27] modification_time, [28-31] track_ID.
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

        // Walk the mdia's child boxes to find mdhd (media header) which contains the timescale.
        const mdiaData = childData.subarray(childOffset, childOffset + childSize);

        iterateChildBoxes(mdiaData, (mdiaChildType, mdiaChildData, mdiaChildOffset, mdiaChildSize) => {

          if(mdiaChildType !== "mdhd") {

            return;
          }

          // mdhd layout (FullBox): [0-3] size, [4-7] "mdhd", [8] version, [9-11] flags.
          // Version 0: [12-15] creation_time, [16-19] modification_time, [20-23] timescale.
          // Version 1: [12-19] creation_time, [20-27] modification_time, [28-31] timescale.
          if(mdiaChildSize < 16) {

            return;
          }

          const mdhdVersion = mdiaChildData.readUInt8(mdiaChildOffset + 8);

          if((mdhdVersion === 0) && (mdiaChildSize >= 24)) {

            timescale = mdiaChildData.readUInt32BE(mdiaChildOffset + 20);
          } else if((mdhdVersion === 1) && (mdiaChildSize >= 32)) {

            timescale = mdiaChildData.readUInt32BE(mdiaChildOffset + 28);
          }
        });
      }
    });

    // Store the track if both trackId and timescale were successfully extracted. TypeScript's control flow analysis cannot track mutations made inside the
    // iterateChildBoxes callbacks, so these variables appear "always null" to the linter despite being set at runtime.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if((trackId !== null) && (timescale !== null) && (timescale > 0)) {

      result.set(trackId, timescale);
    }
  });

  return result;
}
