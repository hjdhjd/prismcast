/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mp4Parser.ts: Low-level MP4 box parsing for PrismCast.
 */

/*
 * MP4 BOX PARSING
 *
 * MP4 files consist of a sequence of "boxes" (also called "atoms"). Each box has a simple structure:
 *
 * - 4 bytes: size (big-endian uint32) - total box size including header
 * - 4 bytes: type (4 ASCII characters, e.g., 'ftyp', 'moov', 'moof', 'mdat')
 * - (size - 8) bytes: payload
 *
 * Special case: when size == 1, the next 8 bytes contain a 64-bit extended size.
 *
 * This parser handles streaming input - data arrives in chunks, and we buffer incomplete boxes until we have enough data to emit a complete box.
 *
 * NESTED BOX PARSING
 *
 * Container boxes like moof and traf contain child boxes in their payload. The iterateChildBoxes() function walks these children, and detectMoofKeyframe() uses it to
 * parse traf -> tfhd/trun structures for keyframe detection. This supports the fMP4 segmenter's ability to track keyframe frequency and verify that segments start with
 * sync samples.
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
 * Extracts the default_sample_flags value from a tfhd (track fragment header) box. The tfhd optionally carries default flags that apply to all samples in the
 * fragment when individual sample flags are not present in the trun. The default_sample_flags field is present only when tfhd flags bit 0x020 is set.
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
 * @returns The default_sample_flags value, or null if the field is not present or the box is malformed.
 */
function extractDefaultSampleFlags(data: Buffer, offset: number, size: number): number | null {

  // Need at least the FullBox header (12 bytes) plus track_ID (4 bytes) = 16 bytes.
  if(size < 16) {

    return null;
  }

  const tfhdFlags = data.readUInt32BE(offset + 8) & 0x00FFFFFF;

  // default_sample_flags is only present when flag bit 0x020 is set.
  if(!(tfhdFlags & 0x000020)) {

    return null;
  }

  // Walk past optional fields that precede default_sample_flags. Each field is present only if its corresponding flag bit is set.
  let pos = offset + 16;

  if(tfhdFlags & 0x000001) {

    pos += 8;
  }

  if(tfhdFlags & 0x000002) {

    pos += 4;
  }

  if(tfhdFlags & 0x000008) {

    pos += 4;
  }

  if(tfhdFlags & 0x000010) {

    pos += 4;
  }

  // Bounds check before reading default_sample_flags.
  if((pos + 4) > (offset + size)) {

    return null;
  }

  return data.readUInt32BE(pos);
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
function extractFirstSampleFlags(data: Buffer, offset: number, size: number, defaultSampleFlags: number | null): number | null {

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
export function detectMoofKeyframe(moofData: Buffer): boolean | null {

  let hasExplicitKeyframe = false;
  let hasExplicitNonKeyframe = false;

  // Walk the moof's child boxes looking for traf (track fragment) boxes.
  iterateChildBoxes(moofData, (type, data, offset, size) => {

    if(type !== "traf") {

      return;
    }

    // Create a subarray for this traf so we can iterate its child boxes. Buffer.subarray() shares memory with the parent, so this is O(1) with no data copying.
    const trafData = data.subarray(offset, offset + size);

    let defaultSampleFlags: number | null = null;

    // Walk the traf's child boxes. We need tfhd for default_sample_flags (fallback) and trun for the actual first-sample flags. tfhd always precedes trun in the
    // spec-mandated box ordering, so defaultSampleFlags will be populated before any trun is processed.
    iterateChildBoxes(trafData, (childType, childData, childOffset, childSize) => {

      if(childType === "tfhd") {

        defaultSampleFlags = extractDefaultSampleFlags(childData, childOffset, childSize);
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
