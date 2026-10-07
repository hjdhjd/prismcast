/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * preroll.helpers.ts: The synthetic preroll encoder the preroll suites share. It stands in for FFmpeg at generatePreroll's encoder port and answers each encode
 * with a small fragmented MP4 built here box by box - an init segment (ftyp and a moov declaring one video track) followed by one moof and mdat pair per
 * fragment - so a suite drives the real resolve, split and store path, and every populated read behind it, with no FFmpeg binary.
 */
import type { Nullable } from "../types/index.ts";
import type { PrerollEncoder } from "./preroll.ts";

// The path the synthetic encoder's resolver answers, standing in for a resolved FFmpeg binary that nothing ever executes.
export const SYNTHETIC_FFMPEG_PATH = "/synthetic/ffmpeg";

// The synthetic track's identity and timing. A sample lasts one thirtieth of a second at this timescale, so a fragment's duration is any multiple of that.
const TRACK_ID = 1;
const TIMESCALE = 90000;
const SAMPLE_DURATION = 3000;

/**
 * Builds one MP4 box: a 4-byte size and a 4-byte type ahead of the payload, the size counting the 8-byte header.
 * @param type - The four-character box type.
 * @param payload - The box's contents.
 * @returns The box.
 */
function makeBox(type: string, payload: Buffer): Buffer {

  const box = Buffer.alloc(8 + payload.length);

  box.writeUInt32BE(box.length, 0);
  box.write(type, 4, 4, "ascii");
  payload.copy(box, 8);

  return box;
}

/**
 * Builds the init segment the box parser reads a track's timescale from: an ftyp, then a moov holding one trak whose tkhd names the track and whose mdia carries
 * the timescale in its mdhd and the video handler in its hdlr. Each header is version 0 and carries only the fields the parser reads.
 * @returns The init segment.
 */
function makeInitSegment(): Buffer {

  const tkhd = Buffer.alloc(16);
  const mdhd = Buffer.alloc(16);
  const hdlr = Buffer.alloc(12);

  tkhd.writeUInt32BE(TRACK_ID, 12);
  mdhd.writeUInt32BE(TIMESCALE, 12);
  hdlr.write("vide", 8, 4, "ascii");

  const mdia = makeBox("mdia", Buffer.concat([ makeBox("mdhd", mdhd), makeBox("hdlr", hdlr) ]));

  return Buffer.concat([ makeBox("ftyp", Buffer.from("isom0000", "ascii")), makeBox("moov", makeBox("trak", Buffer.concat([ makeBox("tkhd", tkhd), mdia ]))) ]);
}

/**
 * Builds one media fragment: a moof whose traf declares a default sample duration in its tfhd, a decode time in its tfdt and a sample count in its trun, followed
 * by an mdat. The fragment lasts the sample count times the default duration, the reading the preroll split takes as the segment's duration.
 * @param durationSec - How long the fragment lasts, in seconds.
 * @param decodeTime - The fragment's base media decode time, in timescale units.
 * @returns The moof and mdat pair.
 */
function makeFragment(durationSec: number, decodeTime: number): Buffer {

  const tfhd = Buffer.alloc(12);
  const tfdt = Buffer.alloc(8);
  const trun = Buffer.alloc(8);

  // Flag 0x000008 marks the default sample duration as present, the field the trun's total falls back to when its samples carry no durations of their own.
  tfhd.writeUInt32BE(0x000008, 0);
  tfhd.writeUInt32BE(TRACK_ID, 4);
  tfhd.writeUInt32BE(SAMPLE_DURATION, 8);
  tfdt.writeUInt32BE(decodeTime, 4);
  trun.writeUInt32BE(Math.round((durationSec * TIMESCALE) / SAMPLE_DURATION), 4);

  const traf = makeBox("traf", Buffer.concat([ makeBox("tfhd", tfhd), makeBox("tfdt", tfdt), makeBox("trun", trun) ]));

  return Buffer.concat([ makeBox("moof", traf), makeBox("mdat", Buffer.from("synthetic media", "ascii")) ]);
}

/**
 * Builds a fragmented MP4 in the shape a preroll encode produces: the init segment followed by one fragment per duration, each fragment's decode time continuing
 * from the one before it. An empty duration list yields the init segment alone, the incomplete output an encode can leave.
 * @param fragmentDurations - Each fragment's duration in seconds, a multiple of one thirtieth of a second.
 * @returns The fragmented MP4.
 */
export function makeSyntheticFmp4(fragmentDurations: readonly number[]): Buffer {

  const fragments: Buffer[] = [];
  let decodeTime = 0;

  for(const durationSec of fragmentDurations) {

    fragments.push(makeFragment(durationSec, decodeTime));
    decodeTime += Math.round(durationSec * TIMESCALE);
  }

  return Buffer.concat([ makeInitSegment(), ...fragments ]);
}

/**
 * Returns the init segment every synthetic fragmented MP4 opens with, so a row can compare what a route serves against the bytes the split should have kept.
 * @returns The init segment.
 */
export function syntheticInitSegment(): Buffer {

  return makeInitSegment();
}

/**
 * The synthetic encoder: the port generatePreroll takes, and the record of every encode it was asked to run.
 */
export interface SyntheticPrerollEncoder extends PrerollEncoder {

  // Each encode in call order, with the binary and the arguments it was handed.
  readonly runs: { readonly args: readonly string[]; readonly ffmpegBin: string }[];
}

/**
 * Builds a synthetic encoder whose resolver answers the given binary and whose every encode answers the given output.
 * @param options - What the resolver and each encode answer.
 * @param options.binary - The binary the resolver answers, or null for no usable FFmpeg. Defaults to SYNTHETIC_FFMPEG_PATH.
 * @param options.output - The fragmented MP4 each encode resolves with, or the error it rejects with.
 * @returns The encoder.
 */
export function makeSyntheticPrerollEncoder({ binary = SYNTHETIC_FFMPEG_PATH, output }: { binary?: Nullable<string>; output: Buffer | Error }): SyntheticPrerollEncoder {

  const runs: { args: readonly string[]; ffmpegBin: string }[] = [];

  return {

    resolve: async (): Promise<string | undefined> => binary ?? undefined,
    run: async (ffmpegBin: string, args: string[]): Promise<Buffer> => {

      runs.push({ args, ffmpegBin });

      if(output instanceof Error) {

        throw output;
      }

      return output;
    },
    runs
  };
}
