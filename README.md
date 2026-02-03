<SPAN ALIGN="CENTER" STYLE="text-align:center">
<DIV ALIGN="CENTER" STYLE="text-align:center">

[![PrismCast: Browser-based live TV capture for Channels DVR](https://raw.githubusercontent.com/hjdhjd/prismcast/main/prismcast.svg)](https://github.com/hjdhjd/prismcast)

# PrismCast

[![Downloads](https://img.shields.io/npm/dt/prismcast?color=636382&logo=icloud&logoColor=%23FFFFFF&style=for-the-badge)](https://www.npmjs.com/package/prismcast)
[![Version](https://img.shields.io/npm/v/prismcast?color=636382&label=PrismCast&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxMDAwIiBoZWlnaHQ9IjEwMDAiIHZpZXdCb3g9IjAgMCAxMDAwIDEwMDAiPjxnIGZpbGw9IiNmZmYiPjxwYXRoIGQ9Ik0zODYgMjIwYy0xOCA2LTIyIDExLTQ2IDU1bC0xMSAxOS01NSA5OS00NCA4MS0xNSAyNi05IDE3LTUgOC0zMyA2MS02NCAxMTdjLTQgOS00IDI5IDAgMzdsNCA3YzYgMTQgMjQgMjggNDEgMzEgMTAgMiA1MzIgMiA1NDAgMCAzOS0xMCA1My00NyAzMi04NmExNTE0IDE1MTQgMCAwIDAtNjgtMTEzIDU1NyA1NTcgMCAwIDAtNzMtMTRsLTM2LTYtMTctMi0xNC0yLTE0LTItMTQtMi0yMS0zYy0zNy01LTQ4IDExLTE1IDIxbDI0IDcgMzIgOSA0MSA5IDEzIDMgMTIgMiA1NiAxNGMzIDEgNCAzIDExIDE1bDMzIDU3cTI2IDM5IDEwIDUwYy02IDItMjc5IDQtMjgyIDItMy0zLTEtNDc1IDItNDc4cTQtMyA3IDJsMyA2YTU3NiA1NzYgMCAwIDEgMzkgNjNsMjkgNTBjMTYgMjggMjIgMzIgMzkgMjRxMTgtNyAxMS0yMWwtNTMtOTAtNS03LTUtOS03LTEyYy0yMC0zNi00Ny01Mi03My00NW0tMjYgMjYyYy0xIDEzOS0xIDE0MC00IDE0MGwtNCAxLTc4IDM2LTIxIDEwLTI0IDExLTc3IDM0Yy00IDAgMi0xMiAyMS00N2wxMy0yMyAxMC0xOCA0MS03OCAzOS03MSA2LTEwIDYtMTEgNy0xMyA3LTEyIDYtMTIgMTItMjEgMzAtNTZjOS0xNiA4LTE2IDktNXptMCAyMTRjMCA0NSAyIDQxLTE2IDQxLTMzIDItMTUzIDItMTUzIDBsNS0zIDE0LTYgMTYtNyA1OS0yNiAyMS05IDQwLTE4YzE0LTcgMTMtOSAxNCAyOG00MzctNDM0cS0yMyAxMC04IDMzYTM0NyAzNDcgMCAwIDEgNTYgMTE3bDMgMTAgNCAyNGMxMSA1MyA1IDExOC0xNCAxNjZxLTExIDI0IDYgMzFjMjQgMTIgNDAtMTMgNTItODZhMzgxIDM4MSAwIDAgMC0xMC0xNTRjLTEyLTQ3LTM0LTk3LTU3LTEyOHEtMTYtMTktMzItMTNtLTc4IDYzcS0yMCA3IDMgNDJhMjUwIDI1MCAwIDAgMSAzMSAyMTJjLTQgMTIgNiAyNSAyMSAyNyAxNyAxIDI3LTIwIDMyLTcyYTMyMSAzMjEgMCAwIDAtNDMtMTg2cS0yMS0zNS00NC0yMyIvPjxwYXRoIGQ9Ik02NTQgMzY3YTMzOSAzMzkgMCAwIDAtNDkgMjNsLTc5IDM3LTQyIDIwLTEzIDYtMzEgMTZjLTE2IDEwLTIgMzEgMTYgMjNsNTctMjIgMjItOCAxOC03IDgtMyAyMS05IDIwLTcgNi0zIDIyLTkgMTItNSAxOS03YzIxLTkgMjYtMTYgMTktMzJxLTctMTktMjYtMTNtMzYgMTA3LTQwIDctMTQgMi0xMyAzLTE5IDMtMzIgNi0xNSAyLTE1IDMtNTEgMTAtMTIgMmMtMTMgMi0xOCAxMC0xMSAxNSA0IDIgMjUgMyAzOSAybDMyLTJjNzctNCAxMzktNyAxNTAtOWw5LTJjMjEtMyAxOS00NC0yLTQzeiIvPjwvZz48L3N2Zz4=&style=for-the-badge)](https://www.npmjs.com/package/prismcast)
[![Channels DVR](https://img.shields.io/badge/Channels%20DVR-Ready-636382?logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQxIiBoZWlnaHQ9IjE2NSIgdmlld0JveD0iMCAwIDI0MSAxNjUiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+CjxnIGNsaXAtcGF0aD0idXJsKCNjbGlwMCkiPgo8cGF0aCBkPSJNNDYuMSAzNS4ySDcuOTAwMDJWMTQyLjVINDYuMVYzNS4yWiIgZmlsbD0iI0VEREE2RiIvPgo8cGF0aCBkPSJNNDYuMSAxNDJINy45MDAwMlYxNTYuOEg0Ni4xVjE0MloiIGZpbGw9IiM3NTkyQ0EiLz4KPHBhdGggZD0iTTg0LjEgMzUuMkg0NS45VjE0Mi41SDg0LjFWMzUuMloiIGZpbGw9IiNBNEM0NUUiLz4KPHBhdGggZD0iTTg0LjEgMTQySDQ1LjlWMTU2LjhIODQuMVYxNDJaIiBmaWxsPSIjNDg1MUEzIi8+CjxwYXRoIGQ9Ik0xMjIuMSAzNS4ySDgzLjlWMTQyLjVIMTIyLjFWMzUuMloiIGZpbGw9IiM2NUJCQzUiLz4KPHBhdGggZD0iTTEyMi4xIDE0Mkg4My45VjE1Ni44SDEyMi4xVjE0MloiIGZpbGw9IiNFNDZENjIiLz4KPHBhdGggZD0iTTE2MC4xIDM1LjJIMTIxLjlWMTQyLjVIMTYwLjFWMzUuMloiIGZpbGw9IiNDNjgzQTAiLz4KPHBhdGggZD0iTTE2MC4xIDE0MkgxMjEuOVYxNTYuOEgxNjAuMVYxNDJaIiBmaWxsPSIjNTQ0QTlFIi8+CjxwYXRoIGQ9Ik0xOTguMSAzNS4ySDE1OS45VjE0Mi41SDE5OC4xVjM1LjJaIiBmaWxsPSIjRTQ2RDYyIi8+CjxwYXRoIGQ9Ik0xOTguMSAxNDJIMTU5LjlWMTU2LjhIMTk4LjFWMTQyWiIgZmlsbD0iIzY1QkFDNCIvPgo8cGF0aCBkPSJNMjM2LjEgMzUuMkgxOTcuOVYxNDIuNUgyMzYuMVYzNS4yWiIgZmlsbD0iIzc1OTJDQSIvPgo8cGF0aCBkPSJNMjM2LjEgMTQySDE5Ny45VjE1Ni44SDIzNi4xVjE0MloiIGZpbGw9IiM2MDQyOUEiLz4KPHBhdGggZD0iTTIzNi45IDI3LjZIMTM1LjhDMTQyLjMgMjAuOCAxNTUuNiA2LjkgMTU3LjMgNUMxNTguNiAzLjUgMTU4LjIgMS45IDE1Ny4yIDAuODk5OTk3QzE1Ni4xIC0wLjEwMDAwMyAxNTQuMiAtMC41MDAwMDMgMTUzLjEgMC41OTk5OTdDMTUxLjkgMS44IDEyNy4xIDI0LjkgMTI0LjIgMjcuNkgxMTYuOUMxMTQgMjQuOSA4OS4xIDEuOCA4OCAwLjU5OTk5N0M4Ni45IC0wLjUwMDAwMyA4NSAtMC4xMDAwMDMgODMuOSAwLjg5OTk5N0M4Mi45IDEuOSA4Mi41IDMuNSA4My44IDQuOUM4NS41IDYuNyA5OC44IDIwLjcgMTA1LjMgMjcuNUg0QzEuOCAyNy41IDAgMjkuMyAwIDMxLjVWMTYwLjJDMCAxNjIuNCAxLjggMTY0LjIgNCAxNjQuMkgyMzYuOUMyMzkuMSAxNjQuMiAyNDAuOSAxNjIuNCAyNDAuOSAxNjAuMlYzMS42QzI0MC45IDI5LjQgMjM5LjEgMjcuNiAyMzYuOSAyNy42Wk0yMzIuOSAxNTYuM0g4VjM1LjZIMjMyLjlWMTU2LjNaIiBmaWxsPSJ3aGl0ZSIvPgo8L2c+CjxkZWZzPgo8Y2xpcFBhdGggaWQ9ImNsaXAwIj4KPHJlY3Qgd2lkdGg9IjI0MC45IiBoZWlnaHQ9IjE2NC4zIiBmaWxsPSJ3aGl0ZSIvPgo8L2NsaXBQYXRoPgo8L2RlZnM+Cjwvc3ZnPgo=&style=for-the-badge)](https://getchannels.com/)

## Browser-based live TV capture for [Channels DVR](https://getchannels.com) and [Plex](https://plex.tv).

</DIV>
</SPAN>

PrismCast captures live video from web-based TV streaming sites and delivers it as HLS streams for [Channels DVR](https://getchannels.com/) and as MPEG-TS streams for [Plex](https://www.plex.tv/) via built-in HDHomeRun emulation. It uses Google Chrome to navigate to streaming sites, captures the video output, and serves it on your network. Most channels require a cable or streaming TV subscription - log in once with your TV provider credentials and Chrome remembers your session for future use.

This project is inspired by and builds upon the excellent work of [Chrome Capture for Channels](https://github.com/fancybits/chrome-capture-for-channels) by the Channels DVR team. I'm grateful to them for creating the original foundation that made PrismCast possible.

The name PrismCast reflects what the project does: like a prism transforming light into a spectrum of colors, PrismCast takes video from diverse streaming sources and refracts it into a unified HLS format for your DVR.

## A Note About This Project

PrismCast started as an experiment: could I create a complete, production-quality application using only AI tools? Every line of code in this project was written by AI (Claude), but built on a foundation of my existing open source projects, coding style, and design philosophy. The AI learned from my prior work and preferences to produce code that feels like mine—because in many ways, it is. The result is a modern, fully-featured streaming server that I use daily.

I share this not as a gimmick, but because I think it's genuinely interesting. The AI handled everything from the initial architecture to the nuanced edge cases of browser automation and video streaming. My role was to provide direction, review the output, and iterate on the design. It's been a fascinating collaboration between human taste and AI capability, and I hope the code quality speaks for itself.

## Why PrismCast?

If you're already using Chrome Capture for Channels and it's working well for you, that's wonderful! There's no need to switch. However, if you're looking for something different, PrismCast offers a modern TypeScript codebase, a real-time web interface, intelligent recovery, and the flexibility to easily add your own channels. The site profile system makes it straightforward to add support for new streaming sites, and contributions are always welcome!

## Features

### Channels and Streaming
- **Preconfigured channels** - PrismCast comes ready to stream most major US television networks out of the box. Just authenticate with your TV provider and you're ready to go.
- **Custom channel support** - Easily add your own streaming sources through the web interface, from YouTube live streams to niche international channels. If a site plays video in Chrome, there's a good chance PrismCast can capture it.
- **Plex integration** - Built-in HDHomeRun emulation lets Plex discover PrismCast as a network tuner. Add it as a DVR source in Plex for live TV and recording.
- **Multiple concurrent streams** - Stream up to 10 channels simultaneously (configurable), perfect for recording multiple shows at once.
- **Session persistence** - Log in to your TV provider once and Chrome remembers your session across restarts.
- **Quality presets** - Choose from 480p to 4K with automatic adaptation to your display capabilities.

### Web Interface
- **Real-time dashboard** - Monitor all active streams with live health status, duration, memory usage, and (when recording via Channels DVR) the name of the show being recorded.
- **Channel management** - Add, edit, and delete custom channels directly in the browser. No config files to edit.
- **Live log viewer** - Stream server logs in real-time with level filtering, perfect for troubleshooting.
- **Configuration UI** - Adjust all settings through an intuitive web interface with instant validation.
- **Dark mode** - Automatic dark theme based on your system preferences.
- **Backup and restore** - Download your settings and channels for safekeeping, restore them anytime.

### Reliability
- **Intelligent playback recovery** - Issue-aware recovery system that chooses the right fix for different problems. Buffering issues get different treatment than paused playback.
- **Circuit breaker protection** - Streams that fail repeatedly are automatically terminated, preventing resource exhaustion.
- **Health monitoring** - Built-in `/health` endpoint for integration with monitoring systems.
- **Graceful degradation** - If your display can't support your chosen quality preset, PrismCast automatically uses the best available resolution.

### Technical
- **Native HLS segmentation** - Built-in fMP4 segmenter with no external dependencies for segment generation.
- **Flexible capture modes** - Choose between FFmpeg-based capture (more stable for long recordings) or native Chrome capture (no dependencies).
- **Site profile system** - Data-driven configuration for handling different streaming sites. Profiles define how to enter fullscreen, handle iframes, manage multi-channel players, and more. Adding support for a new site often requires just a few lines of configuration.
- **Gracenote integration** - Channels can include station IDs for automatic guide data matching in Channels DVR.
- **MPEG-TS output** - In addition to HLS, PrismCast serves MPEG-TS streams for HDHomeRun-compatible clients. FFmpeg remuxes fMP4 to MPEG-TS with codec copy (no transcoding).
- **Modern codebase** - Clean TypeScript with ESM modules, full type safety, and comprehensive documentation.

## Requirements

- **macOS, Linux, or Windows** - PrismCast is developed on macOS and also runs on Linux (natively or via Docker) and Windows.
- **Node.js 22** or later (not required for Docker deployment)
- **Google Chrome** (PrismCast will try to find it automatically, or you can specify the path; included in the Docker image)
- **Channels DVR**, **Plex**, or any client that can consume HLS or MPEG-TS streams

## Installation

PrismCast can be installed as a Node.js package or deployed as a Docker container. For Docker, see [Docker / Container Deployment](#docker--container-deployment) below.

The recommended approach for native installation is to install PrismCast globally and run it as a system service:

```sh
npm install -g prismcast
```

Once installed, you can start PrismCast with:

```sh
prismcast
```

### Running as a Service

For the best experience, install PrismCast as a service that starts automatically at login:

```sh
prismcast service install
```

This configures your system's service manager (launchd on macOS, systemd on Linux, Task Scheduler on Windows) to run PrismCast in the background.

Other service commands:

```sh
prismcast service status    # Check if the service is running
prismcast service stop      # Stop the service
prismcast service start     # Start the service
prismcast service restart   # Restart the service
prismcast service uninstall # Remove the service
```

## Quick Start with Channels DVR

1. **Start PrismCast** and open `http://localhost:5589` in your browser
2. **Authenticate with your TV provider** - Click "Login" on any channel that requires authentication, complete the login in the browser window that opens, then click "Done"
3. **Add to Channels DVR**:
   - Go to Settings → Custom Channels → Add Source
   - Select **M3U Playlist**
   - Enter: `http://<your-prismcast-host>:5589/playlist`
   - Set Stream Format to **HLS**

That's it! Your channels will appear in the Channels DVR guide.

## Quick Start with Plex

PrismCast includes built-in HDHomeRun emulation, allowing Plex to discover it as a network tuner.

1. **Start PrismCast** — HDHomeRun emulation starts automatically on port 5004
2. **Add to Plex**:
   - Go to Settings → Live TV & DVR → Set Up Plex DVR
   - Enter your PrismCast server address with the HDHR port: `<your-prismcast-host>:5004`
   - Plex will detect PrismCast as an HDHomeRun tuner and import available channels
3. **Authenticate** — If channels require TV provider login, go to the PrismCast web interface at `http://localhost:5589` and use the Channels tab to log in

HDHomeRun emulation requires FFmpeg capture mode (the default). It is automatically disabled in native capture mode.

## Configuration

PrismCast includes a web-based configuration interface at `http://localhost:5589/#config`. From there you can:

- **Manage channels** - View all available channels, add your own custom channels, or override the defaults
- **Adjust quality settings** - Choose from presets like 720p, 1080p, or 4K
- **Configure HLS parameters** - Segment duration, buffer size, idle timeout
- **Configure HDHomeRun** - Enable or disable Plex integration, set the HDHR port and device name
- **Tune recovery behavior** - Adjust how aggressively PrismCast recovers from playback issues
- **Backup and restore** - Download your configuration for safekeeping

Configuration is stored in `~/.prismcast/config.json` and your TV provider sessions are preserved in `~/.prismcast/chromedata/`.

## Platform Support

**macOS** is the primary development and testing platform. PrismCast is thoroughly tested on macOS and should work reliably there.

**Linux** is supported both natively and via Docker. The Docker image includes everything needed to run PrismCast (Chrome, virtual display, VNC access) and is the recommended approach for Linux server deployments. Native Linux installation works with Node.js and Google Chrome installed manually.

**Windows** is supported and users have reported success running PrismCast on Windows 11. Windows is not the primary development platform, so bug reports and pull requests are always appreciated.

## Docker / Container Deployment

PrismCast provides a prebuilt Docker image with everything included: Google Chrome, a virtual display (Xvfb), VNC access, and a browser-based noVNC interface for managing TV provider authentication. The image is available on [Docker Hub](https://hub.docker.com/r/bnhf/prismcast).

### Quick Start with Docker Compose

The repository includes a ready-to-use Docker Compose file. This is the recommended approach for Docker deployments.

1. Download the compose file and environment template:

```bash
curl -O https://raw.githubusercontent.com/hjdhjd/prismcast/main/prismcast.yaml
curl -O https://raw.githubusercontent.com/hjdhjd/prismcast/main/prismcast.env.example
```

2. Optionally, copy the environment template and customize it:

```bash
cp prismcast.env.example prismcast.env
```

3. Start the container:

```bash
docker compose -f prismcast.yaml up -d
```

4. Open `http://localhost:5589` for the PrismCast web interface and `http://localhost:6080/vnc.html` for browser-based VNC access to Chrome.

### Quick Start with Docker Run

If you prefer not to use Docker Compose:

```bash
docker run -d \
  --name prismcast \
  --shm-size=1g \
  -p 5589:5589 \
  -p 5900:5900 \
  -p 6080:6080 \
  -p 5004:5004 \
  -v prismcast-data:/root/.prismcast \
  bnhf/prismcast:latest
```

### Ports

| Port | Service | Description |
|------|---------|-------------|
| 5589 | PrismCast | Web interface and HLS/MPEG-TS streaming |
| 5900 | VNC | Direct VNC access to the Chrome browser |
| 6080 | noVNC | Browser-based VNC access (no VNC client needed) |
| 5004 | HDHomeRun | HDHomeRun emulation for Plex |

### TV Provider Authentication

TV provider authentication requires interacting with the Chrome browser running inside the container. The container includes two built-in options:

1. **noVNC (recommended)** - Open `http://localhost:6080/vnc.html` in any browser for a web-based view of the Chrome instance. No VNC client needed. Use this to complete TV provider logins, then return to the PrismCast web interface and click "Done" on the channel.
2. **VNC** - Connect any VNC client to `localhost:5900` for direct access. Set the `NOVNC_PASSWORD` environment variable to require a password for VNC connections.

Your TV provider sessions are stored in the persistent volume and survive container restarts.

### Display Resolution

The virtual display resolution must match or exceed your configured quality preset. The default is 1920x1080, which supports all presets up to 1080p High. Adjust `SCREEN_WIDTH` and `SCREEN_HEIGHT` if you need a different resolution.

| Preset | Minimum Resolution |
|--------|-------------------|
| 480p | 854x480 |
| 720p / 720p High | 1280x720 |
| 1080p / 1080p High | 1920x1080 |
| 4K | 3840x2160 |

### Container Environment Variables

The container accepts environment variables for both the virtual display and PrismCast itself. Display variables are set in the compose file's `environment:` section or via `-e` flags with `docker run`. PrismCast variables can be set the same way.

**Display and VNC:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DISPLAY_NUM` | 99 | X11 display number |
| `SCREEN_WIDTH` | 1920 | Virtual display width in pixels |
| `SCREEN_HEIGHT` | 1080 | Virtual display height in pixels |
| `SCREEN_DEPTH` | 24 | Virtual display color depth |
| `NOVNC_PASSWORD` | (none) | Password for VNC/noVNC access. If unset, VNC is open without authentication. |

**PrismCast:**

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 5589 | HTTP server port |
| `HOST` | 0.0.0.0 | HTTP server bind address |
| `CHROME_BIN` | (auto) | Path to Chrome executable |
| `QUALITY_PRESET` | 720p-high | Video quality: 480p, 720p, 720p-high, 1080p, 1080p-high, 4k |
| `VIDEO_BITRATE` | 12000000 | Video bitrate in bps |
| `AUDIO_BITRATE` | 256000 | Audio bitrate in bps |
| `FRAME_RATE` | 60 | Target frame rate |
| `CAPTURE_MODE` | ffmpeg | Capture mode: "ffmpeg" (more stable) or "native" |
| `HDHR_ENABLED` | true | Enable HDHomeRun emulation for Plex |
| `HDHR_PORT` | 5004 | HDHomeRun emulation server port |
| `HDHR_FRIENDLY_NAME` | PrismCast | Device name shown in Plex |
| `HLS_SEGMENT_DURATION` | 2 | HLS segment duration in seconds |
| `HLS_MAX_SEGMENTS` | 10 | Maximum segments kept in memory per stream |
| `HLS_IDLE_TIMEOUT` | 30000 | Idle stream timeout in milliseconds |
| `MAX_CONCURRENT_STREAMS` | 10 | Maximum simultaneous streams |

### Persistent Storage

The compose file mounts a Docker volume at `/root/.prismcast`, which stores:

- **Configuration** - `config.json` with all PrismCast settings
- **Custom channels** - `channels.json` with user-defined channel definitions
- **Chrome profile** - TV provider login sessions and cookies
- **Logs** - `prismcast.log` for troubleshooting

This volume persists across container restarts and image updates. Back up this volume to preserve your configuration and login sessions.

### Building from Source

If you prefer to build the Docker image yourself:

```bash
git clone https://github.com/hjdhjd/prismcast.git
cd prismcast
docker buildx build --platform linux/amd64 -f Dockerfile -t prismcast:local .
```

Then use `prismcast:local` as the image name in your compose file or `docker run` command.

## Contributing

Contributions are welcome! Whether it's bug reports, feature requests, new channel definitions, or pull requests, I appreciate your interest in improving PrismCast. If you've got a streaming site that works well with PrismCast, consider submitting a pull request to add it to the preconfigured channels.

## License

[ISC License](LICENSE.md)

Copyright (c) 2024-2026 HJD

## Acknowledgments

Special thanks to the [Channels DVR](https://getchannels.com/) team for creating such a wonderful DVR platform and for their work on [Chrome Capture for Channels](https://github.com/fancybits/chrome-capture-for-channels), which inspired this project.

## Development Dashboard

This is mostly of interest to the true developer nerds amongst us.

[![License](https://img.shields.io/npm/l/prismcast?color=636382&label=PrismCast&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxMDAwIiBoZWlnaHQ9IjEwMDAiIHZpZXdCb3g9IjAgMCAxMDAwIDEwMDAiPjxnIGZpbGw9IiNmZmYiPjxwYXRoIGQ9Ik0zODYgMjIwYy0xOCA2LTIyIDExLTQ2IDU1bC0xMSAxOS01NSA5OS00NCA4MS0xNSAyNi05IDE3LTUgOC0zMyA2MS02NCAxMTdjLTQgOS00IDI5IDAgMzdsNCA3YzYgMTQgMjQgMjggNDEgMzEgMTAgMiA1MzIgMiA1NDAgMCAzOS0xMCA1My00NyAzMi04NmExNTE0IDE1MTQgMCAwIDAtNjgtMTEzIDU1NyA1NTcgMCAwIDAtNzMtMTRsLTM2LTYtMTctMi0xNC0yLTE0LTItMTQtMi0yMS0zYy0zNy01LTQ4IDExLTE1IDIxbDI0IDcgMzIgOSA0MSA5IDEzIDMgMTIgMiA1NiAxNGMzIDEgNCAzIDExIDE1bDMzIDU3cTI2IDM5IDEwIDUwYy02IDItMjc5IDQtMjgyIDItMy0zLTEtNDc1IDItNDc4cTQtMyA3IDJsMyA2YTU3NiA1NzYgMCAwIDEgMzkgNjNsMjkgNTBjMTYgMjggMjIgMzIgMzkgMjRxMTgtNyAxMS0yMWwtNTMtOTAtNS03LTUtOS03LTEyYy0yMC0zNi00Ny01Mi03My00NW0tMjYgMjYyYy0xIDEzOS0xIDE0MC00IDE0MGwtNCAxLTc4IDM2LTIxIDEwLTI0IDExLTc3IDM0Yy00IDAgMi0xMiAyMS00N2wxMy0yMyAxMC0xOCA0MS03OCAzOS03MSA2LTEwIDYtMTEgNy0xMyA3LTEyIDYtMTIgMTItMjEgMzAtNTZjOS0xNiA4LTE2IDktNXptMCAyMTRjMCA0NSAyIDQxLTE2IDQxLTMzIDItMTUzIDItMTUzIDBsNS0zIDE0LTYgMTYtNyA1OS0yNiAyMS05IDQwLTE4YzE0LTcgMTMtOSAxNCAyOG00MzctNDM0cS0yMyAxMC04IDMzYTM0NyAzNDcgMCAwIDEgNTYgMTE3bDMgMTAgNCAyNGMxMSA1MyA1IDExOC0xNCAxNjZxLTExIDI0IDYgMzFjMjQgMTIgNDAtMTMgNTItODZhMzgxIDM4MSAwIDAgMC0xMC0xNTRjLTEyLTQ3LTM0LTk3LTU3LTEyOHEtMTYtMTktMzItMTNtLTc4IDYzcS0yMCA3IDMgNDJhMjUwIDI1MCAwIDAgMSAzMSAyMTJjLTQgMTIgNiAyNSAyMSAyNyAxNyAxIDI3LTIwIDMyLTcyYTMyMSAzMjEgMCAwIDAtNDMtMTg2cS0yMS0zNS00NC0yMyIvPjxwYXRoIGQ9Ik02NTQgMzY3YTMzOSAzMzkgMCAwIDAtNDkgMjNsLTc5IDM3LTQyIDIwLTEzIDYtMzEgMTZjLTE2IDEwLTIgMzEgMTYgMjNsNTctMjIgMjItOCAxOC03IDgtMyAyMS05IDIwLTcgNi0zIDIyLTkgMTItNSAxOS03YzIxLTkgMjYtMTYgMTktMzJxLTctMTktMjYtMTNtMzYgMTA3LTQwIDctMTQgMi0xMyAzLTE5IDMtMzIgNi0xNSAyLTE1IDMtNTEgMTAtMTIgMmMtMTMgMi0xOCAxMC0xMSAxNSA0IDIgMjUgMyAzOSAybDMyLTJjNzctNCAxMzktNyAxNTAtOWw5LTJjMjEtMyAxOS00NC0yLTQzeiIvPjwvZz48L3N2Zz4=&logoColor=%23FFFFFF&style=for-the-badge)](https://github.com/hjdhjd/prismcast/blob/main/LICENSE.md)
[![Build Status](https://img.shields.io/github/actions/workflow/status/hjdhjd/prismcast/ci.yml?color=636382&logo=github&logoColor=%23FFFFFF&style=for-the-badge)](https://github.com/hjdhjd/prismcast/actions?query=workflow%3A%22Continuous+Integration%22)
[![Dependencies](https://img.shields.io/librariesio/release/npm/prismcast?color=636382&logo=nodedotjs&logoColor=%23FFFFFF&style=for-the-badge)](https://libraries.io/npm/prismcast)
[![GitHub commits since latest release](https://img.shields.io/github/commits-since/hjdhjd/prismcast/latest?color=636382&logo=github&logoColor=%23FFFFFF&style=for-the-badge)](https://github.com/hjdhjd/prismcast/commits/main)
