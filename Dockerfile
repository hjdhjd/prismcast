# docker buildx build --platform linux/amd64 -f Dockerfile -t bnhf/prismcast:latest -t bnhf/prismcast:2026.01.29 . --push --no-cache
FROM node:22-slim

# Prevent interactive prompts during package installation.
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies, Chrome dependencies, x11vnc, and noVNC.
RUN apt-get update && apt-get install -y \
    # Basic utilities.
    curl \
    wget \
    gnupg \
    ca-certificates \
    # Xvfb and X11 utilities.
    xvfb \
    x11vnc \
    x11-xkb-utils \
    xfonts-100dpi \
    xfonts-75dpi \
    xfonts-scalable \
    x11-apps \
    xauth \
    # noVNC for web-based VNC access.
    novnc \
    # Chrome dependencies (comprehensive list from cc4c).
    gconf-service \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libgcc1 \
    libgconf-2-4 \
    libgdk-pixbuf2.0-0 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxkbcommon0 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    lsb-release \
    xdg-utils \
    # Fonts for proper rendering.
    fonts-liberation \
    fonts-ipafont-gothic \
    fonts-wqy-zenhei \
    fonts-thai-tlwg \
    fonts-kacst \
    fonts-freefont-ttf \
    # Process management.
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome. Chromium is not supported — PrismCast requires Google Chrome for the puppeteer-stream extension.
RUN wget -q -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y /tmp/google-chrome.deb \
    && rm /tmp/google-chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Install PrismCast globally from npm.
RUN npm install -g prismcast

# Create a Chrome wrapper script that passes --no-sandbox for container environments. Chrome refuses to run as root without this flag.
RUN echo '#!/bin/bash\nexec /usr/bin/google-chrome-stable --no-sandbox --disable-setuid-sandbox "$@"' > /usr/local/bin/chrome-no-sandbox \
    && chmod +x /usr/local/bin/chrome-no-sandbox

# Copy the startup script into the container.
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create the VNC password directory. Users can mount a password file here for authenticated VNC access.
RUN mkdir -p /root/.vnc

# Set environment variables for the virtual display, Chrome binary, and Puppeteer.
ENV DISPLAY=:99
ENV CHROME_BIN=/usr/local/bin/chrome-no-sandbox
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

# Expose all ports the container listens on.
# 5589 - PrismCast web UI and streaming.
# 5900 - VNC server for direct VNC client access.
# 6080 - noVNC web interface for browser-based Chrome access.
# 5004 - HDHomeRun emulation.
EXPOSE 5589 5900 6080 5004

# Health check verifies that the PrismCast web server is responding.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s \
    CMD wget -q --spider http://localhost:5589/health || exit 1

# Use the entrypoint script to start Xvfb, x11vnc, noVNC, and PrismCast.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
