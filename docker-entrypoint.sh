#!/bin/bash
# docker-entrypoint.sh
# 2026.01.29

set -e

# Configuration with defaults
DISPLAY_NUM=${DISPLAY_NUM:-99}
SCREEN_WIDTH=${SCREEN_WIDTH:-1920}
SCREEN_HEIGHT=${SCREEN_HEIGHT:-1080}
SCREEN_DEPTH=${SCREEN_DEPTH:-24}
VNC_PORT=${VNC_PORT:-5900}
NOVNC_PORT=${NOVNC_PORT:-6080}

export DISPLAY=:${DISPLAY_NUM}

echo "Starting PrismCast with noVNC support..."
echo "  Display: ${DISPLAY}"
echo "  Screen: ${SCREEN_WIDTH}x${SCREEN_HEIGHT}x${SCREEN_DEPTH}"
echo "  VNC Port: ${VNC_PORT}"
echo "  noVNC Port: ${NOVNC_PORT}"
echo "  PrismCast Port: 5589"

# Cleanup function
cleanup() {
    echo "Shutting down..."
    # Kill PrismCast gracefully first (it has its own shutdown handler)
    if [ -n "$PRISMCAST_PID" ]; then
        kill -TERM $PRISMCAST_PID 2>/dev/null || true
        wait $PRISMCAST_PID 2>/dev/null || true
    fi
    # Kill remaining background jobs
    kill $(jobs -p) 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

# Clean up stale X11 lock files from previous runs (fixes restart issues)
rm -f /tmp/.X${DISPLAY_NUM}-lock /tmp/.X11-unix/X${DISPLAY_NUM}

# Start Xvfb (virtual framebuffer)
echo "Starting Xvfb..."
Xvfb ${DISPLAY} -screen 0 ${SCREEN_WIDTH}x${SCREEN_HEIGHT}x${SCREEN_DEPTH} &
XVFB_PID=$!
sleep 2

# Verify Xvfb started
if ! kill -0 $XVFB_PID 2>/dev/null; then
    echo "ERROR: Xvfb failed to start"
    exit 1
fi
echo "Xvfb started successfully"

# Start x11vnc (VNC server for the virtual display)
echo "Starting x11vnc..."
if [ -f /root/.vnc/passwd ]; then
    # Use password if configured
    x11vnc -display ${DISPLAY} -forever -shared -rfbauth /root/.vnc/passwd -rfbport ${VNC_PORT} -quiet &
else
    # No password (use NOVNC_PASSWORD env var to set one)
    if [ -n "$NOVNC_PASSWORD" ]; then
        x11vnc -storepasswd "$NOVNC_PASSWORD" /root/.vnc/passwd
        x11vnc -display ${DISPLAY} -forever -shared -rfbauth /root/.vnc/passwd -rfbport ${VNC_PORT} -quiet &
    else
        x11vnc -display ${DISPLAY} -forever -shared -nopw -rfbport ${VNC_PORT} -quiet &
    fi
fi
X11VNC_PID=$!
sleep 1

# Verify x11vnc started
if ! kill -0 $X11VNC_PID 2>/dev/null; then
    echo "ERROR: x11vnc failed to start"
    exit 1
fi
echo "x11vnc started successfully"

# Start noVNC (web-based VNC client)
echo "Starting noVNC..."
/usr/share/novnc/utils/novnc_proxy --vnc localhost:${VNC_PORT} --listen ${NOVNC_PORT} &
NOVNC_PID=$!
sleep 1

# Verify noVNC started
if ! kill -0 $NOVNC_PID 2>/dev/null; then
    echo "ERROR: noVNC failed to start"
    exit 1
fi
echo "noVNC started successfully"

echo ""
echo "=============================================="
echo "  noVNC available at: http://localhost:${NOVNC_PORT}/vnc.html"
echo "  PrismCast UI at:    http://localhost:5589"
echo "=============================================="
echo ""

# Start PrismCast (logs to file by default)
echo "Starting PrismCast..."
LOGFILE="/root/.prismcast/prismcast.log"

# Ensure the log directory exists
mkdir -p /root/.prismcast

# Start PrismCast in the background
prismcast "$@" &
PRISMCAST_PID=$!

# Wait for the log file to be created (max 10 seconds)
for i in {1..20}; do
    if [ -f "$LOGFILE" ]; then
        break
    fi
    sleep 0.5
done

# Tail the log file to stdout for Portainer/Docker logs (only new entries)
if [ -f "$LOGFILE" ]; then
    tail -n 0 -f "$LOGFILE" &
    TAIL_PID=$!
fi

# Wait for PrismCast to exit
wait $PRISMCAST_PID
EXIT_CODE=$?

# Clean up tail process
if [ -n "$TAIL_PID" ]; then
    kill $TAIL_PID 2>/dev/null || true
fi

exit $EXIT_CODE
