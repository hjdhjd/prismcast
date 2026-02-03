#!/bin/bash
# docker-entrypoint.sh
# 2026.01.29

set -e

# Set configuration defaults for the virtual display, VNC, and noVNC.
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

# Graceful shutdown handler. We terminate PrismCast first because it has its own shutdown handler that closes the browser and active streams cleanly. After
# PrismCast exits, we kill the remaining background services (Xvfb, x11vnc, noVNC, tail).
cleanup() {
  echo "Shutting down..."
  if [ -n "$PRISMCAST_PID" ]; then
    kill -TERM $PRISMCAST_PID 2>/dev/null || true
    wait $PRISMCAST_PID 2>/dev/null || true
  fi
  kill $(jobs -p) 2>/dev/null || true
  exit 0
}
trap cleanup SIGTERM SIGINT

# Remove stale X11 lock files from previous container runs. Without this, Xvfb refuses to start after an unclean shutdown.
rm -f /tmp/.X${DISPLAY_NUM}-lock /tmp/.X11-unix/X${DISPLAY_NUM}

# Start Xvfb (virtual framebuffer).
echo "Starting Xvfb..."
Xvfb ${DISPLAY} -screen 0 ${SCREEN_WIDTH}x${SCREEN_HEIGHT}x${SCREEN_DEPTH} &
XVFB_PID=$!
sleep 2

# Verify that Xvfb started successfully.
if ! kill -0 $XVFB_PID 2>/dev/null; then
  echo "ERROR: Xvfb failed to start."
  exit 1
fi
echo "Xvfb started successfully."

# Start x11vnc (VNC server for the virtual display).
echo "Starting x11vnc..."
if [ -f /root/.vnc/passwd ]; then
  # Use an existing VNC password file if one has been configured.
  x11vnc -display ${DISPLAY} -forever -shared -rfbauth /root/.vnc/passwd -rfbport ${VNC_PORT} -quiet &
else
  # No existing password file. If NOVNC_PASSWORD is set, create one from the environment variable. Otherwise, run without authentication.
  if [ -n "$NOVNC_PASSWORD" ]; then
    x11vnc -storepasswd "$NOVNC_PASSWORD" /root/.vnc/passwd
    x11vnc -display ${DISPLAY} -forever -shared -rfbauth /root/.vnc/passwd -rfbport ${VNC_PORT} -quiet &
  else
    x11vnc -display ${DISPLAY} -forever -shared -nopw -rfbport ${VNC_PORT} -quiet &
  fi
fi
X11VNC_PID=$!
sleep 1

# Verify that x11vnc started successfully.
if ! kill -0 $X11VNC_PID 2>/dev/null; then
  echo "ERROR: x11vnc failed to start."
  exit 1
fi
echo "x11vnc started successfully."

# Start noVNC (web-based VNC client).
echo "Starting noVNC..."
/usr/share/novnc/utils/novnc_proxy --vnc localhost:${VNC_PORT} --listen ${NOVNC_PORT} &
NOVNC_PID=$!
sleep 1

# Verify that noVNC started successfully.
if ! kill -0 $NOVNC_PID 2>/dev/null; then
  echo "ERROR: noVNC failed to start."
  exit 1
fi
echo "noVNC started successfully."

echo ""
echo "=============================================="
echo "  noVNC available at: http://localhost:${NOVNC_PORT}/vnc.html"
echo "  PrismCast UI at:    http://localhost:5589"
echo "=============================================="
echo ""

# Start PrismCast in the background. PrismCast logs to a file by default.
echo "Starting PrismCast..."
LOGFILE="/root/.prismcast/prismcast.log"

# Ensure the log directory exists before PrismCast starts writing to it.
mkdir -p /root/.prismcast

# Launch PrismCast, forwarding any command-line arguments from docker run.
prismcast "$@" &
PRISMCAST_PID=$!

# Wait for PrismCast to create its log file (up to 10 seconds).
for i in {1..20}; do
  if [ -f "$LOGFILE" ]; then
    break
  fi
  sleep 0.5
done

# Tail the log file to stdout so that Portainer and docker logs can display PrismCast output. We use -n 0 to skip existing log entries and only show new ones.
if [ -f "$LOGFILE" ]; then
  tail -n 0 -f "$LOGFILE" &
  TAIL_PID=$!
fi

# Wait for PrismCast to exit.
wait $PRISMCAST_PID
EXIT_CODE=$?

# Clean up the tail process if it was started.
if [ -n "$TAIL_PID" ]; then
  kill $TAIL_PID 2>/dev/null || true
fi

exit $EXIT_CODE
