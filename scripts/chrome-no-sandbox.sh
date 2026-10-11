#!/bin/bash

# Chrome uses the last --enable-features switch. Merge Puppeteer's features with
# the container's VA-API features so neither list replaces the other.
features=VaapiVideoDecoder,VaapiVideoEncoder,AcceleratedVideoEncoder,VaapiIgnoreDriverChecks
args=()
for arg in "$@"; do
    case "$arg" in
        --enable-features=*)
            extra_features=${arg#--enable-features=}
            if [[ -n "$extra_features" ]]; then
                features+=",$extra_features"
            fi
            ;;
        *) args+=("$arg") ;;
    esac
done

exec /usr/bin/google-chrome-stable --no-sandbox --disable-setuid-sandbox --disable-gpu-sandbox \
    "--enable-features=$features" --ignore-gpu-blocklist "${args[@]}"
