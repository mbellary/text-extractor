#!/bin/sh
set -euo pipefail

# Where we’ll write the Google SA key
: "${GOOGLE_APPLICATION_CREDENTIALS:=/app/secrets/gcp-key.json}"
: "${GOOGLE_APPLICATION_CREDENTIALS_JSON:?GOOGLE_APPLICATION_CREDENTIALS_JSON is not set}"

# Create dir & lock down permissions
mkdir -p "$(dirname "$GOOGLE_APPLICATION_CREDENTIALS")"
umask 077

# Write the secret to file without printing it anywhere
# Note: the secret is plain JSON in the env var (not base64)
printf "%s" "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > "$GOOGLE_APPLICATION_CREDENTIALS"

# Optional sanity check (do NOT cat the file—avoids leaking secret)
[ -s "$GOOGLE_APPLICATION_CREDENTIALS" ] || {
  echo "Credential file is empty at $GOOGLE_APPLICATION_CREDENTIALS" >&2
  exit 1
}

# Hand off to your app (console script from pyproject.toml)
exec worker
# Or if you prefer module form:
# exec python -m text_extractor.worker
