#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${GROQ_API_KEY:-}" ]]; then
  echo "GROQ_API_KEY is required" >&2
  exit 2
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required to call Groq STT" >&2
  exit 2
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

audio_path="$tmp_dir/rustzap-groq-smoke.wav"
response_path="$tmp_dir/groq-response.json"

if [[ -z "${GROQ_STT_AUDIO_PATH:-}" ]]; then
  echo "GROQ_STT_AUDIO_PATH=/path/to/audio.wav is required" >&2
  exit 2
fi

if [[ ! -f "$GROQ_STT_AUDIO_PATH" ]]; then
  echo "GROQ_STT_AUDIO_PATH does not exist: ${GROQ_STT_AUDIO_PATH}" >&2
  exit 2
fi
cp "$GROQ_STT_AUDIO_PATH" "$audio_path"

http_status="$(
  curl -sS \
    -o "$response_path" \
    -w "%{http_code}" \
    -X POST "https://api.groq.com/openai/v1/audio/transcriptions" \
    -H "Authorization: Bearer ${GROQ_API_KEY}" \
    -F "file=@${audio_path};type=audio/wav" \
    -F "model=${GROQ_STT_MODEL:-whisper-large-v3-turbo}" \
    -F "language=${GROQ_STT_LANGUAGE:-pt}" \
    -F "response_format=${GROQ_STT_RESPONSE_FORMAT:-verbose_json}"
)"

if [[ "$http_status" != 2* ]]; then
  echo "Groq STT returned HTTP ${http_status}" >&2
  cat "$response_path" >&2
  exit 1
fi

if ! grep -q '"text"' "$response_path"; then
  echo "Groq STT response did not contain text" >&2
  cat "$response_path" >&2
  exit 1
fi

echo "Groq STT smoke test passed"
