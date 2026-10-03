#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
streamr_root="${1:?Usage: run-streamr.sh /path/to/streamr [controller|leader|memory]}"
streamr_root="$(cd "$streamr_root" && pwd)"
mode="${2:-controller}"
case "$mode" in
  controller|leader) backend=rocksdb; checkpoint_mode="$mode" ;;
  memory) backend=memory; checkpoint_mode=controller ;;
  *) echo 'Mode must be controller, leader or memory' >&2; exit 2 ;;
esac
builder="${STREAMR_DEV_IMAGE:-streamr-state-dev}"
queue_wrapper="${STREAMR_BUILD_WRAPPER:-$streamr_root/scripts/rust-build}"
if [[ ! -x "$queue_wrapper" ]]; then
  echo 'Set STREAMR_BUILD_WRAPPER to the shared scripts/rust-build queue wrapper.' >&2
  exit 2
fi
cd "$repo_root"
directory="target/streamr-identity/$mode"
cache="$repo_root/target/capture-build/cargo-cache"
build_target="${STREAMR_CAPTURE_TARGET:-$streamr_root/target}"
mkdir -p "$directory" "$cache/registry" "$cache/git" "$build_target"
python3 test/streamr-reference/streamr_capture.py prepare "$directory" \
  --runtime-directory "/arc/$directory"
"$queue_wrapper" podman run --rm \
  -e CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}" \
  -e STREAMR_TEST_BACKEND="$backend" \
  -e STREAMR_TEST_CHECKPOINT_MODE="$checkpoint_mode" \
  -e STREAMR_IDENTITY_QUERY="/arc/$directory/query.sql" \
  -e STREAMR_IDENTITY_OUTPUT="/arc/$directory/streamr-internal.jsonl" \
  -v "$streamr_root:/app:z" -v "$build_target:/app/target:z" \
  -v "$repo_root:/arc:z" \
  -v "$cache/registry:/usr/local/cargo/registry:z" \
  -v "$cache/git:/usr/local/cargo/git:z" \
  -w /app "$builder" \
  cargo +1.96.0 test --locked -p arroyo-sql-testing arcstream_identity_capture \
  -- --ignored --nocapture --test-threads=1 \
  > "$directory/runtime.log" 2>&1 || {
    tail -80 "$directory/runtime.log"
    exit 1
  }
oracle=flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl
for phase in initial recovered; do
  input="$directory/streamr-internal.jsonl"
  if [[ "$phase" == initial ]]; then input="$directory/streamr-internal.initial.jsonl"; fi
  capture="$directory/$phase.jsonl"
  python3 test/streamr-reference/streamr_capture.py adapt "$input" "$capture"
  python3 test/streamr-reference/compare_identity.py "$oracle" "$capture" \
    > "$directory/$phase-comparison.json"
  cat "$directory/$phase-comparison.json"
done
STREAMR_CAPTURE_ROOT="$streamr_root" STREAMR_CAPTURE_MODE="$mode" \
STREAMR_CAPTURE_BUILDER_ID="$(podman image inspect "$builder" --format '{{.Id}}')" \
python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import subprocess

streamr = Path(os.environ["STREAMR_CAPTURE_ROOT"])
directory = Path("target/streamr-identity") / os.environ["STREAMR_CAPTURE_MODE"]
paths = [directory / name for name in ("query.sql", "input.jsonl", "initial.jsonl", "recovered.jsonl")]
paths += list(Path("test/streamr-reference").glob("*.py"))
paths += [Path("test/streamr-reference/identity-capture.sql"),
          Path("test/streamr-reference/run-streamr.sh"),
          Path("flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl")]
data = {"arcstream_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "arcstream_working_tree": subprocess.check_output(["git", "status", "--porcelain"], text=True),
        "streamr_revision": subprocess.check_output(["git", "-C", str(streamr), "rev-parse", "HEAD"], text=True).strip(),
        "streamr_working_tree": subprocess.check_output(["git", "-C", str(streamr), "status", "--porcelain"], text=True),
        "streamr_capture_hook_sha256": hashlib.sha256((streamr / "crates/arroyo-sql-testing/src/smoke_tests.rs").read_bytes()).hexdigest(),
        "builder_id": os.environ["STREAMR_CAPTURE_BUILDER_ID"], "mode": os.environ["STREAMR_CAPTURE_MODE"],
        "artifacts_sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}}
(directory / "provenance.json").write_text(json.dumps(data, indent=2) + "\n")
PY
