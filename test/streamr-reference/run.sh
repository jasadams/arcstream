#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
engine="${CONTAINER_ENGINE:-podman}"
builder="${REFERENCE_MAVEN_IMAGE:-docker.io/library/maven@sha256:32ce79e40d744b18c6ce9fe65d6b58189cbabc938cfeff657a534a359a5d3f92}"
mkdir -p target/reference/maven-cache

"$engine" run --rm \
  -v "$repo_root/flink/identity-resolution:/app:z" \
  -v "$repo_root/target/reference/maven-cache:/root/.m2:z" \
  -w /app "$builder" mvn -B test \
  > target/reference/flink-tests.log 2>&1 || {
    tail -80 target/reference/flink-tests.log
    exit 1
  }
python3 -m unittest discover -s test/streamr-reference -v \
  > target/reference/comparator-tests.log 2>&1 || {
    cat target/reference/comparator-tests.log
    exit 1
  }
python3 test/streamr-reference/compare_identity.py \
  flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl \
  flink/identity-resolution/target/reference/identity.jsonl \
  > target/reference/identity-comparison.json
python3 test/streamr-reference/compare_profile.py \
  flink/identity-resolution/src/test/resources/reference/profile-expected.jsonl \
  flink/identity-resolution/target/reference/profile.jsonl \
  flink/identity-resolution/target/reference/profile-fixture.json \
  > target/reference/profile-comparison.json
python3 test/streamr-reference/compare_session.py \
  flink/identity-resolution/src/test/resources/reference/session-expected.jsonl \
  flink/identity-resolution/target/reference/session.jsonl \
  > target/reference/session-comparison.json

REFERENCE_BUILDER="$builder" python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import subprocess

paths = [Path("flink/identity-resolution/pom.xml")]
for root in (Path("flink/identity-resolution/src"), Path("test/streamr-reference")):
    paths.extend(path for path in root.rglob("*") if path.is_file()
                 and "__pycache__" not in path.parts)
hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}
provenance = {
    "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "working_tree_status": subprocess.check_output(["git", "status", "--porcelain"], text=True),
    "builder": os.environ["REFERENCE_BUILDER"],
    "source_sha256": hashes,
    "identity_capture_sha256": hashlib.sha256(Path(
        "flink/identity-resolution/target/reference/identity.jsonl").read_bytes()).hexdigest(),
    "profile_capture_sha256": hashlib.sha256(Path(
        "flink/identity-resolution/target/reference/profile.jsonl").read_bytes()).hexdigest(),
    "profile_fixture_sha256": hashlib.sha256(Path(
        "flink/identity-resolution/target/reference/profile-fixture.json").read_bytes()).hexdigest(),
    "session_capture_sha256": hashlib.sha256(Path(
        "flink/identity-resolution/target/reference/session.jsonl").read_bytes()).hexdigest(),
    "session_fixture_sha256": hashlib.sha256(Path(
        "flink/identity-resolution/target/reference/session-fixture.json").read_bytes()).hexdigest(),
    "scope": "Flink keyed-operator reference; Streamr and Kafka qualification pending",
}
Path("target/reference/provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
PY
cat target/reference/identity-comparison.json
cat target/reference/profile-comparison.json
cat target/reference/session-comparison.json
printf 'Flink and comparator checks passed. Evidence: %s/target/reference\n' "$repo_root"
