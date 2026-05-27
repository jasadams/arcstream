#!/usr/bin/env bash
set -euo pipefail

REPO="jasadams/arcstream"
BRANCH="main"
BASE_URL="https://raw.githubusercontent.com/${REPO}/${BRANCH}"
INSTALL_DIR="arcstream"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${GREEN}[arcstream]${NC} $*"; }
warn()  { echo -e "${YELLOW}[arcstream]${NC} $*"; }
error() { echo -e "${RED}[arcstream]${NC} $*" >&2; }

check_prereqs() {
    local ok=true

    if ! command -v docker >/dev/null 2>&1; then
        error "docker not found. Install: https://docs.docker.com/get-docker/"
        ok=false
    fi

    if ! docker compose version >/dev/null 2>&1; then
        error "docker compose (v2) not found. Update Docker Desktop or install the compose plugin."
        ok=false
    fi

    if ! docker info >/dev/null 2>&1; then
        error "Docker daemon is not running. Start Docker and try again."
        ok=false
    fi

    if [ "$ok" = false ]; then
        exit 1
    fi

    local mem_kb
    if [ -f /proc/meminfo ]; then
        mem_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    elif command -v sysctl >/dev/null 2>&1; then
        mem_kb=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 ))
    else
        mem_kb=0
    fi

    if [ "$mem_kb" -gt 0 ] && [ "$mem_kb" -lt 12000000 ]; then
        warn "This system has $(( mem_kb / 1024 / 1024 ))GB RAM. Arcstream recommends at least 12GB."
        warn "The stack may run slowly or fail to start with insufficient memory."
    fi
}

download_files() {
    if [ -d "$INSTALL_DIR" ]; then
        info "Directory '$INSTALL_DIR' already exists, updating files..."
    else
        info "Creating $INSTALL_DIR/..."
    fi

    mkdir -p "${INSTALL_DIR}/config/pinot-schemas" "${INSTALL_DIR}/config/pinot-tables"

    local files=(
        "deploy/docker-compose.yml:docker-compose.yml"
        "deploy/config/flink-conf.yaml:config/flink-conf.yaml"
        "deploy/config/core-site.xml:config/core-site.xml"
        "deploy/config/log4j-console.properties:config/log4j-console.properties"
        "deploy/config/init-scylla.cql:config/init-scylla.cql"
        "pinot/schemas/events.json:config/pinot-schemas/events.json"
        "pinot/schemas/profiles.json:config/pinot-schemas/profiles.json"
        "pinot/schemas/sessions.json:config/pinot-schemas/sessions.json"
        "pinot/tables/events.json:config/pinot-tables/events.json"
        "pinot/tables/profiles.json:config/pinot-tables/profiles.json"
        "pinot/tables/sessions.json:config/pinot-tables/sessions.json"
    )

    for entry in "${files[@]}"; do
        local src="${entry%%:*}"
        local dst="${entry##*:}"
        curl -fsSL "${BASE_URL}/${src}" -o "${INSTALL_DIR}/${dst}"
    done

    info "Downloaded $(echo "${files[@]}" | wc -w) files"
}

start_services() {
    cd "${INSTALL_DIR}"
    info "Pulling images and starting services..."
    docker compose up -d
}

wait_for_ready() {
    info "Waiting for services to initialize (this takes 2-4 minutes)..."

    local timeout=300
    local elapsed=0

    printf "  "
    while ! curl -sf http://localhost:3000/ >/dev/null 2>&1; do
        printf "."
        sleep 5
        elapsed=$((elapsed + 5))
        if [ $elapsed -ge $timeout ]; then
            echo ""
            warn "Timed out waiting for dashboard after ${timeout}s."
            warn "Services may still be starting. Check status with:"
            warn "  cd arcstream && docker compose ps"
            warn "  cd arcstream && docker compose logs dashboard"
            return 1
        fi
    done
    echo ""
}

print_summary() {
    echo ""
    echo -e "${BOLD}══════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Arcstream is running!${NC}"
    echo -e "${BOLD}══════════════════════════════════════════${NC}"
    echo ""
    echo "  Dashboard:          http://localhost:3000"
    echo "  GraphQL Playground: http://localhost:8080/graphql/playground"
    echo "  GraphQL API:        http://localhost:8080/graphql"
    echo "  Flink Web UI:       http://localhost:8081"
    echo "  Pinot Controller:   http://localhost:9000"
    echo "  MinIO Console:      http://localhost:9101"
    echo ""
    echo "  Stop:    cd arcstream && docker compose down"
    echo "  Logs:    cd arcstream && docker compose logs -f"
    echo "  Reset:   cd arcstream && docker compose down -v"
    echo ""
}

main() {
    echo ""
    echo -e "${BOLD}Arcstream${NC} — Real-time Customer Data Platform"
    echo ""
    check_prereqs
    download_files
    start_services
    wait_for_ready
    print_summary
}

main "$@"
