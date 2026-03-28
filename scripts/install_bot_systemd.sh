#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Build and install the Solana bot plus systemd units.

Usage:
  sudo bash scripts/install_bot_systemd.sh [options]

Options:
  --source-config PATH     Route manifest to transform for deployment.
                           Default: <repo>/sol_usdc_routes_amm_fast.toml
  --bot-config PATH        Installed bot config path.
                           Default: /etc/solana-bot/bot.toml
  --signer-keypair PATH    Keypair used by signerd.
                           Default: /etc/solana-bot/signing-keypair.json
  --signer-socket PATH     Secure Unix socket path for signerd.
                           Default: /run/solana-bot/signerd.sock
  --grpc-endpoint URL      Shredstream gRPC endpoint consumed by the bot.
                           Default: http://127.0.0.1:50051
  --install-dir DIR        Binary install directory.
                           Default: /usr/local/bin
  --systemd-dir DIR        systemd unit directory.
                           Default: /etc/systemd/system
  --state-dir DIR          Service state directory.
                           Default: /var/lib/solana-bot
  --user NAME              Service user.
                           Default: solbot
  --group NAME             Service group.
                           Default: solbot
  --overwrite-config       Replace an existing bot config.
  --skip-build             Reuse already built release binaries.
  --start                  Enable and start services after install.
  -h, --help               Show this help.
EOF
}

require_root() {
    if [[ ${EUID} -ne 0 ]]; then
        echo "run this installer as root" >&2
        exit 1
    fi
}

render_template() {
    local template="$1"
    local destination="$2"

    sed \
        -e "s|__BOT_USER__|$service_user|g" \
        -e "s|__BOT_GROUP__|$service_group|g" \
        -e "s|__WORKDIR__|$state_dir|g" \
        -e "s|__BIN_DIR__|$install_dir|g" \
        -e "s|__BOT_CONFIG__|$bot_config_path|g" \
        -e "s|__SIGNER_SOCKET__|$signer_socket_path|g" \
        -e "s|__SIGNER_KEYPAIR__|$signer_keypair_path|g" \
        "$template" >"$destination"
}

ensure_service_account() {
    if ! getent group "$service_group" >/dev/null 2>&1; then
        groupadd --system "$service_group"
    fi

    if ! id -u "$service_user" >/dev/null 2>&1; then
        local nologin_shell="/usr/sbin/nologin"
        if [[ ! -x "$nologin_shell" ]]; then
            nologin_shell="/usr/bin/false"
        fi
        useradd \
            --system \
            --gid "$service_group" \
            --home-dir "$state_dir" \
            --create-home \
            --shell "$nologin_shell" \
            "$service_user"
    fi

    install -d -m 0755 "$install_dir" "$systemd_dir"
    install -d -o root -g "$service_group" -m 0750 "$(dirname "$bot_config_path")"
    install -d -o "$service_user" -g "$service_group" -m 0750 "$state_dir"
}

build_binaries() {
    (
        cd "$repo_root"
        cargo build --release -p bot --bin bot --bin botctl
        cargo build --release -p signerd --bin signerd
    )
}

install_binaries() {
    local bot_bin="$repo_root/target/release/bot"
    local botctl_bin="$repo_root/target/release/botctl"
    local signerd_bin="$repo_root/target/release/signerd"

    for artifact in "$bot_bin" "$botctl_bin" "$signerd_bin"; do
        if [[ ! -x "$artifact" ]]; then
            echo "missing built binary: $artifact" >&2
            echo "run without --skip-build after fetching dependencies" >&2
            exit 1
        fi
    done

    install -m 0755 "$bot_bin" "$install_dir/bot"
    install -m 0755 "$botctl_bin" "$install_dir/botctl"
    install -m 0755 "$signerd_bin" "$install_dir/signerd"
}

install_config() {
    if [[ -f "$bot_config_path" && $overwrite_config -eq 0 ]]; then
        echo "keeping existing config: $bot_config_path"
        return
    fi

    bash "$repo_root/scripts/render_bot_config.sh" \
        --source "$source_config" \
        --output "$bot_config_path" \
        --signer-socket "$signer_socket_path" \
        --grpc-endpoint "$grpc_endpoint"

    chown root:"$service_group" "$bot_config_path"
    chmod 0640 "$bot_config_path"
}

install_units() {
    render_template \
        "$repo_root/deploy/systemd/solana-signerd.service.in" \
        "$systemd_dir/solana-signerd.service"
    render_template \
        "$repo_root/deploy/systemd/solana-bot.service.in" \
        "$systemd_dir/solana-bot.service"
    chmod 0644 \
        "$systemd_dir/solana-signerd.service" \
        "$systemd_dir/solana-bot.service"
}

install_bot_dropins() {
    local dropin_dir="$systemd_dir/solana-bot.service.d"
    local dropin_path="$dropin_dir/10-upstreams.conf"
    local -a wants=()
    local -a after=()

    if systemctl list-unit-files agave-rpc.service --no-legend 2>/dev/null | grep -Fq "agave-rpc.service"; then
        wants+=("agave-rpc.service")
        after+=("agave-rpc.service")
    fi

    if systemctl list-unit-files jito-shredstream-proxy.service --no-legend 2>/dev/null | grep -Fq "jito-shredstream-proxy.service"; then
        wants+=("jito-shredstream-proxy.service")
        after+=("jito-shredstream-proxy.service")
    elif systemctl list-unit-files yellowstone-shredstream-bridge.service --no-legend 2>/dev/null | grep -Fq "yellowstone-shredstream-bridge.service"; then
        wants+=("yellowstone-shredstream-bridge.service")
        after+=("yellowstone-shredstream-bridge.service")
    fi

    if [[ ${#wants[@]} -eq 0 ]]; then
        rm -f "$dropin_path"
        rmdir "$dropin_dir" 2>/dev/null || true
        return
    fi

    install -d -m 0755 "$dropin_dir"
    {
        echo "[Unit]"
        echo "Wants=${wants[*]}"
        echo "After=${after[*]}"
    } >"$dropin_path"
    chmod 0644 "$dropin_path"
}

enable_units() {
    systemctl daemon-reload
    systemctl enable solana-signerd.service solana-bot.service >/dev/null
}

start_units() {
    if [[ ! -f "$signer_keypair_path" ]]; then
        echo "signer keypair not found: $signer_keypair_path" >&2
        echo "copy the hot-wallet keypair before using --start" >&2
        exit 1
    fi

    systemctl restart solana-signerd.service
    systemctl restart solana-bot.service
}

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source_config="$repo_root/sol_usdc_routes_amm_fast.toml"
bot_config_path="/etc/solana-bot/bot.toml"
signer_keypair_path="/etc/solana-bot/signing-keypair.json"
signer_socket_path="/run/solana-bot/signerd.sock"
grpc_endpoint="http://127.0.0.1:50051"
install_dir="/usr/local/bin"
systemd_dir="/etc/systemd/system"
state_dir="/var/lib/solana-bot"
service_user="solbot"
service_group="solbot"
overwrite_config=0
skip_build=0
start_services=0

while (($# > 0)); do
    case "$1" in
        --source-config)
            source_config="${2:-}"
            shift 2
            ;;
        --bot-config)
            bot_config_path="${2:-}"
            shift 2
            ;;
        --signer-keypair)
            signer_keypair_path="${2:-}"
            shift 2
            ;;
        --signer-socket)
            signer_socket_path="${2:-}"
            shift 2
            ;;
        --grpc-endpoint)
            grpc_endpoint="${2:-}"
            shift 2
            ;;
        --install-dir)
            install_dir="${2:-}"
            shift 2
            ;;
        --systemd-dir)
            systemd_dir="${2:-}"
            shift 2
            ;;
        --state-dir)
            state_dir="${2:-}"
            shift 2
            ;;
        --user)
            service_user="${2:-}"
            shift 2
            ;;
        --group)
            service_group="${2:-}"
            shift 2
            ;;
        --overwrite-config)
            overwrite_config=1
            shift
            ;;
        --skip-build)
            skip_build=1
            shift
            ;;
        --start)
            start_services=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

require_root

if [[ ! -f "$source_config" ]]; then
    echo "source config not found: $source_config" >&2
    exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemctl is required on the target host" >&2
    exit 1
fi

ensure_service_account

if [[ $skip_build -eq 0 ]]; then
    build_binaries
fi

install_binaries
install_config
install_units
install_bot_dropins
enable_units

if [[ $start_services -eq 1 ]]; then
    start_units
fi

cat <<EOF
Installed:
  binaries:        $install_dir/{bot,botctl,signerd}
  bot config:      $bot_config_path
  signer keypair:  $signer_keypair_path
  signer socket:   $signer_socket_path
  units:           $systemd_dir/solana-{bot,signerd}.service

Next checks:
  1. ensure $signer_keypair_path contains the hot-wallet keypair readable by $service_user
  2. review $bot_config_path for the final route set and wallet constraints
  3. verify local RPC 127.0.0.1:8899, WebSocket 127.0.0.1:8900, and a live Shredstream provider on $grpc_endpoint
  4. start services with:
       systemctl restart solana-signerd.service
       systemctl restart solana-bot.service
  5. verify:
       systemctl --no-pager --full status solana-signerd.service solana-bot.service
       curl -sf http://127.0.0.1:8081/monitor/overview
       botctl status
EOF
