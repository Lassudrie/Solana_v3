#!/usr/bin/env bash

set -euo pipefail

# Fresh-machine installer for a private non-voting Agave RPC node with
# optional Yellowstone gRPC plugin support.
#
# Defaults are pinned to the versions that were validated on the source machine:
# - Agave v3.1.11
# - Yellowstone v12.2.0+solana.3.1.10 patched locally to use
#   agave-geyser-plugin-interface = 3.1.11
#
# Run as root on Ubuntu 24.04.
#
# Example:
#   VALIDATOR_KEYPAIR_SRC=/root/validator-keypair.json \
#   bash install_solana_yellowstone.sh
#
# Optional:
#   YELLOWSTONE_ENABLED=0
#   RPC_BIND_ADDRESS=0.0.0.0
#   START_SERVICE=0

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must run as root." >&2
  exit 1
fi

SOL_USER="${SOL_USER:-sol}"
SOL_GROUP="${SOL_GROUP:-${SOL_USER}}"
SOL_HOME="/home/${SOL_USER}"

CLUSTER="${CLUSTER:-mainnet-beta}"
RPC_BIND_ADDRESS="${RPC_BIND_ADDRESS:-127.0.0.1}"
RPC_PORT="${RPC_PORT:-8899}"
DYNAMIC_PORT_RANGE="${DYNAMIC_PORT_RANGE:-8000-8025}"
ENABLE_FULL_RPC_API="${ENABLE_FULL_RPC_API:-0}"
LIMIT_LEDGER_SIZE="${LIMIT_LEDGER_SIZE:-50000000}"
ONLY_KNOWN_RPC="${ONLY_KNOWN_RPC:-1}"

LEDGER_DIR="${LEDGER_DIR:-/mnt/solana/ledger}"
ACCOUNTS_DIR="${ACCOUNTS_DIR:-/mnt/solana/accounts}"
SNAPSHOTS_DIR="${SNAPSHOTS_DIR:-/mnt/solana/snapshots}"
LOG_FILE="${LOG_FILE:-${SOL_HOME}/agave-rpc.log}"
IDENTITY_KEYPAIR="${IDENTITY_KEYPAIR:-${SOL_HOME}/validator-keypair.json}"
VALIDATOR_KEYPAIR_SRC="${VALIDATOR_KEYPAIR_SRC:-}"

AGAVE_VERSION="${AGAVE_VERSION:-v3.1.11}"
AGAVE_VERSION_STRIPPED="${AGAVE_VERSION#v}"
AGAVE_SRC_DIR="${AGAVE_SRC_DIR:-${SOL_HOME}/src/agave-${AGAVE_VERSION}}"
AGAVE_INSTALL_DIR="${AGAVE_INSTALL_DIR:-${SOL_HOME}/.local/share/agave-validator/install-${AGAVE_VERSION_STRIPPED}}"
AGAVE_BIN_PATH="${AGAVE_BIN_PATH:-${AGAVE_INSTALL_DIR}/bin/agave-validator}"

YELLOWSTONE_ENABLED="${YELLOWSTONE_ENABLED:-1}"
YELLOWSTONE_BASE_TAG="${YELLOWSTONE_BASE_TAG:-v12.2.0+solana.3.1.10}"
YELLOWSTONE_INTERFACE_VERSION="${YELLOWSTONE_INTERFACE_VERSION:-3.1.11}"
YELLOWSTONE_INSTALL_LABEL="${YELLOWSTONE_INSTALL_LABEL:-v12.2.0-solana-3.1.11-patched}"
YELLOWSTONE_SRC_DIR="${YELLOWSTONE_SRC_DIR:-${SOL_HOME}/src/yellowstone-grpc-${YELLOWSTONE_BASE_TAG}}"
YELLOWSTONE_INSTALL_DIR="${YELLOWSTONE_INSTALL_DIR:-${SOL_HOME}/.local/share/yellowstone-grpc/${YELLOWSTONE_INSTALL_LABEL}}"
YELLOWSTONE_LIBPATH="${YELLOWSTONE_LIBPATH:-${YELLOWSTONE_INSTALL_DIR}/lib/libyellowstone_grpc_geyser.so}"
YELLOWSTONE_CONFIG_CHECK="${YELLOWSTONE_CONFIG_CHECK:-${YELLOWSTONE_INSTALL_DIR}/bin/config-check}"
YELLOWSTONE_GRPC_ADDRESS="${YELLOWSTONE_GRPC_ADDRESS:-127.0.0.1:10000}"
YELLOWSTONE_PROMETHEUS_ADDRESS="${YELLOWSTONE_PROMETHEUS_ADDRESS:-127.0.0.1:8999}"
GEYSER_PLUGIN_CONFIG="${GEYSER_PLUGIN_CONFIG:-/etc/solana/yellowstone-grpc-geyser-config.json}"

START_SERVICE="${START_SERVICE:-1}"

APT_PACKAGES=(
  build-essential
  clang
  cmake
  curl
  git
  jq
  libclang-dev
  libssl-dev
  libudev-dev
  llvm
  lld
  pkg-config
  protobuf-compiler
  tmux
)

log() {
  printf '[install] %s\n' "$*"
}

run_as_sol() {
  runuser -u "${SOL_USER}" -- bash -lc "$*"
}

require_keypair() {
  if [[ -f "${IDENTITY_KEYPAIR}" ]]; then
    return
  fi

  if [[ -n "${VALIDATOR_KEYPAIR_SRC}" && -f "${VALIDATOR_KEYPAIR_SRC}" ]]; then
    install -o "${SOL_USER}" -g "${SOL_GROUP}" -m 0600 "${VALIDATOR_KEYPAIR_SRC}" "${IDENTITY_KEYPAIR}"
    return
  fi

  cat >&2 <<EOF
Missing validator identity keypair.

Provide one of:
  1. Copy the existing identity to ${IDENTITY_KEYPAIR}
  2. Re-run with VALIDATOR_KEYPAIR_SRC=/path/to/validator-keypair.json

For migration, you should reuse the existing validator identity.
EOF
  exit 1
}

install_os_packages() {
  log "Installing OS packages"
  export DEBIAN_FRONTEND=noninteractive
  apt update
  apt install -y "${APT_PACKAGES[@]}"
  chmod 1777 /tmp
}

ensure_sol_user() {
  if ! id -u "${SOL_USER}" >/dev/null 2>&1; then
    log "Creating user ${SOL_USER}"
    adduser --disabled-password --gecos '' "${SOL_USER}"
  fi
}

prepare_dirs() {
  log "Preparing directories"
  install -d -o "${SOL_USER}" -g "${SOL_GROUP}" \
    "${LEDGER_DIR}" \
    "${ACCOUNTS_DIR}" \
    "${SNAPSHOTS_DIR}" \
    "${SOL_HOME}/bin" \
    "${SOL_HOME}/src" \
    "${SOL_HOME}/tmp" \
    "${SOL_HOME}/.local/share" \
    /etc/solana
}

install_kernel_tuning() {
  log "Installing sysctl and limits"
  cat >/etc/sysctl.d/21-agave-validator.conf <<'EOF'
# Increase max UDP buffer sizes
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728

# Increase memory mapped files limit
vm.max_map_count = 1000000

# Increase number of allowed open file descriptors
fs.nr_open = 1000000
EOF

  cat >/etc/security/limits.d/90-solana-nofiles.conf <<'EOF'
# Increase process file descriptor count limit
* - nofile 1000000

# Increase memory locked limit (kB)
* - memlock 2000000
EOF

  sysctl --system
}

install_logrotate() {
  log "Installing logrotate config"
  cat >/etc/logrotate.d/agave-rpc <<EOF
${LOG_FILE} {
  daily
  rotate 7
  compress
  delaycompress
  missingok
  notifempty
  copytruncate
  su ${SOL_USER} ${SOL_GROUP}
}
EOF
}

install_rust() {
  log "Installing Rust toolchain for ${SOL_USER}"
  run_as_sol '
    if [[ ! -x "$HOME/.cargo/bin/rustup" ]]; then
      curl https://sh.rustup.rs -sSf | sh -s -- -y
    fi
    . "$HOME/.cargo/env"
    rustup toolchain install stable
    rustup default stable
  '
}

checkout_repo() {
  local repo_url="$1"
  local repo_dir="$2"
  local repo_ref="$3"

  run_as_sol "
    if [[ ! -d '${repo_dir}/.git' ]]; then
      git clone '${repo_url}' '${repo_dir}'
    fi
    cd '${repo_dir}'
    git fetch --tags --force origin
    git checkout -f '${repo_ref}'
  "
}

build_agave() {
  log "Building Agave ${AGAVE_VERSION}"
  checkout_repo "https://github.com/anza-xyz/agave.git" "${AGAVE_SRC_DIR}" "${AGAVE_VERSION}"

  run_as_sol "
    . \"\$HOME/.cargo/env\"
    cd '${AGAVE_SRC_DIR}'
    cargo build --release --bin agave-validator
    install -d '${AGAVE_INSTALL_DIR}/bin'
    install -m 0755 target/release/agave-validator '${AGAVE_BIN_PATH}'
  "
}

install_validator_wrapper() {
  log "Installing validator wrapper"
  cat >"${SOL_HOME}/bin/validator.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

ENV_FILE="/etc/default/agave-rpc"
AGAVE_BIN_DEFAULT="/home/sol/.local/share/agave-validator/install/bin/agave-validator"
AGAVE_BIN_FALLBACK="/home/sol/.local/share/solana/install/active_release/bin/agave-validator"

AGAVE_BIN="${AGAVE_BIN_DEFAULT}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1091
  source "${ENV_FILE}"
fi

: "${CLUSTER:=mainnet-beta}"
: "${AGAVE_BIN_PATH:=}"
: "${RPC_BIND_ADDRESS:=127.0.0.1}"
: "${RPC_PORT:=8899}"
: "${DYNAMIC_PORT_RANGE:=8000-8025}"
: "${IDENTITY_KEYPAIR:=/home/sol/validator-keypair.json}"
: "${LEDGER_DIR:=/mnt/solana/ledger}"
: "${ACCOUNTS_DIR:=/mnt/solana/accounts}"
: "${SNAPSHOTS_DIR:=/mnt/solana/snapshots}"
: "${LOG_FILE:=/home/sol/agave-rpc.log}"
: "${ENABLE_FULL_RPC_API:=0}"
: "${LIMIT_LEDGER_SIZE:=50000000}"
: "${ONLY_KNOWN_RPC:=1}"
: "${GEYSER_PLUGIN_CONFIG:=}"
: "${EXTRA_FLAGS:=}"

if [[ -n "${AGAVE_BIN_PATH}" ]]; then
  AGAVE_BIN="${AGAVE_BIN_PATH}"
elif [[ ! -x "${AGAVE_BIN}" && -x "${AGAVE_BIN_FALLBACK}" ]]; then
  AGAVE_BIN="${AGAVE_BIN_FALLBACK}"
fi

if [[ ! -x "${AGAVE_BIN}" ]]; then
  echo "agave-validator introuvable: ${AGAVE_BIN}" >&2
  exit 1
fi

declare -a KNOWN_VALIDATORS
declare -a ENTRYPOINTS
EXPECTED_GENESIS_HASH=""

case "${CLUSTER}" in
  mainnet-beta)
    KNOWN_VALIDATORS=(
      7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2
      GdnSyH3YtwcxFvQrVVJMm1JhTS4QVX7MFsX56uJLUfiZ
      DE1bawNcRJB9rVm3buyMVfr8mBEoyyu73NBovf2oXJsJ
      CakcnaRDHka2gXyfbEd2d3xsvkJkqsLw2akB3zsN1D2S
    )
    ENTRYPOINTS=(
      entrypoint.mainnet-beta.solana.com:8001
      entrypoint2.mainnet-beta.solana.com:8001
      entrypoint3.mainnet-beta.solana.com:8001
      entrypoint4.mainnet-beta.solana.com:8001
      entrypoint5.mainnet-beta.solana.com:8001
    )
    EXPECTED_GENESIS_HASH=5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d
    ;;
  testnet)
    KNOWN_VALIDATORS=(
      5D1fNXzvv5NjV1ysLjirC4WY92RNsVH18vjmcszZd8on
      dDzy5SR3AXdYWVqbDEkVFdvSPCtS9ihF5kJkHCtXoFs
      Ft5fbkqNa76vnsjYNwjDZUXoTWpP7VYm3mtsaQckQADN
      eoKpUABi59aT4rR9HGS3LcMecfut9x7zJyodWWP43YQ
      9QxCLckBiJc783jnMvXZubK4wH86Eqqvashtrwvcsgkv
    )
    ENTRYPOINTS=(
      entrypoint.testnet.solana.com:8001
      entrypoint2.testnet.solana.com:8001
      entrypoint3.testnet.solana.com:8001
    )
    EXPECTED_GENESIS_HASH=4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY
    ;;
  devnet)
    KNOWN_VALIDATORS=(
      dv1ZAGvdsz5hHLwWXsVnM94hWf1pjbKVau1QVkaMJ92
      dv2eQHeP4RFrJZ6UeiZWoc3XTtmtZCUKxxCApCDcRNV
      dv4ACNkpYPcE3aKmYDqZm9G5EB3J4MRoeE7WNDRBVJB
      dv3qDFk1DTF36Z62bNvrCXe9sKATA6xvVy6A798xxAS
    )
    ENTRYPOINTS=(
      entrypoint.devnet.solana.com:8001
      entrypoint2.devnet.solana.com:8001
      entrypoint3.devnet.solana.com:8001
      entrypoint4.devnet.solana.com:8001
      entrypoint5.devnet.solana.com:8001
    )
    EXPECTED_GENESIS_HASH=EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG
    ;;
  *)
    echo "cluster non supporté: ${CLUSTER}" >&2
    exit 1
    ;;
esac

mkdir -p "${LEDGER_DIR}" "${ACCOUNTS_DIR}" "${SNAPSHOTS_DIR}" "$(dirname "${LOG_FILE}")"

cmd=(
  "${AGAVE_BIN}"
  --identity "${IDENTITY_KEYPAIR}"
  --ledger "${LEDGER_DIR}"
  --accounts "${ACCOUNTS_DIR}"
  --snapshots "${SNAPSHOTS_DIR}"
  --log "${LOG_FILE}"
  --rpc-port "${RPC_PORT}"
  --rpc-bind-address "${RPC_BIND_ADDRESS}"
  --private-rpc
  --dynamic-port-range "${DYNAMIC_PORT_RANGE}"
  --expected-genesis-hash "${EXPECTED_GENESIS_HASH}"
  --wal-recovery-mode skip_any_corrupted_record
  --no-voting
)

if [[ "${ENABLE_FULL_RPC_API}" == "1" ]]; then
  cmd+=(--full-rpc-api)
fi

case "${LIMIT_LEDGER_SIZE}" in
  0|false|FALSE|no|NO)
    ;;
  1|true|TRUE|yes|YES)
    cmd+=(--limit-ledger-size)
    ;;
  *)
    cmd+=(--limit-ledger-size "${LIMIT_LEDGER_SIZE}")
    ;;
esac

if [[ "${ONLY_KNOWN_RPC}" == "1" ]]; then
  cmd+=(--only-known-rpc)
fi

if [[ -n "${GEYSER_PLUGIN_CONFIG}" ]]; then
  if [[ ! -f "${GEYSER_PLUGIN_CONFIG}" ]]; then
    echo "geyser plugin config introuvable: ${GEYSER_PLUGIN_CONFIG}" >&2
    exit 1
  fi
  cmd+=(--geyser-plugin-config "${GEYSER_PLUGIN_CONFIG}")
fi

for validator in "${KNOWN_VALIDATORS[@]}"; do
  cmd+=(--known-validator "${validator}")
done

for entrypoint in "${ENTRYPOINTS[@]}"; do
  cmd+=(--entrypoint "${entrypoint}")
done

if [[ -n "${EXTRA_FLAGS}" ]]; then
  # Intentional word splitting for operator-supplied CLI flags.
  # shellcheck disable=SC2206
  extra_args=(${EXTRA_FLAGS})
  cmd+=("${extra_args[@]}")
fi

exec "${cmd[@]}"
EOF

  chown "${SOL_USER}:${SOL_GROUP}" "${SOL_HOME}/bin/validator.sh"
  chmod 0755 "${SOL_HOME}/bin/validator.sh"
}

build_yellowstone() {
  if [[ "${YELLOWSTONE_ENABLED}" != "1" ]]; then
    return
  fi

  log "Building Yellowstone ${YELLOWSTONE_BASE_TAG} patched to interface ${YELLOWSTONE_INTERFACE_VERSION}"
  checkout_repo "https://github.com/rpcpool/yellowstone-grpc.git" "${YELLOWSTONE_SRC_DIR}" "${YELLOWSTONE_BASE_TAG}"

  run_as_sol "
    . \"\$HOME/.cargo/env\"
    export TMPDIR=\"\$HOME/tmp\"
    mkdir -p \"\$TMPDIR\"
    cd '${YELLOWSTONE_SRC_DIR}'
    sed -i -E 's/agave-geyser-plugin-interface = \"=[0-9]+\\.[0-9]+\\.[0-9]+\"/agave-geyser-plugin-interface = \"=${YELLOWSTONE_INTERFACE_VERSION}\"/' Cargo.toml
    cargo build --release -p yellowstone-grpc-geyser --bin config-check
    install -d '${YELLOWSTONE_INSTALL_DIR}/lib' '${YELLOWSTONE_INSTALL_DIR}/bin'
    install -m 0755 target/release/deps/libyellowstone_grpc_geyser.so '${YELLOWSTONE_LIBPATH}'
    install -m 0755 target/release/config-check '${YELLOWSTONE_CONFIG_CHECK}'
  "
}

install_yellowstone_config() {
  if [[ "${YELLOWSTONE_ENABLED}" != "1" ]]; then
    return
  fi

  log "Installing Yellowstone config"
  cat >"${GEYSER_PLUGIN_CONFIG}" <<EOF
{
  "libpath": "${YELLOWSTONE_LIBPATH}",
  "log": {
    "level": "info"
  },
  "grpc": {
    "address": "${YELLOWSTONE_GRPC_ADDRESS}"
  },
  "prometheus": {
    "address": "${YELLOWSTONE_PROMETHEUS_ADDRESS}"
  }
}
EOF
  chmod 0644 "${GEYSER_PLUGIN_CONFIG}"

  run_as_sol "'${YELLOWSTONE_CONFIG_CHECK}' -c '${GEYSER_PLUGIN_CONFIG}'"
}

install_env_file() {
  log "Installing /etc/default/agave-rpc"
  cat >/etc/default/agave-rpc <<EOF
CLUSTER=${CLUSTER}

# Keep the RPC private by default. Change to 0.0.0.0 only if you also
# restrict access at the firewall or reverse proxy layer.
RPC_BIND_ADDRESS=${RPC_BIND_ADDRESS}
RPC_PORT=${RPC_PORT}
DYNAMIC_PORT_RANGE=${DYNAMIC_PORT_RANGE}

# Optional explicit validator binary path. Leave empty to use the default
# install location managed by the local supervisor.
AGAVE_BIN_PATH=${AGAVE_BIN_PATH}

IDENTITY_KEYPAIR=${IDENTITY_KEYPAIR}
LEDGER_DIR=${LEDGER_DIR}
ACCOUNTS_DIR=${ACCOUNTS_DIR}
SNAPSHOTS_DIR=${SNAPSHOTS_DIR}
LOG_FILE=${LOG_FILE}

# Keep this local RPC node lean by default.
ENABLE_FULL_RPC_API=${ENABLE_FULL_RPC_API}
# 50,000,000 shreds is the validator-enforced minimum and targets roughly
# ~1h of ledger at high load, or roughly ~100 GB order of magnitude.
LIMIT_LEDGER_SIZE=${LIMIT_LEDGER_SIZE}
ONLY_KNOWN_RPC=${ONLY_KNOWN_RPC}

# Optional Yellowstone gRPC plugin config path. Leave empty until the
# plugin has been built and validated.
GEYSER_PLUGIN_CONFIG=$([[ "${YELLOWSTONE_ENABLED}" == "1" ]] && printf '%s' "${GEYSER_PLUGIN_CONFIG}")
YELLOWSTONE_GRPC_ADDRESS=${YELLOWSTONE_GRPC_ADDRESS}
YELLOWSTONE_PROMETHEUS_ADDRESS=${YELLOWSTONE_PROMETHEUS_ADDRESS}

# Extra flags appended as-is at the end of the command line if needed.
EXTRA_FLAGS=
EOF
  chmod 0644 /etc/default/agave-rpc
}

install_service() {
  log "Installing systemd service"
  cat >/etc/systemd/system/agave-rpc.service <<EOF
[Unit]
Description=Agave local private RPC node (non-voting)
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=${SOL_USER}
Group=${SOL_GROUP}
WorkingDirectory=${SOL_HOME}
EnvironmentFile=-/etc/default/agave-rpc
ExecStart=${SOL_HOME}/bin/validator.sh
ExecReload=/bin/kill -HUP \$MAINPID
KillSignal=SIGINT
Restart=always
RestartSec=10
TimeoutStopSec=120
LimitNOFILE=1000000
LimitMEMLOCK=2000000000
UMask=0077

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable agave-rpc
}

start_service_if_requested() {
  if [[ "${START_SERVICE}" != "1" ]]; then
    log "Skipping service start because START_SERVICE=${START_SERVICE}"
    return
  fi

  log "Starting agave-rpc service"
  systemctl restart agave-rpc
}

main() {
  install_os_packages
  ensure_sol_user
  prepare_dirs
  install_kernel_tuning
  install_logrotate
  install_rust
  require_keypair
  build_agave
  install_validator_wrapper
  build_yellowstone
  install_yellowstone_config
  install_env_file
  install_service
  start_service_if_requested

  cat <<EOF

Install complete.

Useful commands:
  systemctl status agave-rpc --no-pager
  journalctl -u agave-rpc -f
  curl -s http://${RPC_BIND_ADDRESS}:${RPC_PORT} -X POST -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"getVersion"}'
  curl -s http://${RPC_BIND_ADDRESS}:${RPC_PORT} -X POST -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'

Yellowstone:
  ${YELLOWSTONE_GRPC_ADDRESS}
  ${YELLOWSTONE_PROMETHEUS_ADDRESS}
EOF
}

main "$@"
