#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Render a deployment-ready bot config from a route manifest.

Usage:
  bash scripts/render_bot_config.sh \
    --source /path/to/source.toml \
    --output /path/to/bot.toml

Options:
  --source PATH          Source TOML manifest to transform.
  --output PATH          Destination TOML path.
  --signer-socket PATH   Secure Unix signer socket path.
                         Default: /run/solana-bot/signerd.sock
  --grpc-endpoint URL    Shredstream gRPC endpoint for the bot.
                         Default: http://127.0.0.1:50051
  -h, --help             Show this help.
EOF
}

source_config=""
output_path=""
signer_socket="/run/solana-bot/signerd.sock"
grpc_endpoint="http://127.0.0.1:50051"

while (($# > 0)); do
    case "$1" in
        --source)
            source_config="${2:-}"
            shift 2
            ;;
        --output)
            output_path="${2:-}"
            shift 2
            ;;
        --signer-socket)
            signer_socket="${2:-}"
            shift 2
            ;;
        --grpc-endpoint)
            grpc_endpoint="${2:-}"
            shift 2
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

if [[ -z "$source_config" || -z "$output_path" ]]; then
    echo "--source and --output are required" >&2
    usage >&2
    exit 1
fi

if [[ ! -f "$source_config" ]]; then
    echo "source config not found: $source_config" >&2
    exit 1
fi

has_runtime_event_source=0
has_runtime_monitor_server=0
has_runtime_control=0
has_shredstream_table=0

if grep -Eq '^\[runtime\.event_source\]$' "$source_config"; then
    has_runtime_event_source=1
fi
if grep -Eq '^\[runtime\.monitor_server\]$' "$source_config"; then
    has_runtime_monitor_server=1
fi
if grep -Eq '^\[runtime\.control\]$' "$source_config"; then
    has_runtime_control=1
fi
if grep -Eq '^\[shredstream\]$' "$source_config"; then
    has_shredstream_table=1
fi

mkdir -p "$(dirname "$output_path")"

awk \
    -v signer_socket="$signer_socket" \
    -v grpc_endpoint="$grpc_endpoint" \
    -v has_runtime_event_source="$has_runtime_event_source" \
    -v has_runtime_monitor_server="$has_runtime_monitor_server" \
    -v has_runtime_control="$has_runtime_control" \
    -v has_shredstream_table="$has_shredstream_table" '
function print_signing_overrides() {
    print "provider = \"secure_unix\""
    print "owner_pubkey = \"\""
    print "socket_path = \"" signer_socket "\""
    print "validate_execution_accounts = true"
}

function print_runtime_overrides(printed_any) {
    printed_any = 0
    if (!has_runtime_event_source) {
        print ""
        print "[runtime.event_source]"
        print "mode = \"shredstream\""
        printed_any = 1
    }
    if (!has_runtime_monitor_server) {
        print ""
        print "[runtime.monitor_server]"
        print "enabled = true"
        printed_any = 1
    }
    if (!has_runtime_control) {
        print ""
        print "[runtime.control]"
        print "idle_sleep_millis = 0"
        print "max_events_per_tick = 256"
        printed_any = 1
    }
    return printed_any
}

function print_shredstream_table() {
    if (!has_shredstream_table) {
        print ""
        print "[shredstream]"
        print "grpc_endpoint = \"" grpc_endpoint "\""
    }
}

function print_strategy_overrides() {
    print "min_profit_quote_atoms = 0"
    print "min_profit_bps = 0"
    print "sizing.mode = \"legacy\""
    print "sizing.fixed_trade_size = true"
    print "sizing.min_trade_floor_sol_lamports = 100000000"
}

BEGIN {
    inserted_runtime_overrides = 0
    inserted_shredstream_table = 0
    in_signing = 0
    saw_signing = 0
    in_strategy = 0
    saw_strategy = 0
    printed_strategy_overrides = 0
    in_runtime_event_source = 0
    printed_runtime_event_source_mode = 0
    in_runtime_monitor_server = 0
    printed_runtime_monitor_server_enabled = 0
    in_runtime_control = 0
    printed_runtime_control_idle_sleep_millis = 0
    printed_runtime_control_max_events_per_tick = 0
    in_shredstream_table = 0
    printed_shredstream_grpc_endpoint = 0
}

/^\[/ {
    if (in_strategy && !printed_strategy_overrides) {
        print_strategy_overrides()
    }
    if (in_runtime_event_source && !printed_runtime_event_source_mode) {
        print "mode = \"shredstream\""
    }
    if (in_runtime_monitor_server && !printed_runtime_monitor_server_enabled) {
        print "enabled = true"
    }
    if (in_runtime_control && !printed_runtime_control_idle_sleep_millis) {
        print "idle_sleep_millis = 0"
    }
    if (in_runtime_control && !printed_runtime_control_max_events_per_tick) {
        print "max_events_per_tick = 256"
    }
    if (in_shredstream_table && !printed_shredstream_grpc_endpoint) {
        print "grpc_endpoint = \"" grpc_endpoint "\""
    }

    if (!inserted_runtime_overrides && $0 == "[state]") {
        print_runtime_overrides()
        inserted_runtime_overrides = 1
    }

    if (!inserted_shredstream_table && $0 == "[shredstream.reducers]") {
        print_shredstream_table()
        inserted_shredstream_table = 1
    }

    in_signing = ($0 == "[signing]")
    if (in_signing) {
        saw_signing = 1
        print $0
        print_signing_overrides()
        next
    }

    in_strategy = ($0 == "[strategy]")
    printed_strategy_sizing_mode = 0
    if (in_strategy) {
        saw_strategy = 1
        print $0
        print_strategy_overrides()
        printed_strategy_overrides = 1
        next
    }

    in_runtime_event_source = ($0 == "[runtime.event_source]")
    printed_runtime_event_source_mode = 0
    if (in_runtime_event_source) {
        print $0
        print "mode = \"shredstream\""
        printed_runtime_event_source_mode = 1
        next
    }

    in_runtime_monitor_server = ($0 == "[runtime.monitor_server]")
    printed_runtime_monitor_server_enabled = 0
    if (in_runtime_monitor_server) {
        print $0
        print "enabled = true"
        printed_runtime_monitor_server_enabled = 1
        next
    }

    in_runtime_control = ($0 == "[runtime.control]")
    printed_runtime_control_idle_sleep_millis = 0
    printed_runtime_control_max_events_per_tick = 0
    if (in_runtime_control) {
        print $0
        print "idle_sleep_millis = 0"
        print "max_events_per_tick = 256"
        printed_runtime_control_idle_sleep_millis = 1
        printed_runtime_control_max_events_per_tick = 1
        next
    }

    in_shredstream_table = ($0 == "[shredstream]")
    printed_shredstream_grpc_endpoint = 0
    if (in_shredstream_table) {
        print $0
        print "grpc_endpoint = \"" grpc_endpoint "\""
        printed_shredstream_grpc_endpoint = 1
        next
    }

    print $0
    next
}

{
    if (in_signing) {
        if ($0 ~ /^(provider|owner_pubkey|socket_path|keypair_path|keypair_base58|validate_execution_accounts)[[:space:]]*=/) {
            next
        }
        print
        next
    }

    if (in_strategy) {
        if ($0 ~ /^(min_profit_quote_atoms|min_profit_bps|sizing\.mode|sizing\.fixed_trade_size|sizing\.min_trade_floor_sol_lamports)[[:space:]]*=/) {
            next
        }
        print
        next
    }

    if (in_runtime_event_source) {
        if ($0 ~ /^mode[[:space:]]*=/) {
            next
        }
        print
        next
    }

    if (in_runtime_monitor_server) {
        if ($0 ~ /^enabled[[:space:]]*=/) {
            next
        }
        print
        next
    }

    if (in_runtime_control) {
        if ($0 ~ /^(idle_sleep_millis|max_events_per_tick)[[:space:]]*=/) {
            next
        }
        print
        next
    }

    if (in_shredstream_table) {
        if ($0 ~ /^(grpc_endpoint|endpoint)[[:space:]]*=/) {
            next
        }
        print
        next
    }

    print
}

END {
    if (in_strategy && !printed_strategy_overrides) {
        print_strategy_overrides()
    }
    if (in_runtime_event_source && !printed_runtime_event_source_mode) {
        print "mode = \"shredstream\""
    }
    if (in_runtime_monitor_server && !printed_runtime_monitor_server_enabled) {
        print "enabled = true"
    }
    if (in_runtime_control && !printed_runtime_control_idle_sleep_millis) {
        print "idle_sleep_millis = 0"
    }
    if (in_runtime_control && !printed_runtime_control_max_events_per_tick) {
        print "max_events_per_tick = 256"
    }
    if (in_shredstream_table && !printed_shredstream_grpc_endpoint) {
        print "grpc_endpoint = \"" grpc_endpoint "\""
    }

    if (!saw_signing) {
        print ""
        print "[signing]"
        print_signing_overrides()
    }

    if (!saw_strategy) {
        print ""
        print "[strategy]"
        print_strategy_overrides()
    }

    if (!inserted_runtime_overrides) {
        print_runtime_overrides()
    }

    if (!inserted_shredstream_table) {
        print_shredstream_table()
    }
}
' "$source_config" >"$output_path"
