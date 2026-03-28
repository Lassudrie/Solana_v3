#!/usr/bin/env python3
"""Generate explicit triangular routes from an existing route manifest."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from datetime import date
from itertools import product
from pathlib import Path
from typing import Any

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

STABLE_LIKE_SYMBOLS = {"usdc", "usdt", "usd1", "usdg", "eurc", "usde", "pyusd"}
SOL_LIKE_SYMBOLS = {"sol", "jitosol", "msol", "jupsol", "stsol", "bsol"}


@dataclass(frozen=True)
class AccountDependency:
    account_key: str
    pool_id: str
    decoder_key: str


@dataclass(frozen=True)
class ExecutionProtection:
    enabled: bool
    tight_max_quote_slot_lag: int
    base_extra_buy_leg_slippage_bps: int
    failure_step_bps: int
    max_extra_buy_leg_slippage_bps: int
    recovery_success_count: int


@dataclass(frozen=True)
class LegCandidate:
    source_route_id: str
    venue: str
    pool_id: str
    side: str
    input_mint: str
    output_mint: str
    fee_bps: int | None
    execution: tuple[tuple[str, Any], ...]
    lookup_tables: tuple[str, ...]
    account_dependencies: tuple[AccountDependency, ...]
    message_mode: str
    default_compute_unit_limit: int
    minimum_compute_unit_limit: int
    default_compute_unit_price_micro_lamports: int
    default_jito_tip_lamports: int
    max_quote_slot_lag: int
    max_alt_slot_lag: int
    execution_protection: ExecutionProtection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="amm_12_pairs_fast.toml", help="source manifest path")
    parser.add_argument(
        "--output",
        default="amm_12_pairs_triangular_fast.toml",
        help="output manifest path",
    )
    parser.add_argument("--default-compute-unit-limit", type=int, default=450_000)
    parser.add_argument("--minimum-compute-unit-limit", type=int, default=420_000)
    parser.add_argument("--max-quote-slot-lag", type=int, default=16)
    parser.add_argument("--tight-max-quote-slot-lag", type=int, default=4)
    parser.add_argument(
        "--base-extra-buy-leg-slippage-bps",
        type=int,
        default=35,
    )
    parser.add_argument("--max-extra-buy-leg-slippage-bps", type=int, default=125)
    parser.add_argument("--failure-step-bps", type=int, default=25)
    parser.add_argument("--recovery-success-count", type=int, default=3)
    parser.add_argument("--stable-profit-floor", type=int, default=15_000)
    parser.add_argument("--sol-profit-floor", type=int, default=15_000_000)
    parser.add_argument(
        "--include-source-routes",
        action="store_true",
        help="append generated triangular routes after the original route definitions",
    )
    return parser.parse_args()


def sanitize_symbol(symbol: str, fallback: str) -> str:
    filtered = "".join(ch.lower() for ch in symbol if ch.isalnum())
    return filtered or fallback[:6].lower()


def short_id(address: str) -> str:
    return address[:4].lower()


def infer_symbol_hints(routes: list[dict[str, Any]]) -> dict[str, str]:
    hints = {
        SOL_MINT: "sol",
        USDC_MINT: "usdc",
    }
    for route in routes:
        route_id = route.get("route_id", "")
        parts = route_id.split("-")
        if len(parts) < 2:
            continue
        base_mint = route.get("base_mint")
        quote_mint = route.get("quote_mint") or route.get("input_mint")
        if base_mint:
            hints.setdefault(base_mint, sanitize_symbol(parts[0], base_mint))
        if quote_mint:
            hints.setdefault(quote_mint, sanitize_symbol(parts[1], quote_mint))
    return hints


def symbol_for_mint(symbol_hints: dict[str, str], mint: str) -> str:
    return symbol_hints.get(mint, sanitize_symbol(mint, mint))


def render_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return f'"{value}"'
    raise TypeError(f"unsupported TOML value type: {type(value)!r}")


def derive_leg_mints(route: dict[str, Any], leg: dict[str, Any]) -> tuple[str, str]:
    input_mint = leg.get("input_mint")
    output_mint = leg.get("output_mint")
    if input_mint and output_mint:
        return input_mint, output_mint

    base_mint = route.get("base_mint")
    quote_mint = route.get("quote_mint")
    if not base_mint or not quote_mint:
        raise ValueError(
            f"route {route.get('route_id')} is missing base/quote mint required to derive leg mints"
        )

    if leg["side"] == "buy_base":
        return quote_mint, base_mint
    if leg["side"] == "sell_base":
        return base_mint, quote_mint
    raise ValueError(f"unsupported swap side {leg['side']!r}")


def top_level_prefix(source_path: Path) -> str:
    text = source_path.read_text(encoding="utf-8")
    prefix, _, _ = text.partition("[[routes.definitions]]")
    routes_marker_index = prefix.rfind("[routes]")
    if routes_marker_index != -1:
        prefix = prefix[: routes_marker_index + len("[routes]")]
    return prefix.rstrip()


def unique_by_insertion(items: list[Any]) -> list[Any]:
    seen: set[Any] = set()
    result: list[Any] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def build_execution_protection(
    route: dict[str, Any],
    *,
    max_quote_slot_lag: int,
    default_tight_max_quote_slot_lag: int,
    default_base_extra_buy_leg_slippage_bps: int,
    default_max_extra_buy_leg_slippage_bps: int,
    default_failure_step_bps: int,
    default_recovery_success_count: int,
) -> ExecutionProtection:
    config = route.get("execution_protection", {})
    if not config.get("enabled", False):
        return ExecutionProtection(
            enabled=True,
            tight_max_quote_slot_lag=min(default_tight_max_quote_slot_lag, max_quote_slot_lag),
            base_extra_buy_leg_slippage_bps=default_base_extra_buy_leg_slippage_bps,
            failure_step_bps=default_failure_step_bps,
            max_extra_buy_leg_slippage_bps=default_max_extra_buy_leg_slippage_bps,
            recovery_success_count=default_recovery_success_count,
        )
    return ExecutionProtection(
        enabled=True,
        tight_max_quote_slot_lag=min(
            max_quote_slot_lag,
            max(
                1,
                int(config.get("tight_max_quote_slot_lag", default_tight_max_quote_slot_lag)),
            ),
        ),
        base_extra_buy_leg_slippage_bps=max(
            default_base_extra_buy_leg_slippage_bps,
            int(
                config.get(
                    "base_extra_buy_leg_slippage_bps",
                    default_base_extra_buy_leg_slippage_bps,
                )
            ),
        ),
        failure_step_bps=max(
            default_failure_step_bps,
            int(config.get("failure_step_bps", default_failure_step_bps)),
        ),
        max_extra_buy_leg_slippage_bps=max(
            default_max_extra_buy_leg_slippage_bps,
            int(
                config.get(
                    "max_extra_buy_leg_slippage_bps",
                    default_max_extra_buy_leg_slippage_bps,
                )
            ),
        ),
        recovery_success_count=max(
            1,
            int(config.get("recovery_success_count", default_recovery_success_count)),
        ),
    )


def parse_source_routes(
    source_path: Path,
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], list[LegCandidate]], dict[str, list[dict[str, Any]]]]:
    data = tomllib.loads(source_path.read_text(encoding="utf-8"))
    routes = data.get("routes", {}).get("definitions", [])
    enabled_two_leg_routes = [
        route
        for route in routes
        if route.get("enabled", True)
        and route.get("kind", "two_leg") != "triangular"
        and len(route.get("legs", [])) == 2
        and route.get("input_mint") == route.get("output_mint")
    ]

    catalog_by_direction: dict[tuple[str, str], list[LegCandidate]] = {}
    routes_by_profit_mint: dict[str, list[dict[str, Any]]] = {}
    dedupe: dict[tuple[str, str, str], LegCandidate] = {}

    for route in enabled_two_leg_routes:
        profit_mint = route["input_mint"]
        routes_by_profit_mint.setdefault(profit_mint, []).append(route)
        execution = route.get("execution", {})
        max_quote_slot_lag = min(
            args.max_quote_slot_lag,
            int(execution.get("max_quote_slot_lag", args.max_quote_slot_lag)),
        )
        execution_protection = build_execution_protection(
            route,
            max_quote_slot_lag=max_quote_slot_lag,
            default_tight_max_quote_slot_lag=args.tight_max_quote_slot_lag,
            default_base_extra_buy_leg_slippage_bps=args.base_extra_buy_leg_slippage_bps,
            default_max_extra_buy_leg_slippage_bps=args.max_extra_buy_leg_slippage_bps,
            default_failure_step_bps=args.failure_step_bps,
            default_recovery_success_count=args.recovery_success_count,
        )

        account_dependencies_by_pool: dict[str, list[AccountDependency]] = {}
        for dependency in route.get("account_dependencies", []):
            parsed = AccountDependency(
                account_key=dependency["account_key"],
                pool_id=dependency["pool_id"],
                decoder_key=dependency["decoder_key"],
            )
            account_dependencies_by_pool.setdefault(parsed.pool_id, []).append(parsed)

        for leg in route["legs"]:
            input_mint, output_mint = derive_leg_mints(route, leg)
            candidate = LegCandidate(
                source_route_id=route["route_id"],
                venue=leg["venue"],
                pool_id=leg["pool_id"],
                side=leg["side"],
                input_mint=input_mint,
                output_mint=output_mint,
                fee_bps=leg.get("fee_bps"),
                execution=tuple(leg["execution"].items()),
                lookup_tables=tuple(
                    table["account_key"]
                    for table in execution.get("lookup_tables", [])
                    if table.get("account_key")
                ),
                account_dependencies=tuple(
                    unique_by_insertion(account_dependencies_by_pool.get(leg["pool_id"], []))
                ),
                message_mode=execution.get("message_mode", "v0_required"),
                default_compute_unit_limit=int(
                    execution.get(
                        "default_compute_unit_limit", args.default_compute_unit_limit
                    )
                ),
                minimum_compute_unit_limit=int(
                    execution.get(
                        "minimum_compute_unit_limit", args.minimum_compute_unit_limit
                    )
                ),
                default_compute_unit_price_micro_lamports=int(
                    execution.get("default_compute_unit_price_micro_lamports", 1)
                ),
                default_jito_tip_lamports=int(execution.get("default_jito_tip_lamports", 1)),
                max_quote_slot_lag=max_quote_slot_lag,
                max_alt_slot_lag=int(execution.get("max_alt_slot_lag", 32)),
                execution_protection=execution_protection,
            )
            key = (candidate.pool_id, candidate.input_mint, candidate.output_mint)
            dedupe.setdefault(key, candidate)

    for candidate in dedupe.values():
        catalog_by_direction.setdefault(
            (candidate.input_mint, candidate.output_mint), []
        ).append(candidate)

    for candidates in catalog_by_direction.values():
        candidates.sort(
            key=lambda item: (
                symbol_for_mint({SOL_MINT: "sol", USDC_MINT: "usdc"}, item.input_mint),
                item.venue,
                item.pool_id,
            )
        )

    return enabled_two_leg_routes, catalog_by_direction, routes_by_profit_mint


def build_size_profile(routes: list[dict[str, Any]]) -> tuple[int, int, int, list[int]]:
    min_trade_size = min(
        int(route.get("min_trade_size") or route["default_trade_size"]) for route in routes
    )
    default_trade_size = min(int(route["default_trade_size"]) for route in routes)
    max_trade_size = min(int(route["max_trade_size"]) for route in routes)
    if default_trade_size < min_trade_size:
        default_trade_size = min_trade_size
    if default_trade_size > max_trade_size:
        default_trade_size = max_trade_size

    ladder_values = sorted(
        {
            int(value)
            for route in routes
            for value in route.get("size_ladder", [])
            if min_trade_size <= int(value) <= max_trade_size
        }
        | {min_trade_size, default_trade_size, max_trade_size}
    )
    return min_trade_size, default_trade_size, max_trade_size, ladder_values


def profit_floor_for_mint(
    *,
    mint: str,
    symbol_hints: dict[str, str],
    stable_profit_floor: int,
    sol_profit_floor: int,
) -> int:
    symbol = symbol_for_mint(symbol_hints, mint)
    if mint == SOL_MINT or symbol in SOL_LIKE_SYMBOLS:
        return sol_profit_floor
    if mint == USDC_MINT or symbol in STABLE_LIKE_SYMBOLS:
        return stable_profit_floor
    return stable_profit_floor


def choose_conversion_pool_id(
    profit_mint: str,
    catalog_by_direction: dict[tuple[str, str], list[LegCandidate]],
) -> str | None:
    if profit_mint == SOL_MINT:
        return None
    candidates = catalog_by_direction.get((SOL_MINT, profit_mint), []) + catalog_by_direction.get(
        (profit_mint, SOL_MINT), []
    )
    if not candidates:
        return None
    return candidates[0].pool_id


def route_id_for_triangle(
    *,
    symbol_a: str,
    symbol_b: str,
    symbol_c: str,
    leg_a: LegCandidate,
    leg_b: LegCandidate,
    leg_c: LegCandidate,
) -> str:
    return (
        f"tri-{symbol_a}-{symbol_b}-{symbol_c}"
        f"-{leg_a.venue.replace('_', '-')}-{short_id(leg_a.pool_id)}"
        f"-{leg_b.venue.replace('_', '-')}-{short_id(leg_b.pool_id)}"
        f"-{leg_c.venue.replace('_', '-')}-{short_id(leg_c.pool_id)}"
    )


def render_key_values(lines: list[str], values: dict[str, Any]) -> None:
    for key, value in values.items():
        if value is None:
            continue
        lines.append(f"{key} = {render_value(value)}")


def render_leg(lines: list[str], leg: LegCandidate) -> None:
    lines.extend(
        [
            "",
            "[[routes.definitions.legs]]",
            f'venue = "{leg.venue}"',
            f'pool_id = "{leg.pool_id}"',
            f'side = "{leg.side}"',
            f'input_mint = "{leg.input_mint}"',
            f'output_mint = "{leg.output_mint}"',
        ]
    )
    if leg.fee_bps is not None:
        lines.append(f"fee_bps = {leg.fee_bps}")
    lines.extend(["", "[routes.definitions.legs.execution]"])
    render_key_values(lines, dict(leg.execution))


def render_route(
    *,
    route_id: str,
    profit_mint: str,
    intermediate_mint_1: str,
    intermediate_mint_2: str,
    min_profit_quote_atoms: int,
    size_profile: tuple[int, int, int, list[int]],
    conversion_pool_id: str | None,
    leg_a: LegCandidate,
    leg_b: LegCandidate,
    leg_c: LegCandidate,
) -> str:
    min_trade_size, default_trade_size, max_trade_size, size_ladder = size_profile
    message_mode = (
        "v0_required"
        if "v0_required"
        in {leg_a.message_mode, leg_b.message_mode, leg_c.message_mode}
        else "v0_or_legacy"
    )
    default_compute_unit_limit = max(
        450_000,
        leg_a.default_compute_unit_limit,
        leg_b.default_compute_unit_limit,
        leg_c.default_compute_unit_limit,
    )
    minimum_compute_unit_limit = max(
        420_000,
        leg_a.minimum_compute_unit_limit,
        leg_b.minimum_compute_unit_limit,
        leg_c.minimum_compute_unit_limit,
    )
    max_quote_slot_lag = min(
        leg_a.max_quote_slot_lag,
        leg_b.max_quote_slot_lag,
        leg_c.max_quote_slot_lag,
    )
    tight_max_quote_slot_lag = min(
        protection.tight_max_quote_slot_lag
        for protection in (
            leg_a.execution_protection,
            leg_b.execution_protection,
            leg_c.execution_protection,
        )
    )
    base_extra_buy_leg_slippage_bps = max(
        protection.base_extra_buy_leg_slippage_bps
        for protection in (
            leg_a.execution_protection,
            leg_b.execution_protection,
            leg_c.execution_protection,
        )
    )
    failure_step_bps = max(
        protection.failure_step_bps
        for protection in (
            leg_a.execution_protection,
            leg_b.execution_protection,
            leg_c.execution_protection,
        )
    )
    max_extra_buy_leg_slippage_bps = max(
        protection.max_extra_buy_leg_slippage_bps
        for protection in (
            leg_a.execution_protection,
            leg_b.execution_protection,
            leg_c.execution_protection,
        )
    )
    recovery_success_count = max(
        protection.recovery_success_count
        for protection in (
            leg_a.execution_protection,
            leg_b.execution_protection,
            leg_c.execution_protection,
        )
    )

    lookup_tables = unique_by_insertion(
        list(leg_a.lookup_tables) + list(leg_b.lookup_tables) + list(leg_c.lookup_tables)
    )
    account_dependencies = unique_by_insertion(
        list(leg_a.account_dependencies)
        + list(leg_b.account_dependencies)
        + list(leg_c.account_dependencies)
    )

    lines = [
        "",
        "[[routes.definitions]]",
        'enabled = true',
        'route_class = "amm_fast_path"',
        'kind = "triangular"',
        f'route_id = "{route_id}"',
        f'input_mint = "{profit_mint}"',
        f'output_mint = "{profit_mint}"',
        f'base_mint = "{intermediate_mint_1}"',
        f'quote_mint = "{profit_mint}"',
        f"min_profit_quote_atoms = {min_profit_quote_atoms}",
    ]
    if conversion_pool_id:
        lines.append(f'sol_quote_conversion_pool_id = "{conversion_pool_id}"')
    lines.extend(
        [
            f"min_trade_size = {min_trade_size}",
            f"default_trade_size = {default_trade_size}",
            f"max_trade_size = {max_trade_size}",
            f"size_ladder = [{', '.join(str(value) for value in size_ladder)}]",
        ]
    )
    if not account_dependencies:
        lines.append("account_dependencies = []")

    render_leg(lines, leg_a)
    render_leg(lines, leg_b)
    render_leg(lines, leg_c)

    for dependency in account_dependencies:
        lines.extend(
            [
                "",
                "[[routes.definitions.account_dependencies]]",
                f'account_key = "{dependency.account_key}"',
                f'pool_id = "{dependency.pool_id}"',
                f'decoder_key = "{dependency.decoder_key}"',
            ]
        )

    lines.extend(
        [
            "",
            "[routes.definitions.execution]",
            f'message_mode = "{message_mode}"',
            f"default_compute_unit_limit = {default_compute_unit_limit}",
            f"minimum_compute_unit_limit = {minimum_compute_unit_limit}",
            "default_compute_unit_price_micro_lamports = "
            + str(
                max(
                    leg_a.default_compute_unit_price_micro_lamports,
                    leg_b.default_compute_unit_price_micro_lamports,
                    leg_c.default_compute_unit_price_micro_lamports,
                )
            ),
            "default_jito_tip_lamports = "
            + str(
                max(
                    leg_a.default_jito_tip_lamports,
                    leg_b.default_jito_tip_lamports,
                    leg_c.default_jito_tip_lamports,
                )
            ),
            f"max_quote_slot_lag = {max_quote_slot_lag}",
            "max_alt_slot_lag = "
            + str(
                min(
                    leg_a.max_alt_slot_lag,
                    leg_b.max_alt_slot_lag,
                    leg_c.max_alt_slot_lag,
                )
            ),
        ]
    )
    for lookup_table in lookup_tables:
        lines.extend(
            [
                "",
                "[[routes.definitions.execution.lookup_tables]]",
                f'account_key = "{lookup_table}"',
            ]
        )
    lines.extend(
        [
            "",
            "[routes.definitions.execution_protection]",
            "enabled = true",
            f"tight_max_quote_slot_lag = {tight_max_quote_slot_lag}",
            f"base_extra_buy_leg_slippage_bps = {base_extra_buy_leg_slippage_bps}",
            f"failure_step_bps = {failure_step_bps}",
            f"max_extra_buy_leg_slippage_bps = {max_extra_buy_leg_slippage_bps}",
            f"recovery_success_count = {recovery_success_count}",
        ]
    )
    return "\n".join(lines)


def build_manifest(source_path: Path, output_path: Path, args: argparse.Namespace) -> int:
    source_text = source_path.read_text(encoding="utf-8").rstrip()
    prefix = top_level_prefix(source_path)
    source_routes, catalog_by_direction, routes_by_profit_mint = parse_source_routes(
        source_path, args
    )
    symbol_hints = infer_symbol_hints(source_routes)
    profit_mints = sorted(routes_by_profit_mint)
    all_mints = sorted(
        {
            mint
            for route in source_routes
            for mint in (
                route.get("input_mint"),
                route.get("output_mint"),
                route.get("base_mint"),
                route.get("quote_mint"),
            )
            if mint
        }
    )

    rendered_routes: dict[str, str] = {}
    skipped_missing_conversion = 0
    for profit_mint in profit_mints:
        size_profile = build_size_profile(routes_by_profit_mint[profit_mint])
        conversion_pool_id = choose_conversion_pool_id(profit_mint, catalog_by_direction)
        if profit_mint != SOL_MINT and not conversion_pool_id:
            skipped_missing_conversion += 1
            continue

        for intermediate_mint_1 in all_mints:
            if intermediate_mint_1 == profit_mint:
                continue
            for intermediate_mint_2 in all_mints:
                if intermediate_mint_2 in {profit_mint, intermediate_mint_1}:
                    continue
                leg_a_candidates = catalog_by_direction.get((profit_mint, intermediate_mint_1), [])
                leg_b_candidates = catalog_by_direction.get(
                    (intermediate_mint_1, intermediate_mint_2), []
                )
                leg_c_candidates = catalog_by_direction.get((intermediate_mint_2, profit_mint), [])
                if not leg_a_candidates or not leg_b_candidates or not leg_c_candidates:
                    continue

                for leg_a, leg_b, leg_c in product(
                    leg_a_candidates, leg_b_candidates, leg_c_candidates
                ):
                    if len({leg_a.pool_id, leg_b.pool_id, leg_c.pool_id}) != 3:
                        continue
                    route_id = route_id_for_triangle(
                        symbol_a=symbol_for_mint(symbol_hints, profit_mint),
                        symbol_b=symbol_for_mint(symbol_hints, intermediate_mint_1),
                        symbol_c=symbol_for_mint(symbol_hints, intermediate_mint_2),
                        leg_a=leg_a,
                        leg_b=leg_b,
                        leg_c=leg_c,
                    )
                    rendered_routes.setdefault(
                        route_id,
                        render_route(
                            route_id=route_id,
                            profit_mint=profit_mint,
                            intermediate_mint_1=intermediate_mint_1,
                            intermediate_mint_2=intermediate_mint_2,
                            min_profit_quote_atoms=profit_floor_for_mint(
                                mint=profit_mint,
                                symbol_hints=symbol_hints,
                                stable_profit_floor=args.stable_profit_floor,
                                sol_profit_floor=args.sol_profit_floor,
                            ),
                            size_profile=size_profile,
                            conversion_pool_id=conversion_pool_id,
                            leg_a=leg_a,
                            leg_b=leg_b,
                            leg_c=leg_c,
                        ),
                    )

    generated_header = (
        "# Auto-generated triangular routes on "
        + date.today().isoformat()
        + ".\n"
        + f"# Source manifest: {source_path.name}. Profit mints: {', '.join(symbol_for_mint(symbol_hints, mint) for mint in profit_mints)}.\n"
        + f"# Triangular routes: {len(rendered_routes)}."
        + (
            f" Skipped profit mints without SOL conversion pool: {skipped_missing_conversion}."
            if skipped_missing_conversion
            else ""
        )
        + "\n"
    )
    header = (
        source_text + "\n\n" + generated_header
        if args.include_source_routes
        else prefix + "\n\n" + generated_header
    )
    output_path.write_text(
        header + "".join(rendered_routes[route_id] for route_id in sorted(rendered_routes)) + "\n",
        encoding="utf-8",
    )
    return len(rendered_routes)


def main() -> int:
    args = parse_args()
    source_path = Path(args.source)
    output_path = Path(args.output)
    route_count = build_manifest(source_path, output_path, args)
    print(f"generated {route_count} triangular routes -> {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
