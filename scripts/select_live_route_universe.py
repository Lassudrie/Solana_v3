#!/usr/bin/env python3
"""Select a tighter live route universe from a source manifest."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import tomllib
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
JITOSOL_MINT = "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn"

PRIMARY_PROFIT_MINTS = {SOL_MINT, USDC_MINT, USDT_MINT}
MAJOR_PROFIT_MINTS = PRIMARY_PROFIT_MINTS | {JITOSOL_MINT}


@dataclass(frozen=True)
class RouteBlock:
    route_id: str
    text: str
    index: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="/etc/solana-bot/bot.toml")
    parser.add_argument("--output", default="/etc/solana-bot/bot.top1000.toml")
    parser.add_argument("--monitor-endpoint", default="http://127.0.0.1:8081")
    parser.add_argument("--limit", type=int, default=1_000)
    parser.add_argument(
        "--preserve-eligible-two-leg",
        action="store_true",
        default=True,
        help="always keep live-eligible 2-leg routes before filling with triangular routes",
    )
    return parser.parse_args()


def fetch_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)


def split_route_blocks(source_text: str) -> tuple[str, list[RouteBlock]]:
    marker = "[[routes.definitions]]"
    first = source_text.find(marker)
    if first == -1:
        raise ValueError("source manifest does not contain [[routes.definitions]]")

    prefix = source_text[:first].rstrip()
    indices: list[int] = []
    start = 0
    while True:
        index = source_text.find(marker, start)
        if index == -1:
            break
        indices.append(index)
        start = index + len(marker)

    blocks: list[RouteBlock] = []
    for position, block_start in enumerate(indices):
        block_end = indices[position + 1] if position + 1 < len(indices) else len(source_text)
        block_text = source_text[block_start:block_end].rstrip()
        route_id = extract_route_id(block_text)
        blocks.append(RouteBlock(route_id=route_id, text=block_text, index=position))
    return prefix, blocks


def extract_route_id(block_text: str) -> str:
    for line in block_text.splitlines():
        line = line.strip()
        if line.startswith("route_id = "):
            return line.split('"', 2)[1]
    raise ValueError("route block is missing route_id")


def safe_log10(value: int | float) -> float:
    return math.log10(max(float(value), 1.0))


def closeness_score(value: float, target: float, span: float) -> float:
    if span <= 0:
        return 1.0
    return max(0.0, 1.0 - abs(value - target) / span)


def summarize_distribution(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 1.0
    if len(values) == 1:
        return values[0], 1.0
    target = statistics.median(values)
    quartiles = statistics.quantiles(values, n=4)
    span = max(quartiles[2] - quartiles[0], 0.2)
    return target, span


def route_kind(route: dict[str, Any]) -> str:
    return route.get("kind", "two_leg")


def route_max_quote_slot_lag(route: dict[str, Any]) -> int:
    execution = route.get("execution") or {}
    return int(execution.get("max_quote_slot_lag") or 0)


def total_fee_bps(route: dict[str, Any]) -> int:
    return sum(int(leg.get("fee_bps") or 0) for leg in route.get("legs", []))


def distinct_venue_count(route: dict[str, Any]) -> int:
    return len({leg["venue"] for leg in route.get("legs", [])})


def route_pool_metrics(route: dict[str, Any], pool_map: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    metrics: list[dict[str, Any]] = []
    missing: list[str] = []
    for leg in route.get("legs", []):
        pool_id = leg["pool_id"]
        metric = pool_map.get(pool_id)
        if metric is None:
            missing.append(pool_id)
            continue
        metrics.append(metric)
    return metrics, missing


def route_effective_live_eligible(
    route: dict[str, Any],
    route_health: dict[str, Any],
    pool_map: dict[str, dict[str, Any]],
    latest_slot: int,
) -> bool:
    if route_health.get("health_state") == "shadow_only":
        return False

    metrics, missing_pools = route_pool_metrics(route, pool_map)
    if missing_pools or not metrics:
        return False

    max_quote_slot_lag = route_max_quote_slot_lag(route)
    if max_quote_slot_lag <= 0:
        return False

    for metric in metrics:
        if metric.get("health_state") != "healthy":
            return False
        confidence = str(metric.get("confidence") or "").lower()
        if confidence not in {"verified", "executable"}:
            return False
        last_update_slot = int(metric.get("last_update_slot") or 0)
        if latest_slot - last_update_slot > max_quote_slot_lag:
            return False

    return True


def route_score(
    route: dict[str, Any],
    route_health: dict[str, Any],
    pool_map: dict[str, dict[str, Any]],
    latest_slot: int,
    effective_eligible: bool,
    target_depth: float,
    depth_span: float,
    target_size: float,
    size_span: float,
) -> float:
    metrics, missing_pools = route_pool_metrics(route, pool_map)
    if not metrics:
        return -100_000.0

    min_depth = min(int(metric.get("reserve_depth") or 1) for metric in metrics)
    max_slot_lag = max(int(metric.get("slot_lag") or 0) for metric in metrics)
    health_states = [metric.get("health_state", "unknown") for metric in metrics]
    confidences = [metric.get("confidence", "unknown") for metric in metrics]

    depth_score = closeness_score(safe_log10(min_depth), target_depth, depth_span)
    size_score = closeness_score(
        safe_log10(int(route.get("default_trade_size") or 1)),
        target_size,
        size_span,
    )

    score = 0.0
    if effective_eligible:
        score += 10_000.0
    else:
        blocking_reason = route_health.get("blocking_reason")
        if not blocking_reason and missing_pools:
            blocking_reason = "missing_pool_state"
        if blocking_reason == "pool_state_not_executable":
            score -= 2_000.0
        elif blocking_reason == "pool_quarantined":
            score -= 3_000.0
        elif blocking_reason == "pool_repair_pending":
            score -= 2_500.0
        elif blocking_reason == "execution_failure_budget":
            score -= 3_500.0
        elif blocking_reason == "snapshot_stale":
            score -= 2_250.0
        else:
            score -= 1_500.0

    if route_kind(route) == "two_leg":
        score += 200.0

    score += 220.0 * depth_score
    score += 120.0 * size_score
    score += 35.0 * max(0, distinct_venue_count(route) - 1)
    score -= 4.0 * total_fee_bps(route)
    score -= 2.0 * max_slot_lag

    route_lag = route_max_quote_slot_lag(route)
    if route_lag > 0:
        age_ratio = max_slot_lag / route_lag
        score -= 40.0 * max(0.0, age_ratio - 0.5)

    profit_mint = route.get("input_mint")
    if profit_mint in MAJOR_PROFIT_MINTS:
        score += 45.0
    if profit_mint in PRIMARY_PROFIT_MINTS:
        score += 8.0

    degraded_count = sum(state == "degraded" for state in health_states)
    score -= 120.0 * degraded_count
    if any(state == "quarantined" for state in health_states):
        score -= 5_000.0
    if any(state == "repair_pending" for state in health_states):
        score -= 3_000.0
    if any(state == "executable_refresh_pending" for state in health_states):
        score -= 1_000.0

    score += 30.0 * sum(confidence == "executable" for confidence in confidences)
    score += 10.0 * sum(confidence == "verified" for confidence in confidences)
    score -= 100.0 * sum(confidence == "invalid" for confidence in confidences)

    score -= 5_000.0 * len(missing_pools)
    return score


def select_routes(
    config_routes: list[dict[str, Any]],
    route_items: list[dict[str, Any]],
    pool_items: list[dict[str, Any]],
    latest_slot: int,
    limit: int,
    preserve_eligible_two_leg: bool,
) -> tuple[list[str], dict[str, Any]]:
    route_map = {route["route_id"]: route for route in config_routes}
    route_health_map = {item["route_id"]: item for item in route_items}
    pool_map = {item["pool_id"]: item for item in pool_items}

    effective_eligible_routes: list[dict[str, Any]] = []
    monitor_eligible_routes = [
        route_map[item["route_id"]]
        for item in route_items
        if item.get("eligible_live") and item["route_id"] in route_map
    ]
    coverage_routes: list[dict[str, Any]] = []
    effective_live = {}
    eligible_depth_logs = []
    eligible_size_logs = []
    for route in config_routes:
        route_id = route["route_id"]
        route_health = route_health_map.get(route_id, {})
        effective_live[route_id] = route_effective_live_eligible(
            route,
            route_health,
            pool_map,
            latest_slot,
        )
        metrics, missing_pools = route_pool_metrics(route, pool_map)
        if not missing_pools and metrics:
            coverage_routes.append(route)
        if not effective_live[route_id]:
            continue
        effective_eligible_routes.append(route)
        metrics, _ = route_pool_metrics(route, pool_map)
        if metrics:
            eligible_depth_logs.append(
                safe_log10(min(int(metric.get("reserve_depth") or 1) for metric in metrics))
            )
        eligible_size_logs.append(safe_log10(int(route.get("default_trade_size") or 1)))

    target_depth, depth_span = summarize_distribution(eligible_depth_logs)
    target_size, size_span = summarize_distribution(eligible_size_logs)

    scored_routes: list[tuple[float, dict[str, Any]]] = []
    for route in config_routes:
        route_id = route["route_id"]
        route_health = route_health_map.get(route_id, {})
        score = route_score(
            route,
            route_health,
            pool_map,
            latest_slot,
            effective_live.get(route_id, False),
            target_depth,
            depth_span,
            target_size,
            size_span,
        )
        scored_routes.append((score, route))

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()

    if preserve_eligible_two_leg:
        eligible_two_leg = [
            route
            for route in effective_eligible_routes
            if route_kind(route) == "two_leg"
        ]
        eligible_two_leg.sort(key=lambda route: route["route_id"])
        for route in eligible_two_leg[:limit]:
            selected.append(route)
            selected_ids.add(route["route_id"])

    for _, route in sorted(scored_routes, key=lambda item: (item[0], item[1]["route_id"]), reverse=True):
        if len(selected) >= limit:
            break
        if route["route_id"] in selected_ids:
            continue
        selected.append(route)
        selected_ids.add(route["route_id"])

    report = {
        "selected_count": len(selected),
        "selected_by_kind": Counter(route_kind(route) for route in selected),
        "selected_by_profit_mint": Counter(route["input_mint"] for route in selected),
        "coverage_count": len(coverage_routes),
        "coverage_by_kind": Counter(route_kind(route) for route in coverage_routes),
        "effective_eligible_count": len(effective_eligible_routes),
        "effective_eligible_by_kind": Counter(route_kind(route) for route in effective_eligible_routes),
        "monitor_eligible_count": len(monitor_eligible_routes),
        "monitor_eligible_by_kind": Counter(route_kind(route) for route in monitor_eligible_routes),
        "target_depth_log10": round(target_depth, 4),
        "depth_span_log10": round(depth_span, 4),
        "target_size_log10": round(target_size, 4),
        "size_span_log10": round(size_span, 4),
    }
    return [route["route_id"] for route in selected], report


def write_selected_manifest(
    source_path: Path,
    output_path: Path,
    selected_ids: list[str],
    report: dict[str, Any],
) -> None:
    source_text = source_path.read_text(encoding="utf-8")
    prefix, blocks = split_route_blocks(source_text)
    selected_set = set(selected_ids)
    selected_blocks = [block for block in blocks if block.route_id in selected_set]
    if len(selected_blocks) != len(selected_set):
        found = {block.route_id for block in selected_blocks}
        missing = sorted(selected_set - found)
        raise ValueError(f"selected route blocks are missing from source manifest: {missing[:10]}")

    selected_blocks.sort(key=lambda block: block.index)
    selected_kind_counts = report["selected_by_kind"]
    header = (
        f"# Live universe selection generated on {date.today().isoformat()}.\n"
        f"# Source manifest: {source_path}.\n"
        f"# Selected routes: {report['selected_count']} / {len(blocks)}.\n"
        f"# Selected by kind: {dict(selected_kind_counts)}.\n"
    )
    rendered = prefix.rstrip() + "\n\n" + header + "\n\n" + "\n\n".join(
        block.text for block in selected_blocks
    ) + "\n"
    output_path.write_text(rendered, encoding="utf-8")


def main() -> int:
    args = parse_args()
    source_path = Path(args.source)
    output_path = Path(args.output)

    source_text = source_path.read_text(encoding="utf-8")
    config_routes = tomllib.loads(source_text)["routes"]["definitions"]

    endpoint = args.monitor_endpoint.rstrip("/")
    overview = fetch_json(f"{endpoint}/monitor/overview")
    route_items = fetch_json(f"{endpoint}/monitor/routes?status=all&limit=10000")["items"]
    pool_items = fetch_json(f"{endpoint}/monitor/pools?status=all&limit=10000")["items"]

    selected_ids, report = select_routes(
        config_routes=config_routes,
        route_items=route_items,
        pool_items=pool_items,
        latest_slot=int(overview["latest_slot"]),
        limit=args.limit,
        preserve_eligible_two_leg=args.preserve_eligible_two_leg,
    )
    write_selected_manifest(source_path, output_path, selected_ids, report)

    serializable_report = {
        key: dict(value) if isinstance(value, Counter) else value
        for key, value in report.items()
    }
    print(json.dumps(serializable_report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
