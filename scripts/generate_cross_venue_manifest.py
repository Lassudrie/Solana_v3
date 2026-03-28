#!/usr/bin/env python3
"""Generate a low-latency multi-venue manifest from official Orca/Raydium APIs."""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import permutations
from pathlib import Path

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
MEMO_PROGRAM_ID = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
ORCA_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
RAYDIUM_PROGRAM_ID = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
SOL_USDC_CONVERSION_POOL = "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv"

# Known bad pool discovered live: state is not executable and keeps the bot in warmup.
EXCLUDED_ORCA_POOLS = {
    "Ckp1kwZqosaLU1h3zWtuaMBubyWM7LX3cxYezRVin7p2",
}

EXCLUDED_RAYDIUM_POOLS = {
    # Live route pair GEOD/USDC: this CLMM repeatedly returns invalid snapshot
    # liquidity and only produces route-specific quote failures.
    "BkX9RSHoDJGkU7pbHiksv3DprdLM3KWGoDSfGN9cGJ5d",
}


@dataclass(frozen=True)
class OrcaPool:
    address: str
    token_mint_a: str
    token_mint_b: str
    token_symbol_a: str
    token_symbol_b: str
    token_decimals_a: int
    token_decimals_b: int
    token_program_id: str
    token_vault_a: str
    token_vault_b: str
    whirlpools_config: str
    tick_spacing: int
    fee_rate: int
    address_lookup_table: str | None
    tvl_usdc: float


@dataclass(frozen=True)
class RayPool:
    address: str
    token_mint_a: str
    token_mint_b: str
    token_symbol_a: str
    token_symbol_b: str
    token_decimals_a: int
    token_decimals_b: int
    token_program_id: str
    token_vault_a: str
    token_vault_b: str
    amm_config: str
    observation_id: str
    ex_bitmap_account: str | None
    tick_spacing: int
    fee_rate: int
    lookup_table_account: str | None
    tvl_usdc: float


@dataclass(frozen=True)
class RaySimplePool:
    address: str
    token_mint_a: str
    token_mint_b: str
    token_symbol_a: str
    token_symbol_b: str
    token_decimals_a: int
    token_decimals_b: int
    token_program_id: str
    amm_coin_vault: str
    amm_pc_vault: str
    program_id: str
    amm_authority: str
    amm_open_orders: str
    market_program: str
    market: str
    market_bids: str
    market_asks: str
    market_event_queue: str
    market_coin_vault: str
    market_pc_vault: str
    market_vault_signer: str
    lookup_table_account: str | None
    fee_rate: int
    tvl_usdc: float


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def sanitize_symbol(symbol: str, fallback: str) -> str:
    filtered = "".join(ch.lower() for ch in symbol if ch.isalnum())
    return filtered or fallback[:6].lower()


def canonical_symbol(mint: str, symbol: str) -> str:
    if mint == SOL_MINT:
        return "sol"
    if mint == USDC_MINT:
        return "usdc"
    return sanitize_symbol(symbol, mint)


def short_id(address: str) -> str:
    return address[:4].lower()


def fetch_orca_pools(min_tvl: int, page_size: int, max_pages: int) -> list[OrcaPool]:
    pools: list[OrcaPool] = []
    base_url = (
        "https://api.orca.so/v2/solana/pools/search"
        f"?q=&minTvl={min_tvl}&verifiedOnly=true&size={page_size}"
    )
    url = base_url
    for _ in range(max_pages):
        payload = fetch_json(url)
        for item in payload.get("data", []):
            if item.get("poolType") != "whirlpool":
                continue
            pools.append(
                OrcaPool(
                    address=item["address"],
                    token_mint_a=item["tokenMintA"],
                    token_mint_b=item["tokenMintB"],
                    token_symbol_a=item["tokenA"]["symbol"],
                    token_symbol_b=item["tokenB"]["symbol"],
                    token_decimals_a=int(item["tokenA"]["decimals"]),
                    token_decimals_b=int(item["tokenB"]["decimals"]),
                    token_program_id=item["tokenA"]["programId"],
                    token_vault_a=item["tokenVaultA"],
                    token_vault_b=item["tokenVaultB"],
                    whirlpools_config=item["whirlpoolsConfig"],
                    tick_spacing=int(item["tickSpacing"]),
                    fee_rate=int(item["feeRate"]),
                    address_lookup_table=item.get("addressLookupTable"),
                    tvl_usdc=float(item["tvlUsdc"]),
                )
            )
        next_cursor = payload.get("meta", {}).get("cursor", {}).get("next")
        if not next_cursor:
            break
        url = (
            base_url
            + "&cursor="
            + urllib.parse.quote(next_cursor, safe="")
        )
    return pools


def fetch_raydium_list(max_pages: int, page_size: int) -> list[dict]:
    pools: list[dict] = []
    for page in range(1, max_pages + 1):
        payload = fetch_json(
            "https://api-v3.raydium.io/pools/info/list"
            f"?poolType=all&poolSortField=liquidity&sortType=desc&pageSize={page_size}&page={page}"
        )
        batch = payload["data"]["data"]
        if not batch:
            break
        pools.extend(batch)
        if not payload["data"].get("hasNextPage"):
            break
    return pools


def fetch_raydium_keys(pool_ids: list[str]) -> dict[str, dict]:
    details: dict[str, dict] = {}
    chunk_size = 20
    for start in range(0, len(pool_ids), chunk_size):
        chunk = pool_ids[start : start + chunk_size]
        payload = fetch_json(
            "https://api-v3.raydium.io/pools/key/ids?ids=" + ",".join(chunk)
        )
        for item in payload.get("data", []):
            details[item["id"]] = item
    return details


def pair_key(mint_a: str, mint_b: str) -> tuple[str, str]:
    return tuple(sorted((mint_a, mint_b)))


def route_sizes(
    quote_mint: str,
    quote_symbol: str,
    quote_decimals: int | None,
) -> tuple[int, int, int, list[int]]:
    stable_like_quotes = {"usdc", "usdt", "usd1", "usdg", "eurc", "usde", "pyusd"}
    sol_like_quotes = {"sol", "jitosol", "msol", "jupsol", "stsol", "bsol"}
    if quote_mint == USDC_MINT or quote_symbol in stable_like_quotes or (
        quote_decimals is not None and quote_decimals <= 6
    ):
        ladder = [2_500_000, 5_000_000, 10_000_000, 15_000_000, 20_000_000, 25_000_000]
        return ladder[0], 15_000_000, ladder[-1], ladder
    if quote_mint == SOL_MINT or quote_symbol in sol_like_quotes or (
        quote_decimals is not None and quote_decimals >= 8
    ):
        ladder = [2_500_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000, 100_000_000]
        return ladder[0], 10_000_000, ladder[-1], ladder
    ladder = [1_000_000, 2_500_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000]
    return ladder[0], 10_000_000, ladder[-1], ladder


def choose_base_quote(
    pair: tuple[str, str],
    quote_priority: list[str],
) -> tuple[str, str]:
    left, right = pair
    if pair == tuple(sorted((SOL_MINT, USDC_MINT))):
        return SOL_MINT, USDC_MINT
    for quote_mint in quote_priority:
        if quote_mint in pair:
            return (right if left == quote_mint else left), quote_mint
    raise ValueError(f"unsupported pair {pair}")


def leg_input_mint(base_mint: str, quote_mint: str, side: str) -> str:
    return quote_mint if side == "buy_base" else base_mint


def render_lookup_tables(table_keys: list[str]) -> str:
    tables: list[str] = []
    for key in table_keys:
        if key and key not in tables:
            tables.append(key)
    rendered = []
    for table in tables:
        rendered.append("")
        rendered.append("[[routes.definitions.execution.lookup_tables]]")
        rendered.append(f'account_key = "{table}"')
    return "\n".join(rendered)


def venue_tag(pool: OrcaPool | RayPool | RaySimplePool) -> str:
    if isinstance(pool, OrcaPool):
        return "orca"
    return "ray"


def pool_address(pool: OrcaPool | RayPool | RaySimplePool) -> str:
    return pool.address


def pool_lookup_table(pool: OrcaPool | RayPool | RaySimplePool) -> str | None:
    return pool.address_lookup_table if isinstance(pool, OrcaPool) else pool.lookup_table_account


def pool_tvl(pool: OrcaPool | RayPool | RaySimplePool) -> float:
    return pool.tvl_usdc


def token_symbol_for_mint(pool: OrcaPool | RayPool | RaySimplePool, mint: str) -> str:
    if pool.token_mint_a == mint:
        return pool.token_symbol_a
    if pool.token_mint_b == mint:
        return pool.token_symbol_b
    raise ValueError(f"pool {pool.address} does not contain mint {mint}")


def token_decimals_for_mint(pool: OrcaPool | RayPool | RaySimplePool, mint: str) -> int:
    if pool.token_mint_a == mint:
        return pool.token_decimals_a
    if pool.token_mint_b == mint:
        return pool.token_decimals_b
    raise ValueError(f"pool {pool.address} does not contain mint {mint}")


def top_n_unique(items, key_fn, id_fn, limit: int):
    selected = []
    seen = set()
    for item in sorted(items, key=key_fn, reverse=True):
        item_id = id_fn(item)
        if item_id in seen:
            continue
        seen.add(item_id)
        selected.append(item)
        if len(selected) >= limit:
            break
    return selected


def route_id_from_rendered(rendered: str) -> str:
    for line in rendered.splitlines():
        if line.startswith('route_id = "'):
            return line.split('"')[1]
    raise ValueError("rendered route is missing route_id")


def render_account_dependencies(
    first_pool: OrcaPool | RayPool | RaySimplePool,
    second_pool: OrcaPool | RayPool | RaySimplePool,
) -> list[str]:
    lines: list[str] = []
    seen: set[tuple[str, str]] = set()
    for pool in [first_pool, second_pool]:
        if isinstance(pool, OrcaPool):
            decoder_key = "orca-whirlpool-v1"
        elif isinstance(pool, RayPool):
            decoder_key = "raydium-clmm-v1"
        else:
            continue
        key = (pool.address, decoder_key)
        if key in seen:
            continue
        seen.add(key)
        lines.extend(
            [
                "",
                "[[routes.definitions.account_dependencies]]",
                f'account_key = "{pool.address}"',
                f'pool_id = "{pool.address}"',
                f'decoder_key = "{decoder_key}"',
            ]
        )
    return lines


def render_leg(
    pool: OrcaPool | RayPool | RaySimplePool,
    side: str,
    base_mint: str,
    quote_mint: str,
) -> list[str]:
    input_mint = leg_input_mint(base_mint, quote_mint, side)
    output_mint = base_mint if side == "buy_base" else quote_mint
    if isinstance(pool, OrcaPool):
        a_to_b = input_mint == pool.token_mint_a
        return [
            "[[routes.definitions.legs]]",
            'venue = "orca_whirlpool"',
            f'pool_id = "{pool.address}"',
            f'side = "{side}"',
            f"fee_bps = {pool.fee_rate // 100}",
            "",
            "[routes.definitions.legs.execution]",
            'kind = "orca_whirlpool"',
            f'program_id = "{ORCA_PROGRAM_ID}"',
            f'token_program_id = "{pool.token_program_id}"',
            f'whirlpool = "{pool.address}"',
            f'token_mint_a = "{pool.token_mint_a}"',
            f'token_vault_a = "{pool.token_vault_a}"',
            f'token_mint_b = "{pool.token_mint_b}"',
            f'token_vault_b = "{pool.token_vault_b}"',
            f"tick_spacing = {pool.tick_spacing}",
            f'a_to_b = {"true" if a_to_b else "false"}',
        ]

    if isinstance(pool, RayPool):
        zero_for_one = input_mint == pool.token_mint_a
        lines = [
            "[[routes.definitions.legs]]",
            'venue = "raydium_clmm"',
            f'pool_id = "{pool.address}"',
            f'side = "{side}"',
            f"fee_bps = {pool.fee_rate // 100}",
            "",
            "[routes.definitions.legs.execution]",
            'kind = "raydium_clmm"',
            f'program_id = "{RAYDIUM_PROGRAM_ID}"',
            f'token_program_id = "{pool.token_program_id}"',
            f'token_program_2022_id = "{TOKEN_2022_PROGRAM_ID}"',
            f'memo_program_id = "{MEMO_PROGRAM_ID}"',
            f'pool_state = "{pool.address}"',
            f'amm_config = "{pool.amm_config}"',
            f'observation_state = "{pool.observation_id}"',
        ]
        if pool.ex_bitmap_account:
            lines.append(f'ex_bitmap_account = "{pool.ex_bitmap_account}"')
        lines.extend(
            [
                f'token_mint_0 = "{pool.token_mint_a}"',
                f'token_vault_0 = "{pool.token_vault_a}"',
                f'token_mint_1 = "{pool.token_mint_b}"',
                f'token_vault_1 = "{pool.token_vault_b}"',
                f"tick_spacing = {pool.tick_spacing}",
                f'zero_for_one = {"true" if zero_for_one else "false"}',
            ]
        )
        return lines

    return [
        "[[routes.definitions.legs]]",
        'venue = "raydium"',
        f'pool_id = "{pool.address}"',
        f'side = "{side}"',
        f"fee_bps = {pool.fee_rate}",
        "",
        "[routes.definitions.legs.execution]",
        'kind = "raydium_simple_pool"',
        f'program_id = "{pool.program_id}"',
        f'token_program_id = "{pool.token_program_id}"',
        f'amm_pool = "{pool.address}"',
        f'amm_authority = "{pool.amm_authority}"',
        f'amm_open_orders = "{pool.amm_open_orders}"',
        f'amm_coin_vault = "{pool.amm_coin_vault}"',
        f'amm_pc_vault = "{pool.amm_pc_vault}"',
        f'market_program = "{pool.market_program}"',
        f'market = "{pool.market}"',
        f'market_bids = "{pool.market_bids}"',
        f'market_asks = "{pool.market_asks}"',
        f'market_event_queue = "{pool.market_event_queue}"',
        f'market_coin_vault = "{pool.market_coin_vault}"',
        f'market_pc_vault = "{pool.market_pc_vault}"',
        f'market_vault_signer = "{pool.market_vault_signer}"',
        f'user_source_mint = "{input_mint}"',
        f'user_destination_mint = "{output_mint}"',
    ]


def render_route(
    base_symbol: str,
    quote_symbol: str,
    base_mint: str,
    quote_mint: str,
    quote_decimals: int,
    sol_quote_conversion_pool_id: str | None,
    first_pool: OrcaPool | RayPool | RaySimplePool,
    second_pool: OrcaPool | RayPool | RaySimplePool,
) -> str:
    route_id = (
        f"{base_symbol}-{quote_symbol}-{venue_tag(first_pool)}-{short_id(pool_address(first_pool))}"
        f"-{venue_tag(second_pool)}-{short_id(pool_address(second_pool))}"
    )
    min_trade_size, default_trade_size, max_trade_size, size_ladder = route_sizes(
        quote_mint,
        quote_symbol,
        quote_decimals,
    )

    lines = [
        "",
        "[[routes.definitions]]",
        'enabled = true',
        'route_class = "amm_fast_path"',
        f'route_id = "{route_id}"',
        f'input_mint = "{quote_mint}"',
        f'output_mint = "{quote_mint}"',
        f'base_mint = "{base_mint}"',
        f'quote_mint = "{quote_mint}"',
    ]
    if quote_mint != SOL_MINT and sol_quote_conversion_pool_id:
        lines.append(f'sol_quote_conversion_pool_id = "{sol_quote_conversion_pool_id}"')
    lines.extend(
        [
            f"min_trade_size = {min_trade_size}",
            f"default_trade_size = {default_trade_size}",
            f"max_trade_size = {max_trade_size}",
            f"size_ladder = [{', '.join(str(value) for value in size_ladder)}]",
            "",
        ]
    )
    lines.extend(render_leg(first_pool, "buy_base", base_mint, quote_mint))
    lines.append("")
    lines.extend(render_leg(second_pool, "sell_base", base_mint, quote_mint))
    lines.extend(render_account_dependencies(first_pool, second_pool))
    lines.extend(
        [
            "",
            "[routes.definitions.execution]",
            'message_mode = "v0_required"',
            "default_compute_unit_limit = 300000",
            "default_compute_unit_price_micro_lamports = 1",
            "default_jito_tip_lamports = 1",
            "max_quote_slot_lag = 32",
            "max_alt_slot_lag = 32",
        ]
    )
    lookup_tables = render_lookup_tables(
        [pool_lookup_table(first_pool) or "", pool_lookup_table(second_pool) or ""]
    )
    if lookup_tables:
        lines.append(lookup_tables)
    lines.extend(
        [
            "",
            "[routes.definitions.execution_protection]",
            "enabled = true",
            "tight_max_quote_slot_lag = 24",
            "base_extra_buy_leg_slippage_bps = 25",
            "failure_step_bps = 25",
            "max_extra_buy_leg_slippage_bps = 100",
            "recovery_success_count = 3",
        ]
    )
    return "\n".join(lines)


def render_raydium_only_route(
    base_symbol: str,
    quote_symbol: str,
    base_mint: str,
    quote_mint: str,
    quote_decimals: int,
    sol_quote_conversion_pool_id: str | None,
    first_pool: RayPool,
    second_pool: RayPool,
) -> str:
    route_id = (
        f"{base_symbol}-{quote_symbol}-ray-{short_id(first_pool.address)}"
        f"-ray-{short_id(second_pool.address)}"
    )
    min_trade_size, default_trade_size, max_trade_size, size_ladder = route_sizes(
        quote_mint,
        quote_symbol,
        quote_decimals,
    )
    first_zero_for_one = quote_mint == first_pool.token_mint_a
    second_zero_for_one = base_mint == second_pool.token_mint_a
    lines = [
        "",
        "[[routes.definitions]]",
        'enabled = true',
        'route_class = "amm_fast_path"',
        f'route_id = "{route_id}"',
        f'input_mint = "{quote_mint}"',
        f'output_mint = "{quote_mint}"',
        f'base_mint = "{base_mint}"',
        f'quote_mint = "{quote_mint}"',
    ]
    if quote_mint != SOL_MINT and sol_quote_conversion_pool_id:
        lines.append(f'sol_quote_conversion_pool_id = "{sol_quote_conversion_pool_id}"')
    lines.extend(
        [
            f"min_trade_size = {min_trade_size}",
            f"default_trade_size = {default_trade_size}",
            f"max_trade_size = {max_trade_size}",
            f"size_ladder = [{', '.join(str(value) for value in size_ladder)}]",
            "",
            "[[routes.definitions.legs]]",
            'venue = "raydium_clmm"',
            f'pool_id = "{first_pool.address}"',
            'side = "buy_base"',
            f"fee_bps = {first_pool.fee_rate // 100}",
            "",
            "[routes.definitions.legs.execution]",
            'kind = "raydium_clmm"',
            f'program_id = "{RAYDIUM_PROGRAM_ID}"',
            f'token_program_id = "{first_pool.token_program_id}"',
            f'token_program_2022_id = "{TOKEN_2022_PROGRAM_ID}"',
            f'memo_program_id = "{MEMO_PROGRAM_ID}"',
            f'pool_state = "{first_pool.address}"',
            f'amm_config = "{first_pool.amm_config}"',
            f'observation_state = "{first_pool.observation_id}"',
        ]
    )
    if first_pool.ex_bitmap_account:
        lines.append(f'ex_bitmap_account = "{first_pool.ex_bitmap_account}"')
    lines.extend(
        [
            f'token_mint_0 = "{first_pool.token_mint_a}"',
            f'token_vault_0 = "{first_pool.token_vault_a}"',
            f'token_mint_1 = "{first_pool.token_mint_b}"',
            f'token_vault_1 = "{first_pool.token_vault_b}"',
            f"tick_spacing = {first_pool.tick_spacing}",
            f'zero_for_one = {"true" if first_zero_for_one else "false"}',
            "",
            "[[routes.definitions.legs]]",
            'venue = "raydium_clmm"',
            f'pool_id = "{second_pool.address}"',
            'side = "sell_base"',
            f"fee_bps = {second_pool.fee_rate // 100}",
            "",
            "[routes.definitions.legs.execution]",
            'kind = "raydium_clmm"',
            f'program_id = "{RAYDIUM_PROGRAM_ID}"',
            f'token_program_id = "{second_pool.token_program_id}"',
            f'token_program_2022_id = "{TOKEN_2022_PROGRAM_ID}"',
            f'memo_program_id = "{MEMO_PROGRAM_ID}"',
            f'pool_state = "{second_pool.address}"',
            f'amm_config = "{second_pool.amm_config}"',
            f'observation_state = "{second_pool.observation_id}"',
        ]
    )
    if second_pool.ex_bitmap_account:
        lines.append(f'ex_bitmap_account = "{second_pool.ex_bitmap_account}"')
    lines.extend(
        [
            f'token_mint_0 = "{second_pool.token_mint_a}"',
            f'token_vault_0 = "{second_pool.token_vault_a}"',
            f'token_mint_1 = "{second_pool.token_mint_b}"',
            f'token_vault_1 = "{second_pool.token_vault_b}"',
            f"tick_spacing = {second_pool.tick_spacing}",
            f'zero_for_one = {"true" if second_zero_for_one else "false"}',
            "",
            "[[routes.definitions.account_dependencies]]",
            f'account_key = "{first_pool.address}"',
            f'pool_id = "{first_pool.address}"',
            'decoder_key = "raydium-clmm-v1"',
            "",
            "[[routes.definitions.account_dependencies]]",
            f'account_key = "{second_pool.address}"',
            f'pool_id = "{second_pool.address}"',
            'decoder_key = "raydium-clmm-v1"',
            "",
            "[routes.definitions.execution]",
            'message_mode = "v0_required"',
            "default_compute_unit_limit = 300000",
            "default_compute_unit_price_micro_lamports = 1",
            "default_jito_tip_lamports = 1",
            "max_quote_slot_lag = 32",
            "max_alt_slot_lag = 32",
        ]
    )
    lookup_tables = render_lookup_tables(
        [first_pool.lookup_table_account or "", second_pool.lookup_table_account or ""]
    )
    if lookup_tables:
        lines.append(lookup_tables)
    lines.extend(
        [
            "",
            "[routes.definitions.execution_protection]",
            "enabled = true",
            "tight_max_quote_slot_lag = 24",
            "base_extra_buy_leg_slippage_bps = 25",
            "failure_step_bps = 25",
            "max_extra_buy_leg_slippage_bps = 100",
            "recovery_success_count = 3",
        ]
    )
    return "\n".join(lines)


def build_raydium_clmm_pool(stub: dict, detail: dict) -> RayPool:
    return RayPool(
        address=detail["id"],
        token_mint_a=detail["mintA"]["address"],
        token_mint_b=detail["mintB"]["address"],
        token_symbol_a=detail["mintA"]["symbol"],
        token_symbol_b=detail["mintB"]["symbol"],
        token_decimals_a=int(detail["mintA"]["decimals"]),
        token_decimals_b=int(detail["mintB"]["decimals"]),
        token_program_id=detail["mintA"]["programId"],
        token_vault_a=detail["vault"]["A"],
        token_vault_b=detail["vault"]["B"],
        amm_config=detail["config"]["id"],
        observation_id=detail["observationId"],
        ex_bitmap_account=detail.get("exBitmapAccount"),
        tick_spacing=int(detail["config"]["tickSpacing"]),
        fee_rate=int(detail["config"]["tradeFeeRate"]),
        lookup_table_account=detail.get("lookupTableAccount"),
        tvl_usdc=float(stub["tvl"]),
    )


def build_raydium_simple_pool(stub: dict, detail: dict) -> RaySimplePool:
    return RaySimplePool(
        address=detail["id"],
        token_mint_a=detail["mintA"]["address"],
        token_mint_b=detail["mintB"]["address"],
        token_symbol_a=detail["mintA"]["symbol"],
        token_symbol_b=detail["mintB"]["symbol"],
        token_decimals_a=int(detail["mintA"]["decimals"]),
        token_decimals_b=int(detail["mintB"]["decimals"]),
        token_program_id=detail["mintA"]["programId"],
        amm_coin_vault=detail["vault"]["A"],
        amm_pc_vault=detail["vault"]["B"],
        program_id=detail["programId"],
        amm_authority=detail["authority"],
        amm_open_orders=detail["openOrders"],
        market_program=detail["marketProgramId"],
        market=detail["marketId"],
        market_bids=detail["marketBids"],
        market_asks=detail["marketAsks"],
        market_event_queue=detail["marketEventQueue"],
        market_coin_vault=detail["marketBaseVault"],
        market_pc_vault=detail["marketQuoteVault"],
        market_vault_signer=detail["marketAuthority"],
        lookup_table_account=detail.get("lookupTableAccount"),
        fee_rate=int(math.floor(float(stub["feeRate"]) * 10_000)),
        tvl_usdc=float(stub["tvl"]),
    )


def supports_raydium_simple_route(stub: dict) -> bool:
    pool_types = stub.get("pooltype") or []
    if isinstance(pool_types, str):
        pool_types = [pool_types]
    return "OpenBookMarket" in pool_types and bool(stub.get("marketId"))


def build_manifest(
    template_path: Path,
    output_path: Path,
    min_orca_tvl: int,
    min_ray_tvl: int,
    max_orca_pages: int,
    max_ray_pages: int,
    include_intra_ray: bool,
    include_raydium_simple: bool,
    top_orca_per_pair: int,
    top_ray_clmm_per_pair: int,
    top_ray_simple_per_pair: int,
    include_extra_quotes: bool,
    extra_quote_min_conversion_tvl: int,
    extra_quote_min_pairs: int,
    max_extra_quotes: int,
) -> tuple[int, int]:
    template_text = template_path.read_text(encoding="utf-8")
    prefix, _, _ = template_text.partition("[[routes.definitions]]")
    routes_marker_index = prefix.rfind("[routes]")
    if routes_marker_index != -1:
        prefix = prefix[: routes_marker_index + len("[routes]")]
    prefix = prefix.rstrip()

    orca_pools = fetch_orca_pools(min_orca_tvl, page_size=100, max_pages=max_orca_pages)
    ray_list = fetch_raydium_list(max_pages=max_ray_pages, page_size=100)

    orca_by_pair: dict[tuple[str, str], list[OrcaPool]] = defaultdict(list)
    for pool in orca_pools:
        if pool.address in EXCLUDED_ORCA_POOLS:
            continue
        pair = pair_key(pool.token_mint_a, pool.token_mint_b)
        orca_by_pair[pair].append(pool)
    selected_orca_by_pair = {
        pair: top_n_unique(
            pools,
            key_fn=lambda pool: pool.tvl_usdc,
            id_fn=lambda pool: pool.address,
            limit=max(1, top_orca_per_pair),
        )
        for pair, pools in orca_by_pair.items()
    }

    ray_clmm_stubs_by_pair: dict[tuple[str, str], list[dict]] = defaultdict(list)
    ray_simple_stubs_by_pair: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for item in ray_list:
        if item["id"] in EXCLUDED_RAYDIUM_POOLS:
            continue
        pair = pair_key(item["mintA"]["address"], item["mintB"]["address"])
        tvl = float(item["tvl"])
        if tvl < min_ray_tvl:
            continue
        if item.get("type") == "Concentrated":
            ray_clmm_stubs_by_pair[pair].append(item)
        elif (
            include_raydium_simple
            and item.get("type") == "Standard"
            and supports_raydium_simple_route(item)
        ):
            ray_simple_stubs_by_pair[pair].append(item)

    selected_ray_clmm_by_pair = {
        pair: top_n_unique(
            stubs,
            key_fn=lambda item: float(item["tvl"]),
            id_fn=lambda item: item["id"],
            limit=max(1, top_ray_clmm_per_pair),
        )
        for pair, stubs in ray_clmm_stubs_by_pair.items()
    }
    selected_ray_simple_by_pair = {
        pair: top_n_unique(
            stubs,
            key_fn=lambda item: float(item["tvl"]),
            id_fn=lambda item: item["id"],
            limit=max(1, top_ray_simple_per_pair),
        )
        for pair, stubs in ray_simple_stubs_by_pair.items()
    }

    candidate_pairs = sorted(
        set(selected_orca_by_pair)
        | set(selected_ray_clmm_by_pair)
        | set(selected_ray_simple_by_pair)
    )
    ray_ids = set()
    for pair in candidate_pairs:
        for stub in selected_ray_clmm_by_pair.get(pair, []):
            ray_ids.add(stub["id"])
        for stub in selected_ray_simple_by_pair.get(pair, []):
            ray_ids.add(stub["id"])
        if include_intra_ray:
            for pair, stubs in ray_clmm_stubs_by_pair.items():
                for stub in top_n_unique(
                    stubs,
                    key_fn=lambda item: float(item["tvl"]),
                    id_fn=lambda item: item["id"],
                    limit=max(2, top_ray_clmm_per_pair),
                ):
                    ray_ids.add(stub["id"])
    ray_details = fetch_raydium_keys(sorted(ray_ids))

    def pools_for_pair(
        pair: tuple[str, str],
    ) -> list[OrcaPool | RayPool | RaySimplePool]:
        pools: list[OrcaPool | RayPool | RaySimplePool] = []
        pools.extend(selected_orca_by_pair.get(pair, []))
        for ray_stub in selected_ray_clmm_by_pair.get(pair, []):
            ray_detail = ray_details.get(ray_stub["id"])
            if ray_detail is not None:
                pools.append(build_raydium_clmm_pool(ray_stub, ray_detail))
        for ray_stub in selected_ray_simple_by_pair.get(pair, []):
            ray_detail = ray_details.get(ray_stub["id"])
            if ray_detail is not None:
                pools.append(build_raydium_simple_pool(ray_stub, ray_detail))
        return pools

    quote_priority = [USDC_MINT, SOL_MINT]
    sol_quote_conversion_pool_ids: dict[str, str] = {
        USDC_MINT: SOL_USDC_CONVERSION_POOL,
    }
    extra_quote_descriptions: list[str] = []
    if include_extra_quotes:
        extra_quote_candidates: list[tuple[int, float, str, str, str]] = []
        for pair in candidate_pairs:
            if SOL_MINT not in pair or USDC_MINT in pair:
                continue
            pools = pools_for_pair(pair)
            if len(pools) < 2:
                continue
            quote_mint = pair[0] if pair[1] == SOL_MINT else pair[1]
            best_pool = max(pools, key=pool_tvl)
            pair_count = 0
            for extra_pair in candidate_pairs:
                if SOL_MINT in extra_pair or USDC_MINT in extra_pair or quote_mint not in extra_pair:
                    continue
                if len(pools_for_pair(extra_pair)) >= 2:
                    pair_count += 1
            if pair_count < extra_quote_min_pairs:
                continue
            conversion_tvl = pool_tvl(best_pool)
            if conversion_tvl < extra_quote_min_conversion_tvl:
                continue
            quote_symbol = canonical_symbol(
                quote_mint,
                token_symbol_for_mint(best_pool, quote_mint),
            )
            extra_quote_candidates.append(
                (
                    pair_count,
                    conversion_tvl,
                    quote_mint,
                    quote_symbol,
                    pool_address(best_pool),
                )
            )
        extra_quote_candidates.sort(reverse=True)
        for pair_count, conversion_tvl, quote_mint, quote_symbol, pool_id in extra_quote_candidates[
            : max_extra_quotes
        ]:
            quote_priority.append(quote_mint)
            sol_quote_conversion_pool_ids[quote_mint] = pool_id
            extra_quote_descriptions.append(
                f"{quote_symbol}:{pair_count}pairs/{int(conversion_tvl)}tvl"
            )

    routes_by_id: dict[str, str] = {}
    pair_count = 0
    for pair in candidate_pairs:
        pools = pools_for_pair(pair)
        if len(pools) < 2:
            continue
        try:
            base_mint, quote_mint = choose_base_quote(pair, quote_priority)
        except ValueError:
            continue
        anchor_pool = pools[0]
        base_symbol = canonical_symbol(base_mint, token_symbol_for_mint(anchor_pool, base_mint))
        quote_symbol = canonical_symbol(
            quote_mint,
            token_symbol_for_mint(anchor_pool, quote_mint),
        )
        quote_decimals = token_decimals_for_mint(anchor_pool, quote_mint)
        sol_quote_conversion_pool_id = sol_quote_conversion_pool_ids.get(quote_mint)
        for first_pool, second_pool in permutations(pools, 2):
            rendered = render_route(
                base_symbol=base_symbol,
                quote_symbol=quote_symbol,
                base_mint=base_mint,
                quote_mint=quote_mint,
                quote_decimals=quote_decimals,
                sol_quote_conversion_pool_id=sol_quote_conversion_pool_id,
                first_pool=first_pool,
                second_pool=second_pool,
            )
            routes_by_id.setdefault(route_id_from_rendered(rendered), rendered)
        pair_count += 1

    if include_intra_ray:
        for pair, stubs in sorted(ray_clmm_stubs_by_pair.items()):
            unique_stubs = top_n_unique(
                stubs,
                key_fn=lambda item: float(item["tvl"]),
                id_fn=lambda item: item["id"],
                limit=max(2, top_ray_clmm_per_pair),
            )
            if len(unique_stubs) < 2:
                continue
            pool_a = ray_details.get(unique_stubs[0]["id"])
            pool_b = ray_details.get(unique_stubs[1]["id"])
            if pool_a is None or pool_b is None:
                continue
            ray_a = build_raydium_clmm_pool(unique_stubs[0], pool_a)
            ray_b = build_raydium_clmm_pool(unique_stubs[1], pool_b)
            try:
                base_mint, quote_mint = choose_base_quote(pair, quote_priority)
            except ValueError:
                continue
            base_symbol = canonical_symbol(base_mint, token_symbol_for_mint(ray_a, base_mint))
            quote_symbol = canonical_symbol(
                quote_mint,
                token_symbol_for_mint(ray_a, quote_mint),
            )
            quote_decimals = token_decimals_for_mint(ray_a, quote_mint)
            sol_quote_conversion_pool_id = sol_quote_conversion_pool_ids.get(quote_mint)
            rendered = render_raydium_only_route(
                base_symbol=base_symbol,
                quote_symbol=quote_symbol,
                base_mint=base_mint,
                quote_mint=quote_mint,
                quote_decimals=quote_decimals,
                sol_quote_conversion_pool_id=sol_quote_conversion_pool_id,
                first_pool=ray_a,
                second_pool=ray_b,
            )
            routes_by_id.setdefault(route_id_from_rendered(rendered), rendered)
            rendered = render_raydium_only_route(
                base_symbol=base_symbol,
                quote_symbol=quote_symbol,
                base_mint=base_mint,
                quote_mint=quote_mint,
                quote_decimals=quote_decimals,
                sol_quote_conversion_pool_id=sol_quote_conversion_pool_id,
                first_pool=ray_b,
                second_pool=ray_a,
            )
            routes_by_id.setdefault(route_id_from_rendered(rendered), rendered)

    header = (
        prefix
        + "\n\n"
        + "# Auto-generated from official Orca and Raydium APIs on "
        + date.today().isoformat()
        + ".\n"
        + f"# Supported pairs with at least two pools: {pair_count}. Routes: {len(routes_by_id)}.\n"
        + f"# Selection: Orca/Raydium tvl >= {min_orca_tvl}/{min_ray_tvl}, quote priority = {', '.join(canonical_symbol(mint, mint) for mint in quote_priority[:2])}"
        + (
            f" + extra quotes [{', '.join(extra_quote_descriptions)}]"
            if extra_quote_descriptions
            else ""
        )
        + ".\n"
    )
    output_path.write_text(header + "".join(routes_by_id.values()) + "\n", encoding="utf-8")
    return pair_count, len(routes_by_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--template",
        default="amm_12_pairs_fast.toml",
        help="template manifest to reuse for the shared top-level config",
    )
    parser.add_argument(
        "--output",
        default="amm_cross_venue_generated_fast.toml",
        help="output manifest path",
    )
    parser.add_argument("--min-orca-tvl", type=int, default=50_000)
    parser.add_argument("--min-ray-tvl", type=int, default=50_000)
    parser.add_argument("--max-orca-pages", type=int, default=10)
    parser.add_argument("--max-ray-pages", type=int, default=10)
    parser.add_argument(
        "--include-intra-ray",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="also generate top-2 Raydium CLMM fee-tier routes for pairs with multiple Ray pools",
    )
    parser.add_argument(
        "--include-raydium-simple",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="include Raydium hybrid AMM/OpenBook pools in generated routes",
    )
    parser.add_argument("--top-orca-per-pair", type=int, default=1)
    parser.add_argument("--top-ray-clmm-per-pair", type=int, default=1)
    parser.add_argument("--top-ray-simple-per-pair", type=int, default=1)
    parser.add_argument(
        "--include-extra-quotes",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="also generate routes quoted in non-SOL/non-USDC assets that have a tracked SOL/<quote> conversion pool",
    )
    parser.add_argument("--extra-quote-min-conversion-tvl", type=int, default=1_000_000)
    parser.add_argument("--extra-quote-min-pairs", type=int, default=3)
    parser.add_argument("--max-extra-quotes", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pair_count, route_count = build_manifest(
        template_path=Path(args.template),
        output_path=Path(args.output),
        min_orca_tvl=args.min_orca_tvl,
        min_ray_tvl=args.min_ray_tvl,
        max_orca_pages=args.max_orca_pages,
        max_ray_pages=args.max_ray_pages,
        include_intra_ray=args.include_intra_ray,
        include_raydium_simple=args.include_raydium_simple,
        top_orca_per_pair=args.top_orca_per_pair,
        top_ray_clmm_per_pair=args.top_ray_clmm_per_pair,
        top_ray_simple_per_pair=args.top_ray_simple_per_pair,
        include_extra_quotes=args.include_extra_quotes,
        extra_quote_min_conversion_tvl=args.extra_quote_min_conversion_tvl,
        extra_quote_min_pairs=args.extra_quote_min_pairs,
        max_extra_quotes=args.max_extra_quotes,
    )
    print(f"generated {route_count} routes across {pair_count} pairs -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
