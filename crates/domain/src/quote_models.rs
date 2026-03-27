use solana_sdk::pubkey::Pubkey;

use crate::types::PoolId;

pub const ORCA_WHIRLPOOL_TICK_ARRAY_SIZE: i32 = 88;
pub const RAYDIUM_CLMM_TICK_ARRAY_SIZE: i32 = 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializedTick {
    pub tick_index: i32,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickArrayWindow {
    pub start_tick_index: i32,
    pub end_tick_index: i32,
    pub initialized_tick_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectionalConcentratedQuoteModel {
    pub loaded_tick_arrays: usize,
    pub expected_tick_arrays: usize,
    pub complete: bool,
    pub windows: Vec<TickArrayWindow>,
    pub initialized_ticks: Vec<InitializedTick>,
}

impl DirectionalConcentratedQuoteModel {
    pub fn is_executable(&self) -> bool {
        self.loaded_tick_arrays > 0 && !self.windows.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcentratedQuoteModel {
    pub pool_id: PoolId,
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub current_tick_index: i32,
    pub tick_spacing: u16,
    pub required_a_to_b: bool,
    pub required_b_to_a: bool,
    pub a_to_b: Option<DirectionalConcentratedQuoteModel>,
    pub b_to_a: Option<DirectionalConcentratedQuoteModel>,
    pub last_update_slot: u64,
    pub write_version: u64,
}

impl ConcentratedQuoteModel {
    pub fn has_required_directions(&self) -> bool {
        (!self.required_a_to_b
            || self
                .a_to_b
                .as_ref()
                .map(DirectionalConcentratedQuoteModel::is_executable)
                .unwrap_or(false))
            && (!self.required_b_to_a
                || self
                    .b_to_a
                    .as_ref()
                    .map(DirectionalConcentratedQuoteModel::is_executable)
                    .unwrap_or(false))
    }

    pub fn direction(&self, a_to_b: bool) -> Option<&DirectionalConcentratedQuoteModel> {
        if a_to_b {
            self.a_to_b.as_ref()
        } else {
            self.b_to_a.as_ref()
        }
    }
}

pub fn derive_orca_tick_arrays(
    program_id: Pubkey,
    whirlpool: Pubkey,
    tick_spacing: u16,
    current_tick_index: i32,
    a_to_b: bool,
) -> [Pubkey; 3] {
    let offsets = if a_to_b { [0, -1, -2] } else { [0, 1, 2] };
    offsets.map(|offset| {
        let start_tick_index = tick_array_start_index(
            current_tick_index,
            tick_spacing,
            ORCA_WHIRLPOOL_TICK_ARRAY_SIZE,
            offset,
        );
        Pubkey::find_program_address(
            &[
                b"tick_array",
                whirlpool.as_ref(),
                start_tick_index.to_string().as_bytes(),
            ],
            &program_id,
        )
        .0
    })
}

pub fn derive_raydium_tick_arrays(
    program_id: Pubkey,
    pool_state: Pubkey,
    tick_spacing: u16,
    current_tick_index: i32,
    zero_for_one: bool,
) -> [Pubkey; 3] {
    let offsets = if zero_for_one { [0, -1, -2] } else { [0, 1, 2] };
    offsets.map(|offset| {
        let start_tick_index = tick_array_start_index(
            current_tick_index,
            tick_spacing,
            RAYDIUM_CLMM_TICK_ARRAY_SIZE,
            offset,
        );
        Pubkey::find_program_address(
            &[
                b"tick_array",
                pool_state.as_ref(),
                &start_tick_index.to_be_bytes(),
            ],
            &program_id,
        )
        .0
    })
}

pub fn tick_array_start_index(
    current_tick_index: i32,
    tick_spacing: u16,
    tick_array_size: i32,
    offset: i32,
) -> i32 {
    let ticks_in_array = i32::from(tick_spacing) * tick_array_size;
    let real_index = current_tick_index.div_euclid(ticks_in_array);
    (real_index + offset) * ticks_in_array
}

pub fn tick_array_end_index(start_tick_index: i32, tick_spacing: u16, tick_array_size: i32) -> i32 {
    start_tick_index
        .saturating_add(i32::from(tick_spacing).saturating_mul(tick_array_size.saturating_sub(1)))
}
