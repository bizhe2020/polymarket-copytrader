# blue-walnut Research Memory

## Workspace

- Repo root: `/Users/laoji/Documents/polymarket-copytrader`
- Current environment when this memory was written:
  - editor/session: VSCode plugin
  - date: `2026-03-28`
  - timezone: `Asia/Shanghai`

## User Intent

- Primary goal: reverse engineer and incubate a replicable `blue-walnut` pair-unit strategy on Polymarket.
- Current preference:
  - keep the research focused on the mainline strategy incubation path
  - treat explanation / attribution / recall-style tasks as side tasks
  - use the progress doc only as a tracker
  - use the results doc only for solid conclusions

## Documentation Workflow

- Progress tracker only:
  - `strategys/blue_walnut_双边补腿文档.md`
- Solid findings / conclusions only:
  - `strategys/blue_walnut_双边补腿研究结果.md`
- Working rule requested by user:
  - each time, only mark TODO progress in the tracker
  - only write durable conclusions into the results document

## Current Mainline

- Current mainline phase: `Phase S. 策略主线孵化`
- Explanation-style tasks were intentionally downgraded to side tasks.
- Current strategy direction:
  - active families: `btc`, `eth`, `sol`
  - deferred family: `xrp`

## High-Confidence Research Conclusions

### 1. Core structure

- `blue-walnut` is not mainly directional betting on final market resolution.
- The strategy is centered on constructing redeemable pair units.
- First-leg entry looks more like a rule than a generic prediction problem.
- Second-leg completion is family-specific and regime-sensitive.

### 2. Entry structure

- Entry is best approximated by:
  - `family-specific open window + price band`
- `size` is not a strong hard gate right now.
- `size` is better treated as:
  - a wide sanity check
  - or a soft ranking feature

### 3. Second-leg structure

- Second-leg acceptance is centered around a parity anchor.
- The most important acceptance band is near `[-2c, +2c]`.
- The sweet spot that is both fast and most consistent with hard lock is `[-2c, -1c]`.

### 4. Family-specific wait budgets

- `btc`: default `20s / 30s`, relatively stable
- `eth`: default `30s / 60s`, slower family
- `sol`: default `20s / 30s`, but needs overrides
- `xrp`: too regime-sensitive for v0 rollout, deferred

### 5. Bridge-cycle conclusion

- Not every cycle needs to be individually `< 1`.
- A meaningful fraction of non-positive local cycles are bridge / inventory-aware continuation cycles.
- Persistent profitability looks more like maintaining a positive cumulative redeemable-unit ledger than requiring every local pair to be hard-lock positive.

## Current Mainline Artifacts

### Entry

- `var/research_blue_walnut/entry_ruleset_v1.json`
- `var/research_blue_walnut/entry_ruleset_v1.csv`
- Core rule summary:
  - `btc`: `<=12s`, price `0.48-0.50`
  - `eth`: `<=15s`, price `0.473-0.50`
  - `sol`: `<=12s`, price `0.49-0.50`
  - `xrp`: `<=15s`, price `0.47-0.49`

### Second-leg policy

- `var/research_blue_walnut/second_leg_policy_ruleset_v0.json`
- `var/research_blue_walnut/second_leg_policy_ruleset_v0.csv`
- Core rule summary:
  - `btc`: `fast_20_30`
  - `eth`: `slow_30_60`
  - `sol`: `fast_20_30`
  - `xrp`: `fast_20_30` but highly regime-sensitive

### Strategy incubation bundle

- `var/research_blue_walnut/strategy_blueprint_v0.json`
- `var/research_blue_walnut/strategy_playbook_v0.json`
- `var/research_blue_walnut/btc_family_default_strategy_v0.json`
- `var/research_blue_walnut/eth_sol_override_strategies_v0.json`
- `var/research_blue_walnut/strategy_rollout_bundle_v0.json`
- `var/research_blue_walnut/strategy_config_skeleton_v0.json`

## Current Strategy Package Shape

### Rollout order

- phase 1: `btc`
- phase 2: `eth`, `sol`
- deferred: `xrp`

### BTC family default skeleton

- state machine:
  - `observe_open`
  - `enter_first_leg`
  - `monitor_second_leg`
  - `complete_or_timeout`
- defaults:
  - open observe window: `12s`
  - entry band: `0.48-0.50`
  - second-leg route: `fast_20_30`
  - wait budget: `20s / 30s`
- notable overrides:
  - `strong_aligned -> fast_20 -> 20/20`
  - `strong_opp -> slow_30 -> 30/30`

### ETH override skeleton

- default route: `slow_30_60`
- default wait budget: `30/60`
- overrides:
  - `strong_aligned -> fast_20_30 -> 20/30`
  - `strong_opp -> slow_30 -> 30/30`

### SOL override skeleton

- default route: `fast_20_30`
- default wait budget: `20/30`
- overrides:
  - `aligned -> mixed_20_60 -> 20/60`
  - `opp -> slow_30_60 -> 30/60`

## Key Code Files

- `polymarket_copytrader/entry_ruleset_v1.py`
- `polymarket_copytrader/second_leg_policy_ruleset.py`
- `polymarket_copytrader/strategy_blueprint_v0.py`
- `polymarket_copytrader/strategy_playbook_v0.py`
- `polymarket_copytrader/btc_family_default_strategy_v0.py`
- `polymarket_copytrader/eth_sol_override_strategies_v0.py`
- `polymarket_copytrader/strategy_rollout_bundle_v0.py`
- `polymarket_copytrader/strategy_config_skeleton_v0.py`
- `polymarket_copytrader/config/skeleton_assembler.py`       ← skeleton loader/validator
- `polymarket_copytrader/pair_unit_strategy.py`              ← scanner-facing family rule engine
- `polymarket_copytrader/pair_live_paper.py`                 ← Plan B runtime / independent first-leg scanner
- `polymarket_copytrader/follower.py`                         ← legacy copytrader path
- `polymarket_copytrader/cli.py`

## Most Recent Mainline State

### Phase S completed: skeleton + scanner-facing runtime split

- `strategy_config_skeleton_v0.json` — per-family entry/second-leg/override config
- `skeleton_assembler.py` — validates and normalizes skeleton JSON → bundle dict
- `pair_unit_strategy.py` — scanner-facing family rule engine:
  - `evaluate_market_candidate`
  - `open_market_candidate`
  - `evaluate_second_leg_completion`
  - `tick`
  - `on_market_open`
- `pair_live_paper.py` — Plan B runtime that loads `strategy_config_skeleton_v0` directly
- `follower.py` — reverted to legacy copytrader responsibility; no pair-unit mainline integration

### Plan B mainline

1. **Runtime entrypoint**
   - Main recommended runtime is `pair-live-paper`
   - `cli.py` should treat `pair-live-paper` as the Plan B entrypoint

2. **First-leg trigger**
   - Independent market scanner
   - Family-specific open window + price band from skeleton
   - `min_seconds_to_resolution = 300s` remains a hard family gate
   - Optional scanner-level resolution window (for opening-hourly experiments) is additive on top of family rules

3. **Legacy path**
   - `CopyTraderApp` / `follower.py` stays available for mirror-copy behavior
   - It is no longer the strategy incubation mainline

## Notes For Continuation

- Do not pull the mainline back into broad explanation studies unless explicitly requested.
- Keep `xrp` deferred in the first runnable version.
- Update docs using this rule:
  - tracker doc: progress only
  - results doc: solid conclusions only
- Next recommended mainline actions (in order):
  1. **Plan B live paper run**: run `pair-live-paper` in paper mode and verify BTC hourly first-leg entries trigger from scanner conditions, not target trades
  2. **Second-leg alignment**: finish replacing `pair_live_paper.py` pending-entry completion heuristics with the same acceptance-band logic used by `pair_unit_strategy.py`
  3. **Integration tests**: add Plan B integration tests for skeleton-driven first-leg gating and scanner-only trigger mode
  4. **Regime detection hardening**: `detect_regime()` in `pair_unit_strategy.py` currently uses a simple elapsed-time heuristic — replace with real external market signal when available
