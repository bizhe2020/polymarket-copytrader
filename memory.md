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
- `polymarket_copytrader/pair_unit_strategy.py`              ← runtime state machine runner
- `polymarket_copytrader/follower.py` ← pure copytrader (pair-unit removed in clean-up)
- `polymarket_copytrader/pair_live_paper.py` ← **Plan B 主入口**（独立 market scanner + skeleton）
- `polymarket_copytrader/pair_unit_strategy.py` ← family-specific decision engine（scanner-facing API）
- `polymarket_copytrader/models.py` ← StateSnapshot（pair-unit 字段已移除）
- `polymarket_copytrader/cli.py`

## Architecture — Post Clean-Up

### 执行路径（两条，不混淆）

| 路径 | 入口 | 触发 | 角色 |
|------|------|------|------|
| Legacy copytrader | `follower.py` | 目标钱包交易 | 纯跟单，`CopyTraderApp` |
| **Plan B（主）** | `pair_live_paper.py` | 市场开盘独立触发 | skeleton 驱动 |

### 已确认矛盾已解决

- `follower.py` 已移除所有 pair-unit 接入：`_pair_unit_strategy`、`_pair_unit_enabled`、`_evaluate_pending_pair_units`、`_pair_unit_decision_to_follow_decision`、`_resolve_asset_id_for_market`
- `models.py` 已移除 `StateSnapshot.asset_pair_units_open`
- `store.py` 已移除 `asset_pair_units_open` 序列化
- `CopyTraderApp` 现在是纯 copytrader，不再混入 Plan B

### Plan B 架构（下一步接入）

```
strategy_config_skeleton_v0.json
    → skeleton_assembler.py  (load_skeleton)
    → pair_live_paper.py  (独立 scanner + runtime orchestration)
    → pair_unit_strategy.py  (family-specific decision engine, scanner-facing API)
```

### 已实现（Skeleton 层）

- `entry_ruleset_v1.py` — family-specific entry ruleset（含 `min_seconds_to_resolution=300s`）
- `strategy_blueprint_v0.py` / `btc/eth_sol strategy` — skeleton 配置生成
- `strategy_config_skeleton_v0.py` — 配置汇总
- `config/skeleton_assembler.py` — 验证 + 标准化
- `pair_unit_strategy.py` — `evaluate_first_leg_entry`、`evaluate_second_leg_completion`、`on_market_open`
- `test_pair_unit_entry_resolution_gate.py` — 4 个测试全绿

### 待接入（Scanner 层）

- `pair_live_paper.py` 还未接入 `strategy_config_skeleton_v0`（当前用通用 scanner 参数）
- `detect_regime()` 仍是 elapsed-time 占位，需要真实外部市场信号

## Notes For Continuation

- Do not pull the mainline back into broad explanation studies unless explicitly requested.
- Keep `xrp` deferred in the first runnable version.
- Update docs using this rule:
  - tracker doc: progress only
  - results doc: solid conclusions only
- Next recommended mainline actions (in order):
  1. **接入 skeleton**：让 `pair_live_paper.py` 直接消费 `strategy_config_skeleton_v0`（替换通用 scanner 参数）
  2. **改造 `pair_unit_strategy.py`**：去掉"目标交易驱动"语义，明确为 scanner-facing API
  3. **CLI 更新**：主入口改为 `pair-live-paper`，不再推荐 `CopyTraderApp` 作为 Plan B 入口
  4. **Live paper Plan B 验证**：跑 `pair-live-paper skeleton-driven` 验证 BTC first-leg entry
  5. **Regime hardening**：`detect_regime()` 替换为真实外部市场信号

