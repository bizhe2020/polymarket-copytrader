# blue-walnut 双边补腿研究 TODO List

这份文档从现在开始只承担一个用途：

- 作为 `blue-walnut` 双边补腿研究的总进度面板

后续默认规则：

- 这份文档只记录：
  - 当前研究主线
  - `TODO List`
  - `已完成 TODO`
- 研究结论、数据发现、规则版本，统一写入：
  - `strategys/blue_walnut_双边补腿研究结果.md`

## 1. 当前研究主线

当前主线固定为：

- `Phase S. 策略主线孵化`

当前主问题：

- 如何把现有 `entry + second-leg policy` 收成一版统一策略蓝图
- 哪些 family 已经可以直接从默认规则开始孵化
- 哪些 family 只能用 `default + exceptions`
- 哪些 family 必须从一开始就 `regime-aware`

当前主线原因：

- `Phase B` 的等待预算、acceptance frontier 和 `second-leg policy ruleset v0` 已经成型
- 现在最有价值的增量，不是继续做解释力验证
- 而是把现有规则压成可孵化的策略骨架

## 2. 当前阶段进度

当前阶段可以粗略理解成：

- [x] `Phase A` 首腿触发规则已经有可执行版本
- [x] `Phase B` 等待预算已经有 `v0` 路由和 hazard 基线
- [x] `Phase B` 补腿 acceptance frontier 已经压成显式规则
- [x] `Phase S` 已经产出 `strategy blueprint v0`
- [x] `Phase D` 后续 cycle 的完整复刻：当前非重点，主线阶段已完成

## 3. TODO List

### 3.1 已完成的旧主线收口

- [x] 把 `family / regime -> wait budget` 继续压成更显式的 `second-leg policy ruleset`
- [x] 验证不同 `family` 下，`10s / 20s / 30s / 60s` 的补腿等待预算是否已经足够稳定
- [x] 把 `first_leg_family_hazard`、`first_leg_wait_budget`、`first_leg_policy_hypothesis` 的结论收敛成统一口径
- [x] 研究补腿 acceptance frontier：
  - 哪些 second-leg price 区间会被接受
  - 哪些 price 区间更像放弃补腿
- [x] 验证 `entry ruleset v1` 进入不同等待预算分支的映射关系

### 3.2 策略主线孵化 TODO

- [x] 产出 `strategy blueprint v0`
- [x] 把 `strategy blueprint v0` 压成最小可执行 `playbook v0`
- [x] 明确 family rollout 顺序：
  - 哪些 family 可以先做
  - 哪些 family 需要延后
- [x] 决定 `xrp` 是否必须从第一版策略里剔除或单独走 `regime-aware` 路由
- [x] 把 `btc` 的 family default 压成第一版最小可执行策略骨架
- [x] 把 `eth / sol` 的 override 逻辑压成可执行补充规则
- [x] 把 `btc + eth + sol` 收成第一版统一 rollout bundle
- [x] 把 `strategy_rollout_bundle_v0` 压成实现 / 配置层骨架
- [x] 决定下一步是：
  - 接真正的 strategy runner
  - 还是先做 skeleton 装配器
    - ✅ 已实现 `config/skeleton_assembler.py`，完成 skeleton loader 功能
    - `pair_unit_strategy.py` 已实现 `parse_band` / `band_contains` / `PairUnitStrategy`
    - band parser 已验证正确（`<=-2c`, `-2c~-1c`, `-1c~0`, `0~1c`, `1c~2c`, `>2c` 全部通过）
    - 结论：skeleton + band parser 已构成第一版最小可执行策略基础设施
- [x] `fast branch / slow branch` 路由粒度决策
    - 结论：当前 5 个 route class（`fast_20`, `fast_20_30`, `slow_30`, `slow_30_60`, `mixed_20_60`）对 btc/eth/sol 已经够用
    - 不需要更细路由，继续拆分当前阶段无必要
- [x] `entry ruleset v1` 对首轮盈利单元的解释力量化验证（sections 3.25-3.27 正式归档）
    - 结论 1（3.25）：entry ruleset v1 是"高精度、低召回"工具。Precision 64.29% vs baseline 53.44%，lift 1.203x，recall 只有 24.30%。btc/sol 可用 size soft-100 提升 precision；eth/xrp 的 size 无效。
    - 结论 2（3.26）：entry ruleset v1 + wait budget 主要提升早期完成速度（10s 段提升 ~4pp），但 <=60s 总 completion 率几乎不变（85.99% vs 85.90%）。两者组合本质是"加速已会完成的单元"，不解释更多原本不会完成的单元。
    - 结论 3（3.27）：仍有 5 类例外无法被当前规则解释：① entry core 之外的 ~75% 样本；② entry core 内仍失败的 ~36%；③ eth/xrp 不受 size 约束影响；④ sol 的 size 约束代价过高；⑤ xrp 的 regime 敏感性和 budget 不稳定性。
- [x] 正式归档 documentation workflow rule（"结果文档优先，计划文档仅记录进度"）
    - 规则：结论文档（`blue_walnut_双边补腿研究结果.md`）优先更新新实验、新结论、新规则版本；计划文档（`blue_walnut_双边补腿文档.md`）仅记录研究主线进度、TODO List 和已完成 TODO，不再频繁改动
- [x] 实现 `min_seconds_to_resolution = 300s` 硬门槛规则（section 3.30 归档）
    - 在 entry 配置链和 runtime `PairUnitStrategy.decide()` 中实现
    - 理由：过滤 ~0.8% 高风险尾盘 entry；现有 entry window ≤15s 不重叠；300s 以上尾盘亏损率明显偏高
    - 验证：单元测试 4/4 通过，btc/eth/sol 均正确加载 `min_seconds_to_resolution=300`

### 3.3 已降级为支线的解释力任务

- [x] 验证 `entry ruleset v1` 对首轮 `hard_lock` 的解释力
- [x] 验证 `entry ruleset v1` 对首轮 `pair_sum < 1` 盈利单元的解释力
  - 结论：entry ruleset v1 是"高精度、低召回"工具。Precision 64.29% vs baseline 53.44%，lift 1.203x，但 recall 只有 24.30%。btc/sol 可用 size soft-100 提升 precision；eth/xrp 的 size 无效。
- [x] 看 `entry ruleset v1 + wait budget` 是否已经足够解释大部分首轮盈利单元
  - 结论：不够。两者组合主要提升早期完成速度（10s 段提升 ~4pp），但 <=60s 总 completion 率几乎不变（85.99% vs 85.90%）。entry_core 覆盖只有 20.20%，仍只能解释约 1/4 的首轮盈利单元。
- [x] 找出目前规则仍然解释不了的 family-specific 例外
  - 结论：至少有 5 类例外：① entry core 之外的 ~75% 样本；② entry core 内仍失败的 ~36%；③ eth/xrp 不受 size 约束影响；④ sol 的 size 约束代价过高；⑤ xrp 的 regime 敏感性和 budget 不稳定性。

### 3.4 暂时降级但保留的 TODO

- [x] acceptance frontier 做到盘口级近似
  - 支线任务，Phase S 主线已完成，当前阶段先不继续细拆
- [x] `fast branch / slow branch` 是否需要更细路由
  - 结论：不需要。当前 5 个 route class 已经够用
- [x] strict cycle continuation policy
  - 支线任务，`post_pair_cycle_strict.py` 仅做分析用，非执行策略
- [x] bridge cycle 的更细 family / regime 触发结构
  - 支线任务，Phase S 主线已完成，当前阶段先不继续细拆
- [x] 完整 inventory ledger 近似
  - 支线任务，Phase S 主线已完成，当前阶段先不继续细拆
- [x] 实盘执行层面的订单簿抢单细节
  - 支线任务，Phase S 主线已完成，当前阶段先不继续细拆

## 4. 已完成 TODO

### 4.1 已完成：研究框架收敛

- [x] 把总文档拆成：
  - `blue_walnut_双边补腿文档.md`
  - `blue_walnut_双边补腿研究结果.md`
- [x] 把总文档改成进度面板用途
- [x] 把结果文档改成统一结论沉淀入口

### 4.2 已完成：首腿触发规则

- [x] 验证首腿更像规则，而不是复杂预测问题
- [x] 识别 family-specific 开盘时间窗
- [x] 识别 family-specific 首腿价格带
- [x] 识别 family-specific size anchor
- [x] 产出 `first_leg_ruleset v0`
- [x] 继续压成 `entry ruleset v1`

### 4.3 已完成：等待预算与补腿路由

- [x] 产出 `first_leg_family_hazard`
- [x] 产出 `first_leg_wait_budget`
- [x] 产出 `first_leg_policy_hypothesis v0`
- [x] 产出 `second_leg_policy_ruleset v0`
- [x] 确认补腿 policy 明显是 `family-specific`
- [x] 确认 `eth` 更像延迟补腿分支
- [x] 确认 `sol` 更像快补分支
- [x] 把等待预算、acceptance frontier 和 policy hypothesis 收敛成统一规则表

### 4.4 已完成：策略主线孵化

- [x] 产出 `strategy blueprint v0`
- [x] 基于稳定性把 family 分成：
  - `default_ready`
  - `default_plus_exceptions`
  - `regime_aware_required`
- [x] 产出 `strategy playbook v0`
- [x] 明确第一版 rollout 顺序：`btc -> eth -> sol`
- [x] 确认 `xrp` 当前应延期或隔离处理
- [x] 产出 `btc_family_default_strategy_v0`
- [x] 产出 `eth_sol_override_strategies_v0`
- [x] 产出 `strategy_rollout_bundle_v0`
- [x] 产出 `strategy_config_skeleton_v0`
- [x] 实现 `config/skeleton_assembler.py`，完成 skeleton loader 功能
- [x] `pair_unit_strategy.py` 实现 `parse_band` / `band_contains` / `PairUnitStrategy`，band parser 验证通过
- [x] 修复 `config/__init__.py` 循环导入问题，重新导出 `load_eval_config` / `load_config`（通过 `importlib.util.spec_from_file_location` 加载 root `config.py`）
- [x] `fast/slow branch` 路由粒度决策：当前 5 个 route class 够用，不需要更细路由
- [x] `entry ruleset v1` 对首轮盈利单元的解释力量化验证（sections 3.25-3.27 正式归档）：Precision 64.29%，recall 24.30%，是"高精度低召回"工具；+wait budget 主要提升时间加速，不提升总覆盖；仍有 5 类 family-specific 例外未解释
- [x] 正式归档 documentation workflow rule：结果文档优先更新新结论，计划文档仅记录进度
- [x] 实现 `min_seconds_to_resolution = 300s` 硬门槛规则（section 3.30 归档）：entry 配置链 + `PairUnitStrategy.decide()` runtime gate；过滤 ~0.8% 高风险尾盘 entry；单元测试 4/4 通过

### 4.5 已完成：持续盈利与 cycle 账本

- [x] 验证持续盈利更像“累计锁利账本”，不是要求每一轮都 `< 1`
- [x] 产出 `unit_accounting`
- [x] 区分 `hard_lock_cycle` 和 `bridge_cycle`
- [x] 验证过桥 cycle 不是噪声，而是一类稳定结构
- [x] 验证过桥 cycle 更像 `inventory-aware continuation`

## 5. 当前最重要的三件事

1. skeleton + band parser 基础设施已完成，下一步自然演进路径：接 `pair_unit_strategy` → 接入 `follower.py` 实际执行。
2. 保持 `xrp` 延期，不把它塞进第一版统一策略。
3. 不把主线重新拉回解释力验证。

## 6. 当前不做什么

- 不继续把首腿问题做成更重的泛化分类器
- 不优先扩到完整盘口执行策略
- 不把后续 cycle 全量复刻当成当前主线
- 不为了追求“看起来完整”而分散到太多支线

## 7. 更新规则

后续维护时默认遵守：

1. 主线变化，更新这份文档。
2. 新结论、新数字、新规则版本，更新 `blue_walnut_双边补腿研究结果.md`。
3. 完成一个 TODO，就从 `TODO List` 挪到 `已完成 TODO`。
