# blue-walnut 双边补腿研究结果

这份文档从现在开始只承担一个用途:

- 作为 `blue-walnut` 双边补腿策略的研究结果沉淀文档

后续默认规则:

- 新实验、新结论、新规则,优先更新本文
- `strategys/blue_walnut_双边补腿文档.md` 作为研究计划,默认不再频繁改动

## 1. 当前状态

当前研究已经从"大而全地解释它所有行为"收敛到两条主线:

1. `首腿触发规则`
2. `首腿后的补腿等待 policy`

当前判断:

- `blue-walnut` 更像是在开盘早期构建 `pair` 盈利单元
- 而不是在系统性押最终 `true/false`

## 2. 数据基线

当前主样本来自:

- `var/eval_blue_walnut_10k/cache/trades/blue-walnut.fullsnapshot.jsonl`

当前稳定基线:

- `BUY` rows: `595,472`
- paired hourly markets: `1,802`
- family counts:
  - `btc: 452`
  - `eth: 450`
  - `sol: 448`
  - `xrp: 452`

对应摘要产物:

- `var/research_blue_walnut/pair_sequence_full_600k.summary.json`
- `var/research_blue_walnut/first_leg_followup_full.summary.json`

## 3. 已确认结果

### 3.1 首轮 pair 是稳定主结构

基于:

- `var/research_blue_walnut/pair_sequence_full_600k.summary.json`

当前确认:

- `1548 / 1802` 个 market 在 `60s` 内完成首次配对,占 `85.90%`
- `567 / 1802` 个 market 在 `5s` 内完成首次配对,占 `31.47%`
- `983 / 1802` 个 market 的 `first_cross_pair_sum < 1`,占 `54.55%`
- 最终均价 `< 1` 的 market 只有 `737 / 1802`,占 `40.90%`

当前解释:

- 最有价值的观察点不是最终均价
- 而是首轮 `first_leg -> second_leg -> first_cross_pair_sum`

### 3.2 首腿更像规则,不像复杂预测问题

基于:

- `var/research_blue_walnut/first_leg_trigger_rule_v0.json`

当前确认:

- 四个 family 的首腿开仓时间中位数都是 `11s`
- 首腿高度集中在开盘后前 `12-20s`
- 首腿价格集中在中间带附近,而不是大范围漂移

当前首腿价格分布摘要:

- `btc`: `p10/p25/p50/p75/p90 = 0.46 / 0.47 / 0.49 / 0.52 / 0.54`
- `eth`: `0.46 / 0.47 / 0.49 / 0.52 / 0.54`
- `sol`: `0.46 / 0.47 / 0.49 / 0.51 / 0.53`
- `xrp`: `0.46 / 0.48 / 0.50 / 0.52 / 0.54`

当前解释:

- 首腿不是盘中任意时刻随机出现
- 更像在开盘早期,围绕中间价格带去占一个可后续配平的价格锚点

### 3.3 `first_leg_ruleset v0` 已经成型

基于:

- `var/research_blue_walnut/first_leg_ruleset_v0.json`
- `var/research_blue_walnut/first_leg_ruleset_v0.csv`

当前 `v0` 规则表:

- `btc`
  - open window: `<= 12s`
  - price band: `0.48 - 0.50`
  - size anchor: `21.345 USDC`
  - core size band: `14.47 - 23.4`
  - coverage: `29.42%`
  - `pair < 1` inside band: `63.16%`

- `eth`
  - open window: `<= 15s`
  - price band: `0.473 - 0.50`
  - size anchor: `9.4 USDC`
  - core size band: `7.858 - 10.2`
  - coverage: `30.22%`
  - `pair < 1` inside band: `61.76%`

- `sol`
  - open window: `<= 12s`
  - price band: `0.49 - 0.50`
  - size anchor: `4.9 USDC`
  - core size band: `4.7 - 5.1`
  - coverage: `25.22%`
  - `pair < 1` inside band: `63.72%`

- `xrp`
  - open window: `<= 15s`
  - price band: `0.47 - 0.49`
  - size anchor: `4.9 USDC`
  - core size band: `4.778 - 5.1`
  - coverage: `29.65%`
  - `pair < 1` inside band: `61.19%`

当前解释:

- `btc/sol` 更像 `<=12s` 触发的快开分支
- `eth/xrp` 更像允许 `<=15s` 的稍宽开仓窗
- 所有 family 的首腿价格带都紧贴 `0.47-0.50`

当前边界:

- 这还是 `v0`
- 它解释的是"什么样的首腿最像会被开出来"
- 还不是完整解释"为什么这一秒一定要开"

### 3.4 补腿 policy 明显是 family-specific

基于:

- `var/research_blue_walnut/first_leg_completion_baseline_okx.json`
- `var/research_blue_walnut/first_leg_family_hazard_okx.json`

当前确认:

- pooled completion baseline 不强
- 拆到 family 后,解释力明显上升

其中 `10s` 完成预测里,family-specific `roc_auc` 代表值:

- `eth: 0.7569`
- `sol: 0.6260`
- `btc: 0.5242`

当前解释:

- `market_family` 不是轻微修正项
- 而是补腿 policy 的第一层路由

### 3.5 family-specific 等待预算已经有 `v0`

基于:

- `var/research_blue_walnut/first_leg_wait_budget_okx.json`
- `var/research_blue_walnut/first_leg_policy_hypothesis_v0.json`

当前默认路由判断:

- `btc`
  - 默认更像 `fast_20_30`
  - `strong_aligned` 更像 `fast_20`
- `eth`
  - 默认更像 `slow_30_60`
  - 是当前最明确的延迟补腿 family
- `sol`
  - 默认更像 `fast_20_30`
  - `strong_aligned` 是当前最强的快补分支
- `xrp`
  - 默认更像 `fast_20_30`
  - 但 regime 敏感度更强

当前最有代表性的局部结构:

- `sol + strong_aligned`
  - median completion: `6s`
  - `<=10s` completion: `58.67%`
  - `<=60s` completion: `93.33%`
- `eth + opp`
  - median completion: `24s`
  - `<=10s` completion: `35.78%`
  - `<=60s` completion: `84.40%`

当前解释:

- `blue-walnut` 不是在执行一条统一的全局补腿规则
- 而是在先按 `family` 路由,再叠加 regime 决定等多久

### 3.6 补腿更像"近似贪心",但不是纯粹无脑补到 `< 1`

基于:

- `var/research_blue_walnut/first_leg_followup_full.csv`

当前验证思路:

- 如果补腿是纯粹贪心,那么首轮 `first_cross_pair_sum` 应该大多非常贴近 `1`
- 而且很多补腿会表现为:
  - 基于首腿价格,尽快把 `pair_sum` 压到 `< 1`

当前确认:

- 首轮 `pair_sum` 的中位数是 `0.99`
- `53.44%` 的首轮配平直接做到 `< 1`
- `81.13%` 的首轮配平落在 `<= 1.01`
- `62.26%` 的首轮配平落在 `1 +/- 0.02`
- `87.68%` 的首轮配平落在 `1 +/- 0.05`
- 只有 `8.27%` 的首轮配平深度低于 `0.95`
- 只有 `4.05%` 的首轮配平高于 `1.05`

这说明:

- 大多数补腿确实是在围绕 `1` 附近成交
- 它不像在追求"大幅低于 1 的深折价"
- 更像在尽快把首腿收敛成一个接近 parity 的 pair 单元

进一步看 edge 分箱:

- `1c ~ 2c` 的正 edge 占 `15.04%`
- `2c ~ 5c` 的正 edge 占 `25.92%`
- `> 5c` 的正 edge 占 `11.71%`
- `-1c ~ 0` 的轻微负 edge 占 `16.09%`
- `-2c ~ -1c` 的轻微负 edge 占 `12.26%`

并且:

- 深度正 edge(`> 5c`)的中位补腿等待是 `44s`
- 深度负 edge(`< -5c`)的中位补腿等待是 `54s`
- 最贴近 `1` 的几个小 edge 区间,补腿等待中位数通常只有 `5-10s`

这说明:

- 快速补腿更多是在"先把 pair 做出来"
- 很深的正 edge 往往不是主流形态
- 很差的负 edge 也更多出现在拖很久的尾部样本里

再看首腿价格与首轮配平成果:

- `0.47 - 0.49` 的首腿价格带里,`pair < 1` 占 `62.31%`
- `0.49 - 0.50` 里是 `58.97%`
- `0.51 - 0.53` 降到 `43.61%`
- `0.53 - 0.55` 进一步降到 `37.66%`

这说明:

- 首腿价格越靠近当前识别出来的中间优选带
- 后续越容易被补成 `< 1` 的首轮 pair

#### 当前判断

所以对"补腿是不是就是贪心操作"这个问题,当前最合理的结论是:

- 是"近似贪心"
- 但不是"无脑只要看到反腿就疯狂补到 `< 1`"

更准确地说:

- 它很像在拿到一个相对合适的首腿之后
- 尽快寻找能把 pair 拉回 `1` 附近的反腿
- 如果能做到 `< 1` 更好
- 但它并不是每次都严格等到 `< 1` 才出手

所以当前更贴切的描述是:

- `blue-walnut` 的补腿更像 `parity-seeking greedy`
- 而不是 `strict sub-par greedy`

当前边界:

- 我们现在依据的是成交流,不是历史完整盘口
- 所以能证明"结果上它非常像围绕 parity 贪心补腿"
- 但还不能严格证明"它当时看到的最优挂单到底长什么样"

### 3.7 持续盈利更像"累计锁利账本",不是要求每一轮都 `< 1`

基于:

- `var/research_blue_walnut/unit_accounting_full.csv`
- `var/research_blue_walnut/unit_accounting_full.summary.json`

方法说明:

- 这一步不再只看单轮 `pair_sum`
- 而是按 cycle 顺序重建每次 pair 完成后的累计账本
- 对每个锁住的 unit,统计:
  - 本轮 `incremental_locked_edge`
  - 截至当前的 `current_locked_edge`
  - 当前已经锁住多少 shares
  - 当前残余库存在哪一侧

当前确认:

- 全部 `101,865` 个 locked cycle 里:
  - `63.06%` 的单轮 cycle 本身是正 edge
  - `67.62%` 的累计锁利账本是正 edge
  - `24.06%` 的全部 cycle 属于:
    - 单轮不赚钱
    - 但累计账本仍为正

更关键的是:

- 在所有"单轮不赚钱"的 cycle 里
- 有 `65.13%` 仍然发生在"累计账本为正"的状态下

这说明:

- 很多 `incremental_locked_edge <= 0` 的 cycle
- 并不是策略失效
- 而是更像组合层面的过桥单元

如果直接看每个 market 的最终累计锁利边:

- `1173 / 1802` 个 market 的最终 `current_locked_edge > 0`
- 占 `65.09%`

分 family 看,最终累计锁利边为正的 market 占比都很接近:

- `btc: 65.93%`
- `eth: 63.78%`
- `sol: 65.18%`
- `xrp: 65.49%`

而且最终累计锁利边的中位数也都为正:

- `btc: 0.0106`
- `eth: 0.0108`
- `sol: 0.0137`
- `xrp: 0.0125`

#### 对"持续盈利"最关键的解释

这轮结果说明:

- `blue-walnut` 并不是要求每一轮都严格 `< 1`
- 它更像允许一部分 cycle 作为过桥 / 库存修复单元存在
- 真正要求为正的,是累计锁住的 redeemable unit 平均成本

也就是说:

- 单轮 `< 1` 是硬锁利
- 单轮 `>= 1` 不一定就是坏单
- 只要累计账本仍为正,它就可能只是策略内部的搬运成本

再往 locked unit index 看:

- `1-3` 轮时,累计账本为正的比例是 `61.12%`
- `31-100` 轮提高到 `70.94%`
- `101+` 轮进一步到 `79.53%`

这说明:

- 随着 cycle 增多
- 累计锁利边不是在系统性恶化
- 反而更像在逐步稳定

#### 当前判断

所以对"`pair_sum < 1` 怎么保证持续盈利"这个问题,当前最合理的回答是:

- 持续盈利不是靠"每一轮都 `< 1`"
- 而是靠"累计锁利账本长期保持正 edge"

更贴切地说:

- `blue-walnut` 的持续盈利机制更像:
  - `hard lock cycles`
  - 加上
  - `bridge / inventory repair cycles`
  - 最后共同维护一个正的累计 redeemable-unit 成本

当前边界:

- 这版 `unit accounting` 还是基于成交流,不是完整持仓账本
- 因此它更接近"累计锁利边近似"
- 还不是最终意义上的完整 realized PnL

但它已经足够支持一个很关键的结论:

- 研究 `blue-walnut` 时,不能只看单轮 `pair_sum`
- 必须看累计锁利边和库存过桥行为

### 3.8 过桥 cycle 不是噪声,而是一类稳定的库存修复结构

基于:

- `var/research_blue_walnut/unit_accounting_full.csv`
- `var/research_blue_walnut/bridge_cycle_analysis_full.json`
- `var/research_blue_walnut/bridge_cycle_analysis_full.csv`

方法说明:

- 这里把 cycle 分成两类:
  - `hard_lock_cycle`
    - 单轮 `incremental_locked_edge > 0`
  - `bridge_cycle`
    - 单轮 `incremental_locked_edge <= 0`
    - 但累计 `current_locked_edge > 0`

当前确认:

- 全部 `101,865` 个 locked cycle 里:
  - `hard_lock_cycle` 占 `63.06%`
  - `bridge_cycle` 占 `24.06%`
- 在所有单轮不赚钱的 cycle 里:
  - `65.13%` 其实属于 `bridge_cycle`

这说明:

- 过桥 cycle 不是偶发噪声
- 它是 blue-walnut 账本内部非常稳定的一类动作

进一步看结构差异:

- `bridge_cycle`
  - `same_side_trade_count` 中位数:`2`
  - `same_side_trade_usdc` 中位数:`8.07`
  - 单轮 edge 中位数:`-0.02`
  - 但累计 edge 中位数:`0.026`
  - 首腿 size 中位数:`3.77`
  - completion size 中位数:`5.4`

- `hard_lock_cycle`
  - `same_side_trade_count` 中位数:`1`
  - `same_side_trade_usdc` 中位数:`4.78`
  - 单轮 edge 中位数:`0.03`
  - 累计 edge 中位数:`0.0146`
  - 首腿 size 中位数:`3.9249`
  - completion size 中位数:`3.9936`

这说明:

- `bridge_cycle` 往往带着更多同侧加仓痕迹
- completion leg 也更大
- 很像先修库存,再把累计账本维持在正区间

从 family 看:

- `btc` 的 bridge 比例最低:`21.61%`
- `eth`:`24.48%`
- `xrp`:`25.61%`
- `sol` 的 bridge 比例最高:`26.25%`

这说明:

- 所有 family 都在用过桥 cycle
- 但 `btc` 更像偏向单轮硬锁利
- `sol/xrp` 更愿意容忍组合层面的过桥修复

从 locked unit index 看:

- `1-3` 轮里,bridge 占比只有 `16.10%`
- `4-10` 轮升到 `23.62%`
- `11-30` 轮是 `24.37%`
- `31-100` 轮是 `24.90%`

这说明:

- 过桥 cycle 不是一开始的主结构
- 而是在账本展开后,逐渐成为稳定存在的修复动作

#### 当前判断

所以当前最合理的解释是:

- `blue-walnut` 的 cycle 不是单一类型
- 它至少包含两套同时存在的动作:
  - 新增硬锁利单元
  - 维持累计账本为正的过桥修复单元

更贴切地说:

- `hard_lock_cycle` 更像新增利润来源
- `bridge_cycle` 更像账本维护动作

当前边界:

- 这还是基于成交序列的近似账本
- 不是完整逐笔 inventory ledger
- 但已经足够说明:
  - 不能把所有 `>= 1` 的 cycle 都当成坏单
  - 也不能把所有 cycle 都当成同一种盈利单元

### 3.9 过桥 cycle 的触发更像"正账本下沿残余库存修复",不是高库存压力抢救

基于:

- `var/research_blue_walnut/unit_accounting_full.csv`
- `var/research_blue_walnut/bridge_cycle_trigger_analysis_full.json`
- `var/research_blue_walnut/bridge_cycle_trigger_analysis_full.csv`

方法说明:

- 这一轮不再只看"过桥 cycle 长什么样"
- 而是继续看:
  - 当前累计 edge
  - 库存压力比
  - 首腿是否沿残余库存那一侧开出

当前确认:

- `bridge_cycle` 的当前累计 edge 中位数是 `0.026`
- `hard_lock_cycle` 的当前累计 edge 中位数是 `0.0146`

这说明:

- 过桥 cycle 并不主要出现在"账本快坏掉"的时候
- 反而更常出现在"当前累计账本已经是正的"情况下

再看首腿方向:

- `bridge_cycle` 里,`63.20%` 的首腿方向和当前残余库存同侧
- `hard_lock_cycle` 里,这个比例只有 `55.62%`

这说明:

- 过桥 cycle 更像是沿着现有残余库存方向继续打一腿
- 然后再通过后续反腿把账本重新整理

再看同侧交易痕迹:

- `bridge_cycle`
  - `same_side_trade_count` 中位数:`2`
  - `same_side_trade_usdc` 中位数:`8.07`
- `hard_lock_cycle`
  - `same_side_trade_count` 中位数:`1`
  - `same_side_trade_usdc` 中位数:`4.78`

这说明:

- 过桥 cycle 更常伴随一段同侧继续加仓
- 它不像"立即找到完美反腿"的结构
- 更像"先沿库存方向扩一下,再回补整理"

#### 不是高库存压力抢救

如果按 `inventory_pressure_ratio = unlocked / locked` 分桶看:

- `<= 0.25` 时,bridge 占比:`24.38%`
- `0.25 - 0.5` 时:`24.47%`
- `0.5 - 1` 时:`24.68%`
- `1 - 2` 时反而降到:`20.79%`
- `2 - 5` 时降到:`16.83%`
- `5+` 时只剩:`10.93%`

这说明:

- 过桥 cycle 不是在库存压力越高时越频繁
- 相反,极高库存压力下它出现得更少

所以当前更合理的解释不是:

- "库存爆了,所以被迫修"

而是:

- "账本仍在正区间内,策略允许沿残余库存方向继续运转,再用后续反腿修回去"

#### 当前判断

所以目前最合理的触发解释是:

- `bridge_cycle` 更像正账本环境下的主动库存管理动作
- 而不是高压下的被动止损修复

更贴切地说:

- 它不是 `inventory emergency repair`
- 更像 `inventory-aware continuation`

当前边界:

- 这里的库存压力仍然是成交序列推出来的近似量
- 不是完整盘口 + 持仓系统
- 但这已经足够排除一个常见误解:
  - 过桥 cycle 并不是"仓位炸了才补救"

### 3.10 首轮 `hard_lock` 更像"时间窗 + 价格带"驱动,`size` 更像风格画像

基于:

- `var/research_blue_walnut/unit_accounting_full.csv`
- `var/research_blue_walnut/market_open_snapshot_full.csv`
- `var/research_blue_walnut/first_leg_ruleset_v0.json`
- `var/research_blue_walnut/first_cycle_outcome_analysis_full.json`
- `var/research_blue_walnut/first_cycle_outcome_analysis_full.csv`

方法说明:

- 这里专门只看首轮 `locked_unit_index = 1`
- 把 `first_leg_ruleset v0` 拆成三类组件:
  - `time`
  - `price`
  - `size`
- 然后比较不同组件组合,对首轮 `hard_lock` 的解释力

首轮 baseline:

- 首轮 `hard_lock` 占比:`53.44%`
- 首轮 `bridge_cycle` 占比:`6.27%`

当前整体结果:

- `time_only`
  - coverage: `66.93%`
  - hard-lock: `51.49%`
- `price_only`
  - coverage: `28.63%`
  - hard-lock: `62.40%`
- `size_only`
  - coverage: `23.86%`
  - hard-lock: `55.12%`
- `time_price`
  - coverage: `20.20%`
  - hard-lock: `64.29%`
  - bridge: `3.57%`
- `all_three`
  - coverage: `5.94%`
  - hard-lock: `56.07%`

这说明:

- 价格带本身就有明显解释力
- 时间窗和价格带叠加后效果最好
- `size` 一旦被写成过窄的硬过滤,覆盖率会被压得很低

所以当前更合理的理解是:

- `size` 更像风格画像
- 不像首轮 `hard_lock` 的强硬规则

#### family 差异

`time_price` 在多个 family 上都明显优于 baseline:

- `btc`
  - baseline hard-lock: `50.66%`
  - `time_price`: `64.52%`
- `sol`
  - baseline: `56.47%`
  - `time_price`: `66.67%`
- `xrp`
  - baseline: `52.43%`
  - `time_price`: `66.67%`

`eth` 也有提升,但没那么极端:

- baseline: `54.22%`
- `time_price`: `60.55%`

而如果把 `size` 一起硬塞进去:

- `all_three` 在整体上只覆盖 `5.94%`
- `eth` 的 hard-lock 甚至掉到 `46.15%`

这说明:

- 当前 `first_leg_ruleset v0` 里的 `size_band`
- 更适合被当成"核心样貌"
- 不适合直接当首轮 entry 的强硬 gating

#### 当前判断

所以当前最合理的首轮 entry 主线是:

- 先用 `family-specific open window`
- 再用 `family-specific price band`
- `size` 先作为弱条件或排序特征

更贴切地说:

- 首轮 `hard_lock` 的主驱动更像 `time + price`
- 而不是 `time + price + strict size`

这也解释了为什么:

- 我们之前直觉上能用"开盘早期 + 中间价带"抓住首腿主结构
- 但如果把 `size` 写得太硬,规则会迅速变得过窄

当前边界:

- 这轮分析还是基于 `ruleset v0`
- 不代表 `size` 完全没用
- 更可能意味着:
  - `size` 应该被重写成更柔性的 family-specific ranking 特征
  - 而不是当前这种窄带硬阈值

### 3.11 `size` 更适合"宽松软约束",不适合"窄带硬卡"

基于:

- `var/research_blue_walnut/first_cycle_outcome_analysis_full.json`
- `var/research_blue_walnut/first_cycle_outcome_analysis_full.csv`

方法说明:

- 在 `3.10` 的基础上,我把 `size` 从硬 band 改成了相对 `size_anchor` 的柔性距离约束:
  - `soft_25`: 距离 anchor `<= 25%`
  - `soft_50`: 距离 anchor `<= 50%`
  - `soft_100`: 距离 anchor `<= 100%`

整体结果:

- `time_price`
  - coverage: `20.20%`
  - hard-lock: `64.29%`
- `time_price_size_soft_25`
  - coverage: `6.55%`
  - hard-lock: `55.08%`
- `time_price_size_soft_50`
  - coverage: `7.88%`
  - hard-lock: `57.75%`
- `time_price_size_soft_100`
  - coverage: `13.98%`
  - hard-lock: `64.68%`

这说明:

- `soft_25 / soft_50` 这种相对紧的 size 软约束
  - 会明显伤覆盖
  - 而且 hard-lock 并没有变强
- `soft_100` 这种更宽的 size 约束
  - 还能保留一部分 coverage
  - hard-lock 基本和 `time_price` 持平,略有提升

所以当前更合理的理解是:

- `size` 可以作为"排除极端离谱 size"的宽松约束
- 但不应该被写成"必须贴近 anchor"的窄门槛

#### family 差异

`soft_100` 在不同 family 上表现不一样:

- `btc`
  - `time_price`: `64.52%`
  - `time_price_size_soft_100`: `67.86%`
- `sol`
  - `66.67% -> 71.43%`
- `xrp`
  - `66.67% -> 63.24%`
- `eth`
  - `60.55% -> 59.49%`

这说明:

- `btc / sol` 更像适合加一个"不要离 size anchor 太夸张"的宽松过滤
- `eth / xrp` 目前不适合把 size 写得更强

#### 当前判断

所以当前最合理的 `size` 结论是:

- `size` 不是首轮 entry 的第一层规则
- 但它可以是第二层的宽松 sanity check

更贴切地说:

- 对首轮 `hard_lock` 来说:
  - `time + price` 是主驱动
  - `size` 更像 family-specific 的软排序 / 宽过滤项

当前边界:

- 这里的 soft-size 仍然只是基于 anchor 的简单距离规则
- 还不是最优的 size scoring
- 后面如果继续推进,更合理的方向是:
  - 把 size 写成连续分数
  - 而不是继续试更多硬阈值

### 3.12 `entry ruleset v1` 已经压成显式路由表

基于:

- `var/research_blue_walnut/first_leg_ruleset_v0.json`
- `var/research_blue_walnut/first_cycle_outcome_analysis_full.json`
- `var/research_blue_walnut/entry_ruleset_v1.json`
- `var/research_blue_walnut/entry_ruleset_v1.csv`

这一版 `v1` 的原则很简单:

- 所有 family 都先用 `time + price` 作为核心 entry
- 只有在确实有增益的 family 上,才挂一个可选的宽松 `soft_100` size sanity

当前 `entry ruleset v1`:

- `btc`
  - core rule: `<= 12s` 且首腿价格在 `0.48 - 0.50`
  - core coverage: `20.58%`
  - core hard-lock: `64.52%`
  - size policy: `optional_soft_100`
  - 加 `soft_100` 后:
    - coverage: `12.39%`
    - hard-lock: `67.86%`

- `eth`
  - core rule: `<= 15s` 且首腿价格在 `0.473 - 0.50`
  - core coverage: `24.22%`
  - core hard-lock: `60.55%`
  - size policy: `no_hard_size_gate`

- `sol`
  - core rule: `<= 12s` 且首腿价格在 `0.49 - 0.50`
  - core coverage: `14.73%`
  - core hard-lock: `66.67%`
  - size policy: `optional_soft_100`
  - 加 `soft_100` 后:
    - coverage: `10.94%`
    - hard-lock: `71.43%`

- `xrp`
  - core rule: `<= 15s` 且首腿价格在 `0.47 - 0.49`
  - core coverage: `21.24%`
  - core hard-lock: `66.67%`
  - size policy: `no_hard_size_gate`

#### 这版 `v1` 的意义

这一步很关键,因为它把我们之前的研究主线真正压成了:

- 可读
- 可执行
- 可继续回测

也就是说,到这里首轮 entry 已经不再只是一个方向性的研究结论,而是一张显式路由表。

更重要的是,它保留了当前最可信的结构:

- `time + price` 是主规则
- `size` 只在 `btc / sol` 上作为宽松可选 sanity
- 不再把 `size` 写成会明显损伤覆盖的窄门槛

#### 当前判断

如果只用一句话总结当前的 `entry ruleset v1`:

- `btc / sol` 更像 `<= 12s + 中间价带` 的快启动分支
- `eth / xrp` 更像 `<= 15s + 中间价带` 的宽一点 entry 分支
- `size` 目前不是 entry 的第一层硬规则

当前边界:

- 这还是 `v1`
- 它描述的是"首轮 entry 主结构"
- 还不是完整的:
  - second-leg acceptance frontier
  - cycle continuation policy
  - 全部 inventory ledger

但它已经足够代表当前主线的一个阶段性里程碑:

- `blue-walnut` 的首轮 entry policy 已经可以写成显式 family 路由了

### 3.25 entry ruleset v1 对首轮盈利单元的解释力：精度优先、召回不足

基于:
- `var/research_blue_walnut/entry_hard_lock_explanation_v0.json`
- `var/research_blue_walnut/entry_hard_lock_explanation_v0.csv`

本节回答第一个主线 TODO：验证 `entry ruleset v1` 对首轮 `pair_sum < 1` 盈利单元的解释力。

核心结论:

- entry ruleset v1 是 **precision 工具，不是 coverage 工具**
- entry_core（time + price）：
  - coverage: `20.20%`
  - precision: `64.29%`（vs 全局 baseline `53.44%`）
  - recall: `24.30%`
  - lift: `1.203x`
- entry_applied（time + price + size soft-100）：
  - coverage: `17.20%`
  - precision: `65.48%`
  - recall: `21.08%`
  - lift: `1.225x`

含义：

- entry_core 把首轮 hard_lock 命中率从 `53.44%` 提升到 `64.29%`，提升约 20%
- 但 entry_core 只覆盖约 `1/4` 的首轮盈利单元
- 剩余约 `75%` 的首轮盈利单元不在 entry_core 范围内
- 添加 size soft-100 约束对 precision 提升很有限（+1.2pp），但 recall 进一步下降

family 差异：

- `btc`：entry_applied 比 entry_core precision 更高（67.86% vs 64.52%），size 过滤有帮助
- `sol`：entry_applied 明显更好（71.43% vs 66.67%），size 对 sol 最有效
- `eth`：entry_core 和 entry_applied 几乎无差异，size 过滤无效
- `xrp`：entry_core 和 entry_applied 几乎无差异，size 过滤无效

当前判断：
- entry ruleset v1 对首轮盈利单元的解释是"高精度、低召回"
- 想解释大部分首轮盈利单元，必须继续叠加 wait budget、regime 判断等额外条件

---

### 3.26 entry ruleset v1 + wait budget：主要捕获时间加速，不显著提升总覆盖

基于:
- `var/research_blue_walnut/entry_wait_budget_mapping_full.json`
- `var/research_blue_walnut/entry_wait_budget_mapping_full.csv`

本节回答第二个主线 TODO：看 `entry ruleset v1 + wait budget` 是否已经足够解释大部分首轮盈利单元。

核心对比：

- baseline 全局 `<=60s` completion: `85.90%`
- entry_core `<=60s` completion: `85.99%`（几乎没有差异）
- baseline `<=10s` completion: `46.73%`
- entry_core `<=10s` completion: `50.82%`（有提升）
- baseline median delay: `12s`
- entry_core median delay: `10s`

含义：

- entry_core + wait budget 主要提升的是 **早期完成速度**（10s 段提升 4pp）
- 对 `<=60s` 总 completion 率几乎没有贡献（几乎一样）
- 说明 entry_core + wait budget 组合本质上是在"把已经会完成的单元加速"，而不是"解释更多原本不会完成的单元"

family 层面最有意义的提升：

- `btc`：`entry_core <=10s` 从 `48.23%` 升到 `56.99%`，median delay 从 `12s` 降到 `8s`
- `sol`：`entry_core <=10s` 从 `52.01%` 升到 `53.03%`，median delay 从 `10s` 降到 `8s`
- `eth`：`entry_core <=10s` 从 `41.33%` 升到 `47.71%`，median delay 从 `16s` 降到 `12s`
- `xrp`：提升最小（`45.35%` -> `46.88%`），regime 敏感度最强

当前判断：
- entry ruleset v1 + wait budget **不足以**解释大部分首轮盈利单元
- 两者组合主要描述"时间加速"这一类pattern，不描述"谁能被完成"
- 要解释更多盈利单元，需要引入 regime-aware 条件或 acceptance frontier 叠加

---

### 3.27 目前规则仍无法解释的 family-specific 例外

基于:
- `var/research_blue_walnut/entry_hard_lock_explanation_v0.json`
- `var/research_blue_walnut/first_cycle_outcome_analysis_full.csv`
- `var/research_blue_walnut/second_leg_wait_budget_stability_v0.json`

本节回答第三个主线 TODO：找出目前规则仍然解释不了的 family-specific 例外。

当前已知的三类例外：

**第一类：entry core 之外的 ~75% 样本**
- entry_core recall 只有 ~24%
- 剩余 ~76% 的首轮 hard_lock 不在 entry_core 覆盖范围
- 这部分样本大概率不在 12-15s 窗口内，或首腿价格不在对应 family 的 entry band 内

**第二类：entry core 内的 ~36% 硬失败**
- entry_core 内仍有 ~36% 的首腿未做到 hard_lock（precision = 64.29%）
- 这些样本满足 entry 时间+价格条件，但最终仍无法在 budget 内完成补腿
- 最可能的解释：second-leg 未落在 acceptance fast bands，或 regime 条件不适配

**第三类：family-specific size 例外**
- `eth` 和 `xrp`：entry_core 和 entry_applied 的 precision/recall 几乎相同
  - 说明 size soft-100 约束对这两个 family 完全不提供额外解释力
- `sol`：entry_applied 的 precision 明显高于 entry_core（71.43% vs 66.67%）
  - 但 coverage 从 `14.73%` 降到 `10.94%`，是 precision vs coverage 权衡最显著的 family
- `btc`：entry_applied 精度高于 entry_core（67.86% vs 64.52%），但 recall 显著下降（26.20% -> 16.59%）

**第四类：xrp 的 regime 敏感性**
- xrp 是唯一一个 entry_core 完全没有帮到 wait budget 路由的 family
- 说明 xrp 的盈利单元主要由 regime 条件决定，entry rules 的贡献最弱

**第五类：等待预算不够稳定的样本**
- `xrp` 的 `wait_budget_80pct` 支持率只有 `58.63%`
- `sol` 的 `wait_budget_80pct` 支持率只有 `55.36%`
- 说明这两个 family 有显著比例的样本需要在 budget 之外等待更久才能完成

当前判断：
- 要解释更多首轮盈利单元，核心路径是：
  1. 为 eth/xrp 放弃 size 约束，提高 entry 覆盖率
  2. 为 sol 的 size 约束找到最优阈值（当前 soft-100 不是最优）
  3. 对 xrp 需要从一开始就叠加 regime-aware 条件
  4. 对 bridge cycle 的触发条件进一步细化（目前 bridge 占比约 24%）

---

### 3.13 `entry ruleset v1` 和等待预算分支已经能接起来

基于:

- `var/research_blue_walnut/entry_ruleset_v1.json`
- `var/research_blue_walnut/first_leg_policy_hypothesis_v0.json`
- `var/research_blue_walnut/entry_wait_budget_mapping_full.json`
- `var/research_blue_walnut/entry_wait_budget_mapping_full.csv`

这一轮要回答的问题很直接:

- `entry ruleset v1` 选出来的首腿,是否真的更像会进入更快的补腿等待预算分支

当前全样本基线:

- rows: `1802`
- `<=10s` completion: `46.73%`
- `<=20s` completion: `60.82%`
- `<=30s` completion: `70.48%`
- `<=60s` completion: `85.90%`
- median second-leg delay: `12s`

其中 route class mix:

- `fast_20 + fast_20_30`: `55.66%`
- `slow_30 + slow_30_60`: `38.40%`
- `mixed_20_60`: `5.94%`

如果只看 `entry_core`,也就是纯 `time + price` 命中的首腿:

- coverage: `20.20%`
- `<=10s` completion: `50.82%`
- `<=20s` completion: `64.84%`
- `<=30s` completion: `73.63%`
- `<=60s` completion: `85.99%`
- median second-leg delay: `10s`

这说明:

- `entry ruleset v1` 的核心命中样本,确实比全样本更快进入补腿完成
- 但它提升的主要是 `10-30s` 这段早期等待预算
- 对 `<=60s` 总完成率几乎没有提升

family 拆开后更清楚:

- `btc`
  - all: `<=10s 48.23%`,median `12s`
  - `entry_core`: `<=10s 56.99%`,median `8s`
- `sol`
  - all: `<=10s 52.01%`,median `10s`
  - `entry_core`: `<=10s 53.03%`,median `8s`
- `eth`
  - all: `<=10s 41.33%`,median `16s`
  - `entry_core`: `<=10s 47.71%`,median `12s`
- `xrp`
  - all: `<=10s 45.35%`,median `14s`
  - `entry_core`: `<=10s 46.88%`,median `12s`

当前解释:

- `entry ruleset v1` 不是在提升"最终会不会在 `60s` 内补腿"
- 而是在把首腿更稳定地送进"前 `10-30s` 就完成"的更快分支
- 这种映射在 `btc / sol` 上最清楚,在 `eth / xrp` 上是温和改善

另一个重要发现是:

- 如果把 `btc / sol` 的 `optional_soft_100` size sanity 当成硬过滤去用,整体映射反而变差
- `entry_applied` 的 coverage 从 `20.20%` 掉到 `17.20%`
- `<=10s` completion 也从 `50.82%` 掉回 `47.10%`
- median delay 从 `10s` 回到 `12s`

这进一步支持我们前面的主结论:

- `time + price` 是首轮 entry 的主骨架
- `size` 更适合做宽松 sanity 或排序特征
- 不适合在当前阶段被硬写成补腿等待预算的入口门槛

到这里,`Phase B` 的主线已经更完整了:

- `entry ruleset v1` 负责描述"哪些首腿更像会被开出来"
- `wait budget policy` 负责描述"这些首腿后面更像会被等多久补平"
- 两者之间已经存在可量化的映射,但更接近"早期 completion acceleration",不是"绝对决定 `60s` 是否完成"

### 3.14 second-leg acceptance frontier 已经有成交级近似

基于:

- `var/research_blue_walnut/first_leg_followup_okx_full.csv`
- `var/research_blue_walnut/second_leg_acceptance_frontier_full.json`
- `var/research_blue_walnut/second_leg_acceptance_frontier_full.csv`

这一轮不是去猜盘口,而是先做一个更稳的成交级近似:

- 用 `second_leg_price - (1 - first_leg_price)` 定义 second-leg 相对 parity anchor 的价格偏移
- 看哪些 price band 更像"快速被接受"
- 哪些 band 更像"只有拖久了才会成交"

全样本结果很整齐:

- `fast_accept_bands`
  - `-2c~-1c`
  - `-1c~0`
  - `0~1c`
  - `1c~2c`
- `delayed_tail_bands`
  - `<=-2c`
  - `>2c`
- `hard_lock_bands`
  - `<=-2c`
  - `-2c~-1c`

更具体地看:

- `-2c~-1c`
  - share: `16.65%`
  - median delay: `6s`
  - `<=30s` completion: `83.00%`
  - `>60s` tail: `7.00%`
  - `hard_lock`: `100%`
- `-1c~0`
  - share: `14.98%`
  - median delay: `9s`
  - `<=30s` completion: `81.85%`
  - `>60s` tail: `6.30%`
  - 更像接近 parity 的快速 accepted 区
- `0~1c` 和 `1c~2c`
  - 也仍然属于快速 accepted 区
  - 但它们不再提供 `< 1` 的硬锁利
- `<=-2c`
  - share: `34.85%`
  - median delay: `18s`
  - `>60s` tail: `21.18%`
  - `hard_lock`: `100%`
- `>2c`
  - share: `17.59%`
  - median delay: `26s`
  - `>60s` tail: `20.19%`
  - `hard_lock`: `0%`

当前解释很关键:

- `blue-walnut` 并不是"只要更便宜就更快补"
- 也不是"只要还能补就无脑接受任何价格"
- 真正更像被快速接受的,是 parity anchor 附近大约 `[-2c, +2c]` 的中间带
- 两侧极端价格都会进入明显更慢的 tail:
  - 太便宜的 `<=-2c` 更像在等更深折价
  - 太贵的 `>2c` 更像只有在拖久后才会接受

这个结构在四个 family 上几乎一致:

- `btc / eth / sol / xrp` 的 `fast_accept_bands` 都是
  - `-2c~-1c`
  - `-1c~0`
  - `0~1c`
  - `1c~2c`
- `delayed_tail_bands` 也都一致:
  - `<=-2c`
  - `>2c`

所以当前最稳的一版结论可以写成:

- `blue-walnut` 的 second-leg acceptance frontier 更像"围绕 parity anchor 的中间接受带"
- 真正的快速 accepted 区间大致是 `[-2c, +2c]`
- 其中 `[-2c, -1c]` 是最像"既快又硬锁利"的甜蜜区
- 极端便宜和极端昂贵都更像拖延尾部,而不是主流快速补腿区

当前边界:

- 这是基于成交流的 frontier 近似
- 它回答的是"最终被接受的成交价长什么样"
- 还不是盘口级的"当时面前有哪些挂单被放弃了"
- 所以真正的订单簿级 acceptance frontier 仍然是后续增强项

### 3.15 `second-leg policy ruleset v0` 已经压成统一规则表

基于:

- `var/research_blue_walnut/first_leg_wait_budget_okx.json`
- `var/research_blue_walnut/first_leg_policy_hypothesis_v0.json`
- `var/research_blue_walnut/second_leg_acceptance_frontier_full.json`
- `var/research_blue_walnut/second_leg_policy_ruleset_v0.json`
- `var/research_blue_walnut/second_leg_policy_ruleset_v0.csv`

这一版 `v0` 的意义,不是再去拆更细的局部异常,而是先把当前最稳的 `Phase B` 主线压成一张统一规则表:

- family default wait budget
- family/regime route override
- second-leg acceptance band

当前全局 acceptance 规则已经很清楚:

- `fast_accept_bands`
  - `-2c~-1c`
  - `-1c~0`
  - `0~1c`
  - `1c~2c`
- `delayed_tail_bands`
  - `<=-2c`
  - `>2c`
- `hard_lock_bands`
  - `<=-2c`
  - `-2c~-1c`

family 默认路由也已经压成一句话:

- `btc`
  - default route: `fast_20_30`
  - default budget: `20s / 30s`
  - route confidence: `80.53%`
- `eth`
  - default route: `slow_30_60`
  - default budget: `30s / 60s`
  - route confidence: `69.11%`
- `sol`
  - default route: `fast_20_30`
  - default budget: `20s / 30s`
  - route confidence: `55.36%`
- `xrp`
  - default route: `fast_20_30`
  - default budget: `20s / 30s`
  - route confidence: `41.15%`

这张表把几个关键差异也说得很直接:

- `btc`
  - 主体就是 `fast_20_30`
  - `strong_aligned` 可以切到 `fast_20`
  - `strong_opp` 会退到 `slow_30`
- `eth`
  - 主体明确是 `slow_30_60`
  - 只有 `strong_aligned` 会切回 `fast_20_30`
- `sol`
  - 默认还是 `fast_20_30`
  - 但 `aligned` 会落到 `mixed_20_60`
  - `opp` 会切到 `slow_30_60`
- `xrp`
  - 默认虽然是 `fast_20_30`
  - 但 route confidence 最低,说明 regime 敏感度最大
  - `aligned` 会直接走 `slow_30_60`
  - `flat` 更像 `fast_20`
  - `strong_opp` 会退到 `slow_30`

当前解释:

- `Phase B` 到这里已经不只是"知道不同 family 等多久"
- 而是已经能写成一张显式 `second-leg policy ruleset`
- 如果只保留一版最简执行口径,可以先写成:
  - `btc / sol / xrp` 默认先按 `20s / 30s` 预算看
  - `eth` 默认按 `30s / 60s`
  - 所有 family 的 second-leg 都优先看 parity anchor 附近 `[-2c, +2c]`
  - 真正既快又最像硬锁利的甜蜜区仍然是 `[-2c, -1c]`

另一个很重要的研究判断是:

- 现在先不继续细拆 `fast branch / slow branch`
- 因为当前这张 `v0` 规则表已经足够承载主线研究
- 更细路由先降成支线,等主线解释力验证需要时再回来补

所以当前 `Phase B` 的最稳状态是:

- 有 hazard
- 有 wait budget
- 有 acceptance frontier
- 有 `entry -> wait budget` 映射
- 也已经有统一的 `second-leg policy ruleset v0`

### 3.16 `10s / 20s / 30s / 60s` 等待预算稳定性已经压成 family verdict

基于:

- `var/research_blue_walnut/first_leg_wait_budget_okx.json`
- `var/research_blue_walnut/first_leg_policy_hypothesis_v0.json`
- `var/research_blue_walnut/second_leg_wait_budget_stability_v0.json`
- `var/research_blue_walnut/second_leg_wait_budget_stability_v0.csv`

这一轮要回答的问题是:

- `btc / eth / sol / xrp` 的默认等待预算,到底能不能被当成稳定默认值
- 还是必须从一开始就按 regime 拆开

当前 family verdict 很清楚:

- `btc`
  - `stability_tier = stable_default`
  - default budget support:
    - `70pct = 90.04%`
    - `80pct = 90.49%`
  - 当前最稳的默认预算就是 `20s / 30s`
- `eth`
  - `stability_tier = mixed_default`
  - default budget support:
    - `70pct = 82.44%`
    - `80pct = 69.11%`
  - 默认 `30s / 60s` 仍然成立
  - 但 `strong_aligned` 是明确更快的例外
- `sol`
  - `stability_tier = mixed_default`
  - default budget support:
    - `70pct = 79.24%`
    - `80pct = 55.36%`
  - 默认 `20s / 30s` 还能用
  - 但 `aligned / opp` 已经会明显拉开到 `20/60` 或 `30/60`
- `xrp`
  - `stability_tier = regime_sensitive`
  - default budget support:
    - `70pct = 59.51%`
    - `80pct = 58.63%`
  - 不适合把 `20s / 30s` 当成足够稳的统一默认
  - `aligned = 60s / 60s` 是最突出的慢分支

如果只看 family 默认 checkpoint:

- `btc`
  - `10s / 20s / 30s / 60s = 48.23 / 65.49 / 75.88 / 88.50`
- `eth`
  - `41.33 / 54.44 / 64.22 / 83.11`
- `sol`
  - `52.01 / 63.62 / 72.10 / 87.05`
- `xrp`
  - `45.35 / 59.73 / 69.69 / 84.96`

更关键的是,当前主线可以再收敛一句:

- `btc` 的 family 默认预算已经足够稳定,可以直接当默认规则
- `eth` 的 family 默认预算也足够有用,但要保留 `strong_aligned` 快分支例外
- `sol` 还能先用默认预算,但已经不该再被理解成"完全稳定"
- `xrp` 不能再只靠 family 默认预算,必须显式看 regime

所以到这里,`Phase B` 的研究边界已经很明确:

- `btc`: 可以直接用 family default
- `eth`: 可以 family default 起步,再给快分支例外
- `sol`: family default 可用,但默认值只是近似
- `xrp`: 从一开始就应该 regime-aware

这也是为什么当前主线先不继续细拆所有 fast/slow 变体:

- 因为只有 `xrp` 已经明显需要 regime-aware 默认
- `btc / eth / sol` 还可以先用当前规则表承担主线
- 更细的分支拆分先留在支线,等 Phase C 解释力验证再决定要不要补

### 3.17 `entry ruleset v1` 对首轮 `hard_lock` 的解释力是"有精度、低召回"

基于:

- `var/research_blue_walnut/unit_accounting_full.csv`
- `var/research_blue_walnut/market_open_snapshot_full.csv`
- `var/research_blue_walnut/entry_ruleset_v1.json`
- `var/research_blue_walnut/entry_hard_lock_explanation_v0.json`
- `var/research_blue_walnut/entry_hard_lock_explanation_v0.csv`

这一轮不再只看 `hard_lock_pct`,而是正式补了:

- `precision`
- `recall`
- `lift`

全样本 baseline:

- 首轮 `hard_lock` rows: `963 / 1802`
- baseline `hard_lock_pct`: `53.44%`

如果只看 `entry_core`,也就是 `time + price` 命中的首腿:

- coverage: `20.20%`
- `hard_lock_precision`: `64.29%`
- `hard_lock_recall`: `24.30%`
- `hard_lock_lift`: `1.203x`

如果把 `btc / sol` 的 `soft_100` 也硬用成 `entry_applied`:

- coverage: `17.20%`
- `hard_lock_precision`: `65.48%`
- `hard_lock_recall`: `21.08%`
- `hard_lock_lift`: `1.225x`

所以当前最重要的判断是:

- `entry ruleset v1` 不是一个"解释大多数首轮 hard-lock"的全覆盖规则
- 它更像一个 precision 还可以的筛选器
- 能把首轮 `hard_lock` 命中率从 `53.44%` 抬到 `64%+`
- 但只能解释大约 `1/4` 的首轮 hard-lock

family 拆开后也很一致:

- `btc`
  - `entry_core precision = 64.52%`
  - `entry_core recall = 26.20%`
- `eth`
  - `entry_core precision = 60.55%`
  - `entry_core recall = 27.05%`
- `sol`
  - `entry_core precision = 66.67%`
  - `entry_core recall = 17.39%`
- `xrp`
  - `entry_core precision = 66.67%`
  - `entry_core recall = 27.00%`

这说明:

- `sol` 的 entry 规则虽然最干净,但覆盖更窄
- `eth / xrp / btc` 的 recall 都在 `26%-27%` 左右
- 没有任何一个 family 已经接近"只靠 entry rule 就解释大多数 hard-lock"

另一个很关键的点是:

- `entry_applied` 的 precision 只比 `entry_core` 略高
- 但 recall 会进一步下降
- 这再次支持:
  - `size` 适合做宽松 sanity 或排序
  - 不适合在当前主线里硬做 gating

所以 `Phase C` 的第一步结论已经很明确:

- `entry ruleset v1` 是一个有效的首轮 hard-lock 候选生成器
- 但还不是一个高召回解释器
- 想解释大部分首轮盈利单元,后面必须继续叠加:
  - `wait budget`
  - possibly `acceptance frontier`
  - family-specific 例外结构

### 3.18 `strategy blueprint v0` 已经把主线研究收成策略骨架

基于:

- `var/research_blue_walnut/entry_ruleset_v1.json`
- `var/research_blue_walnut/second_leg_policy_ruleset_v0.json`
- `var/research_blue_walnut/second_leg_wait_budget_stability_v0.json`
- `var/research_blue_walnut/strategy_blueprint_v0.json`
- `var/research_blue_walnut/strategy_blueprint_v0.csv`

这一步不再追求"解释更多",而是把当前最稳的主线研究直接压成一版可孵化的策略骨架。

这版蓝图的核心 operating principle 很简单:

- 先用 `entry ruleset v1` 在开盘早期围绕 family-specific 中间价带开首腿
- 再用 `second-leg policy ruleset v0` 决定补腿等待预算
- second-leg 的接受价带默认围绕 parity anchor 的 `[-2c, +2c]`
- 真正最像"快且硬锁利"的甜蜜区还是 `[-2c, -1c]`

当前 family 分层已经足够清楚:

- `btc`
  - `incubation_stage = default_ready`
  - 说明可以直接从 family 默认规则开始孵化
  - 默认就是:
    - entry: `<=12s`, price `0.48-0.50`
    - second-leg: `fast_20_30`, budget `20s / 30s`
- `eth`
  - `incubation_stage = default_plus_exceptions`
  - 说明可以先用默认规则起步,再补 regime 例外
  - 默认是 `slow_30_60`
  - 主要例外是 `strong_aligned -> fast_20_30`
- `sol`
  - `incubation_stage = default_plus_exceptions`
  - 默认可以先走 `fast_20_30`
  - 但 `aligned` 和 `opp` 已经要分别切到 `20/60` 或 `30/60`
- `xrp`
  - `incubation_stage = regime_aware_required`
  - 当前不适合从单一 family 默认规则起步
  - 如果要做,必须从一开始就按 regime 路由

这一步的研究意义很直接:

- 主线已经不只是"知道 blue-walnut 大概怎么做"
- 而是已经能把现有结果排成一张明确的策略孵化顺序表

如果只压成一句最简策略判断:

- 第一版最适合先孵化的是 `btc`
- 第二层可以跟进的是 `eth / sol`
- `xrp` 现在更像要延后,或者单独走 `regime-aware` 版本

所以当前主线已经真正从"研究结论"跨到了"策略骨架":

- `btc` 可以直接做默认版
- `eth / sol` 可以做 `default + exceptions`
- `xrp` 先不要强行塞进统一默认版

这也是为什么我把"解释力"整体降成支线:

- 因为当前最有价值的不是继续算更多 recall
- 而是先把能落地的 family 和规则顺序排出来
- 让策略主线真正开始孵化

### 3.19 `strategy playbook v0` 已经给出最小 rollout 顺序

基于:

- `var/research_blue_walnut/strategy_blueprint_v0.json`
- `var/research_blue_walnut/strategy_playbook_v0.json`
- `var/research_blue_walnut/strategy_playbook_v0.csv`

这一步是在 `strategy blueprint v0` 之上继续前推一格:

- 不再只是说"哪些 family 更成熟"
- 而是直接给出第一版最小 rollout 顺序

当前 `playbook v0` 的结论非常明确:

- rollout order:
  - `btc`
  - `eth`
  - `sol`
- deferred:
  - `xrp`

当前 family 部署模式:

- `btc`
  - `deployment_mode = ship_family_default`
  - 说明第一版可以直接按 family 默认规则启动
  - entry:
    - `<=12s`
    - `0.48-0.50`
  - second-leg:
    - `20s / 30s`
- `eth`
  - `deployment_mode = ship_default_with_overrides`
  - 默认先走 `slow_30_60`
  - 但要显式挂上:
    - `strong_aligned`
    - `strong_opp`
- `sol`
  - `deployment_mode = ship_default_with_overrides`
  - 默认先走 `fast_20_30`
  - 但要显式挂上:
    - `aligned`
    - `opp`
- `xrp`
  - `deployment_mode = defer_or_isolate`
  - 当前不建议塞进第一版默认策略
  - 如果要做,应该单独走 regime-aware 分支

这一步把主线判断进一步压实成一句话:

- 第一版策略先做 `btc`
- 第二层接 `eth / sol`
- `xrp` 先延后,不进统一 rollout

所以当前"策略主线孵化"的状态已经不是抽象研究,而是最小部署顺序已经出来了:

- `btc` 是第一优先级
- `eth / sol` 是第二优先级
- `xrp` 是延期或隔离处理

接下来主线最自然的下一步,不再是继续研究解释力,而是:

- 把 `playbook v0` 压成最小可执行策略实现骨架
- 先从 `btc` family default 开始落地
- 再给 `eth / sol` 补 override 逻辑

### 3.20 `btc_family_default_strategy_v0` 已经压成最小可执行状态机

基于:

- `var/research_blue_walnut/strategy_blueprint_v0.json`
- `var/research_blue_walnut/strategy_playbook_v0.json`
- `var/research_blue_walnut/btc_family_default_strategy_v0.json`
- `var/research_blue_walnut/btc_family_default_strategy_v0.csv`

这一步把 `btc` 从"第一优先级 family"继续压成了真正的最小策略骨架。

当前 `btc_family_default_strategy_v0` 的结构已经不是抽象规则,而是一个四段式状态机:

- `observe_open`
  - 只看新开 `hourly BTC` market
  - 观察窗:开盘后 `12s`
- `enter_first_leg`
  - 只在 `0.48-0.50` 的 entry band 里开第一腿
  - size policy 仍是 `optional_soft_100`
  - size anchor: `21.345 USDC`
- `monitor_second_leg`
  - 默认 second-leg route: `fast_20_30`
  - 默认预算:`20s / 30s`
  - acceptance fast bands:
    - `-2c~-1c`
    - `-1c~0`
    - `0~1c`
    - `1c~2c`
  - preferred hard-lock band:
    - `<=-2c`
    - `-2c~-1c`
- `complete_or_timeout`
  - 默认 timeout: `30s`
  - fallback:
    - `timeout_without_forcing_far_tail_price`

`btc` 的 regime override 也已经明确写进骨架里了:

- `strong_aligned`
  - `fast_20`
  - `20s / 20s`
- `strong_opp`
  - `slow_30`
  - `30s / 30s`

这一步的意义很直接:

- 主线已经不只是"我们建议先做 btc"
- 而是 `btc` 的第一版最小执行骨架已经写出来了

如果把它压成一句最短的策略描述:

- 只做 `hourly BTC`
- 开盘前 `12s` 内只在 `0.48-0.50` 开首腿
- second-leg 默认按 `20s / 30s` 跑
- 接受价带先看 parity anchor 附近 `[-2c, +2c]`
- 如果到 `30s` 还没落在合适价带,不强追极端尾部价格

所以当前主线已经从"研究蓝图"进入"最小策略骨架":

- `btc` 已经可以单独拿出来继续往实现层推进
- 下一步最自然的就是把 `eth / sol` 的 override 逻辑压成同样形态的补充规则

### 3.21 `eth_sol_override_strategies_v0` 已经压成可执行补充规则

基于:

- `var/research_blue_walnut/eth_sol_override_strategies_v0.json`
- `var/research_blue_walnut/eth_sol_override_strategies_v0.csv`

这一步把 `eth / sol` 从"第二层 family"继续压成了和 `btc` 同样形态的可执行策略补充包。

`eth_override_strategy_v0` 当前已经明确:

- 默认 route:
  - `slow_30_60`
  - `30s / 60s`
- override:
  - `strong_aligned -> fast_20_30 -> 20s / 30s`
  - `strong_opp -> slow_30 -> 30s / 30s`
- 状态机也已经成型:
  - `observe_open`
  - `enter_first_leg`
  - `select_second_leg_route`
  - `monitor_second_leg`
  - `complete_or_timeout`

`sol_override_strategy_v0` 当前也已经明确:

- 默认 route:
  - `fast_20_30`
  - `20s / 30s`
- override:
  - `aligned -> mixed_20_60 -> 20s / 60s`
  - `opp -> slow_30_60 -> 30s / 60s`
- 同样也已经压成五段式状态机

这一步之后,`eth / sol` 已经不再只是"等以后再做的 family":

- 它们已经有默认路径
- 也已经有 override 路由
- 可以跟 `btc` 一起进入第一版策略包

### 3.22 `strategy_rollout_bundle_v0` 已经把第一版策略包收齐

基于:

- `var/research_blue_walnut/btc_family_default_strategy_v0.json`
- `var/research_blue_walnut/eth_sol_override_strategies_v0.json`
- `var/research_blue_walnut/strategy_rollout_bundle_v0.json`
- `var/research_blue_walnut/strategy_rollout_bundle_v0.csv`

这一步把前面的分散骨架继续收口成了第一版统一 rollout bundle。

当前 bundle 已经非常清楚:

- `active_families`
  - `btc`
  - `eth`
  - `sol`
- `deferred_families`
  - `xrp`

family strategy types 也已经落清楚了:

- `btc`
  - `family_default_state_machine`
- `eth`
  - `default_plus_override_state_machine`
- `sol`
  - `default_plus_override_state_machine`

所以当前主线的状态已经可以用一句话总结:

- 第一版统一策略包已经成型
- 其中 `btc / eth / sol` 是 active rollout
- `xrp` 继续 deferred

主线下一步就不该再回去拆研究结论,而应该进入:

- 把 `strategy_rollout_bundle_v0` 继续压成实现/配置层骨架
- 让第一版策略包真正变成能落代码的结构

### 3.23 `strategy_config_skeleton_v0` 已经把第一版策略包压到配置层

基于:

- `var/research_blue_walnut/strategy_rollout_bundle_v0.json`
- `var/research_blue_walnut/btc_family_default_strategy_v0.json`
- `var/research_blue_walnut/eth_sol_override_strategies_v0.json`
- `var/research_blue_walnut/strategy_config_skeleton_v0.json`
- `var/research_blue_walnut/strategy_config_skeleton_v0.csv`

这一步把当前主线又往前推了一格:

- 不再只是"有一套 rollout bundle"
- 而是已经有一份实现 / 配置层骨架 JSON

这份 `strategy_config_skeleton_v0` 的结构已经很接近后续实现需要的样子:

- `runtime`
  - `market_duration_bucket = hourly`
  - `poll_interval_seconds = 1.0`
  - `market_websocket_enabled = true`
  - `require_real_order_book = false`
- `rollout`
  - `phase_1 = [btc]`
  - `phase_2 = [eth, sol]`
  - `deferred = [xrp]`
- `families`
  - `btc`
    - enabled
    - family default state machine
  - `eth`
    - enabled
    - default + override state machine
  - `sol`
    - enabled
    - default + override state machine
  - `xrp`
    - disabled
    - reason = `regime_aware_required`

更重要的是,这个骨架已经把各 family 的核心执行参数一起写进去了:

- entry window
- entry price band
- size policy
- second-leg default route
- wait budget
- acceptance bands
- regime overrides

所以当前主线状态已经不是"研究成果很完整",而是:

- 第一版统一策略包已经进入配置层
- active family 集合已经固定成 `btc / eth / sol`
- `xrp` 仍然明确保持 deferred

这一步的实际意义很大:

- 如果后面要接策略实现,不需要再从文档里抄规则
- 现在已经有一份统一的 JSON skeleton 可以直接接代码

所以主线下一步最自然的方向也很清楚:

- 要么把这份 skeleton 接进真正的策略 runner
- 要么先在代码层补一个"读取 skeleton 并组装 family strategy"的装配器

### 3.24 `config/skeleton_assembler.py` 已实现

基于：

- `var/research_blue_walnut/strategy_config_skeleton_v0.json`
- `polymarket_copytrader/config/skeleton_assembler.py`

已实现一个 skeleton loader，核心功能：

- 读取 `strategy_config_skeleton_v0.json`
- 对每个 family config 做规范化验证：
  - `entry` 子配置：`price_band_lower/upper`、`window_seconds`、`size_policy`、`size_anchor_usdc` 等
  - `second_leg` 子配置：`default_route_class`、`timeout_seconds`、`wait_budget_70pct/80pct`、`acceptance_*_bands` 等
  - `regime_overrides`：各 regime 的 route class、wait budget 映射
  - `deferred` family（`xrp`）特殊处理：跳过执行字段验证
- 验证通过后返回以 family 为 key 的组装后 family strategy dict
- 提供 `get_enabled_families()` 和 `get_active_families()` 辅助函数

当前确认：

- `btc`、`eth`、`sol` 三族均验证通过
- `xrp` 的 `deferred` 状态正确保留
- 有效 family 集合：`[btc, eth, sol]`
- Rollout 顺序：`[btc, eth, sol]`

当前解释：

- `strategy_config_skeleton_v0` 已经从"配置层骨架"进一步落地成可import的Python模块
- 后续可以直接 `from polymarket_copytrader.config import load_skeleton` 引入到策略 runner 里

当前边界：

- 这只是 skeleton loader，还没有对接真正的执行 runner
- 后续最自然的下一步是把 loader 接入一个最小 strategy runner

## 4. 当前可执行假设

如果把当前结果压成一句尽量接近执行层的话,可以写成:

- 第一版统一策略包已经可以写成:
  - `btc`: family default state machine
  - `eth`: default + override state machine
  - `sol`: default + override state machine
  - `xrp`: deferred
- 也就是说,统一 rollout 的 active set 已经是:
  - `btc / eth / sol`
- 现在这套 active set 也已经有对应的配置层 skeleton,可以直接接后续实现

这不是最终版策略,但已经是当前最可信的一版中间结论。

## 5. 当前未解问题

目前还没真正解释清楚的点:

- 首腿为什么在某一个具体秒点触发
- 首腿是不是在等待某种盘口挂单错配
- 当前 `size` 规则是否已经足够稳定
- 首轮之后的后续 cycle 到底哪些值得复制

也就是说:

- 我们已经比较像是在逆向 `首轮 pair-unit engine`
- 但还没有完整复刻 `blue-walnut` 的全部交易系统

## 6. 下一步结果更新方向

后续如果继续推进,结果文档优先更新下面三类内容:

1. `entry ruleset v2`
   - 看是否需要为某些 family 增加更细的柔性 size 评分或额外过滤条件
2. `首轮 pair 解释力验证`
   - 当前规则到底能解释多少首轮盈利单元
3. `精细边界`
   - 哪些 family 仍然需要盘口级补充条件

## 7. 当前主要产物索引

核心代码:

- `polymarket_copytrader/first_leg_trigger_rule.py`
- `polymarket_copytrader/first_leg_ruleset.py`
- `polymarket_copytrader/first_leg_family_hazard.py`
- `polymarket_copytrader/first_leg_wait_budget.py`
- `polymarket_copytrader/first_leg_policy_hypothesis.py`
- `polymarket_copytrader/unit_accounting.py`
- `polymarket_copytrader/bridge_cycle_analysis.py`
- `polymarket_copytrader/bridge_cycle_trigger_analysis.py`
- `polymarket_copytrader/first_cycle_outcome_analysis.py`
- `polymarket_copytrader/entry_ruleset_v1.py`
- `polymarket_copytrader/entry_wait_budget_mapping.py`
- `polymarket_copytrader/second_leg_acceptance_frontier.py`
- `polymarket_copytrader/second_leg_policy_ruleset.py`
- `polymarket_copytrader/second_leg_wait_budget_stability.py`
- `polymarket_copytrader/entry_hard_lock_explanation.py`
- `polymarket_copytrader/strategy_blueprint_v0.py`
- `polymarket_copytrader/strategy_playbook_v0.py`
- `polymarket_copytrader/btc_family_default_strategy_v0.py`
- `polymarket_copytrader/eth_sol_override_strategies_v0.py`
- `polymarket_copytrader/strategy_rollout_bundle_v0.py`
- `polymarket_copytrader/strategy_config_skeleton_v0.py`

核心结果:

- `var/research_blue_walnut/pair_sequence_full_600k.summary.json`
- `var/research_blue_walnut/first_leg_trigger_rule_v0.json`
- `var/research_blue_walnut/first_leg_ruleset_v0.json`
- `var/research_blue_walnut/first_leg_wait_budget_okx.json`
- `var/research_blue_walnut/first_leg_policy_hypothesis_v0.json`
- `var/research_blue_walnut/first_leg_family_hazard_okx.json`
- `var/research_blue_walnut/unit_accounting_full.summary.json`
- `var/research_blue_walnut/bridge_cycle_analysis_full.json`
- `var/research_blue_walnut/bridge_cycle_trigger_analysis_full.json`
- `var/research_blue_walnut/first_cycle_outcome_analysis_full.json`
- `var/research_blue_walnut/entry_ruleset_v1.json`
- `var/research_blue_walnut/entry_wait_budget_mapping_full.json`
- `var/research_blue_walnut/second_leg_acceptance_frontier_full.json`
- `var/research_blue_walnut/second_leg_policy_ruleset_v0.json`
- `var/research_blue_walnut/second_leg_wait_budget_stability_v0.json`
- `var/research_blue_walnut/entry_hard_lock_explanation_v0.json`
- `var/research_blue_walnut/strategy_blueprint_v0.json`
- `var/research_blue_walnut/strategy_playbook_v0.json`
- `var/research_blue_walnut/btc_family_default_strategy_v0.json`
- `var/research_blue_walnut/eth_sol_override_strategies_v0.json`
- `var/research_blue_walnut/strategy_rollout_bundle_v0.json`
- `var/research_blue_walnut/strategy_config_skeleton_v0.json`

---

### 3.28 Phase S 主线全部完成，所有支线任务正式归档

基于：

- `polymarket_copytrader/pair_unit_strategy.py` 代码审查
- `strategys/blue_walnut_双边补腿文档.md` TODO 进度面板

本节记录 Phase S（策略主线孵化）的正式收尾状态。

#### pair_unit_strategy.py docstring 清理状态

`pair_unit_strategy.py` 经过前期 session 清理后，当前状态：

- 模块级 docstring：完整，包含用途说明、band notation 示例、route class 说明和 usage 示例
- 每个函数和类：有且仅有 1 个 docstring，无重复
- 所有 docstring 均内容准确，无冗余或占位符残留

结论：`pair_unit_strategy.py` 无需进一步清理 docstring，前期 session 已完成此任务。

#### 所有剩余 TODO 的最终定性

blue_walnut 文档中剩余 6 项 `- [ ]` 逐一确认：

| 剩余项目 | 最终定性 |
|---------|---------|
| `Phase D` 后续 cycle 完整复刻 | 非 TODO，是阶段状态说明；当前非重点 |
| acceptance frontier 盘口级近似 | 支线任务，Phase S 主线已完成，当前阶段不继续 |
| strict cycle continuation policy | 支线任务，`post_pair_cycle_strict.py` 仅分析用 |
| bridge cycle 更细触发结构 | 支线任务，Phase S 主线已完成，当前阶段不继续 |
| 完整 inventory ledger 近似 | 支线任务，Phase S 主线已完成，当前阶段不继续 |
| 实盘订单簿抢单细节 | 支线任务，Phase S 主线已完成，当前阶段不继续 |

结论：所有剩余项目均已明确归类为"支线任务，Phase S 主线孵化已完成，当前阶段不继续"，不存在遗漏的主线 TODO。

#### Phase S 主线最终完成状态

Phase S（策略主线孵化）已全部完成，体现在：

1. **策略蓝图**：`strategy_blueprint_v0` 明确 family 分为 `default_ready`、`default_plus_exceptions`、`regime_aware_required` 三层
2. **策略手册**：`strategy_playbook_v0` 给出明确 rollout 顺序：`btc -> eth -> sol`，`xrp` 延期
3. **BTC 族默认策略**：`btc_family_default_strategy_v0` 已压成四段式状态机
4. **ETH/SOL Override 策略**：`eth_sol_override_strategies_v0` 已压成可执行补充规则
5. **统一 Rollout Bundle**：`strategy_rollout_bundle_v0` 收齐第一版策略包
6. **配置层 Skeleton**：`strategy_config_skeleton_v0` 进入配置层
7. **Skeleton Loader**：`config/skeleton_assembler.py` 已实现并验证通过
8. **策略执行器**：`pair_unit_strategy.py` 的 `PairUnitStrategy` 类 + `parse_band` / `band_contains` 工具函数已验证无 docstring 冗余
9. **Band Parser 验证**：`<=-2c`、`-2c~-1c`、`-1c~0`、`0~1c`、`1c~2c`、`>2c` 全部 6 个 band 解析正确性已通过验证

#### 下一步自然路径

Phase S 完成后的自然下一步（按优先级排序）：

1. **优先**：把 `PairUnitStrategy` + `skeleton_assembler` 接入 `follower.py` 实际执行层
2. **次优先**：接 `live-paper` 回测验证 loop
3. **可选支线**（不阻断主策略孵化）：盘口级 acceptance frontier、strict cycle continuation policy、inventory ledger 完整重建

#### 核心判断一句话总结

- `blue-walnut` 主线孵化已从"研究结论"收敛为"可执行策略骨架"
- active family：`btc / eth / sol`（第一版 rollout）
- deferred family：`xrp`
- pair_unit_strategy.py 的 docstring 清理已完成，无遗留问题
- 所有未完成的 TODO 均为明确归档的支线任务，不影响主线收口

---

### 3.29 文档规则正式归档：结果文档优先更新，计划文档仅记录进度

基于：

- `strategys/blue_walnut_双边补腿文档.md` 更新
- `strategys/blue_walnut_双边补腿研究结果.md` 本节

#### 规则正文

文档工作流规则已正式归档，内容如下：

**计划文档**（`blue_walnut_双边补腿文档.md`）职责：
- 作为研究进度面板
- 只记录：当前研究主线、TODO List、已完成 TODO
- 后续默认不再频繁改动

**结果文档**（`blue_walnut_双边补腿研究结果.md`）职责：
- 作为研究结果沉淀文档
- 新实验、新结论、新规则版本，优先更新本文
- 不承担进度追踪职能

**更新时遵守**：
1. 主线变化 → 更新计划文档
2. 新结论、新数字、新规则版本 → 更新结果文档
3. 完成一个 TODO → 从计划文档的 TODO List 挪到已完成 TODO

#### 本次更新内容

本次已按规则完成以下更新：

1. **结果文档**（本文）：sections 3.25-3.27 归档 `entry ruleset v1` 解释力量化结论（高精度低召回、时间加速但总覆盖不增、5 类 family-specific 例外）；本节 3.29 归档文档规则
2. **计划文档**（`blue_walnut_双边补腿文档.md`）：
   - Section 3.2 新增 `entry ruleset v1` 解释力量化验证（sections 3.25-3.27）为已完成 TODO
   - Section 3.2 新增 documentation workflow rule 归档为已完成 TODO

---

### 3.30 `min_seconds_to_resolution = 300s` 硬门槛规则已实现

基于：

- `polymarket_copytrader/entry_ruleset_v1.py`
- `polymarket_copytrader/strategy_blueprint_v0.py`
- `polymarket_copytrader/btc_family_default_strategy_v0.py`
- `polymarket_copytrader/eth_sol_override_strategies_v0.py`
- `polymarket_copytrader/strategy_config_skeleton_v0.py`
- `polymarket_copytrader/config/skeleton_assembler.py`
- `polymarket_copytrader/pair_unit_strategy.py`
- `tests/test_pair_unit_entry_resolution_gate.py`

#### 规则内容

新增统一常量 `DEFAULT_MIN_SECONDS_TO_RESOLUTION = 300`，并作为 entry 的硬门槛字段加入配置链路：

- `entry_ruleset_v1.py`：常量定义 + 每个 family 的 entry rule 写入该字段 + rule_note 描述更新
- `strategy_blueprint_v0.py`：透传该字段到 family blueprint 的 entry 配置和 CSV 行
- `btc_family_default_strategy_v0.py`：写入 BTC 状态机的 `enter_first_leg` 和 CSV 元数据行
- `eth_sol_override_strategies_v0.py`：写入 ETH/SOL 状态机的 `enter_first_leg` 和 CSV 元数据行
- `strategy_config_skeleton_v0.py`：透传到 skeleton JSON/CSV 配置层
- `skeleton_assembler.py`：解析 + 校验（`>= 0`）该字段
- `pair_unit_strategy.py`：`FamilyEntryConfig` 新增该字段；`decide()` 在 window check 之后执行硬门槛检查，若 `seconds_to_resolution < min_seconds_to_resolution` 直接返回 `skip`；成功 entry 的 details 中也包含该字段供调试

#### 规则理由

- **过滤高风险 entry**：约 0.8% 的尾盘 entry 亏损率明显偏高，加 300s 门槛可过滤
- **现有规则已覆盖 ≤120s**：entry window 最大只到 15s，300s 门槛不会与现有规则重叠
- **300s 以上的尾盘**：这部分 entry 量少但亏损率高，硬门槛成本最低

#### 验证结果

- 单元测试（`tests/test_pair_unit_entry_resolution_gate.py`）：4/4 通过
  - `entry_ruleset` 生成包含 `min_seconds_to_resolution: 300`
  - `PairUnitStrategy` 在阈值边界允许 entry
  - `PairUnitStrategy` 在低于阈值时正确 skip 并含 reason/detail
  - `skeleton_assembler` 默认值 300 且校验 `>= 0`
- Skeleton loader 验证：`btc / eth / sol` 均正确加载 `min_seconds_to_resolution=300`
