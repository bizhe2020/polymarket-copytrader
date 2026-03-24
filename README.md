# Polymarket Copy Trader

这是一个面向 `Polymarket` 的秒级跟单原型，目标是监控某个公开账户的成交，并在满足风控条件时双向跟单。

当前版本已经实现：

- 通过公开 `profile URL / @handle / wallet` 解析目标账户
- 轮询 `Data API /activity` 监听目标账户的 `TRADE`
- 成交去重、断点续跑、JSONL 落盘
- 按风控规则生成 `BUY/SELL` 跟单决策
- `paper` 模式下按盘口撮合模拟成交与持仓
- 预留真实下单器，优先接官方 `py-clob-client`
- 历史回放模式，先验证策略再考虑实盘
- 多账户评估器，可抓近期窗口成交，按延迟和撮合假设输出收益率曲线与复制还原度

当前版本尚未自动解决：

- Polymarket 没有公开“订阅任意第三方用户成交”的 WebSocket，外部账户只能靠高频轮询
- `@guh123` 这类 profile handle 未在官方 profile API 中显式作为独立字段暴露，自动解析可能失败，必要时需要手工提供钱包地址
- 真实下单需要你提供账户私钥、正确的 `signature_type`、`funder/proxy wallet`，并确保账户已完成 allowance / 资金准备
- 若你所在地区被 geoblock，CLOB 下单会直接失败

## 官方接口依据

- `Gamma API /public-search` 可搜索公开 profile，返回 `proxyWallet`
- `Gamma API /public-profile` 可按钱包读取公开资料
- `Data API /activity` 可按 `user`、`type`、`side` 过滤成交活动
- `Data API /trades` 可用于历史分析
- `CLOB /book` 可读取 token 的订单簿
- 官方 `py-clob-client` 支持 `create_or_derive_api_creds()`、`create_market_order()`、`post_order()`

## 目录

- `configs/sample.json`: 示例配置
- `var/`: 运行态目录，保存状态和日志
- `polymarket_copytrader/`: 主代码

## 快速开始

1. 进入项目目录

```bash
cd /Users/laoji/Documents/projects/polymarket-copytrader
```

2. 复制并修改配置

```bash
cp configs/sample.json configs/local.json
```

3. 先跑解析和模拟模式

```bash
python3 -m polymarket_copytrader.cli resolve-target --config configs/local.json
python3 -m polymarket_copytrader.cli run --config configs/local.json --once
```

4. 回放最近的历史成交

```bash
python3 -m polymarket_copytrader.cli backfill --config configs/local.json --limit 200
python3 -m polymarket_copytrader.cli replay --config configs/local.json --input var/backfill.jsonl
```

5. 跑双账户分钟级评估

```bash
python3 -m polymarket_copytrader.cli evaluate --config configs/eval-two-accounts.json
```

## 真实下单

默认是 `paper` 模式。要切到真实下单：

1. 安装官方 SDK

```bash
python3 -m pip install "py-clob-client>=0.25.0"
```

2. 配置环境变量

```bash
export POLY_PRIVATE_KEY=...
export POLY_FUNDER=...
```

按你的账户类型设置：

- `signature_type = 0`: EOA 浏览器钱包
- `signature_type = 1` 或 `2`: Polymarket 代理钱包 / 智能钱包

然后把配置中的 `execution.mode` 从 `paper` 改成 `live`。

## 运行命令

```bash
python3 -m polymarket_copytrader.cli resolve-target --config configs/local.json
python3 -m polymarket_copytrader.cli doctor --config configs/local.json
python3 -m polymarket_copytrader.cli run --config configs/local.json
python3 -m polymarket_copytrader.cli run --config configs/local.json --once
python3 -m polymarket_copytrader.cli backfill --config configs/local.json --limit 500
python3 -m polymarket_copytrader.cli replay --config configs/local.json --input var/backfill.jsonl
python3 -m polymarket_copytrader.cli evaluate --config configs/eval-two-accounts.json
python3 -m polymarket_copytrader.cli live-paper --config configs/live-paper-two-accounts.json
```

## 策略说明

默认策略已经收敛成 `BUY + redeem-only`：

- 默认只跟 `BUY`，不跟目标账户的 `SELL`
- 默认示例参数已经按 `@guh123` 最近的小额高频成交做过下调，`min_target_usdc_size = 5`
- 只跟单不低于 `min_target_usdc_size` 的目标成交
- `BUY` 用当前 `best_ask`，`SELL` 用当前 `best_bid`
- 只在当前成交价格相对目标成交价格的偏离不超过 `max_slippage_bps` 时跟单
- 对单个 token 设置最大名义仓位 `max_position_per_asset_usdc`
- 每次跟单使用 `fixed_order_usdc`，或使用目标成交金额乘以 `follow_fraction`
- 默认评估口径是 `exit_mode = redeem`，即如果没有卖出信号，就持有到市场结算按 `1/0` 兑付
- 实时 `paper` 默认执行策略是 `IOC`

## Paper 撮合模型

这版 `paper` 模式不再默认“看到就成交”，而是更接近实盘：

- 实时 `run` 模式：先观测目标账户成交，再优先读取 `market websocket` 的实时盘口；拿不到时再退回 `CLOB /book` 和 synthetic fallback
- `BUY` 逐档吃 `asks`，`SELL` 逐档吃 `bids`
- `FOK` 要求整笔都能吃到，否则整笔取消
- `IOC` 允许部分成交，未成交部分直接放弃
- `paper_filled / paper_partial / paper_unfilled` 会写入事件日志，方便你排查“为什么没复制到”

这意味着 `paper` 收益率会比旧版更保守，但也更适合回答“这套复制器接近实盘后还能剩多少 edge”。

`IOC` 和 `FOK` 的建议：

- 你当前这个场景更适合 `IOC`
- 原因是短周期市场里，“部分成交”通常比“整笔完全错过”更接近真实复制目标
- 如果后面发现盘口过薄、`IOC` 经常把你带到太差的价格，再切回 `FOK`

## 为什么“秒级”不等于“同价复制”

即使你把轮询频率设为 1 秒，仍然存在：

- 你看到的是目标账户“已成交”后的公开记录
- 你自己的下单价格取决于你观察到时的订单簿
- 热门市场里，1 秒足够把盘口吃穿

因此这个项目更适合作为“秒级反应 + 强风控”的跟单系统，而不是“无滑点复制器”。

## 评估器

`configs/eval-two-accounts.json` 默认会：

- 解析 `@guh123` 和 `@blue-walnut`
- 抓近 `7` 天成交活动
- 按多个延迟场景执行复制
- 支持 `BUY/SELL` 两侧模拟
- 用延迟后的参考价构建合成盘口快照
- 按 `IOC/FOK` 撮合逻辑模拟是否能成交、是否只成交一部分
- 用 `1m` 历史价格做净值曲线估值
- 输出 `target_reference / copy_reference_0m / cn_vpn_base / cn_vpn_conservative / overseas_base`
- 输出：
  - `var/eval_two_accounts/summary.json`
  - `var/eval_two_accounts/equity_curve.csv`
  - `var/eval_two_accounts/fills.jsonl`

组合层面默认总本金是 `10000 USDC`，两个账户等权各分配 `5000 USDC`。
同时也会输出每个账户单独以 `10000 USDC` 起始资金回放的结果，便于横向比较。

场景含义：

- `target_reference`: 用目标账户成交价作为参考，并给足深度，接近“理论上限”
- `copy_reference_0m`: 你的复制引擎零延迟基线，默认 `5s + FOK`
- `cn_vpn_base`: 中国 IP 挂 VPN 的基础情形，默认 `45s + 35bps + IOC`
- `cn_vpn_conservative`: 更保守的中国 VPN 情形，默认 `120s + 80bps + IOC`
- `overseas_base`: 海外部署的低延迟近似，默认 `8s + 10bps + FOK`

核心参数：

- `observation_delay_seconds`: 你在目标成交后多久才看见并开始跟单
- `extra_slippage_bps`: 在参考价基础上额外拉宽盘口，模拟更差的到达价格
- `execution_policy`: `FOK` 或 `IOC`
- `synthetic_levels`: 合成盘口档位数
- `synthetic_depth_multiplier`: 合成盘口总深度相对目标单量的倍数
- `use_target_trade_price`: 是否直接用目标成交价作为参考价
- `exit_mode`: `redeem` 表示默认持有到结算，`mark_to_market` 表示只按窗口末尾市价估值
- `max_trades_per_target`: 每个账户最多保留最近 N 笔成交，适合对齐 `1k / 10k` 样本回测

看还原度时，优先比较：

- `cn_vpn_*` 相对 `copy_reference_0m` 的 `pnl_capture_ratio`
- `copy_reference_0m` 相对 `target_reference` 的偏差

这样能把“你自己的复制系统损耗”和“分钟级离线回放本身的建模误差”分开看。

对 `eth-updown-15m-1774157400` 这类短周期市场，评估器会从 slug 里的 `15m + start timestamp` 推导结算时点，再用结算后价格接近 `1` 的 outcome 作为赢家做 `redeem` 兑付。

如果当前环境访问 Polymarket 超时，评估器会自动回退到 `var/eval_two_accounts/cache` 里的已缓存成交和价格继续跑。离线结果是否可信，取决于缓存窗口是不是足够新、足够完整。

长窗口抓数时，成交分页进度会落到 `cache/progress/trades/<target>.json`，每成功抓到一页就会把最新 `cursor_start / offset / cached_trades` 写盘。即使中断，下次也会优先从这个进度继续，而不是把前面已抓到的几千笔重跑一遍。

## 实时 Paper

`configs/live-paper-two-accounts.json` 提供了一个双账户实时 `paper` 入口：

- 跟买 `@guh123` 和 `@blue-walnut`
- 默认运行 `24` 小时
- 只跟 `BUY`
- 优先使用官方 `market websocket` 缓存实时盘口
- 默认开启 `require_real_order_book = true`，拿不到真实盘口就直接跳过，不再用 synthetic fallback 假装成交
- 到期市场按 `redeem` 处理
- 每小时把净值写到 `var/live_paper_two_accounts/hourly_stats.csv`

如果你要启用 `market websocket`，先安装实时依赖：

```bash
pip install -e '.[realtime]'
```

启动命令：

```bash
python3 -m polymarket_copytrader.cli live-paper --config configs/live-paper-two-accounts.json
```
