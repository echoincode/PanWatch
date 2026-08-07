# 本地改动功能清单与移植重做方案（port / re-implementation plan）

> 生成日期: 2026-08-07
> 背景: 本地提交了 `7b7c53e`（22 文件 / +1118 -53），但 `git pull` 发现 origin/main 已领先 68 个提交，且上游做了**架构级重构**——把 `src/core/providers/` 整体迁移为独立包 `packages/marketdata/`（含 `engine.py` / `registry.py` / `ports.py` / `symbol.py` 等），并把 orchestrator/kline/quote 等概念重命名为 "routing"（如 `test_kline_routing.py`、`test_quote_routing.py`）。直接 merge 会产生 modify/delete 冲突且功能挂在已废弃路径上。
> 用途: 本文档完整记录本次所有改动的功能点、关键代码与移植指引。用户拉取上游新代码后，依据本文档在新架构（`packages/marketdata/`）上重新实现，再提交。

---

## 0. 提交与文件清单（来自 `git diff ee226b6 7b7c53e`）

```
 .env.example                                |   7 +
 .gitignore                                  |   3 +
 frontend/src/lib/logger-map.ts              |   3 +
 scripts/test_tushare_moneyflow.py           |  99 +++ (新增, 测试脚本)
 server.py                                   |  31 +-
 src/agents/daily_report.py                  |  24 +++
 src/agents/premarket_outlook.py             |  28 +++
 src/agents/tradingagents/agent.py           |  71 +++
 src/agents/tradingagents/toolkit_adapter.py |  52 +-
 src/config.py                               |   2 +
 src/core/ai_client.py                       | 110 +--
 src/core/cyq_aggregator.py                  | 142 +++ (新增)
 src/core/providers/__init__.py              |   6 +
 src/core/providers/base.py                  |  12 +
 src/core/providers/capital_flow/tushare.py  | 175 +++ (新增)
 src/core/providers/cyq/__init__.py          |   1 + (新增)
 src/core/providers/cyq/tushare.py           | 226 +++ (新增)
 src/core/providers/kline/tushare.py         |  23 +
 src/core/providers/orchestrator.py          |  25 +
 src/core/signals/signal_pack.py             | 104 +++
 src/web/api/agents.py                       |   6 +-
 src/web/stock_list.py                       |  21 +-
 22 files changed, 1118 insertions(+), 53 deletions(-)
```

按主题分 6 大块：
1. **CYQ 筹码分布**（新增 `cyq_aggregator.py` + `providers/cyq/` + 接入 signal_pack/agents/toolkit）— 见 §1
2. **Tushare 资金流向兜底**（新增 `providers/capital_flow/tushare.py` + orchestrator/kline 注册）— 见 §2
3. **盘前/日报 Tushare quote 兜底**（`signal_pack` quote 走 orchestrator + `QuoteOrchestrator` 注册 tushare）— 见 §3
4. **AI 并发信号量**（全局 `_get_ai_semaphore`，适配单并发模型）— 见 §4
5. **ETF/基金 标的支持**（`stock_list.py` 搜索扩展）— 见 §5
6. **配置 / seed / 前端映射 / 忽略规则**（`config.py` / `server.py` / `logger-map.ts` / `.gitignore`）— 见 §6

---

## 1. CYQ 筹码分布集成（核心新增）

### 1.1 `src/core/cyq_aggregator.py`（新增，142 行）
- `dataclass CyqData`: `price: float`, `ratio: float`, `cum_ratio: float`
- `dataclass CyqSummary`:
  `symbol, trade_date, current_price, avg_cost, profit_ratio, loss_ratio,
   cost_90_low, cost_90_high, concentration_90, trend, _raw_chip_count`
- `COST90_TREND_THRESHOLD_PP = 2.0`（5 日 trend 阈值）
- `aggregate(bundle, current_price, series=None) -> CyqSummary | None`:
  - **边界**：`price < current_price`（严格小于）才计入「套牢/浮亏」档；排除等于当前价档。
  - 空 `bundle.chips` 返回 `None`（防除零）。
  - `trend`：比较区间首末 `cost_90_low/high` 平均成本位移：`> +2pp`→`accumulating`(集中)，`< -2pp`→`distributing`(派发)，否则 `stable`(稳定)。
  - `current_price` 由调用方传入（不自行取价，口径统一）。

### 1.2 `src/core/providers/cyq/tushare.py`（新增，226 行）+ `__init__.py`
- `class TushareCyqProvider(CyqProvider)`（基类在 `base.py`）。
- **软依赖**：`try: import tushare ... except ImportError: _available=False`；`available` 属性返回标志；`fetch()` 不可用时返回 `success=False`，不崩。
- **私有网关**：复用 `_GatewayProClient`（与 kline/moneyflow provider 同款），支持 `TUSHARE_TOKEN` / `TUSHARE_API_URL` 环境变量 + config 注入。
- **限流**：模块级 `SEMAPHORE = asyncio.Semaphore(2)`，`fetch()` 用 `async with SEMAPHORE` 包裹网关调用。
- **单日/区间**：`extra` 含 `mode=single|range` + `days`；single 取当日 `cyq_chips`，range 取 `days` 日按 `trade_date` 升序返回 `list[CyqBundle]`。
- 返回 `ProviderResponse(success=True, data=list[CyqBundle], provider="tushare")`，`CyqBundle` 含 `symbol` / `trade_date` / `chips`。
- 代码转 `ts_code` 复用 `get_cn_prefix(sym, upper=True)`（`6xxxxx` ↔ `6xxxxx.SH/.SZ`）。

### 1.3 `base.py` — `class CyqProvider(Provider)`（新增 12 行，L137）
继承 `Provider`，协议与 `QuoteProvider` 对齐，提供 `available` 抽象属性。

### 1.4 `orchestrator.py` — `CyqOrchestrator`（TTL 300s）+ `get_cyq_orchestrator()`
- `class CyqOrchestrator(Orchestrator)`: `source_type="cyq"`, `default_ttl_sec=300.0`。
- `get_cyq_orchestrator()` 单例 + 双检锁，注册 `tushare` provider（软依赖，构建期不 import）。

### 1.5 `providers/__init__.py` — 导出 `CyqOrchestrator` / `get_cyq_orchestrator`。

### 1.6 `signal_pack.py` — `SignalPack.cyq` + `include_cyq`
- `SignalPack.cyq: dict[str, dict]`（L48）。
- `_build_cyq` 仅在 `include_cyq and market==CN` 调用；`_build_cyq` 内二次 `sym.startswith(("sh","sz","6","0","3"))` 校验。
- **current_price 取 quote 收盘价**：优先 `self._q[sym].current_price`，回退 `prev_close`（镜像 capital_flow）。
- 并发：`asyncio.gather(*[_build_cyq(sym) for sym in cn])`，`mode=range, days=5`。
- `_cyq_cache`：`symbol+date` 缓存 `CyqSummary`，避免重复计算。
- `SignalPackBuilder.__init__(include_cyq=...)`，`build_pack` 透传；盘前/日报均 `include_cyq=True`。

### 1.7 三处 Agent 渲染
- `premarket_outlook.py` / `daily_report.py`：「- 筹码：平均成本… 主力浮盈/浮亏… 90% 成本区间… 集中度… 5日筹码…」，紧跟「资金流向」段后。文案一致（浮盈/浮亏符号、trend 中文映射：集中/派发/稳定）。
- `tradingagents/agent.py`：`_collect()` 内 CN 标的并发 `asyncio.create_task(get_cyq_orchestrator().fetch(cyq_req))`，`cyq_req = mode=range, days=5`；current_price 取 `quotes.current_price` 回退 `prev_close`；`aggregate(series[-1], float(cur_price), series=series)`；`to_jsonable(summary)` → `panwatch_data["cyq"]`（经 `panwatch_data_context` 注入 toolkit）。异常仅 warning，隔离主流程。
- `tradingagents/toolkit_adapter.py`：`_cyq_to_text(cyq)`（`None`/空返回 `""`，输出 `[Chip Distribution]` 段） + `_cyq_suffix()`（读 `_cache().get("cyq")` 非空追加）。**仅在 kline/price 返回尾部拼接，未新增独立 cyq 工具分支**（L450/L459 调用）。

---

## 2. Tushare 资金流向兜底（新增 capital_flow provider）

### 2.1 `src/core/providers/capital_flow/tushare.py`（新增，175 行）
- `TushareCapitalFlowProvider(CapitalFlowProvider)`，软依赖 + 私有网关 + `asyncio.Semaphore`。
- 调用 Tushare `moneyflow` 接口，按 `ts_code` 批量，映射主力净流入/流出、超大单等。
- 注册进 `CapitalFlowOrchestrator`（与 eastmoney 主源并存，作为兜底）。
- 关键：`asyncio.Semaphore(2)` 限流，避免触发 Tushare 每分钟积分上限。

### 2.2 `orchestrator.py`（+25 行）
- `CapitalFlowOrchestrator` 注册 `tushare`（若原仅 eastmoney，则补注册）。
- 注：kline orchestrator 也补注册了 `tushare`（`providers/kline/tushare.py` +23 行，已在之前提交；本次 +23 为既有补强）。

---

## 3. 盘前/日报 Tushare Quote 兜底

### 3.1 `orchestrator.py` — `get_quote_orchestrator()` 注册 tushare
- 在 `tencent` 之后、`yfinance` 之前注册 `TushareQuoteProvider`（如上游已有 `providers/quote/tushare.py` 则直接注册；否则需新增该 provider）。
- 默认优先级 `tencent < tushare < yfinance`。

### 3.2 `signal_pack.py` — quote 环节走 Orchestrator
- `_build_quote` / `_build_kline` 改为经 `get_quote_orchestrator()` / `get_kline_orchestrator()`，由 Orchestrator 按 `DataSource` 配置故障转移。
- `build()` 升级为 `async`（调用方 `premarket_outlook` / `daily_report` 的 `_collect_agent_data` 本就是 async，2 处调用点同步升级）。
- 详见 `docs/premarket-daily-tushare-fallback.md`（盘前日报 tushare 兜底设计文档）。

---

## 4. AI 全局并发信号量（适配单并发模型）

### 4.1 `src/core/ai_client.py`（+110 / -53）
- 进程级 `_global_ai_sem: asyncio.Semaphore | None = None`。
- `_get_ai_semaphore() -> asyncio.Semaphore`：懒初始化，读 `AI_MAX_CONCURRENCY` 环境变量（默认 1），`asyncio.Semaphore(max(1, n))`。
- `chat()` / `chat_multi()` / `chat_with_tools()` 三个方法均包 `async with _get_ai_semaphore():`。
- import 增加 `asyncio`, `os`。

### 4.2 `config.py`（+2）
- `Settings` 增加 `ai_max_concurrency: int = 1`（注释：模型只支持 1 并发时设 1）。

### 4.3 `src/web/api/agents.py`（+6 / -1）
- `scan_intraday` 内原 `ai_sem = asyncio.Semaphore(3)` 改为 `ai_sem = _get_ai_semaphore()`（统一全局限流，避免绕过）。

### 4.4 `.env.example`（+7）
- 增加 `AI_MAX_CONCURRENCY` 说明占位。

---

## 5. ETF / 基金 标的支持（`stock_list.py`，+21）

- `EASTMONEY_PARAMS["fs"]` 末两项加 `m:1+t:53,m:0+t:53`（沪深 ETF 板块）。
- `_realtime_search` 内：新增 `is_fund` 判定（`Fund`/`ETF`/`LOF`/`QDII`/`HKFund` 或含 "ETF"/"基金"/"联接"）；`HKFund` 归 HK；基金类兜底归 CN；US `type_us` 允许 "5"(ETF)。
- 注释同步更新「只保留股票/基金（排除债券等）」。

---

## 6. 配置 / seed / 前端 / 忽略

### 6.1 `server.py`（+31）
`seed_data_sources()` 新增三条（均 `enabled=False, priority=10, supports_batch=False`）：
- **Tushare 资金流** `type=capital_flow`（test_symbols `["600519"]`）。
- **Tushare 筹码** `type=cyq`（test_symbols `["600519"]`）。
- 既有 **Tushare 日线** 的 config 增加 `api_url` 字段，description 补 `token/api_url 也可读环境变量 TUSHARE_TOKEN/TUSHARE_API_URL`。

### 6.2 `frontend/src/lib/logger-map.ts`（+3）
- `'src.core.providers.cyq.tushare': 'Tushare筹码'`。

### 6.3 `.gitignore`（+3）
- `docs/analysis/`（analysis 目录不提交）。

### 6.4 `scripts/test_tushare_moneyflow.py`（新增，99 行）
- Tushare moneyflow 拉取测试脚本（独立脚本，非 pytest）。

---

## 7. 上游新架构差异与移植指引（关键）

> 上游 commit `a1295b5` 把 `src/core/providers/` 整目录删除，迁移为独立包 `packages/marketdata/`。拉取新代码后，**先确认下列新模块的实际接口**，再按映射重做。

### 7.1 旧路径 → 新包映射（待拉取后核对真实文件名）

| 旧（本次改动） | 上游新位置（推测，需核对） | 移植动作 |
| --- | --- | --- |
| `src/core/providers/base.py` (`Provider`/`CyqProvider`) | `packages/marketdata/src/marketdata/ports.py` 或 `engine.py` | 把 `CyqProvider` 基类加到新 provider 协议模块 |
| `src/core/providers/orchestrator.py` (`Orchestrator`/`*Orchestrator`) | `packages/marketdata/.../engine.py` + `registry.py` | cyq 注册改为新 engine 的 provider 注册 API |
| `src/core/providers/cyq/tushare.py` | `packages/marketdata/src/marketdata/providers/cyq/tushare.py`（或新命名） | 直接搬代码，改 import 路径 + 适配新 `Provider` 基类 |
| `src/core/providers/capital_flow/tushare.py` | 新包下 capital_flow provider 目录 | 同上 |
| `src/core/providers/kline/tushare.py` | 新包下 kline provider 目录 | 同上 |
| `src/core/cyq_aggregator.py` | 保留在 `src/core/`（与 providers 解耦，不依赖市场数据引擎） | 大概率**无需改动路径**，仅 import 体现聚合层 |
| `src/core/signals/signal_pack.py` | 保留 `src/core/signals/` | `_build_cyq`/`_build_quote` 改为调用新 marketdata 包的导出（新函数名如 `get_cyq_data` / quote routing） |
| `src/agents/*` / `toolkit_adapter.py` | 保留 | 仅改 import（如 `from packages.marketdata import ...`） |

### 7.2 概念重命名（上游新命名，需核对）
- `Orchestrator` → 可能改名 `Engine` / `Router`（测试文件 `test_kline_routing.py` / `test_quote_routing.py` 暗示 "routing" 命名）。
- `get_*_orchestrator()` → 可能 `get_*_router()` / `get_*_engine()`。
- `ProviderRequest` / `ProviderResponse` / `ProviderResponse.is_empty` / `.provider` → 在新 `ports.py` 核对是否同名。
- `cache_key` 含 `extra` → 新缓存层（`packages/marketdata/cache.py`，由 `src/core/providers/cache.py` 迁移）是否保留 `extra` 进 key。

### 7.3 移植步骤建议
1. `git fetch && git pull`（接受上游 68 提交，解决任何 text 冲突——本次功能代码尚未并入，pull 后工作区是纯上游）。
2. 阅读 `packages/marketdata/` 的 `engine.py` / `registry.py` / `ports.py` / `symbol.py`，确认新 provider 注册 API 与 `Provider` 协议字段。
3. 按 §7.1 映射逐块重做：
   - 先把 `cyq_aggregator.py` 原样搬入（纯逻辑，无 provider 依赖）。
   - 在 `packages/marketdata` 新增 `cyq` provider 目录，移植 `tushare.py`（改 import + 适配新基类 + 新网关写法）。
   - 在 engine/registry 注册 `cyq` 类型（TTL 300s）与 `capital_flow`/`kline` 的 tushare 兜底。
   - `signal_pack.py` 改为调用新包的 quote/cyq 拉取接口。
   - agents / toolkit 改 import 路径。
4. `server.py` seed 三条（资金流/筹码/日线 api_url）照搬到新 `seed_data_sources`。
5. `logger-map.ts` 映射按新模块路径调整（如 `packages.marketdata.providers.cyq.tushare`）。
6. 跑 `pytest`（注意上游把 `test_kline_orchestrator.py`→`test_kline_routing.py`、`test_quote_orchestrator.py` 删除，需补 `test_cyq_routing.py` 等）。
7. 提交前确认 `docs/analysis/` 仍被忽略（`.gitignore` 已加）。

### 7.4 本次实现中需在新架构重新审视的点
- **设计一致性待确认项**（见 `docs/cyq-integration-review.md` 项 A）：`signal_pack._build_cyq` 直接调 `get_cyq_orchestrator()` 而非走 `_source_policy`，与 capital_flow/news/events 不统一。新架构建议统一走新 routing 的开关控制。
- **项 B**：深度分析 `agent.py` 的 cyq 未受 `DataSource` 禁用控制，消耗积分。新架构建议读配置统一开关。
- **AI 信号量**：`ai_client.py` 改动与 providers 重构无关，应可**直接 cherry-pick / 原样保留**（除非上游也改了 `ai_client.py`）。
- **ETF 支持**（`stock_list.py`）：与 providers 无关，应可**直接保留/原样重做**。

---

## 8. 可直接复用的资产（与上游重构无关，优先保留）

| 文件 | 复用度 | 说明 |
| --- | --- | --- |
| `src/core/cyq_aggregator.py` | ⭐⭐⭐ 高 | 纯算法，无 provider 依赖，原样可用 |
| `src/core/ai_client.py`（`_get_ai_semaphore`） | ⭐⭐⭐ 高 | 除非上游也改 ai_client，否则可直接保留 |
| `src/config.py`（`ai_max_concurrency`） | ⭐⭐⭐ 高 | 独立配置项 |
| `src/web/stock_list.py`（ETF 支持） | ⭐⭐⭐ 高 | 独立功能，原样重做 |
| `src/web/api/agents.py`（ai_sem 统一） | ⭐⭐⭐ 高 | 依赖 `ai_client._get_ai_semaphore` |
| `scripts/test_tushare_moneyflow.py` | ⭐⭐⭐ 高 | 独立脚本 |
| `.gitignore`（`docs/analysis/`） | ⭐⭐⭐ 高 | 独立 |
| `frontend/src/lib/logger-map.ts` | ⭐⭐ 中 | 仅模块路径需随新架构调整 |
| `providers/cyq/tushare.py` 等 | ⭐⭐ 中 | 逻辑可复用，import/基类需适配新包 |
| `signal_pack.py` / `agents/*` | ⭐⭐ 中 | 调用点需改 import 到新 marketdata 包 |

---

## 9. 提交操作回放（供参考，非本次执行）

当前本地状态：
- 已提交 `7b7c53e`（22 文件），工作区干净。
- 本地 main 与 origin/main **分叉**：本地 1 提交，上游 68 提交。
- 已 `git merge --abort`（或 merge 状态已清除），无进行中合并。

**不要**直接 `git push`（会被拒，且不应 `--force`）。正确流程（拉取新代码后）：
```bash
git fetch origin
git pull origin main            # 接受上游 68 提交（纯上游，无本地功能混入）
# 按 §7 在新架构上重做功能
git add <重做后的文件>
git commit -m "feat: 在新 marketdata 架构上重做 CYQ/tushare 兜底/AI 限流/ETF 支持"
git push origin main
```
若希望保留 `7b7c53e` 历史，可在 pull 后 `git checkout 7b7c53e -- <文件>` 取回旧实现作参考，再移植。
