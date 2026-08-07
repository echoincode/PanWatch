"""CYQ 筹码分布查询（轻量）。

仅用于「调用 AI 时自动注入筹码上下文」，不在数据构建层扩散。
设计约束见 docs/analysis/ai-client-converged-design.md §2。

- 软依赖 tushare：未安装或无 TUSHARE_TOKEN 时静默返回 None，不影响主流程。
- token 与 API 地址均从 .env 读取：TUSHARE_TOKEN（必填）、TUSHARE_API_URL
  （可选，默认官方地址；可指向第三方代理/网关以支持代理转发）。
- 仅 A 股（CN）有筹码分布数据；其余市场直接返回 None。
- 独立信号量（不共用 AI 信号量），避免与 AI 调用互相持有导致死锁。
- 任何异常均吞掉并返回 None，保证降级安全。
- 自适应 TTL 缓存（18:00 前缓存到当日 18:00，18:00 后缓存 24h）：
  保证盘前查询拿到最新交易日数据；同一交易日内后续查询复用缓存；
  次日缓存自动过期，拉取新数据。`force=True` 可强制刷新。
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import tushare as ts  # type: ignore

    _TUSHARE_AVAILABLE = True
except ImportError:  # pragma: no cover - 依赖可选
    _TUSHARE_AVAILABLE = False

# 独立于 AI 信号量，限制 tushare 查询并发（避免积分耗尽 + 不阻塞 AI 主流程）。
_CYQ_SEM: asyncio.Semaphore | None = None


def _get_cyq_sem() -> asyncio.Semaphore:
    global _CYQ_SEM
    if _CYQ_SEM is None:
        _CYQ_SEM = asyncio.Semaphore(2)
    return _CYQ_SEM


# 近 N 日对比筹码重心趋势；重心变化阈值（百分点）映射三档。
_CYQ_TREND_DAYS = 5
_CYQ_TREND_THRESHOLD_PP = 2.0

# 区间列名形如 "8.5_11.2"（价格下限_价格上限），标识筹码占比。
_INTERVAL_COL_RE = re.compile(r"^\s*([\d.]+)[_~]([\d.]+)\s*$")

# tushare 官方 API 地址；可通过 .env 的 TUSHARE_API_URL 指向第三方代理/网关。
_TUSHARE_DEFAULT_URL = "http://api.tushare.pro"

# --- 缓存 -----------------------------------------------------------------
# 按 ts_code 缓存文本 + 过期时间戳。
# TTL 自适应：18:00 前缓存到当日 18:00（CYQ 每日 18:00 后更新新数据），
# 18:00 后缓存 24h（下一个 18:00 前数据不变）。
# 保证：盘前查询拿到 T-1 数据后缓存至当日 18:00，同日复用；次日 18:00 前若缓存已过期
# （或 force=True）自动拉取最新交易日数据，不会拿过期缓存。
_CYQ_CACHE: dict[str, tuple[str, float]] = {}  # ts_code -> (text, expire_ts)

# CYQ 数据每日 18:00 左右更新当日分布；此时间点为经验值，非严格保证。
_CYQ_UPDATE_HOUR = 18

# 复用 AIClient 的轻量 .env 加载（幂等），确保 TUSHARE_TOKEN / TUSHARE_API_URL
# 一定从项目根 .env 读取，不依赖调用时序。
from src.core.ai_client import _load_dotenv  # noqa: E402

# 注入给 AI 的分析引导指令（方案 A：零侵入触发 AI 分析筹码，见设计文档 §2.6）。
CYQ_ANALYSIS_HINT = (
    "请结合上方筹码分布（成本主要区间、当前价下方获利盘比例、近期筹码趋势）"
    "分析当前持仓成本结构与多空博弈，并在结论中给出简要判断。"
)


def _to_ts_code(symbol: str, market: Optional[str]) -> Optional[str]:
    """将内部 symbol 转为 tushare ts_code（如 600000.SH）。

    仅 CN 市场可转换；其余返回 None。symbol 也可能已带交易所前缀，做兜底。
    """
    if not symbol:
        return None
    sym = symbol.strip().upper()
    # 已是 ts_code 形态（含 .SH/.SZ/.BJ）
    if re.search(r"\.(SH|SZ|BJ)$", sym):
        return sym

    mkt = (market or "").upper()
    if mkt and mkt != "CN":
        return None
    # 无 market 信息时，按 6 位数字 A 股规则推断；否则不处理
    if not mkt:
        if not re.fullmatch(r"\d{6}", sym):
            return None
    from src.core.cn_symbol import get_cn_prefix

    prefix = get_cn_prefix(sym, upper=True)  # SH / SZ / BJ
    return f"{sym}.{prefix}"


def _cyq_enabled() -> bool:
    """是否启用筹码注入：需 tushare 可用、有 token、且开关未关闭。

    token / url 均从项目根 .env 读取（见 _load_dotenv）。
    """
    if not _TUSHARE_AVAILABLE:
        logger.debug("CYQ 未启用: tushare 未安装")
        return False
    if os.getenv("CYQ_INJECT", "1") == "0":
        logger.debug("CYQ 未启用: CYQ_INJECT=0")
        return False
    _load_dotenv()
    if not os.getenv("TUSHARE_TOKEN"):
        logger.debug("CYQ 未启用: 未配置 TUSHARE_TOKEN")
        return False
    return True


def _compute_cache_ttl() -> float:
    """计算缓存 TTL（秒），自适应 18:00 更新时间。

    - 18:00 前：缓存到当日 18:00（新数据约 18:00 后出）
    - 18:00 后：缓存 24h（下一个 18:00 前数据不变）
    """
    now = time.time()
    now_local = time.localtime(now)
    if now_local.tm_hour < _CYQ_UPDATE_HOUR:
        # 距当日 18:00 的秒数
        update_ts = time.mktime(
            (
                now_local.tm_year,
                now_local.tm_mon,
                now_local.tm_mday,
                _CYQ_UPDATE_HOUR,
                0,
                0,
                now_local.tm_isdst,
            )
        )
        return max(update_ts - now, 300.0)  # 至少 5 分钟
    # 18:00 后缓存 24h
    return 86400.0


def _cache_get(ts_code: str) -> Optional[str]:
    """尝试命中缓存。命中返回文本；未命中或过期返回 None。"""
    entry = _CYQ_CACHE.get(ts_code)
    if not entry:
        return None
    text, expire_ts = entry
    if time.time() > expire_ts:
        _CYQ_CACHE.pop(ts_code, None)
        logger.debug(f"CYQ 缓存过期: {ts_code}")
        return None
    logger.debug(f"CYQ 缓存命中: {ts_code}")
    return text


def _cache_set(ts_code: str, text: str) -> None:
    """写入缓存，TTL 自适应。"""
    ttl = _compute_cache_ttl()
    expire = time.time() + ttl
    _CYQ_CACHE[ts_code] = (text, expire)
    logger.info(
        f"CYQ 缓存写入: {ts_code} TTL={ttl / 3600:.1f}h"
    )


async def fetch_cyq_text(
    symbol: str,
    market: Optional[str] = None,
    current_price: Optional[float] = None,
    force: bool = False,
) -> Optional[str]:
    """查 tushare 筹码分布，返回可直接拼进 prompt 的紧凑文本；失败/非 CN/无 token 返回 None。

    Args:
        symbol: 内部股票代码（如 600000）。
        market: 市场代码（CN/HK/US）；为空时按 6 位数字推断。
        current_price: 当前价；用于计算获利盘比例与边界。为空时用 daily 最新收盘兜底。
        force: 是否强制刷新（跳过缓存）。False 时若同日已有缓存则直接返回。

    缓存策略（自适应 18:00 更新时间）：
        - 18:00 前首次查询：缓存到当日 18:00（新数据约 18:00 后出），同日复用。
        - 18:00 后首次查询：缓存 24h（下一个 18:00 前数据不变）。
        - 次日 18:00 前缓存已过期，自动拉取最新交易日数据，不会拿旧数据。
        - force=True 跳过缓存。
    """
    if not _cyq_enabled():
        return None
    ts_code = _to_ts_code(symbol, market)
    if not ts_code:
        return None

    # 1) 尝试命中缓存（force=True 跳过）
    if not force:
        cached = _cache_get(ts_code)
        if cached is not None:
            return cached

    _load_dotenv()
    token = os.getenv("TUSHARE_TOKEN") or ""
    api_url = os.getenv("TUSHARE_API_URL") or _TUSHARE_DEFAULT_URL

    async with _get_cyq_sem():
        try:
            pro = ts.pro_api(token)
            pro.api_url = api_url
            logger.info(f"CYQ 查询开始: {symbol} ({ts_code}) -> {api_url}")
            # 1) 近 N+1 日筹码分布（用于成本重心趋势）
            chips = await asyncio.to_thread(
                pro.cyq_chips,
                ts_code=ts_code,
                fields="",
                n_days=_CYQ_TREND_DAYS + 1,
            )
            if chips is None or chips.empty:
                logger.warning(f"CYQ 无数据: {symbol} ({ts_code})")
                return None
            if "date" in chips.columns:
                chips = chips.sort_values("date")

            # 提取最新 trade_date（仅用于日志）
            latest_trade_date = str(chips["date"].iloc[-1])

            # 2) 当前价兜底：取 daily 最新收盘
            if current_price is None:
                daily = await asyncio.to_thread(
                    pro.daily,
                    ts_code=ts_code,
                    fields="close",
                    start_date="",
                    end_date="",
                )
                if daily is not None and not daily.empty:
                    current_price = float(daily.iloc[0]["close"])

            text = _format_cyq(chips, current_price)
            if text:
                logger.info(
                    f"CYQ 注入成功: {symbol} ({latest_trade_date}) | {text}"
                )
                # 3) 写入缓存（自适应 TTL）
                _cache_set(ts_code, text)
            return text
        except Exception as e:  # 降级：任何异常都不影响 AI 主流程
            logger.warning(f"CYQ 查询失败 {symbol}: {e}")
            return None


def _parse_intervals(row) -> list[tuple[float, float, float]]:
    """从一行 cyq_chips 解析出 (low, high, pct) 区间列表。"""
    intervals: list[tuple[float, float, float]] = []
    for col in row.index:
        m = _INTERVAL_COL_RE.match(str(col))
        if not m:
            continue
        try:
            low = float(m.group(1))
            high = float(m.group(2))
            pct = float(row[col])
        except (TypeError, ValueError):
            continue
        if pct <= 0:
            continue
        intervals.append((low, high, pct))
    return intervals


def _centroid(intervals: list[tuple[float, float, float]]) -> Optional[float]:
    """成本重心（按区间中点的占比加权）。"""
    total = sum(p for _, _, p in intervals)
    if total <= 0:
        return None
    return sum((low + high) / 2 * p for low, high, p in intervals) / total


def _format_cyq(chips_df, current_price: Optional[float]) -> Optional[str]:
    """把 cyq_chips 的多日数据聚合为紧凑文本。

    cyq_chips 每行返回若干「价格下限_价格上限」列，值为该区间筹码占比。
    聚合：成本主要区间（累计 60%）、当前价下方获利盘、近 N 日成本重心趋势。
    """
    latest = chips_df.iloc[-1]
    intervals = _parse_intervals(latest)
    if not intervals:
        return None

    total_pct = sum(p for _, _, p in intervals)
    if total_pct <= 0:
        return None

    # 成本主要区间：累计占比达到 60% 的范围（从最低价档向上累计）
    intervals.sort(key=lambda x: x[0])
    cum = 0.0
    main_low = intervals[0][0]
    main_high = intervals[0][1]
    for low, high, pct in intervals:
        cum += pct
        main_high = max(main_high, high)
        if cum >= 60.0:
            break

    # 获利盘比例：当前价下方筹码占比
    profit_pct: Optional[float] = None
    if current_price is not None:
        profit = sum(p for low, high, p in intervals if high < current_price)
        profit_pct = round(profit / total_pct * 100, 1)

    main_range = f"{main_low:.2f}~{main_high:.2f} 元"

    # 近 N 日成本重心趋势
    trend_text = ""
    if len(chips_df) >= 2:
        prev = chips_df.iloc[0]
        prev_intervals = _parse_intervals(prev)
        c0, c1 = _centroid(prev_intervals), _centroid(intervals)
        if c0 and c1:
            delta_pp = (c1 - c0) / c0 * 100  # 重心相对变化（百分点近似）
            if delta_pp > _CYQ_TREND_THRESHOLD_PP:
                trend = "趋集中（成本重心上移）"
            elif delta_pp < -_CYQ_TREND_THRESHOLD_PP:
                trend = "趋发散（成本重心下移）"
            else:
                trend = "相对平稳"
            trend_text = f"近 {_CYQ_TREND_DAYS} 日筹码{trend}。"

    if profit_pct is not None:
        return (
            f"筹码：成本主要分布在 {main_range}（占比约 60%），"
            f"当前价下方获利盘约 {profit_pct}%。{trend_text}"
        )
    return f"筹码：成本主要分布在 {main_range}（占比约 60%）。{trend_text}"
