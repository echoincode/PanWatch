import base64
import logging
import os
from asyncio import Semaphore
from pathlib import Path

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

# 全局并发信号量：限制对整个 LLM 后端的并发调用数（账号级配额共享）。
# 上限由环境变量 AI_MAX_CONCURRENCY 控制，默认 4。模块级单例，所有 AIClient
# 实例共享，确保进程内任意来源的 AI 调用总量不超过该上限。
_AI_SEMAPHORE: Semaphore | None = None
_DOTENV_LOADED = False


def _load_dotenv() -> None:
    """轻量加载项目根目录 .env 到 os.environ（仅填充缺失项，不覆盖已设变量）。

    项目未引入 python-dotenv，这里用标准库解析，避免新增依赖。仅做一次。
    """
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.is_file():
        return
    try:
        with env_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip('"').strip("'")
                os.environ.setdefault(key, val)
    except OSError as e:
        logger.warning(f"读取 .env 失败: {e}")


def _get_ai_semaphore() -> Semaphore:
    global _AI_SEMAPHORE
    if _AI_SEMAPHORE is None:
        _load_dotenv()
        cap = int(os.getenv("AI_MAX_CONCURRENCY", "4") or "4")
        _AI_SEMAPHORE = Semaphore(max(1, cap))
    return _AI_SEMAPHORE


class AIClient:
    """OpenAI 协议兼容的 AI 客户端"""

    def __init__(self, base_url: str, api_key: str, model: str = "", proxy: str = ""):
        kwargs = {
            "base_url": base_url,
            "api_key": api_key,
        }
        if proxy:
            kwargs["http_client"] = None  # TODO: 如需代理，用 httpx 配置
        self.client = AsyncOpenAI(**kwargs)
        # 保留原始配置作为实例属性,供需要桥接到第三方 LLM 框架的 agent 使用
        # (e.g. TradingAgents 需要 base_url+api_key 重新构造 langchain 的 LLM)
        self.base_url = base_url
        self.api_key = api_key
        self.model = model
        self.total_tokens_used = 0

    async def chat(
        self,
        system_prompt: str,
        user_content: str,
        images: list[str] | None = None,
        temperature: float | None = 0.4,
        symbol: str | None = None,
        market: str | None = None,
        current_price: float | None = None,
        force_cyq: bool = False,
    ) -> str:
        """
        调用 LLM 获取文本回复。

        Args:
            system_prompt: 系统提示词
            user_content: 用户输入内容
            images: 图片路径列表（用于多模态，可选）
            temperature: 生成温度
            symbol: 标的代码（可选）；传入时自动注入 CYQ 筹码上下文（见 cyq.py）
            market: 市场代码 CN/HK/US（可选，配合 symbol）
            current_price: 当前价（可选，配合 symbol 计算获利盘）
            force_cyq: 是否强制刷新 CYQ 缓存（默认 False，同日复用）
        """
        # 自动注入筹码上下文（可选、降级安全；在 AI 信号量之外执行，避免死锁）
        if symbol:
            from src.core.cyq import fetch_cyq_text, CYQ_ANALYSIS_HINT

            cyq = await fetch_cyq_text(
                symbol, market, current_price, force=force_cyq
            )
            if cyq:
                user_content = (
                    user_content + "\n\n" + cyq + "\n" + CYQ_ANALYSIS_HINT
                )

        messages = [
            {"role": "system", "content": system_prompt},
        ]

        # 构建 user message
        if images:
            content_parts = [{"type": "text", "text": user_content}]
            for img_path in images:
                img_data = self._encode_image(img_path)
                if img_data:
                    content_parts.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{img_data}"}
                    })
            messages.append({"role": "user", "content": content_parts})
        else:
            messages.append({"role": "user", "content": user_content})

        try:
            create_kwargs = {"model": self.model, "messages": messages}
            if temperature is not None:
                create_kwargs["temperature"] = temperature
            async with _get_ai_semaphore():
                response = await self.client.chat.completions.create(**create_kwargs)
            # 记录 token 用量
            if response.usage:
                self.total_tokens_used += response.usage.total_tokens
                logger.debug(
                    f"Token usage: {response.usage.prompt_tokens} + "
                    f"{response.usage.completion_tokens} = {response.usage.total_tokens}"
                )

            return response.choices[0].message.content or ""

        except Exception as e:
            logger.error(f"AI 调用失败: {e}")
            raise

    async def chat_multi(
        self,
        messages: list[dict],
        temperature: float = 0.4,
    ) -> str:
        """
        多轮对话：传入完整 messages 列表。

        Args:
            messages: [{"role": "system"/"user"/"assistant", "content": "..."}]
            temperature: 生成温度
        """
        try:
            async with _get_ai_semaphore():
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=temperature,
                )
            if response.usage:
                self.total_tokens_used += response.usage.total_tokens
                logger.debug(
                    f"Token usage: {response.usage.prompt_tokens} + "
                    f"{response.usage.completion_tokens} = {response.usage.total_tokens}"
                )
            return response.choices[0].message.content or ""
        except Exception as e:
            logger.error(f"AI 多轮对话调用失败: {e}")
            raise

    async def chat_with_tools(
        self,
        messages: list[dict],
        tools: list[dict],
        temperature: float = 0.4,
    ):
        """带 tool use 的对话调用，返回原始 message 对象。"""
        try:
            async with _get_ai_semaphore():
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=tools,
                    temperature=temperature,
                )
            if response.usage:
                self.total_tokens_used += response.usage.total_tokens
            return response.choices[0].message
        except Exception as e:
            logger.error(f"AI tool use 调用失败: {e}")
            raise

    async def list_models(self) -> list[str]:
        """通过 OpenAI 兼容的 /v1/models 拉取可用模型 id 列表。"""
        resp = await self.client.models.list()
        return sorted(m.id for m in resp.data)

    def _encode_image(self, image_path: str) -> str | None:
        """将图片文件编码为 base64"""
        path = Path(image_path)
        if not path.exists():
            logger.warning(f"图片不存在: {image_path}")
            return None
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
