"""门铃：把两个目录当成收发台，新单落地就推一条消息。

两个方向，各配一个目录与一个会话：

  request 目录：新请求落地 → 通知处理方
  done 目录：   回执落地   → 通知发起方

它只负责"叫一声"。推过去的是一条平台消息，不会凭空制造一轮 agent 推理——
AstrBot 的 WebChat 回复要挂在请求上，伪造事件会把对端的事件链搅乱（踩过，别再来）。
真要唤醒，请用官方 `POST /api/v1/chat`。

契约（单子是一个 JSON 文件，字段自选）：

  request: {"id": "20261102-1", "intent": "...", "action": "...", "target": "...", "note": "..."}
  done:    {"id": "20261102-1", "ok": true, "result": "..."}

缺字段就用占位符，不报错、不拒收。
"""
import asyncio
import json
import os
import time
from pathlib import Path

from astrbot.api import logger, AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

try:
    from astrbot.core.message.message_event_result import MessageChain
except Exception:  # 兼容旧版
    from astrbot.api.event import MessageChain  # type: ignore

try:
    from astrbot.api.message_components import Plain
except Exception:
    from astrbot.core.message.components import Plain  # type: ignore

DEFAULT_REQ = "handoff/request"
DEFAULT_DONE = "handoff/done"


def _data_root() -> Path:
    """相对目录挂在 AstrBot 数据目录下，别挂在启动时的工作目录。"""
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path  # type: ignore
        return Path(get_astrbot_data_path())
    except Exception:
        pass
    try:
        from astrbot.core.star.star_tools import StarTools  # type: ignore
        return Path(StarTools.get_data_dir("handoff_watch")).parent
    except Exception:
        return Path.cwd() / "data"


@register("handoff_watch", "门铃", "请求单与回执落地就通知，两个目录当收发台", "0.3.0")
class HandoffWatch(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}
        self.enabled: bool = bool(self._cfg("enable", True))
        self.interval: float = float(self._cfg("interval_seconds", 1.0) or 1.0)
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._seen = {"request": set(), "done": set()}

    async def initialize(self) -> None:
        if not self.enabled:
            logger.info("[handoff_watch] 未启用（enable=false），跳过")
            return
        req, done = self._dir("request"), self._dir("done")
        for path in (req, done):
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as exc:
                logger.warning("[handoff_watch] 建目录失败 %s: %s", path, exc)
        if not self._target("request") and not self._target("done"):
            logger.warning("[handoff_watch] 两个方向都没有配置目标会话，门铃不会响")
        self._task = asyncio.create_task(self._loop())
        logger.info("[handoff_watch] 已启动：%s ⇄ %s，每 %ss 看一眼",
                    req, done, round(self.interval, 2))

    async def terminate(self) -> None:
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("[handoff_watch] 已停止")

    # ---------- 配置 ----------

    def _cfg(self, key: str, default=None):
        try:
            return self.config.get(key, default)
        except Exception:
            return default

    def _dir(self, kind: str) -> str:
        raw = str(self._cfg(f"{kind}_dir", "") or "").strip()
        if not raw:
            raw = DEFAULT_REQ if kind == "request" else DEFAULT_DONE
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = _data_root() / path
        return str(path)

    def _target(self, kind: str) -> str:
        return str(self._cfg("target_umo" if kind == "request" else "reply_umo", "") or "").strip()

    # ---------- 轮询 ----------

    def _scan(self, kind: str) -> set[str]:
        try:
            return {f for f in os.listdir(self._dir(kind)) if f.endswith(".json")}
        except FileNotFoundError:
            return set()
        except Exception as exc:
            logger.warning("[handoff_watch] 扫描 %s 失败: %s", kind, exc)
            return set()

    async def _loop(self) -> None:
        # 启动时先把现有文件记为已见，别把历史单子全推一遍
        for kind in ("request", "done"):
            self._seen[kind] = self._scan(kind)
        while not self._stop.is_set():
            try:
                for kind in ("request", "done"):
                    current = self._scan(kind)
                    for name in sorted(current - self._seen[kind]):
                        await self._notify(kind, name)
                    self._seen[kind] = current
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("[handoff_watch] 轮询异常: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass

    # ---------- 通知 ----------

    def _render(self, kind: str, doc: dict, name: str) -> str:
        ticket = doc.get("id", name)
        if kind == "request":
            return "\n".join([
                "来了一张新单，需要确认。",
                f"事项：{doc.get('intent', '(未写)')}",
                f"动作：{doc.get('action', '?')} → {doc.get('target', '?')}",
                f"原因：{doc.get('note', '(未写)')}",
                f"单号：{ticket}",
            ])
        ok = doc.get("ok")
        head = "回执到了，办成了。" if ok is True else ("回执到了，没办成。" if ok is False else "回执到了。")
        return "\n".join([head, f"结果：{doc.get('result', '(未写)')}", f"单号：{ticket}"])

    async def _notify(self, kind: str, name: str) -> None:
        path = os.path.join(self._dir(kind), name)
        try:
            with open(path, encoding="utf-8") as fh:
                doc = json.load(fh)
            if not isinstance(doc, dict):
                raise ValueError("top level is not an object")
        except Exception as exc:
            logger.warning("[handoff_watch] 读取 %s 失败: %s", name, exc)
            return

        target = self._target(kind)
        if not target:
            logger.warning("[handoff_watch] %s 没有目标会话，只记下: %s", kind, name)
            return

        text = self._render(kind, doc, name)
        try:
            await self.context.send_message(target, MessageChain([Plain(text)]))
            logger.info("[handoff_watch] 响过：%s %s -> %s", kind, name, target)
        except Exception as exc:
            logger.error("[handoff_watch] 推送失败: %s", exc)
        # 只通知，不伪造事件。需要真唤醒请走官方 /api/v1/chat。

    # ---------- 指令 ----------

    @filter.command("handoff")
    async def handoff_cmd(self, event: AstrMessageEvent):
        """看看两头还有多少单：/handoff"""
        wait = sorted(self._scan("request"))
        done = sorted(self._scan("done"))
        tip = f"待办 {len(wait)} 张，回执 {len(done)} 张"
        if wait:
            tip += "\n" + "\n".join(wait[:10])
        yield event.plain_result(tip)
