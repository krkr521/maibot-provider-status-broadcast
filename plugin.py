"""通过 MaiBot 原生指令与 QQ 发送能力播报官方状态。"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import re
import time
from typing import Annotated, Any

from maibot_sdk import Command, Field, MaiBotPlugin, PluginConfigBase
from pydantic import StringConstraints

from .feed import SHANGHAI, SOURCES, fetch_feed
from .state import StatusStore


POLL_SECONDS = 60
QQ_NUMBER = r"^[1-9][0-9]{4,11}$"
QqNumber = Annotated[str, StringConstraints(pattern=QQ_NUMBER)]
COMMAND_SUFFIX = r"(?:[ \t]+(?P<action>开启|关闭|状态|开|关))?$"


class PluginSection(PluginConfigBase):
    enabled: bool = Field(default=True, description="启用服务状态播报插件")
    config_version: str = Field(default="1.0.0", description="配置版本")
    admin_qq_ids: list[QqNumber] = Field(default_factory=list, description="可管理白名单群订阅的机器人管理员 QQ")


class ProviderSection(PluginConfigBase):
    group_whitelist: list[QqNumber] = Field(default_factory=list, description="允许开启本服务播报的 QQ 群号")


class BroadcastConfig(PluginConfigBase):
    plugin: PluginSection = Field(default_factory=PluginSection)
    deepseek: ProviderSection = Field(default_factory=ProviderSection, description="DeepSeek 群白名单")
    openai: ProviderSection = Field(default_factory=ProviderSection, description="OpenAI 群白名单")


@dataclass
class Watch:
    store: StatusStore
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_success: float | None = None
    active_count: int = 0
    failed: bool = False


def is_qq_command(
    message: dict[str, Any],
    platform: str,
    user_id: str,
    group_id: str,
    stream_id: str,
) -> bool:
    if not isinstance(message, dict):
        return False
    info = message.get("message_info")
    if not isinstance(info, dict):
        return False
    sender = info.get("user_info")
    group = info.get("group_info")
    extra = info.get("additional_config")
    if not isinstance(sender, dict) or not isinstance(extra, dict):
        return False
    if group_id:
        if (
            not isinstance(group, dict)
            or group_id != str(group.get("group_id", ""))
            or message.get("is_at") is not True
        ):
            return False
    elif group:
        return False
    return bool(
        platform == "qq" == message.get("platform")
        and re.fullmatch(QQ_NUMBER, user_id)
        and user_id == str(sender.get("user_id", ""))
        and message.get("account_id")
        and user_id != str(message["account_id"])
        and stream_id
        and stream_id == message.get("session_id")
        and message.get("message_id")
        and not extra.get("relayed_by_plugin")
        and not extra.get("is_local_operator")
    )


class StatusBroadcastPlugin(MaiBotPlugin):
    config_model = BroadcastConfig

    def __init__(self) -> None:
        super().__init__()
        self._stop = asyncio.Event()
        self._watches: dict[str, Watch] = {}
        self._tasks: list[asyncio.Task[None]] = []

    def _allowed_groups(self, provider: str) -> set[str]:
        config = self.config.deepseek if provider == "deepseek" else self.config.openai
        return set(config.group_whitelist)

    async def on_load(self) -> None:
        self._stop.clear()
        self._watches = {key: Watch(StatusStore(self.ctx.paths.data_dir / f"{key}.sqlite3")) for key in SOURCES}
        for key, watch in self._watches.items():
            await asyncio.to_thread(watch.store.initialize)
            await asyncio.to_thread(watch.store.revoke_removed_groups, self._allowed_groups(key))
        self._tasks = [
            asyncio.create_task(self._monitor(key, watch), name=f"{key}-status-monitor")
            for key, watch in self._watches.items()
        ]
        self.ctx.logger.info("DeepSeek / OpenAI 状态播报已启动，独立轮询官方 RSS")

    async def on_unload(self) -> None:
        self._stop.set()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self.ctx.logger.info("DeepSeek / OpenAI 状态播报轮询已停止")

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        for key, watch in self._watches.items():
            async with watch.lock:
                await asyncio.to_thread(watch.store.revoke_removed_groups, self._allowed_groups(key))

    async def _monitor(self, provider: str, watch: Watch) -> None:
        source = SOURCES[provider]
        while not self._stop.is_set():
            try:
                async with asyncio.timeout(10):
                    snapshot = await fetch_feed(source)
                if self._stop.is_set():
                    return
                checked = time.time()
                async with watch.lock:
                    await asyncio.to_thread(
                        watch.store.observe,
                        source,
                        snapshot.incidents,
                        self._allowed_groups(provider),
                        checked,
                        revision=snapshot.revision,
                    )
                if watch.last_success is None or watch.failed:
                    self.ctx.logger.info(
                        "%s 官方 RSS 读取成功，收到 %d 条事故记录", source.name, len(snapshot.incidents)
                    )
                watch.active_count = sum(incident.status not in {"", "resolved"} for incident in snapshot.incidents)
                watch.last_success = checked
                watch.failed = False
            except Exception as exc:
                if not watch.failed:
                    self.ctx.logger.warning("%s 官方 RSS 轮询失败: %s", source.name, exc)
                watch.failed = True

            try:
                await self._deliver(provider, watch)
            except Exception:
                self.ctx.logger.exception("%s 播报队列处理失败", source.name)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=POLL_SECONDS)
            except TimeoutError:
                continue

    async def _deliver(self, provider: str, watch: Watch) -> None:
        while not self._stop.is_set():
            async with watch.lock:
                if self._stop.is_set():
                    return
                delivery = await asyncio.to_thread(watch.store.claim_next, self._allowed_groups(provider))
                if delivery is None:
                    return
                try:
                    async with asyncio.timeout(8):
                        sent = await self.ctx.send.text(delivery.text, delivery.stream_id, storage_message=False)
                except Exception as exc:
                    sent = False
                    self.ctx.logger.warning("%s 发送能力异常: %s", SOURCES[provider].name, type(exc).__name__)
                if not sent:
                    self.ctx.logger.warning(
                        "%s 事故 %s/%s 未收到 QQ 发送确认，保留去重记录",
                        SOURCES[provider].name,
                        delivery.incident_id,
                        delivery.status,
                    )
                else:
                    self.ctx.logger.info(
                        "%s 事故 %s/%s 已投递 QQ",
                        SOURCES[provider].name,
                        delivery.incident_id,
                        delivery.status,
                    )

    async def _can_manage_group(self, group_id: str, user_id: str) -> bool:
        if user_id in self.config.plugin.admin_qq_ids:
            return True
        try:
            async with asyncio.timeout(4):
                member = await self.ctx.api.call(
                    "adapter.napcat.group.get_group_member_info",
                    group_id=group_id,
                    user_id=user_id,
                    no_cache=True,
                )
        except Exception as exc:
            self.ctx.logger.warning("QQ 群管理员身份查询失败: %s", type(exc).__name__)
            return False
        return (
            isinstance(member, dict)
            and str(member.get("group_id", "")) == group_id
            and str(member.get("user_id", "")) == user_id
            and member.get("role") in {"owner", "admin"}
        )

    def _poll_status(self, watch: Watch) -> str:
        if watch.failed:
            return "官方 RSS 最近一次读取失败，等待下一轮检查。"
        if watch.last_success is None:
            return "等待首次读取官方 RSS。"
        stamp = datetime.fromtimestamp(watch.last_success, SHANGHAI).strftime("%m-%d %H:%M")
        return f"上次检查 {stamp}（北京时间），官方进行中的事故 {watch.active_count} 起。"

    async def _command(
        self,
        provider: str,
        *,
        platform: str,
        user_id: str,
        group_id: str,
        stream_id: str,
        message: dict[str, Any],
        matched_groups: dict[str, Any],
        **kwargs: Any,
    ) -> tuple[bool, str | None, bool]:
        if not is_qq_command(message, platform, user_id, group_id, stream_id) or self._stop.is_set():
            return True, None, True
        source = SOURCES[provider]
        action = matched_groups.get("action") or "状态"
        if group_id and group_id not in self._allowed_groups(provider):
            text = f"本群未在 {source.name} 播报白名单中，请在麦麦 WebUI 插件设置添加本群。"
        elif group_id and action != "状态" and not await self._can_manage_group(group_id, user_id):
            text = "群聊播报开关由群主、群管理员或机器人管理员操作。"
        else:
            watch = self._watches[provider]
            kind, target = ("group", group_id) if group_id else ("private", user_id)
            async with watch.lock:
                if self._stop.is_set() or (group_id and group_id not in self._allowed_groups(provider)):
                    return True, None, True
                if action != "状态":
                    await asyncio.to_thread(
                        watch.store.set_subscription,
                        kind,
                        target,
                        stream_id,
                        action in {"开", "开启"},
                    )
                enabled = await asyncio.to_thread(watch.store.is_subscribed, kind, target)
                state = "开启" if enabled else "关闭"
                text = f"本会话的 {source.name} 状态播报已{state}。\n{self._poll_status(watch)}"
        await self.ctx.send.text(text, stream_id, storage_message=False)
        return True, text, True

    @Command(
        "deepseek_status_broadcast",
        pattern=r"^/(?i:DeepSeek)播报" + COMMAND_SUFFIX,
        permission="public",
        description="/DeepSeek播报 开启/关闭/状态，管理本会话的 DeepSeek 官方状态播报。",
        timeout_ms=25000,
    )
    async def deepseek_command(self, **kwargs: Any) -> tuple[bool, str | None, bool]:
        return await self._command("deepseek", **kwargs)

    @Command(
        "openai_status_broadcast",
        pattern=r"^/(?i:OpenAI)播报" + COMMAND_SUFFIX,
        permission="public",
        description="/OpenAI播报 开启/关闭/状态，管理本会话的 OpenAI 官方状态播报。",
        timeout_ms=25000,
    )
    async def openai_command(self, **kwargs: Any) -> tuple[bool, str | None, bool]:
        return await self._command("openai", **kwargs)


def create_plugin() -> StatusBroadcastPlugin:
    return StatusBroadcastPlugin()
