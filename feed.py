"""读取两家官方 RSS，将事故进度映射成播报状态。"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
import re
from urllib.parse import urlsplit
from xml.etree import ElementTree

from curl_cffi.requests import AsyncSession


SHANGHAI = timezone(timedelta(hours=8))
MAX_FEED_BYTES = 512 * 1024
ACTIVE_PHASES = {"investigating", "identified", "monitoring"}
LABELS = {"outage": "服务中断", "degraded": "性能下降", "incident": "服务异常", "resolved": "服务恢复"}
OUTAGE = re.compile(r"中断|不可用|\boutages?\b|\bunavailable\b|\bnot available\b", re.I)
DEGRADED = re.compile(r"性能下降|性能异常|响应缓慢|延迟|degrad|latenc|slow|elevated.*error", re.I)
MAINTENANCE = re.compile(r"维护|\bmaintenance\b", re.I)
INCIDENT_PATH = re.compile(r"/{1,2}incidents/([a-zA-Z0-9-]{1,80})")
PHASE = re.compile(r"^status\s*:\s*([a-z_-]+)", re.I)


@dataclass(frozen=True)
class Source:
    name: str
    host: str
    uses_channel_revision: bool = False

    @property
    def feed_url(self) -> str:
        return f"https://{self.host}/feed.rss"


SOURCES = {
    "deepseek": Source("DeepSeek", "status.deepseek.com", uses_channel_revision=True),
    "openai": Source("OpenAI", "status.openai.com"),
}


@dataclass(frozen=True)
class Incident:
    id: str
    updated: float
    status: str
    title: str
    components: str
    link: str

    def notification(self, source: Source, *, detected_at: float | None = None) -> str:
        timestamp = self.updated if detected_at is None else detected_at
        label = "官方更新" if detected_at is None else "检测时间"
        stamp = datetime.fromtimestamp(timestamp, SHANGHAI).strftime("%m-%d %H:%M")
        lines = [f"【{source.name} · {LABELS[self.status]}】", self.title]
        if self.components:
            lines.append(f"涉及：{self.components}")
        lines.extend((f"{label}：{stamp}（北京时间）", self.link))
        return "\n".join(lines)


@dataclass(frozen=True)
class FeedSnapshot:
    incidents: list[Incident]
    revision: float | None


def clean_text(value: str) -> str:
    text = unescape(re.sub(r"<[^>]*>", " ", value))
    text = re.sub(r"[\x00-\x1f\x7f\u202a-\u202e\u2066-\u2069]", " ", text)
    return " ".join(text.split()).replace("@", "＠").replace("[CQ:", "［CQ:")


def rss_timestamp(value: str) -> float:
    stamp = parsedate_to_datetime(value)
    if stamp.tzinfo is None:
        raise ValueError("RSS 时间缺少时区")
    return stamp.timestamp()


def parse_feed(source: Source, payload: bytes) -> FeedSnapshot:
    if len(payload) > MAX_FEED_BYTES or b"<!DOCTYPE" in payload.upper():
        raise ValueError("RSS 内容超出限制")
    feed = ElementTree.fromstring(payload)
    channel = feed.find("channel")
    if feed.tag != "rss" or channel is None or not channel.findtext("title"):
        raise ValueError("不是官方 RSS 格式")
    # DeepSeek 的条目时间是创建时间，频道时间反映整份事故快照的最后更新。
    revision = rss_timestamp(channel.findtext("lastBuildDate", "")) if source.uses_channel_revision else None

    incidents: list[Incident] = []
    for entry in channel.findall("item"):
        url = urlsplit(entry.findtext("link", "").strip())
        match = INCIDENT_PATH.fullmatch(url.path)
        if url.scheme != "https" or url.netloc != source.host or match is None:
            raise ValueError("RSS 条目缺少官方事故地址")
        incident_id = match[1]
        updated = rss_timestamp(entry.findtext("pubDate", ""))
        if revision is not None and updated > revision:
            raise ValueError("RSS 事故时间晚于频道更新时间")
        title = clean_text(entry.findtext("title", ""))
        summary = entry.findtext("description", "")
        plain_summary = clean_text(summary)
        phase_match = PHASE.match(plain_summary)
        if not title or not phase_match:
            raise ValueError("RSS 条目缺少标题或事故状态")
        phase = phase_match[1].lower()

        items = re.findall(r"<li\b[^>]*>(.*?)</li>", summary, flags=re.I | re.S)
        components = "、".join(clean_text(item) for item in items)
        if not components:
            components = plain_summary.partition("Affected components:")[2].strip()
        severity = f"{title} {components}"
        status = ""
        if not MAINTENANCE.search(title):
            if phase == "resolved":
                status = "resolved"
            elif phase in ACTIVE_PHASES:
                if OUTAGE.search(severity):
                    status = "outage"
                elif DEGRADED.search(severity):
                    status = "degraded"
                else:
                    status = "incident"
        incidents.append(
            Incident(
                incident_id,
                updated,
                status,
                title[:220],
                components[:260],
                f"https://{source.host}/incidents/{incident_id}",
            )
        )
    return FeedSnapshot(incidents, revision)


async def fetch_feed(source: Source) -> FeedSnapshot:
    # DeepSeek 的状态站在这台机器上只接受兼容的 TLS 握手。继续校验服务端证书。
    async with AsyncSession(impersonate="chrome", timeout=8) as session:
        async with session.stream(
            "GET",
            source.feed_url,
            allow_redirects=False,
            headers={"Accept": "application/rss+xml"},
        ) as response:
            if response.status_code != 200:
                raise ValueError(f"状态源 HTTP {response.status_code}")
            body = bytearray()
            async for chunk in response.aiter_content():
                body.extend(chunk)
                if len(body) > MAX_FEED_BYTES:
                    raise ValueError("RSS 内容超出限制")
    return parse_feed(source, bytes(body))
