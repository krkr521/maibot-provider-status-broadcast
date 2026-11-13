"""每个服务独立保存订阅、事故进度和 QQ 投递记录。"""

from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import time

from .feed import Incident, Source


@dataclass(frozen=True)
class Delivery:
    stream_id: str
    incident_id: str
    status: str
    text: str


class StatusStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def transaction(self):
        with closing(sqlite3.connect(self.path, timeout=3)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            yield db

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS baseline (id INTEGER PRIMARY KEY CHECK (id = 1), since REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS rss_revision (id INTEGER PRIMARY KEY CHECK (id = 1), updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS incidents (
                    id TEXT PRIMARY KEY, updated REAL NOT NULL, status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    kind TEXT NOT NULL, target TEXT NOT NULL, stream TEXT NOT NULL, since REAL NOT NULL,
                    PRIMARY KEY (kind, target)
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    kind TEXT NOT NULL, target TEXT NOT NULL, incident TEXT NOT NULL, status TEXT NOT NULL,
                    updated REAL NOT NULL, text TEXT NOT NULL, claimed INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (kind, target, incident, status)
                );
            """)

    def observe(
        self,
        source: Source,
        incidents: list[Incident],
        groups: set[str],
        checked: float,
        *,
        revision: float | None = None,
    ) -> None:
        with self.transaction() as db:
            if revision is not None:
                row = db.execute("SELECT updated FROM rss_revision WHERE id = 1").fetchone()
                # 初次切到 RSS 时，沿用 Atom 已记录的最新事故时间作为下限。
                last_revision = row[0] if row else db.execute("SELECT MAX(updated) FROM incidents").fetchone()[0]
                if last_revision is not None and revision < last_revision:
                    raise ValueError("RSS 返回了早于现有记录的快照")
                db.execute(
                    "INSERT INTO rss_revision VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET updated = excluded.updated",
                    (revision,),
                )
            baseline = db.execute("SELECT since FROM baseline WHERE id = 1").fetchone()
            if baseline is None:
                db.execute("INSERT INTO baseline VALUES (1, ?)", (checked,))
            subscriptions = db.execute("SELECT kind, target, since FROM subscriptions").fetchall()
            for incident in sorted(incidents, key=lambda item: item.updated):
                previous = db.execute("SELECT updated, status FROM incidents WHERE id = ?", (incident.id,)).fetchone()
                updated = incident.updated
                if previous:
                    if revision is not None:
                        if previous[1] == incident.status:
                            continue
                        updated = revision
                    if updated <= previous[0]:
                        # RSS 只精确到秒。同一秒的明确恢复可结束已记录的事故。
                        same_second_recovery = (
                            incident.status == "resolved"
                            and previous[1] in {"outage", "degraded", "incident"}
                            and int(updated) == int(previous[0])
                        )
                        if not same_second_recovery:
                            continue
                        updated = previous[0]
                db.execute(
                    "INSERT INTO incidents VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
                    "updated = excluded.updated, status = excluded.status",
                    (incident.id, updated, incident.status),
                )
                if baseline is None or not incident.status or (previous and previous[1] == incident.status):
                    continue
                if previous is None and updated < baseline[0]:
                    continue
                if incident.status == "resolved" and previous and not previous[1]:
                    continue
                notification = incident.notification(source, detected_at=checked if revision is not None else None)
                db.executemany(
                    "INSERT INTO deliveries (kind, target, incident, status, updated, text)"
                    " VALUES (?, ?, ?, ?, ?, ?)"
                    " ON CONFLICT(kind, target, incident, status) DO UPDATE SET"
                    " updated = excluded.updated, text = excluded.text, claimed = 0"
                    " WHERE excluded.updated > deliveries.updated",
                    (
                        (kind, target, incident.id, incident.status, updated, notification)
                        for kind, target, since in subscriptions
                        if since <= updated and (kind == "private" or target in groups)
                    ),
                )

    def set_subscription(self, kind: str, target: str, stream: str, enabled: bool) -> None:
        with self.transaction() as db:
            if enabled:
                db.execute(
                    "INSERT INTO subscriptions VALUES (?, ?, ?, ?) ON CONFLICT(kind, target) "
                    "DO UPDATE SET stream = excluded.stream",
                    (kind, target, stream, time.time()),
                )
            else:
                db.execute("DELETE FROM subscriptions WHERE kind = ? AND target = ?", (kind, target))
                db.execute("DELETE FROM deliveries WHERE kind = ? AND target = ? AND claimed = 0", (kind, target))

    def is_subscribed(self, kind: str, target: str) -> bool:
        with self.transaction() as db:
            return (
                db.execute("SELECT 1 FROM subscriptions WHERE kind = ? AND target = ?", (kind, target)).fetchone()
                is not None
            )

    def revoke_removed_groups(self, groups: set[str]) -> None:
        with self.transaction() as db:
            removed = [
                (row[0],)
                for row in db.execute("SELECT target FROM subscriptions WHERE kind = 'group'")
                if row[0] not in groups
            ]
            db.executemany("DELETE FROM subscriptions WHERE kind = 'group' AND target = ?", removed)
            db.executemany("DELETE FROM deliveries WHERE kind = 'group' AND target = ? AND claimed = 0", removed)

    def claim_next(self, groups: set[str]) -> Delivery | None:
        with self.transaction() as db:
            rows = db.execute("""
                SELECT d.kind, d.target, d.incident, d.status, d.text, s.stream
                FROM deliveries d JOIN subscriptions s USING (kind, target)
                WHERE d.claimed = 0
                ORDER BY d.updated, d.incident, d.kind, d.target
            """)
            for kind, target, incident, status, text, stream in rows:
                if kind == "group" and target not in groups:
                    continue
                # 先提交认领记录。QQ 接收成功但确认丢失时，重启也能按同一状态去重。
                db.execute(
                    "UPDATE deliveries SET claimed = 1 WHERE kind = ? AND target = ? AND incident = ? AND status = ?",
                    (kind, target, incident, status),
                )
                return Delivery(stream, incident, status, text)
        return None
