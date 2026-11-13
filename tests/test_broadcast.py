import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from html import escape
from importlib import import_module
import logging
from pathlib import Path
import sys
import time
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest


package = ModuleType("provider_status_broadcast_tests")
package.__path__ = [str(Path(__file__).resolve().parents[1])]
sys.modules[package.__name__] = package
feed = import_module(f"{package.__name__}.feed")
plugin = import_module(f"{package.__name__}.plugin")
state = import_module(f"{package.__name__}.state")

DEEPSEEK = feed.SOURCES["deepseek"]
OPENAI = feed.SOURCES["openai"]
GROUP = "123456789"
OTHER_GROUP = "123456780"
USER = "123456781"
BOT = "123456782"


def rss(source, phase, title, components="", incident_id="abc-123"):
    summary = escape(f"<b>Status:</b> {phase}<br/>{components}")
    return f"""<rss version="2.0"><channel>
      <title>{source.name}</title>
      <lastBuildDate>Sat, 07 Nov 2026 15:56:50 +0800</lastBuildDate>
      <item>
        <guid>{incident_id}</guid>
        <link>https://{source.host}//incidents/{incident_id}</link>
        <title>{escape(title)}</title>
        <pubDate>Sat, 07 Nov 2026 15:11:38 +0800</pubDate>
        <description>{summary}</description>
      </item>
    </channel></rss>""".encode()


@pytest.mark.parametrize(
    ("source", "phase", "title", "components", "expected"),
    [
        (DEEPSEEK, "investigating", "DeepSeek 网页/API 部分中断", "", "outage"),
        (DEEPSEEK, "monitoring", "DeepSeek API 性能下降", "", "degraded"),
        (DEEPSEEK, "resolved", "DeepSeek 网页/API 部分中断", "", "resolved"),
        (OPENAI, "Identified", "Issues using Codex", "<li>Codex (Partial Outage)</li>", "outage"),
        (OPENAI, "Monitoring", "Elevated error rates in ChatGPT", "", "degraded"),
        (OPENAI, "Investigating", "Issues using Codex", "", "incident"),
        (OPENAI, "Resolved", "Elevated error rates in ChatGPT", "<li>ChatGPT (Operational)</li>", "resolved"),
        (OPENAI, "Resolved", "Scheduled maintenance", "", ""),
    ],
)
def test_official_rss_formats(source, phase, title, components, expected):
    snapshot = feed.parse_feed(source, rss(source, phase, title, components))
    result = snapshot.incidents[0]
    if source.uses_channel_revision:
        assert snapshot.revision > result.updated
    else:
        assert snapshot.revision is None
    assert result.id == "abc-123"
    assert result.status == expected
    assert result.link == f"https://{source.host}/incidents/abc-123"
    if expected:
        assert feed.LABELS[expected] in result.notification(source)


def test_rejects_unofficial_incident_link():
    payload = rss(DEEPSEEK, "monitoring", "DeepSeek API 性能下降").replace(b"status.deepseek.com", b"example.org")
    with pytest.raises(ValueError, match="官方事故地址"):
        feed.parse_feed(DEEPSEEK, payload)


def new_store(tmp_path, name="deepseek"):
    store = state.StatusStore(tmp_path / f"{name}.sqlite3")
    store.initialize()
    return store


def test_transitions_and_restarts_only_announce_new_state_changes(tmp_path):
    store = new_store(tmp_path)
    store.set_subscription("group", GROUP, "group-stream", True)
    store.set_subscription("private", USER, "private-stream", True)
    now = time.time()
    historical = feed.Incident(
        "earlier", now - 60, "resolved", "历史事故", "", "https://status.deepseek.com/incidents/1"
    )
    outage = replace(historical, id="live", status="outage", updated=now + 1)
    store.observe(DEEPSEEK, [historical], {GROUP}, now)
    assert store.claim_next({GROUP}) is None
    store.observe(DEEPSEEK, [outage], {GROUP}, now + 2)
    assert store.claim_next({GROUP}).stream_id == "group-stream"

    store = new_store(tmp_path)
    store.observe(DEEPSEEK, [replace(outage, updated=now + 3)], {GROUP}, now + 4)
    assert store.claim_next({GROUP}).stream_id == "private-stream"
    assert store.claim_next({GROUP}) is None

    degraded = replace(outage, updated=now + 5, status="degraded")
    recovered = replace(outage, updated=now + 6, status="resolved")
    store.observe(DEEPSEEK, [degraded, recovered], {GROUP}, now + 7)
    assert [store.claim_next({GROUP}).status for _ in range(4)] == ["degraded", "degraded", "resolved", "resolved"]
    store.observe(DEEPSEEK, [outage, degraded], {GROUP}, now + 8)
    store.observe(DEEPSEEK, [replace(degraded, updated=now + 9)], {GROUP}, now + 10)
    assert [store.claim_next({GROUP}).status for _ in range(2)] == ["degraded", "degraded"]
    store.observe(DEEPSEEK, [replace(degraded, updated=now + 10)], {GROUP}, now + 11)
    assert store.claim_next({GROUP}) is None

    store.observe(DEEPSEEK, [replace(outage, id="new-incident", updated=now + 11)], {GROUP}, now + 12)
    assert store.claim_next({GROUP}).status == "outage"


def test_existing_active_incident_only_announces_new_updates(tmp_path):
    store = new_store(tmp_path)
    store.set_subscription("private", USER, "private-stream", True)
    now = time.time()
    active = feed.Incident("live", now - 60, "outage", "故障", "", "https://status.deepseek.com/incidents/1")
    store.observe(DEEPSEEK, [active], set(), now)
    store.observe(DEEPSEEK, [replace(active, updated=now + 1)], set(), now + 2)
    assert store.claim_next(set()) is None
    store.observe(DEEPSEEK, [replace(active, updated=now + 3, status="resolved")], set(), now + 4)
    assert store.claim_next(set()).status == "resolved"


@pytest.mark.parametrize("source", [DEEPSEEK, OPENAI], ids=["deepseek", "openai"])
def test_reopened_incident_notifies_each_new_transition_across_restarts(tmp_path, source):
    store = new_store(tmp_path)
    store.set_subscription("group", GROUP, "group-stream", True)
    store.set_subscription("private", USER, "private-stream", True)
    now = time.time()
    revision = {"revision": now} if source.uses_channel_revision else {}
    store.observe(source, [], {GROUP}, now, **revision)

    for index, status in enumerate(("outage", "resolved", "outage", "resolved"), start=1):
        updated = now + 1 if source.uses_channel_revision else now + index
        incident = feed.Incident(
            "reopened", updated, status, "incident", "", f"https://{source.host}/incidents/reopened",
        )
        revision = {"revision": now + index} if source.uses_channel_revision else {}
        store.observe(source, [incident], {GROUP}, now + index + 0.1, **revision)
        notifications = [store.claim_next({GROUP}), store.claim_next({GROUP})]
        assert all(item is not None and item.status == status for item in notifications)
        assert {item.stream_id for item in notifications} == {"group-stream", "private-stream"}
        assert store.claim_next({GROUP}) is None

        store = new_store(tmp_path)
        store.observe(source, [incident], {GROUP}, now + index + 0.2, **revision)
        assert store.claim_next({GROUP}) is None

    old_outage = replace(incident, status="outage", updated=now + 1)
    store.observe(source, [old_outage], {GROUP}, now + 4.5, **revision)
    assert store.claim_next({GROUP}) is None


def test_deepseek_rss_recovers_an_existing_atom_incident_with_unchanged_pubdate(tmp_path):
    store = new_store(tmp_path)
    now = int(time.time())
    url = "https://status.deepseek.com/incidents/live"
    known = feed.Incident("live", now - 30, "outage", "部分中断", "", url)
    store.observe(DEEPSEEK, [known], {GROUP}, now - 20)
    store.set_subscription("group", GROUP, "group-stream", True)
    with store.transaction() as db:
        db.execute("DROP TABLE rss_revision")

    store = new_store(tmp_path)
    rss_outage = replace(known, updated=now - 120)
    store.observe(DEEPSEEK, [rss_outage], {GROUP}, now + 1, revision=now - 30)
    assert store.claim_next({GROUP}) is None

    rss_recovery = replace(rss_outage, status="resolved")
    old = replace(rss_recovery, id="old", updated=now - 86400)
    store.observe(DEEPSEEK, [rss_recovery, old], {GROUP}, now + 61, revision=now + 60)
    delivery = store.claim_next({GROUP})
    assert delivery.status == "resolved" and delivery.incident_id == "live"
    assert "检测时间" in delivery.text
    assert store.claim_next({GROUP}) is None

    store = new_store(tmp_path)
    with pytest.raises(ValueError, match="早于"):
        store.observe(DEEPSEEK, [rss_outage], {GROUP}, now + 70, revision=now - 30)
    store.observe(DEEPSEEK, [rss_recovery], {GROUP}, now + 71, revision=now + 60)
    assert store.claim_next({GROUP}) is None


def test_openai_rss_keeps_atom_delivery_history_at_second_precision(tmp_path):
    store = new_store(tmp_path, "openai")
    store.set_subscription("private", USER, "private-stream", True)
    updated = int(time.time()) + 5
    active = feed.Incident(
        "live", updated + 0.508, "degraded", "Elevated error rates", "", "https://status.openai.com/incidents/live"
    )
    store.observe(OPENAI, [], set(), updated - 1)
    store.observe(OPENAI, [active], set(), updated + 1)
    assert store.claim_next(set()).status == "degraded"
    store.observe(OPENAI, [replace(active, updated=updated)], set(), updated + 2)
    assert store.claim_next(set()) is None
    store.observe(OPENAI, [replace(active, updated=updated, status="resolved")], set(), updated + 3)
    assert store.claim_next(set()).status == "resolved"
    store.observe(OPENAI, [replace(active, updated=updated)], set(), updated + 4)
    assert store.claim_next(set()) is None


def test_opt_out_revocation_and_distinct_provider_state(tmp_path):
    first = new_store(tmp_path)
    second = new_store(tmp_path, "openai")
    first.set_subscription("group", GROUP, "group-stream", True)
    first.set_subscription("group", OTHER_GROUP, "other-group-stream", True)
    first.set_subscription("private", USER, "private-stream", True)
    now = time.time()
    first.observe(DEEPSEEK, [], {GROUP, OTHER_GROUP}, now)
    first.observe(
        DEEPSEEK,
        [feed.Incident("live", now + 1, "degraded", "故障", "", "https://status.deepseek.com/incidents/1")],
        {GROUP},
        now + 2,
    )
    first.set_subscription("private", USER, "private-stream", False)
    assert first.claim_next(set()) is None
    first.revoke_removed_groups({OTHER_GROUP})
    assert not first.is_subscribed("group", GROUP)
    assert first.claim_next({GROUP, OTHER_GROUP}) is None
    assert not second.is_subscribed("group", OTHER_GROUP)
    assert second.claim_next({GROUP, OTHER_GROUP}) is None


def command_args(action, group_id="", *, addressed=True, relayed=False):
    stream = f"group-{group_id}" if group_id else "private"
    info = {
        "user_info": {"user_id": USER},
        "group_info": {"group_id": group_id} if group_id else None,
        "additional_config": {"relayed_by_plugin": "plugin.id"} if relayed else {},
    }
    return {
        "platform": "qq",
        "user_id": USER,
        "group_id": group_id,
        "stream_id": stream,
        "matched_groups": {"action": action},
        "message": {
            "platform": "qq",
            "account_id": BOT,
            "session_id": stream,
            "message_info": info,
            "is_at": addressed,
            "message_id": "test-message",
        },
    }


def make_plugin(tmp_path):
    instance = plugin.create_plugin()
    instance._plugin_config_instance = plugin.BroadcastConfig(deepseek={"group_whitelist": [GROUP]})
    instance._set_context(
        SimpleNamespace(
            paths=SimpleNamespace(data_dir=tmp_path),
            logger=logging.getLogger(__name__),
            api=SimpleNamespace(call=AsyncMock()),
            send=SimpleNamespace(text=AsyncMock(return_value=True)),
        )
    )
    instance._watches = {key: plugin.Watch(new_store(tmp_path, key)) for key in feed.SOURCES}
    return instance


def test_commands_apply_independently_and_check_real_qq_permissions(tmp_path):
    async def scenario():
        instance = make_plugin(tmp_path)
        deepseek = instance._watches["deepseek"].store
        openai = instance._watches["openai"].store
        await instance.deepseek_command(**command_args("开启", GROUP, addressed=False))
        await instance.deepseek_command(**command_args("开启", relayed=True))
        assert instance.ctx.send.text.await_count == 0
        instance.ctx.api.call.return_value = {"role": "member", "group_id": GROUP, "user_id": USER}
        await instance.deepseek_command(**command_args("开启", GROUP))
        assert not deepseek.is_subscribed("group", GROUP)

        instance.ctx.api.call.return_value["role"] = "admin"
        await instance.deepseek_command(**command_args("开启", GROUP))
        await instance.openai_command(**command_args("开启", GROUP))
        assert deepseek.is_subscribed("group", GROUP)
        assert not openai.is_subscribed("group", GROUP)
        await instance.openai_command(**command_args("开启"))
        assert openai.is_subscribed("private", USER)
        await instance.deepseek_command(**command_args("关闭", GROUP))
        assert not deepseek.is_subscribed("group", GROUP)
        assert openai.is_subscribed("private", USER)
        await instance.openai_command(**command_args("关闭"))
        assert not openai.is_subscribed("private", USER)

    asyncio.run(scenario())


def test_ambiguous_send_is_not_replayed(tmp_path):
    async def scenario():
        instance = make_plugin(tmp_path)
        watch = instance._watches["openai"]
        watch.store.set_subscription("private", USER, "private", True)
        now = datetime.now(timezone.utc).timestamp()
        watch.store.observe(OPENAI, [], set(), now)
        watch.store.observe(
            OPENAI,
            [
                feed.Incident(
                    "live", now + 1, "outage", "API unavailable", "", "https://status.openai.com/incidents/live"
                )
            ],
            set(),
            now + 2,
        )
        instance.ctx.send.text.side_effect = TimeoutError()
        await instance._deliver("openai", watch)
        await instance._deliver("openai", plugin.Watch(new_store(tmp_path, "openai")))
        instance.ctx.send.text.assert_awaited_once()

    asyncio.run(scenario())
