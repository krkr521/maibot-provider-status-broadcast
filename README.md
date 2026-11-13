# DeepSeek / OpenAI 服务状态播报 · MaiBot 插件

分别订阅 DeepSeek 和 OpenAI 官方 RSS，将服务中断、性能下降、异常进展和恢复消息发送到已订阅的 QQ 会话。无需模型 API Key。

## 功能

- 两个服务独立轮询，每 60 秒检查一次官方 RSS。
- 每个群聊或私聊分别控制两个服务的订阅。
- 消息附带官方事故链接和北京时间。
- 首次读取建立基线，避免安装后集中补发历史事故。
- SQLite 保存事故状态与投递认领记录，插件重载或宿主重启后继续去重。
- 白名单移除群后撤销该服务的群订阅；停止插件时结束轮询任务。

播报内容和时效取决于官方 RSS 的更新。发送确认丢失或结果不明确时，保留认领记录并记录日志，优先避免同一状态重复推送；这种情况下可能需要人工核对漏发。

## 安装

需要 Python 3.12+、MaiBot 1.3.18–1.4.x、MaiBot Plugin SDK 2.19+（2.x）和 `curl-cffi>=0.16.3`。离线验证环境为 MaiBot 1.4.3 / SDK 2.22.1。

```bash
git clone https://github.com/krkr521/maibot-provider-status-broadcast.git plugins/local_provider-status-broadcast
```

由宿主安装清单声明的 Python 依赖，然后在 WebUI 插件页加载“DeepSeek / OpenAI 服务状态播报”。从 `config.example.toml` 创建运行配置，或使用 WebUI 配置。

QQ 适配器需要提供发送能力和 `adapter.napcat.group.get_group_member_info` API。当前配套运行环境使用 SnowLuma 适配器；没有可用的群成员查询时，可由配置中登记的机器人管理员管理白名单群的订阅。

插件 ID 保持 `local.provider-status-broadcast`，已有配置与订阅数据可继续使用。轮询随 MaiBot 插件生命周期启动和停止，不需要额外 Windows 服务、端口或系统计划任务。

## 配置与权限

```toml
[plugin]
enabled = true
config_version = "1.0.0"
admin_qq_ids = []

[deepseek]
group_whitelist = []

[openai]
group_whitelist = []
```

- QQ 号和群号填写为字符串。
- `admin_qq_ids` 是可管理群订阅的机器人管理员。
- 两个 `group_whitelist` 分别控制对应服务允许在哪些群订阅；空列表拒绝该服务的群订阅。
- 加入白名单后仍需在对应群执行开启命令。群主、群管理员和配置的机器人管理员可以修改群订阅。
- 私聊用户可以直接管理当前私聊的订阅。

## 使用命令

| 服务 | 开启 | 关闭 | 查看状态 |
| --- | --- | --- | --- |
| DeepSeek | `/DeepSeek播报 开启` | `/DeepSeek播报 关闭` | `/DeepSeek播报 状态` |
| OpenAI | `/OpenAI播报 开启` | `/OpenAI播报 关闭` | `/OpenAI播报 状态` |

群聊中请先真正 @ 当前机器人，再输入命令；私聊直接输入。查询状态会显示本会话的订阅状态、最近检查时间和正在进行的事故数量。

## 数据与维护

订阅、事故进度和投递记录保存在宿主分配的插件数据目录，两个服务分别使用自己的 SQLite 数据库。更新源码时保留该数据目录；重新创建数据库会重建基线并丢失原订阅记录。

运行配置、数据库、会话信息和日志均不包含在公开仓库及 Release 源码包中。卸载、加载和重启恢复由 MaiBot 插件 supervisor 管理。

## 开发与验证

```bash
uv sync --group dev
uv run pytest tests -q
uv run ruff check .
```

测试覆盖官方 RSS 解析、状态变化、重启去重、会话隔离、权限和停止时的投递行为。测试使用临时数据库及模拟发送能力，不会向真实 QQ 会话发消息。

## 来源与许可

- [DeepSeek 官方状态 RSS](https://status.deepseek.com/feed.rss)
- [OpenAI 官方状态 RSS](https://status.openai.com/feed.rss)
- 程序源码采用 [MIT](LICENSE) 许可；状态内容来自对应服务的官方公告。

版本变化见 [CHANGELOG](CHANGELOG.md)。
