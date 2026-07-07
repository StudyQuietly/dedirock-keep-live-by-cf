# DediRock Keep Live by Cloudflare Workers

Cloudflare Workers 版 DediRock / Virtualizor VPS 保活工具。

首版功能：

- 支持配置多个 Virtualizor End-user Panel。
- 通过 Virtualizor End-user API 拉取 VPS 列表。
- 支持按 VPS 开关监控、自动启动、独立 cron 表达式、离线阈值和启动防重时间。
- Worker 每分钟唤醒一次，再按每台 VPS 的 cron 表达式判断是否检查。
- 同一时间只允许一轮监控任务运行，并以有限并发检查 VPS，避免上一轮未结束时重复执行。
- VPS 离线次数达到阈值后调用 Virtualizor `act=start`，启动防重时间内不会重复发送启动命令。
- 配置和运行状态分开保存：面板、密钥、策略保存在配置中，检查结果、失败次数、启动冷却和事件日志保存在运行状态中。
- 按每台 VPS 保留最近 100 条普通事件日志；启动和错误日志会进入重要日志，重要日志同样按每台 VPS 保留最近 100 条。
- 支持按日志等级、VPS 名称或关键字筛选日志，并支持清除最近日志或重要日志。
- 提供前端配置页面，使用 `ADMIN_TOKEN` 保护管理 API。

## 部署

### GitHub Actions 自动部署

在 GitHub 仓库的 `Settings -> Secrets and variables -> Actions` 中添加 3 个 Secrets：

```text
CLOUDFLARE_API_TOKEN
CLOUDFLARE_ACCOUNT_ID
ADMIN_TOKEN
```

`CLOUDFLARE_API_TOKEN` 至少需要以下权限：

```text
Workers Scripts: Edit
Workers KV Storage: Edit
Account Settings: Read
```

推送到 `main` 后，Actions 会自动：

1. 查询是否存在 `dedirock-keep-live-config` KV namespace。
2. 不存在则通过 Cloudflare API 创建。
3. 将 KV namespace id 写入 Actions 临时工作区的 `wrangler.toml`。
4. 部署 Worker。
5. 同步 `ADMIN_TOKEN` 到 Worker Secret。

Actions 使用固定版本 `wrangler@4.40.0` 部署，避免 Wrangler 版本变化导致部署行为不一致。

### 手动部署

1. 创建 KV：

```bash
npx --yes wrangler@4.40.0 kv namespace create CONFIG
```

2. 把输出的 KV namespace id 写入 `wrangler.toml`：

```toml
[[kv_namespaces]]
binding = "CONFIG"
id = "你的 KV namespace id"
```

3. 设置管理 Token：

```bash
npx --yes wrangler@4.40.0 secret put ADMIN_TOKEN
```

4. 部署：

```bash
npx --yes wrangler@4.40.0 deploy
```

## 使用

打开 Worker 地址，输入 `ADMIN_TOKEN` 后：

1. 新增面板。
2. 填入 Virtualizor 面板地址，例如 `https://vpanel.dedirock.com:4083`。
3. 填入 Virtualizor 的 API Key 和 API Password。
4. 点击“拉取 VPS”。
5. 为需要监控的 VPS 开启“启用”和“自动启动”。
6. 为每台 VPS 配置 cron 表达式、离线阈值和启动防重分钟数，例如 `*/5 * * * *` 表示每 5 分钟检查一次。
7. 保存配置。

cron 使用 UTC 时间，支持 5 段表达式：分钟、小时、日期、月份、星期。支持 `*`、`*/n`、逗号、范围和范围步进，例如 `0-30/5 * * * *`。日期和星期字段采用同时满足语义；星期 `0` 和 `7` 都表示周日。保存配置时会校验 cron 表达式，非法表达式会返回错误，不会等到定时检查时才失败。

Virtualizor 请求超时时间为 15 秒。超时、HTTP 错误、非 JSON 响应和 Virtualizor 业务错误都会记录到对应 VPS 的事件日志。

## Virtualizor API

项目使用的是 Virtualizor End-user API：

- 拉取 VPS：`act=listvs`
- 查询 VPS：`act=vpsmanage&svs=<VPS_ID>`
- 启动 VPS：`act=start&svs=<VPS_ID>&do=1`

## 安全说明

Virtualizor API Key 和 API Password 会保存到 Worker KV。请确保：

- Worker 管理页面只给自己使用。
- `ADMIN_TOKEN` 使用足够强的随机字符串。
- 不要公开分享 Worker 管理地址和 Token。
