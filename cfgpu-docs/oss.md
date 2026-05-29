# OSS 文件管理设计

## 背景

Agent 调用工具后会产生两类文件：

1. **本地文件**：通过 `write_file`、`bash` 等工具写入服务器 `/mnt/user-data/outputs/` 目录，客户端无法直接访问。
2. **远端临时 URL**：cfgpu MCP 生成图片/视频后返回带过期时间的公网 HTTPS URL（通常 24 小时内有效）。

为让客户端能持久访问这些文件，需要将文件统一托管到 OSS（对象存储服务），通过分享 OSS URL 达成文件分享。**对于已经是公网 URL 的文件，不需要先下载到本地再上传，直接传递原始 URL 即可。**

---

## Alibaba Cloud OSS

### 账号信息

| 字段 | 值 |
|------|----|
| Bucket | `cf-dream` |
| Region | `cn-beijing` |
| Access Key ID | `$OSS_ACCESS_KEY_ID` |
| Access Key Secret | `$OSS_ACCESS_KEY_SECRET` |

> **注意**：不需要配置自定义 endpoint。SDK 通过 `region` 自动使用标准 AliOSS 地址 `oss-cn-beijing.aliyuncs.com`。presigned URL 格式为 `https://cf-dream.oss-cn-beijing.aliyuncs.com/...`。

### Python SDK

SDK 包名：`alibabacloud-oss-v2`，导入名：`alibabacloud_oss_v2 as oss`。

```python
import alibabacloud_oss_v2 as oss
from alibabacloud_oss_v2 import credentials

# 创建客户端（只需 region + credentials，不需要自定义 endpoint）
creds = credentials.StaticCredentialsProvider(
    access_key_id="",
    access_key_secret="",
)
cfg = oss.config.load_default()
cfg.credentials_provider = creds
cfg.region = "cn-beijing"
client = oss.Client(cfg)

# 上传本地文件
with open("/local/path/image.jpg", "rb") as f:
    client.put_object(oss.PutObjectRequest(
        bucket="cf-dream",
        key="agent-artifacts/thread-123/images/image.jpg",
        body=f,
        content_type="image/jpeg",
    ))

# 上传字节流（从 URL 下载后直接上传）
import httpx
resp = httpx.get(url)
client.put_object(oss.PutObjectRequest(
    bucket="cf-dream",
    key="agent-artifacts/thread-123/videos/video.mp4",
    body=resp.content,
    content_length=len(resp.content),
    content_type="video/mp4",
))

# 生成带时效的分享链接（最长 7 天）
import datetime
result = client.presign(
    oss.GetObjectRequest(bucket="cf-dream", key="agent-artifacts/thread-123/images/image.jpg"),
    expires=datetime.timedelta(days=7),
)
presigned_url = result.url
```

### Bucket 组织结构

```
bucket: cf-dream
 └── agent-artifacts/
   └── {thread_id}/
        ├── images/
        │     └── {filename}.jpg
        ├── videos/
        │     └── {filename}.mp4
        ├── audios/
        │     └── {filename}.mp3
        └── files/
              └── {filename}.md
```

用 `thread_id` 作为前缀隔离不同会话的文件。`category`（`images/`、`videos/`、`audios/`、`files/`）由 MIME 类型推断。

### Presigned URL 有效期

AliOSS V4 签名最长有效期为 **7 天**（604800 秒）。`presigned_url_expires_days` 上限设为 7。

---

## deerflow 现有文件工具

### 本地文件操作工具（Sandbox 层）

操作路径均在 `/mnt/user-data/` 虚拟目录下，物理路径映射到服务器本地。

| 工具名 | 文件位置 | 功能 | 可产生需上传的文件 |
|--------|---------|------|-----------------|
| `write_file` | `sandbox/tools.py:1674` | 写入文本文件 | 是（outputs 目录） |
| `read_file` | `sandbox/tools.py:1606` | 读取文本文件 | 否 |
| `bash` | `sandbox/tools.py:1328` | 执行 shell 命令 | 是（可生成任意文件） |
| `present_files` | `tools/builtins/present_file_tool.py:83` | 标记文件为用户可见 artifacts | 是（主动标记的 outputs 文件） |
| `view_image` | `tools/builtins/view_image_tool.py:49` | 读取图片，base64 返回 | 否（只读） |
| `ls` / `glob` / `grep` | `sandbox/tools.py` | 目录/文件搜索 | 否 |
| `str_replace` | `sandbox/tools.py:1734` | 文件内容替换 | 否（修改已有文件） |

**关键约束：**
- `present_files` 只允许展示 `/mnt/user-data/outputs/` 目录下的文件。
- `view_image` 只允许访问 `workspace`、`uploads`、`outputs` 三个子目录，单文件上限 20MB。
- 物理路径：`$DEER_FLOW_HOME/users/{user_id}/threads/{thread_id}/user-data/{subdir}`

### cfgpu MCP 工具（返回远端 URL）

cfgpu 生成工具返回带过期时间的公网 HTTPS URL，**不是本地文件**。

| 工具名 | 文件位置 | 返回格式 |
|--------|---------|---------|
| `generate_image` | `mcps-cfgpu/tools/generate.py:15` | `{"urls": ["https://..."], "expires_at": "...", "task_id": "...", "model_used": "...", "cost_tokens": ...}` |
| `generate_video` | `mcps-cfgpu/tools/generate.py:45` | 同上 |
| `task_wait` | `mcps-cfgpu/tools/tasks.py:21` | 同上（异步版本） |

`expires_at` 是 URL 的过期时间，通常为生成后 24 小时内。

---

## OSS 上传需求

### 哪些文件需要上传

| 文件来源 | 处理方式 | 原因 |
|---------|---------|------|
| cfgpu 返回的 HTTPS URL，未过期 | **直接使用原 URL，不上传** | 已是公网可访问地址 |
| cfgpu 返回的 HTTPS URL，临近过期（`expires_at - 2h` 以内） | 下载 → 上传 AliOSS → 替换 URL | URL 即将失效，需持久化 |
| `write_file` / `bash` 生成的本地文件（outputs 目录） | 上传 AliOSS → 返回 presigned URL | 仅在服务器本地，客户端无法访问 |
| `present_files` 标记的 artifacts | 上传 AliOSS → 替换路径为 presigned URL | 同上 |
| 用户上传文件（uploads 目录） | 暂不处理 | gateway API 已提供访问通道 |

### 上传触发时机（两个介入点）

```
agent 产生文件
    │
    ├── cfgpu 返回 https URL
    │       └── expires_at > now + 2h？
    │               ├── 是 → 直接使用原 URL
    │               └── 否 → 下载字节流 → 上传 AliOSS → presigned URL
    │
    └── 本地文件（/mnt/user-data/outputs/xxx）
            └── 上传 AliOSS → presigned URL
                    （按 thread_id 隔离，默认有效期 7 天）
```

**方案：主路径 + 兜底**

1. **主路径**：在 `present_files` 工具内注入上传逻辑。agent 调用 `present_files` 时，将 `outputs/` 下的文件上传 AliOSS，用 presigned URL 替换本地路径写入 artifacts。
2. **兜底**：consumer 发送结果给前端前，遍历所有 artifact，对仍是本地路径的条目执行上传。

### Presigned URL 有效期策略

| 场景 | 有效期 |
|------|--------|
| 用户下载/分享 | 7 天（AliOSS V4 最大值） |
| 临时预览 | 1 小时 |

### 配置项

```yaml
oss:
  enabled: true
  access_key_id: "$OSS_ACCESS_KEY_ID"
  access_key_secret: "$OSS_ACCESS_KEY_SECRET"
  bucket: "cf-dream"
  region: "cn-beijing"                   # AliOSS region，V4 签名必填
  presigned_url_expires_days: 7          # AliOSS V4 上限
  cfgpu_url_refresh_threshold_hours: 2   # cfgpu URL 剩余有效期低于此值时重新上传
```

---

## AliOSS 集成层设计

### 模块结构

集成层放在 harness 包内（不放 app 层），以便 `present_files` 工具能直接调用，符合 harness/app 分层规则。

```
backend/packages/harness/deerflow/
└── oss/
    ├── __init__.py
    ├── oss_config.py        # OSSConfig Pydantic 模型
    ├── client.py            # AliOSS 客户端封装（单例）
    └── uploader.py          # 高层上传服务（处理本地文件 + 远端 URL）
```

同步更新 `deerflow/config/app_config.py`，将 `OSSConfig` 作为可选字段挂载到 `AppConfig`。

---

### OSSConfig（`oss/oss_config.py`）

```python
class OSSConfig(BaseModel):
    enabled: bool = False
    access_key_id: str = ""
    access_key_secret: str = ""
    bucket: str = "cf-dream"
    region: str = ""                      # e.g. "cn-beijing"；V4 签名必填
    presigned_url_expires_days: int = 7   # max 7 (AliOSS V4)
    cfgpu_url_refresh_threshold_hours: int = 2
```

---

### OSSClient（`oss/client.py`）

职责：封装 alibabacloud_oss_v2 SDK，提供基础上传/URL 生成操作。进程内单例，通过 `get_oss_client()` 获取。

**核心方法：**

| 方法 | 说明 |
|------|------|
| `upload_file(object_key, local_path)` | 上传本地文件，返回 presigned URL |
| `upload_bytes(object_key, data, content_type)` | 上传字节流，返回 presigned URL |
| `_presigned_url(object_key)` | 为已存在对象生成 presigned URL（使用 `client.presign`） |
| `_ensure_bucket()` | 首次使用时确保 bucket 存在（`get_bucket_info` 检查，不存在则 `put_bucket`） |

**Object Key 命名规则：**
```
agent-artifacts/{thread_id}/{category}/{original_filename}
# 例：
agent-artifacts/abc123/images/portrait.jpg
agent-artifacts/abc123/videos/scene_01.mp4
agent-artifacts/abc123/files/report.md
```

---

### OSSUploader（`oss/uploader.py`）

职责：业务层逻辑，判断文件类型并决定上传策略，对调用方屏蔽底层细节。

**核心方法：**

| 方法 | 说明 |
|------|------|
| `upload_local_file(virtual_path, physical_path, thread_id)` | 上传本地文件，返回 presigned URL |
| `handle_remote_url(url, expires_at, thread_id, filename_hint)` | URL 未临近过期则直接返回，否则下载后上传 AliOSS |
| `_needs_reupload(expires_at)` | 判断是否需要重新上传（剩余时效 < threshold） |

---

### OSS 上传的唯一触发点：`present_files` 工具

**设计原则：OSS 上传由 LLM 的显式意图驱动，而非自动扫描文件路径。**

`present_files` 是 LLM 表达"这个文件需要向用户展示"的唯一显式信号，也是 OSS 上传的唯一触发点。其他工具不触发上传，原因如下：

| 工具 | 为什么不触发 OSS 上传 |
|------|----------------------|
| `write_file` | 文件内容已随 `ai_message` custom event（含 `tool_calls.args.content`）流式推送给客户端；OSS 负责"展示"，不负责"内容传输" |
| `bash` | stdout 里可能出现文件路径字符串，但那只是文本，LLM 会自行决定是否进一步 present |
| cfgpu `generate_image/video` | URL 已通过 `tool_result` custom event 推送给客户端；是否持久化为 artifact 由 LLM 决定 |
| 其他工具 | 中间文件不一定需要给用户下载，自动上传会浪费 OSS 存储 |

**不设置 consumer 层兜底扫描**：consumer 不感知文件系统，职责边界清晰。`artifacts` 进入 consumer 时，路径已在 `present_files` 调用时处理完毕。

---

### 集成点 1：`present_file_tool.py`（本地文件 → OSS）

```
OSS 未配置（oss.enabled=false 或无 oss 配置项）：
  filepaths → normalize_path → artifacts: ["/mnt/user-data/outputs/xxx"]
  ← 与当前实现完全一致，客户端通过 Gateway /api/threads/{id}/artifacts/{path} 访问

OSS 已配置（oss.enabled=true）：
  filepaths → normalize_path → resolve_physical_path → upload_local_file()
           → artifacts: ["https://dream-oss.cfgpu.com/.../xxx?OSSAccessKeyId=..."]
```

上传失败不中断 agent 流程：捕获异常，回退到本地虚拟路径，记录 warning 日志，工具仍返回成功。

---

### 集成点 2：`present_urls` 工具（远端 URL → artifacts）

cfgpu `generate_image` / `generate_video` 返回的公网 HTTPS URL，已通过 `tool_result` custom event 推送给客户端，客户端可直接下载。

`present_urls` 工具的作用是让 LLM **主动将 URL 写入 `artifacts`**，使客户端能以"文件卡片"形式渲染。上传逻辑由 `OSSUploader.handle_remote_url` 处理：URL 临近过期则重新下载上传 AliOSS，否则直接存原 URL。OSS 未配置时，原 URL 直接写入 `artifacts`。

---

### 数据流总览

```
Agent 运行
│
├── write_file(content="...") ──→ ai_message tool_call 已含 content，客户端已有
│                                 不触发 OSS 上传
│
├── bash → 生成 /mnt/user-data/outputs/xxx
│       └── [LLM 决定是否展示]
│               └── present_files(["outputs/xxx"])
│                       └── OSSUploader.upload_local_file()
│                               └── artifacts: ["https://dream-oss.cfgpu.com/..."]
│
└── generate_image → tool_result custom event 已推送 URL 给客户端（可直接下载）
                └── [LLM 决定是否写入 artifacts]
                        └── present_urls([url], [expires_at])
                                └── OSSUploader.handle_remote_url()
                                    ├── 未临近过期 → artifacts: ["https://cfgpu/...原URL"]
                                    └── 临近过期   → 下载+上传 → artifacts: ["https://dream-oss.cfgpu.com/..."]

↓
artifacts 由 consumer 打包进 result_payload 发送给前端
```

---

### 依赖

```
alibabacloud-oss-v2>=0.1.0
```

在 `backend/packages/harness/pyproject.toml` 中定义为可选依赖组 `[oss]`，不影响未使用 OSS 的部署：

```toml
[project.optional-dependencies]
oss = ["alibabacloud-oss-v2>=0.1.0", "httpx>=0.28.0"]
```

---

## 上传 OSS 的已知问题与推荐方案

以下问题当前均未实现，记录于此供后续决策参考。

---

### 1. 重复上传去重

**问题**：agent 在同一 session 内多次 `present_files` 同一文件（常见于"调整后重新展示"），会重复上传内容未变的文件。

**推荐方案（优先）**：进程内 mtime + size 指纹缓存

```
{(thread_id, object_key): (mtime, size, presigned_url)} 存于 OSSUploader 内
```

- `mtime` 或 `size` 变了 → 重新上传（覆盖，内容已变）
- 两者未变 → 直接返回缓存 URL，零网络请求

**补充方案（跨 session）**：上传前 HEAD object 取 AliOSS ETag（= 文件 MD5），与本地 MD5 比对；一致则只调 `presign()` 生成新 URL，不重传。代价是每次多一次 HEAD + 本地 MD5 计算。

---

### 2. 并发上传同一文件

**问题**：同一 thread 内两个并行 `present_files` 调用恰好指向同名文件时，两次上传的 object_key 相同，会出现：
- AliOSS put_object 是原子的，最终内容一致（无数据损坏）
- 但若加了内存缓存，缓存更新存在 race condition，可能返回错误 URL

**推荐方案**：在 `OSSUploader` 内对每个 `(thread_id, object_key)` 加 `asyncio.Lock`，确保同一 key 的上传串行执行，缓存写入安全。

---

### 3. 大文件（视频等）上传

**问题**：当前用 `put_object` 单次上传。AliOSS 建议超过 **5 MB** 使用分片上传（multipart upload），原因：
- 单次上传无断点续传；网络中断需重传整个文件
- 大视频文件（几十 MB 到几百 MB）上传时间长，超时风险高

**推荐方案**：在 `OSSClient.upload_file` 中按文件大小选择策略：
- `< 5 MB`：保持现有 `put_object`
- `≥ 5 MB`：改用 SDK 的 `upload_file`（封装了 multipart，自动分片、并发、重试）

```python
# SDK 封装好的分片上传（非 put_object）
from alibabacloud_oss_v2 import transfer
uploader = transfer.Uploader(client)
uploader.upload_file(oss.PutObjectRequest(bucket=..., key=...), filename=local_path)
```

阈值可通过配置项 `multipart_threshold_mb` 控制，默认 5。

---

### 4. Presigned URL 过期

**问题**：artifacts 中写入的 presigned URL 最长有效 **7 天**（AliOSS V4 限制）。7 天后 URL 失效，前端再点击会得到 403。当前无刷新机制。

**场景分类**：

| 场景 | 影响 |
|------|------|
| 用户在 7 天内访问 | 无问题 |
| 用户收藏 URL 超过 7 天后访问 | 403 |
| 前端重新加载历史对话中的 artifact | 403（DB 中存的是过期 URL） |

**推荐方案**：
- **按需刷新**：前端访问 artifact URL 时，若收到 403，请求 Gateway `/api/threads/{id}/artifacts/refresh` 端点，后端调 `presign()` 生成新 URL 返回。不需要重新上传，只需重新签名（`presign()` 无费用）。
- **前提**：Gateway 需存储 object_key（而非 presigned URL）到 artifacts；当前直接存 URL，需改造数据模型。

---

### 5. 文件名与 Object Key 安全性

**问题**：文件名可能包含空格、中文、特殊字符（`#`、`?`、`&` 等），这些字符在 object key 中合法，但可能导致 presigned URL 的 URL 编码问题，或在前端渲染时出错。

另：AliOSS object key 上限 **1023 字节**（UTF-8 编码）。中文文件名 + 路径前缀若过长可能超限。

**推荐方案**：在生成 object_key 前对 filename 做 sanitize：
```python
import re, unicodedata
def sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFC", name)
    name = re.sub(r'[^\w.\-]', '_', name)   # 保留字母数字下划线点横线
    return name[:200]  # 留足前缀空间
```
对中文文件名改用 URL-safe 表示或保留原名但强制 UTF-8 编码校验。

---

### 6. 本地文件缺失

**问题**：`present_files` 解析出 physical_path 后，若文件在上传前被删除（agent 的 bash 命令清理了 outputs），`upload_file` 会抛 `FileNotFoundError`。当前逻辑会捕获异常并 fallback 到本地虚拟路径——但该路径同样不存在，前端访问也会 404。

**推荐方案**：在 `upload_local_file` 前主动检查文件是否存在，若不存在则返回明确错误消息给 LLM（ToolMessage error），而非静默 fallback 到无效路径：
```python
if not Path(physical_path).exists():
    raise FileNotFoundError(f"File not found: {virtual_path}")
```

---

### 7. 旧文件清理（存储成本）

**问题**：当前无任何清理机制。每个 thread 的 outputs 文件上传后永久留存于 bucket，随时间积累存储成本。

**推荐方案**（二选一）：

- **Bucket 生命周期规则**：在 AliOSS 控制台对 `agent-artifacts/` 前缀设置过期策略（如 30 天自动删除），无需代码改动。
- **Thread 删除时联动清理**：当 Gateway 删除 thread 时，同步删除 `agent-artifacts/{thread_id}/` 下所有对象（`list_objects` + `delete_multiple_objects`）。适合精确控制，但需代码改动。

---

### 8. 安全性：Presigned URL 无访问控制

**问题**：presigned URL 是公网可访问的带时效链接，任何人得到 URL 均可下载文件，无需身份验证。URL 一旦通过 artifact 发送给前端，就可能被转发或泄露。

**当前立场**：这是 OSS presigned URL 的设计语义，适合"生成后分享"场景。cfgpu 的 generate_image 返回的 URL 同样是公开可访问的。

**若需更严格控制**：可设置 bucket 私有 ACL + 仅通过 Gateway 代理访问（Gateway 验证用户身份后再从 AliOSS 下载并转发），但会增加 Gateway 带宽压力，不建议对视频等大文件使用。

---

### 实现顺序（已完成）

1. `oss/oss_config.py` + `AppConfig` 新增 `oss` 字段
2. `oss/client.py` AliOSS 客户端封装
3. `oss/uploader.py` 上传服务（含 `handle_remote_url`）
4. 修改 `present_file_tool.py`：改为 `async def`，集成上传逻辑
5. 新增 `present_urls_tool.py`：处理 cfgpu 远端 URL
