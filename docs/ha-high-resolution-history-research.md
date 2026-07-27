# Home Assistant 高分辨率历史数据存储与回填专项研究

更新时间：2026-07-26  
核对版本：Home Assistant Core `2026.7.4`

## 研究问题

Garmin 经常在手机或手表完成同步后，一次返回过去数小时的一批样本。我们需要：

- 保留每个样本的 Garmin 原始采样时间，而不是 HA 收到整批数据的时间；
- 完整保留分钟级或更细的心率、HRV、压力、血氧、呼吸、Body Battery、
  步数和楼层分段；
- 保留睡眠阶段等区间/事件；
- 能从 Home Assistant 查询和显示；
- 优先让 HA Recorder 继续成为唯一数据源，不先建设独立 Garmin 数据库；
- 严格区分受支持的公开 API、可用但不稳定的内部 API，以及直接 SQL 修改。

## 结论摘要

上一阶段“HA 只能保存小时 min/max/mean”的结论不成立，必须修正：

1. HA 2026.7.4 的 `StateMachine.async_set()` 有公开的 `timestamp` 参数。
   Recorder 会把该时间写入 `states.last_updated_ts`，History 查询也按它排序。
   因而 HA **能够在 `states` 表中保存带原始过去时间戳的高分辨率数据**。
2. 这不是一个专门设计的“历史批量导入 API”。逐点回放会修改当前状态并产生
   `state_changed` 事件，从而可能触发自动化和其他监听器。
3. 普通 `states` 默认只保留 10 天。HA 没有按实体配置不同保留周期的公开选项；
   为 Garmin 原始点关闭全局清理，也会保留其他所有实体的原始历史。
4. 公开 `async_import_statistics()` / `async_add_external_statistics()` 明确只接受
   整点的小时统计，不能作为公开的分钟级原始样本导入接口。
5. Recorder 内部任务实际上允许向长期 `statistics` 表写入任意时间戳；查询
   `period="hour"` 时会原样返回同一小时内的多行。这能做到“HA 单一数据库、
   原始时间戳、长期不清理、无状态事件”，但它绕过了公开包装器的整点检查，
   **不属于 HA 承诺支持的用法**。
6. `EventEntity` 只支持“现在发生”的事件，内部 `_trigger_event()` 固定使用
   `utcnow()`，并不是历史事件回填模型。普通 EventBus 虽可传 `time_fired`，
   但同样会即时分发给监听器，且事件表受 Recorder 清理周期影响。
7. HA 当前没有公开、稳定、无副作用、可批量导入任意历史样本的 Recorder API。
   但是，在不建立独立数据库的前提下仍有可行路线，最值得验证的是：

   - 数值序列：Recorder 内部长统计导入的受控兼容层；
   - 睡眠阶段：历史状态回放，或把阶段编码为高分辨率 statistic 后用专用卡片显示；
   - 中长期：向 HA 上游增加正式的高分辨率 external sample/history import API。

因此，当前不需要立即建设独立数据库，也不能再用小时聚合替代原始数据。

## 1. 普通实体为什么通常记录成同步时间

标准实体通过 `Entity.async_write_ha_state()` 写状态。HA 2026.7.4 的实现最终调用
`StateMachine.async_set_internal(..., time_now)`，时间由 HA 写状态时生成，实体本身
没有传入历史采样时间的接口。

来源：

- [`Entity._async_write_ha_state()`，HA 2026.7.4](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/helpers/entity.py#L1182-L1302)
- [官方“Fetching data”文档](https://developers.home-assistant.io/docs/integration_fetching_data/)

因此，如果 08:30 才收到 01:00 至 07:30 的 Garmin 数组，单纯反复修改实体的
`native_value` 并调用 `async_write_ha_state()`，Recorder 看到的是 08:30 附近的
一串更新。

## 2. 关键修正：状态机可以携带过去时间戳

### 2.1 公开 Core 方法确实存在 `timestamp`

HA 2026.7.4：

```python
def async_set(
    self,
    entity_id: str,
    new_state: str,
    attributes: Mapping[str, Any] | None = None,
    force_update: bool = False,
    context: Context | None = None,
    state_info: StateInfo | None = None,
    timestamp: float | None = None,
) -> None:
```

该方法把 `timestamp` 传给 `async_set_internal()`，后者用它构造：

- `State.last_updated`
- `State.last_changed`
- `Context` 的 ULID 时间
- `state_changed` 事件的 `time_fired`

来源：

- [`StateMachine.async_set()`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/core.py#L2327-L2357)
- [`StateMachine.async_set_internal()`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/core.py#L2359-L2466)

`async_set()` 本身没有下划线，官方入门文档也把 `hass.states.async_set()` 作为
集成可调用的 Core API；但官方文档没有把它的 `timestamp` 参数单独定义为
“历史导入契约”。

来源：

- [官方 Creating your first integration](https://developers.home-assistant.io/docs/creating_component_index/)

因此准确分类应当是：

> `StateMachine.async_set()` 是公开 Core 方法；`timestamp` 在公开方法签名中。
> 但 HA 没有承诺它是一套批量、幂等、无副作用的历史数据导入 API。

相比之下，紧接着的 `async_set_internal()` 在源码注释中明确写着仅供 Core
内部使用、不稳定，集成不应直接调用。

### 2.2 Recorder 会保存传入的原始时间

Recorder 处理 `state_changed` 时，从 `new_state.last_updated_timestamp` 创建
`States` 数据库行，而不是重新使用 Recorder 收到事件的当前时间。

来源：

- [`States.from_event()`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/db_schema.py#L480-L516)
- [Recorder 官方数据流程说明](https://www.home-assistant.io/integrations/recorder/)

所以：

```python
hass.states.async_set(
    "sensor.garmin_raw_heart_rate",
    "62",
    force_update=True,
    timestamp=garmin_sample_timestamp,
)
```

可以在 `states.last_updated_ts` 中留下 Garmin 原始时间，而不是同步时间。

### 2.3 History 查询按原始时间读取

History 的 SQL 查询条件和排序字段都是 `States.last_updated_ts`：

- 过滤 `start_time < last_updated_ts < end_time`
- 按 `metadata_id, last_updated_ts` 排序

WebSocket API `history/history_during_period` 返回每条状态的 `last_updated`
时间戳，并允许关闭显著变化过滤及最小响应压缩。

来源：

- [`history/history_during_period` WebSocket API](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/history/websocket_api.py#L77-L149)
- [Recorder history SQL](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/history/__init__.py#L153-L217)
- [官方 REST History API](https://developers.home-assistant.io/docs/api/rest/#get-apihistoryperiodtimestamp)

要保留连续相同数值的每一个原始样本，需要：

- 写入时 `force_update=True`；
- 查询时 `significant_changes_only=False`；
- 查询时 `minimal_response=False`，否则中间连续相同状态会被压缩。

### 2.4 前端能显示，但长期大范围会慢

官方 History Graph 读取 Recorder 状态历史。只要原始 states 尚未被清理，
分钟级数据可以作为普通数值曲线显示。官方同时明确提醒：时间范围大且状态变化
很多时，渲染可能明显延迟。

来源：

- [History graph card](https://www.home-assistant.io/dashboards/history-graph/)

标准 History Graph 适合查看一天或几天的高分辨率曲线。更长范围可以用自定义
Lovelace 卡片调用相同的 HA History WebSocket API，按可见时间窗口加载；数据源
仍然是 HA，不需要外部数据库。

## 3. `async_set(timestamp=...)` 路线的限制

### 3.1 它会改变状态机中的“当前状态”

状态机按调用到达顺序更新当前状态，不会因为传入的是过去时间而只写数据库。
如果倒序回填：

1. 当前实体可能暂时回到昨天的值；
2. `old_state` 链按到达顺序形成，而不是时间顺序；
3. 最后一次调用决定当前状态，与时间戳大小无关。

History SQL 会重新按 `last_updated_ts` 排序，所以历史曲线仍可正确排序；但集成
必须：

- 首次全量回填严格按时间升序；
- 增量只写比现有最新点更新的样本；
- 回填后再恢复真正的当前/最新状态；
- 对 Garmin 修订旧点、补洞和重复点设计额外幂等逻辑。

### 3.2 它会即时发出 `state_changed`

即使事件的 `time_fired` 是过去时间，监听器是在导入发生时立即收到事件。
这可能触发：

- 自动化；
- 模板实体；
- 状态触发器；
- 日志和 Activity 处理；
- 其他订阅状态变化的集成。

History live stream 自己会跳过早于订阅起点的旧事件，但普通状态监听器没有这个
统一保护。

来源：

- [`StateMachine.async_set_internal()` 发出事件](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/core.py#L2448-L2466)
- [History live stream 跳过旧事件](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/history/websocket_api.py#L321-L351)

因此应使用专门的 `sensor.garmin_raw_*` 实体，并明确不在自动化中订阅这些
“历史回放实体”。即便如此，系统级监听器仍会处理事件，所以必须先小规模验证。

### 3.3 原始 states 默认只保留 10 天

Recorder 默认：

```yaml
recorder:
  auto_purge: true
  purge_keep_days: 10
```

HA 2026.7.4 配置校验要求 `purge_keep_days >= 1`。可以用
`auto_purge: false` 禁止自动清理，但这是全局设置，不是只针对 Garmin。

来源：

- [Recorder 官方配置](https://www.home-assistant.io/integrations/recorder/)
- [`CONFIG_SCHEMA` 默认值与范围](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/__init__.py#L96-L118)
- [Recorder 夜间全局清理](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/core.py#L500-L516)

当前实际 HA 数据库已经约有：

- `states` 534,715 行，覆盖约 11 天；
- `statistics_short_term` 631,543 行；
- 长期 `statistics` 1,773,669 行；
- SQLite 文件约 441 MB。

如果为了 Garmin 把所有 `states` 永久保留，其他设备、自动化和系统实体的原始
历史也会永久保留。HA 当前没有公开的“按实体设置不同 purge_keep_days”配置。
这是状态回放方案用于多年原始数据时最大的结构性缺点。

### 3.4 过去的长期统计不会自然重算

Recorder 每 5 分钟编译短期统计，再按小时生成长期统计。补写已经完成统计运行的
过去 states，并不会自动让所有旧的统计窗口重新编译。`compile_missing_statistics`
主要补齐缺失的运行窗口，也只在 Recorder 保留期内工作。

来源：

- [`compile_missing_statistics()`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/statistics.py#L608-L650)
- [`compile_statistics()` 5 分钟流程](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/statistics.py#L652-L824)

这不影响原始 History 查询，但意味着不能指望回放 states 后，HA 自动补齐过去
所有派生长期统计；每日/小时派生值仍要单独导入或重算。

## 4. 公开 statistics API 的真实边界

公开的：

- `async_import_statistics()`
- `async_add_external_statistics()`
- WebSocket `recorder/import_statistics`

都会经过 `_async_import_statistics()`。它明确拒绝非整点时间：

```python
if start.minute != 0 or start.second != 0 or start.microsecond != 0:
    raise HomeAssistantError(
        "Invalid timestamp: timestamps must be from the top of the hour"
    )
```

来源：

- [公开 statistics 导入包装器和整点校验](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/statistics.py#L2764-L2894)
- [WebSocket import_statistics](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/websocket_api.py#L559-L620)

因此，公开 external statistics 可以保存小时/每日派生统计，但不能合法地保存
01:02、01:04、01:06 的三个 Garmin 原始点。

`statistics_short_term` 虽然是 5 分钟粒度，但没有公开的导入 API。它主要由
Recorder 自己从实时 states 编译，而且仍会被清理，既不能完整保存分钟点，也不
满足长期原始存储要求。

## 5. Recorder 内部存在“非整点长期 statistic”路径

继续核对 2026.7.4 源码发现：

1. `Statistics.start_ts` 是普通浮点时间戳；
2. 唯一索引是 `(metadata_id, start_ts)`，数据库没有整点约束；
3. Recorder 实例内部的 `async_import_statistics(metadata, stats, table)`
   可以明确选择 `Statistics` 或 `StatisticsShortTerm`；
4. `_import_statistics_with_session()` 只按 `metadata_id + start` 更新或插入，
   不重复执行整点检查；
5. `period="hour"` 查询 `Statistics` 时不按小时再次聚合，而是按 `start_ts`
   返回范围内全部行；
6. 长期 `statistics` 不随普通 states 的保留期一起清理。

来源：

- [`Statistics` 数据模型和唯一索引](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/db_schema.py#L626-L714)
- [Recorder 内部 `async_import_statistics`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/core.py#L609-L620)
- [`_import_statistics_with_session()`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/statistics.py#L2897-L2928)
- [按时间戳直接查询 statistics 行](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/statistics.py#L1437-L1460)
- [`period="hour"` 选择长期表且不做 day/week/month/year 归并](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/statistics.py#L2114-L2209)

技术上可以让每个样本成为一条长期 statistic：

```text
start = Garmin 原始时间
state = value
mean = value
min = value
max = value
```

它具备几个明显优势：

- HA Recorder 仍是唯一数据库；
- 精确保留每个原始采样时间；
- 长期表不会按 10 天清理；
- 不修改状态机当前值；
- 不发出 `state_changed`，不会触发自动化；
- 通过 HA statistics 查询接口可读取。

但必须明确：**这是内部 API 兼容层，不是受支持的公开方案。**

公开包装器特意限制整点，绕过该校验意味着：

- HA 可以随版本改变内部方法或表模型；
- Statistics Graph 假设数据是统计周期，部分展示可能按一小时宽度解释数据；
- 日/周/月归并使用 `Statistics.duration == 1 hour` 作为权重，非均匀采样时的
  `mean` 语义可能错误；
- Recorder repairs 虽未发现会主动删除非整点行的当前逻辑，但上游没有承诺
  永远接受这种数据；
- 必须为每个目标 HA 版本运行兼容性测试。

这条路径比直接 SQL 安全：写入仍经过 Recorder 队列、SQLAlchemy、元数据管理、
唯一索引及事务。但它仍然属于内部 API。

## 6. 官方集成先例

对 HA Core 2026.7.4 的 `homeassistant/components` 做了源码搜索：

- 没有找到官方集成使用
  `hass.states.async_set(..., timestamp=<历史时间>)` 批量回填历史状态；
- Suez Water、Tibber、Opower、SRP Energy、Anglian Water、WaterFurnace、
  SolarEdge 等官方集成会导入云端历史，但使用的是公开
  `async_add_external_statistics()`，数据按小时统计；
- 没有官方集成把任意分钟级云端历史作为公开 external statistics 导入。

官方先例证明“云端历史回填到 HA Recorder”是被支持的需求，但当前公开契约只
覆盖小时统计，尚未覆盖 Garmin 这种高分辨率原始生理样本。

## 7. 睡眠阶段和事件如何表示

### 7.1 睡眠阶段最自然的 HA 表示：枚举状态转移

例如：

```text
23:41 awake
23:49 light
00:26 deep
01:03 light
01:28 rem
```

把阶段作为 `SensorDeviceClass.ENUM` 的状态，区间结束时间自然由下一次状态变化
确定。HA History 对离散状态本来就按状态带显示。

来源：

- [Sensor enum device class](https://developers.home-assistant.io/docs/core/entity/sensor/)

如果通过 `StateMachine.async_set(timestamp=...)` 按时间升序回放，这种表示能
精确保留阶段区间。但是它仍受 states 清理周期和状态事件副作用影响。

### 7.2 EventEntity 不支持历史回填

官方 Event entity 表示“物理世界现在发生了一个事件”，状态是最近事件发生的
时间。它的 `_trigger_event()` 直接使用 `dt_util.utcnow()`，调用方不能传过去
时间。

来源：

- [Event entity 官方文档](https://developers.home-assistant.io/docs/core/entity/event/)
- [`EventEntity._trigger_event()`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/event/__init__.py#L158-L169)

所以睡眠阶段不应建成 EventEntity；Move IQ 和异常心率事件如果是实时到达可以用
EventEntity，批量历史回填则不合适。

### 7.3 EventBus 能带历史时间，但仍有副作用

`EventBus.async_fire()` 也有公开的 `time_fired` 参数。Recorder 会保存该事件的
时间，但事件会在导入当下分发给所有监听器，且 Event 表受全局 Recorder 清理。

来源：

- [`EventBus.async_fire()`](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/core.py#L1506-L1538)
- [Recorder 对非状态事件的写入](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/core.py#L1031-L1075)

另外，HA 没有像 History states 那样面向普通集成公开一个通用的“按类型查询全部
历史事件数据”前端接口。Logbook 需要额外平台描述，EventEntity 又只保留最近
事件。因此 EventBus 适合实时自动化，不是 Garmin 历史事件档案的理想模型。

### 7.4 Calendar 不负责保存历史

Calendar entity 可以很好地显示开始/结束区间，但 Calendar 集成需要在查询时从
自己的数据源返回事件。Recorder 不会自动把 Calendar 查询结果变成永久历史。
如果没有另外的数据存储，Calendar 只能作为展示适配器，不能承担原始数据落库。

## 8. 可选架构方案

### 方案 A：历史状态回放，全部放入 Recorder `states`

做法：

- 每个原始指标建立专用 `sensor.garmin_raw_*`；
- 使用 `hass.states.async_set(timestamp=原始时间, force_update=True)`；
- 首次全量严格升序，后续增量写入；
- 睡眠阶段使用枚举 sensor；
- History API/History Graph 直接读取；
- 禁止或大幅延长 Recorder 自动清理。

| 维度 | 评价 |
| --- | --- |
| 精确原始时间 | 支持 |
| 原始分钟分辨率 | 支持 |
| HA 唯一数据源 | 支持 |
| 公开 API | 使用公开 Core 方法，但不是专门历史导入契约 |
| 自动化副作用 | 高 |
| 长期保留 | 必须改变全局清理，代价高 |
| 原生 History 显示 | 最好 |
| 实施难度 | 中 |

适合：小规模原型、最近数周的高分辨率历史、睡眠阶段验证。  
不适合：在当前 441 MB Recorder 上直接永久保存多年全部 raw states。

### 方案 B：非整点长期 statistics 兼容层

做法：

- 不直接执行 SQL；
- 通过 Recorder 内部队列向 `Statistics` 写入一条/原始样本；
- 使用 `(statistic_id, start)` 唯一键天然幂等更新；
- 通过 statistics WebSocket API 或专用 Lovelace 卡片读取；
- 每日/小时汇总另外建立正式、公开语义的 statistics。

| 维度 | 评价 |
| --- | --- |
| 精确原始时间 | 支持 |
| 原始分钟分辨率 | 支持 |
| HA 唯一数据源 | 支持 |
| 公开 API | 不支持，属于内部 API |
| 自动化副作用 | 无 |
| 长期保留 | 支持，长期 statistics 不常规清理 |
| 原生 History 显示 | 不走 History；Statistics Graph 需验证 |
| 升级风险 | 中到高，需版本测试 |
| 实施难度 | 中 |

适合：在用户明确接受维护一个受测试的 HA 版本兼容层时，作为当前最符合
“不丢原始点、不建外部库”的务实方案。

### 方案 C：给 Recorder 增加正式高分辨率样本导入 API

做法可以有两种：

1. 扩展 external statistics，增加明确的 sample/raw resolution 和时间间隔语义；
2. 新增 Recorder sample/history import 任务和公开查询 API，不经过状态机事件。

应具备：

- 任意带时区时间戳；
- 批量写入；
- source + metric + timestamp 唯一键；
- 更新/修正旧样本；
- 不改变当前状态；
- 不触发自动化；
- 按来源或指标配置保留期；
- 数值样本和枚举区间；
- 官方 WebSocket 查询；
- 前端按可见窗口分页/降采样显示，但服务器保留原始点。

| 维度 | 评价 |
| --- | --- |
| 精确原始时间 | 支持 |
| 原始分钟分辨率 | 支持 |
| HA 唯一数据源 | 支持 |
| 公开 API | 如果进入上游则完全支持 |
| 自动化副作用 | 无 |
| 长期保留 | 可正确设计 |
| 实施难度 | 高 |
| 长期维护风险 | 上游合并后最低 |

这是长期最正确的方案。短期可以先做一个小型 Core 原型验证数据模型，再决定是否
向上游提交架构讨论和 PR。若仅本地维护 HA Core 补丁，则仍需承担升级成本。

### 方案 D：直接向 HA 数据库执行 SQL

技术上可以直接插入 `states` 或 `statistics`，但不应采用：

- 必须同步维护 `states_meta`、`state_attributes`、上下文和旧状态关系；
- 会绕过 Recorder 队列、事务、缓存和统计元数据管理；
- schema 会随 HA 升级迁移；
- 写入期间与 Recorder 并发可能破坏一致性；
- HA 官方不支持迁移 Recorder 数据库，更不会支持第三方直接改内部表。

来源：

- [Recorder 官方数据库和迁移警告](https://www.home-assistant.io/integrations/recorder/)

分类：**直接 SQL hack，排除。**

### 方案 E：集成自建明细数据库

这仍然是技术上最独立、语义最自由的传统做法，但不是当前首选。只有在 HA Core
扩展和内部兼容层都无法达到可靠性要求时才重新考虑。

## 9. 推荐路线

### 当前架构决策

先保持 HA Recorder 为唯一健康数据源，不新建 Garmin 明细数据库；同时保留：

- 原始样本；
- 公开 HA 实体；
- 正式的小时/每日派生统计。

小时统计只能加速长期趋势，不能替代或删除原始点。

### 分阶段验证

1. **只读/隔离原型：状态时间戳**
   - 选择一天心率和一晚睡眠阶段；
   - 写入专用测试实体；
   - 验证 History API 返回原始时间、点数、重复值和区间；
   - 明确观测自动化、模板、日志及当前状态变化；
   - 测完删除测试实体历史，不直接全量导入。
2. **只读/隔离原型：非整点长期 statistics**
   - 使用专用 statistic ID 导入几十个非整点测试点；
   - 不直接 SQL；
   - 验证重启、Recorder repair、升级、WebSocket 查询和前端显示；
   - 验证同一时间更新、时区、DST、日/周归并语义。
3. **比较两条路线**
   - 如果内部 statistics 路线通过版本和前端测试，优先用于数值 raw series；
   - 睡眠阶段先用少量历史状态，或在专用卡片中读取数值阶段编码；
   - 派生汇总继续使用公开 external statistics。
4. **形成上游方案**
   - 以实际 Garmin 用例提出 HA Recorder 高分辨率 external sample API；
   - 在公开接口成熟后，从兼容层迁移，不改变 HA 作为唯一数据源的目标。

### 当前推荐

在尚未完成两个隔离原型前，不应全量写入生产 Recorder。

功能上最符合当前目标的是“方案 B + 正式派生 statistics”，因为它避免事件副作用
和全局 states 永久保留；但它是内部 API，只能作为受测试、可迁移的兼容层。
长期目标应是方案 C。

方案 A 证明 HA 的 states 能保存原始时间，因此可用于原型和低数量区间数据；
它不适合作为多年全部分钟数据的最终方案，主要原因不是分辨率，而是状态事件和
全局清理模型。

## 10. 必须保留的验证标准

最终实现必须满足：

- 导入后点数等于 Garmin 去重后的原始点数；
- 每个点的 HA 时间戳等于 Garmin 原始时间戳；
- 连续相同数值不能静默丢失；
- 首次全量和再次同步必须幂等；
- Garmin 对旧日数据修订后能够更新，不制造冲突；
- 睡眠跨午夜和 DST 不错位；
- HA 重启、Recorder purge、repair、备份恢复和版本升级后数据仍可查询；
- 不触发控制设备的自动化；
- 前端只按可见窗口取数，不能一次请求多年百万点；
- 小时/每日统计与原始点并存，不得以聚合替代原始数据；
- 不直接执行 SQL。

## 11. 关键官方证据索引

- [Recorder 官方文档](https://www.home-assistant.io/integrations/recorder/)
- [History Graph 官方文档](https://www.home-assistant.io/dashboards/history-graph/)
- [Sensor entity 官方文档](https://developers.home-assistant.io/docs/core/entity/sensor/)
- [Event entity 官方文档](https://developers.home-assistant.io/docs/core/entity/event/)
- [REST History API](https://developers.home-assistant.io/docs/api/rest/#get-apihistoryperiodtimestamp)
- [`StateMachine.async_set()` 源码](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/core.py#L2327-L2357)
- [`States.from_event()` 源码](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/db_schema.py#L480-L516)
- [History WebSocket 源码](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/history/websocket_api.py#L77-L149)
- [公开 statistics 导入整点校验](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/statistics.py#L2764-L2894)
- [Recorder 内部 statistics 任务](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/core.py#L609-L620)
- [`Statistics` schema](https://github.com/home-assistant/core/blob/2026.7.4/homeassistant/components/recorder/db_schema.py#L626-L714)

## 12. 2026-07-26 隔离原型实测

### 实验边界

- 使用 4 个 Garmin 风格心率样本，原始时间分别为 UTC 01:02、01:04、
  01:07、01:11；
- 前两个样本数值均为 62，用于验证连续相同值不会丢失；
- 使用临时 HA 配置目录和临时 SQLite；
- 分别在本地 HA 2026.7.3 和实际主机容器内的 HA 2026.7.4 运行；
- 没有连接或修改生产 `/config/home-assistant_v2.db`；
- 每条路径都验证写入、HA 查询、停止、重新启动和再次查询；
- 路径 B 额外把 01:04 的 62 修订为 77，验证幂等更新。

### 路径 A：`StateMachine.async_set(timestamp=...)`

两套 HA 版本均完整通过：

- 4 个点全部写入并在重启后保留；
- 4 个原始时间戳精确保留；
- 连续两个 62 均保留，没有被状态去重；
- History 查询返回完整原始序列；
- 每次回放都会发出 `state_changed`，共 4 次；
- 状态机当前状态被改成最后一次调用的 61。

**结论：路径 A 已证实有效，但副作用也被证实。** 它可以作为低数量事件、
睡眠阶段或过渡实现；若导入多年分钟级数据，会触发大量事件、改变当前状态，
并受普通 states 全局清理策略影响，因此暂不作为全部数值原始序列的最终实现。

### 路径 B：Recorder 内部非整点长期 statistics

两套 HA 版本得到相同结果：

- 4 个非整点长期统计全部写入；
- 原始时间戳精确保留；
- 不产生 `state_changed`；
- 重启后 4 个点全部保留；
- 同一 `metadata_id + start` 再写入不会新增第 5 行；
- 修订值 77 在同一 HA 进程中立即可查询；
- 重启后仍然查询得到 77。

初版原型曾在修订后读到旧值 62。继续核对后确认这不是 HA 查询缓存，也不要求
重启 HA：原型调用的 `Recorder.async_block_till_done()` 在任务已经离开队列、
但仍在执行的极短窗口内可能直接返回。改为在导入任务之后明确排入
`SynchronizeTask` 屏障，等待屏障完成后，同一进程立即读到 77。本地
HA 2026.7.3 与实际主机的 HA 2026.7.4 均重新完整通过。

- **非整点原始点写入与长期持久化：有效；**
- **同时间戳修订与即时查询：有效；**
- **无需重启 Home Assistant、容器或宿主机；**
- **路径 B 本轮测试项目全部通过。**

路径 B 仍然是数值原始序列更有希望的候选，因为它不触发自动化，也不依赖
普通 states 的短期保留。但它仍使用 HA 内部 API；在投入生产前还需验证
Statistics Graph/WebSocket、日周归并、purge/repair 和 HA 升级兼容性。

### 可重复运行

原型文件：

- `scripts/prototype_ha_history_logic.py`
- `scripts/prototype_ha_history_paths.py`

本地隔离运行：

```bash
rtk .venv/bin/python scripts/prototype_ha_history_paths.py --run-all
```

该命令每次创建新的临时数据库，结束后自动删除，不读取生产 Recorder。
