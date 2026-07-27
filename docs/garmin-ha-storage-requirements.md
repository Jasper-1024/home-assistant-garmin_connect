# Garmin 健康数据接入 Home Assistant：已确认结论与存储要求

更新时间：2026-07-26

## 目标

将 Garmin 已经能够取得的数据，以及当前 `ha_garmin` / Home Assistant
集成已经请求但丢弃或仅放在属性中的数据，完整接入 Home Assistant。

Home Assistant 应当是主要查询入口和长期数据源。除非经过进一步研究确认
Home Assistant 没有受支持的实现路径，否则不接受用小时
`min/max/mean` 代替原始分钟级生理数据，也不优先建设独立数据库。

## 已确认的数据链路问题

当前数据链路：

```text
Garmin API -> ha_garmin -> coordinator -> Home Assistant 实体/属性 -> Recorder
```

已经确认存在三类遗漏：

1. Garmin 原始响应已经取得，但 `ha_garmin` 在返回 coordinator 前丢弃。
2. coordinator 中存在，但 Home Assistant 集成没有建立实体。
3. 数据只保存在实体属性中，不能自然形成独立长期曲线。

另有一类必须单独区分：探测程序成功请求过，但当前集成的日常刷新流程没有请求。

## 已确认取得但被忽略的重要数据

### 睡眠

一次真实睡眠响应包含 20 个顶层数据组，`dailySleepDTO` 包含 41 个字段。
已确认存在下列原始序列：

| 数据 | 已确认样本数 |
| --- | ---: |
| 睡眠阶段 | 27 段 |
| 睡眠动作 | 648 点 |
| 睡眠心率 | 264 点 |
| 夜间 HRV | 105 点 |
| 睡眠 Body Battery | 177 点 |
| 睡眠压力 | 177 点 |
| 睡眠呼吸 | 264 点 |
| 呼吸平均数据 | 10 点 |
| 睡眠血氧 | 526 点 |
| Restless moments | 35 个事件 |

当前 `fetch_core_data()` 只抽取睡眠分数、各阶段总时长、睡眠需求最终值和
睡眠/起床时间等少数汇总值。下列数据已经在 Garmin 响应中，但被丢弃：

- 睡眠需求基准；
- HRV 调整；
- 午睡调整；
- 睡眠历史调整；
- 训练调整和反馈；
- 睡眠需求反馈；
- 睡眠分数各子项、评价和个性化洞察；
- 夜间心率、HRV、压力、呼吸、血氧、Body Battery 的原始时间序列。

### 每日摘要

真实每日摘要约有 94 个字段。已确认进入 coordinator、但没有独立实体的
重要数据包括：

- 起床时 Body Battery；
- 睡眠期间 Body Battery 变化；
- 清醒期间平均呼吸率；
- Body Battery 活动事件和动态反馈；
- Wellness 描述；
- 净热量目标和净剩余热量；
- 活动产生的静息热量。

### 训练和活动

已经成功取得但主要只存在于实体属性中的数据包括：

- 急性、慢性训练负荷；
- ACWR、ACWR 状态及反馈；
- 低有氧、高有氧、无氧负荷和目标范围；
- fitness trend；
- 精确 VO2 Max；
- 活动训练负荷；
- 有氧、无氧训练效果；
- 心率区间时间和其他活动明细。

只放在属性中不能满足独立曲线、长期统计和稳定自动化的需求。

## 核心存储要求

以下高分辨率数据不能只保留小时 `min/max/mean`：

- 分钟级和睡眠期心率；
- 夜间 HRV 原始点；
- 压力原始点；
- 血氧原始点；
- 呼吸原始点；
- Body Battery 变化明细；
- 步数和楼层分段；
- 睡眠阶段、Move IQ、异常心率等区间或事件。

原因是尖峰、异常持续时间、变化速度以及多个生理指标之间的时间关系本身
具有分析价值。小时聚合可以作为附加统计，但不能替代原始记录。

## 已确认的 Home Assistant 表面限制

普通实体通过常规 `Entity.async_write_ha_state()` 写入时，Recorder 默认记录
Home Assistant 收到状态的时间。Garmin 常在手机/手表同步后批量返回带有
过去时间戳的样本，因此直接把数组逐项作为普通实体更新，会把整批数据记录在
同步时刻。

把数组放入实体属性虽然能保存 JSON，但 Home Assistant 的 History 和
Statistics 不会自动解释数组内部的采样时间戳；大型频繁变化属性还会显著
增加 Recorder 负担。

当前 HA 2026.7.4 的公开 `async_import_statistics` /
`async_add_external_statistics` 路径导入小时统计，并要求时间戳位于整点。
这证明它不能直接替代完整分钟级原始记录，但尚不能据此断言 Home Assistant
没有其他受支持或可扩展的高分辨率存储方案。

### 2026-07-26 源码核对后的重要修正

当前实际运行的 HA 2026.7.4 中，
`homeassistant.core.StateMachine.async_set()` 的签名包含：

```python
timestamp: float | None = None
```

该时间戳随后会用于创建 `State` 的更新时间和 `state_changed` 事件的
`time_fired`。因此，“HA 状态机绝对不能写入带过去时间戳的状态”这一说法
不成立；至少从当前源码看，存在直接携带原始时间戳写入状态机的技术路径。

但此路径是否适合自定义集成批量回填仍未确认，必须继续验证：

- `timestamp` 参数是否属于对第三方集成稳定、受支持的契约；
- 回放过去状态时，状态机当前状态是否会被临时倒退；
- 是否会触发自动化、模板、事件监听器和日志；
- Recorder 是否接受乱序状态，以及 History 查询是否正确；
- 已经编译完成的短期/长期统计是否会重新计算；
- 批量写入数千个历史状态的性能、去重和幂等性；
- 普通 states 表的清理策略能否满足长期保存。

这是一条优先研究路线，暂时不能因为存在风险就排除。

### Recorder 长期统计内部路径的新线索

HA 2026.7.4 的公开 statistics 导入包装器会检查时间戳必须位于整点，然后把
任务交给 Recorder。继续核对内部实现后发现：

- `statistics` 数据表本身没有整点数据库约束；
- Recorder 内部 `async_import_statistics(..., table=Statistics)` 队列可以
  接收任意 `StatisticData.start`；
- 内部 `_import_statistics_with_session()` 按 `metadata_id + start` 插入或
  更新记录，没有再次执行整点校验；
- `period="hour"` 的 statistics 查询直接读取 `Statistics` 行，不会把每小时
  内的多条记录再次聚合为一条；
- 长期 `statistics` 表不同于普通 states 和 `statistics_short_term`，通常
  不参与常规短期清理。

因此存在一条技术候选路径：绕过公开包装器，使用 Recorder 内部任务把每个
Garmin 原始样本作为长期 statistic 写入，并保留原始采样时间。这样可以避免
状态机回放触发自动化，也仍然把 Home Assistant Recorder 作为唯一数据源。

但公开接口显式限制整点说明非整点写入当前不属于受承诺的稳定用法。必须验证：

- Recorder repairs、统计修复和元数据管理是否会把非整点行视为异常；
- 日/周/月聚合是否对同一小时内多条记录正确加权；
- Statistics Graph 和 HACS 图表是否能显示非整点长期统计；
- HA 升级时内部 API 与表模型的兼容性；
- 是否能将这一能力封装并测试，而不直接执行 SQL；
- 是否应向上游提出受支持的高分辨率 external statistics 接口。

## 当前 HA Recorder 基线

2026-07-26 对实际 HA 机器执行只读检查：

- Recorder 使用默认配置和默认 SQLite；
- `/config/home-assistant_v2.db` 约 441 MB；
- `states` 约 534,715 行；
- `statistics_short_term` 约 631,543 行；
- 长期 `statistics` 约 1,773,669 行；
- 当前普通 states 覆盖约 11 天，与默认清理周期基本一致。

这说明 Recorder 已经能够管理百万级统计记录，但若通过提高全局
`purge_keep_days` 永久保留原始 states，也会同时长期保留其他 HA 实体历史。
在没有逐实体保留周期的情况下，不能只用“把清理周期改成几年”作为最终方案。

## 尚待专项研究的问题

1. `StateMachine.async_set(timestamp=...)` 是否可作为受支持的历史状态回填
   接口，以及如何避免影响状态机当前状态和自动化。
2. Recorder 是否另有受支持的历史状态回填接口，可以保留原始采样时间。
3. `statistics_short_term` 是否有公开或集成可用的导入路径，以及保留策略。
4. 是否存在官方集成将云端批量历史数据回填成高分辨率 HA 历史的先例。
5. 是否可以在不直接修改 Recorder SQL 表的情况下，让 History Graph、
   Statistics Graph 或其他 HA 前端读取原始历史样本。
6. Event、event entity、calendar、长期统计或其他 HA 原生模型是否适合
   睡眠阶段和异常事件。
7. 若必须增加存储层，能否仍由 HA Recorder 或 HA 集成统一管理和查询，
   而不是引入用户需要单独维护的数据库服务。
8. Recorder 当前数据库结构、时间戳约束、去重、更新和清理机制是否允许
   安全扩展。

## 当前决策

- 暂不确定采用独立 Garmin 明细数据库。
- 暂不接受仅保存小时聚合后删除原始生理样本。
- 小时/每日统计可以保留，但只能作为原始数据之上的派生层。
- 下一步必须以 Home Assistant 官方文档、HA 2026.7.4 源码和官方集成案例
  为依据完成专项研究，再确定实现架构。

## 2026-07-26 原型结论

- 状态时间戳路径已在 HA 2026.7.3 与 2026.7.4 验证：原始时间、重复值、
  重启持久化和 History 查询均正常，但每个回填点都会触发 `state_changed`
  并改变当前状态。
- 非整点长期 statistics 路径已验证：任意分钟原始点可写入并跨重启保留，
  不触发状态事件；同时间戳修订实际已持久化且没有重复行。
- 修正原型的 Recorder 等待竞态后，statistics 修订值在当前 HA 进程内立即
  可查询，不需要重启 HA Core、容器或宿主机；HA 2026.7.3 与 2026.7.4
  均完整通过。
- 当前两条路径都已证明技术有效；非整点长期 statistics 是数值原始序列的
  更优候选，但因使用内部 API，仍需前端、清理修复和升级兼容性验证。
- 详细实验数据与复现命令见 `docs/ha-high-resolution-history-research.md`。

## 当前证据

- [Home Assistant Recorder](https://www.home-assistant.io/integrations/recorder/)
- [Home Assistant Sensor entity and long-term statistics](https://developers.home-assistant.io/docs/core/entity/sensor/)
- [Home Assistant state reported timestamp](https://developers.home-assistant.io/blog/2024/03/20/state_reported_timestamp/)
- [Recorder statistics API changes](https://developers.home-assistant.io/blog/2025/10/16/recorder-statistics-api-changes/)
- 本项目能力审计账本：`/home/js/garmin-capability-audit-ledger.json`
- 当前集成实体定义：`custom_components/garmin_connect/sensor.py`
