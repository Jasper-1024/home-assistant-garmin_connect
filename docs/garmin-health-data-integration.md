# Garmin 健康数据接入 Home Assistant：数据、设计与实施说明

更新时间：2026-08-01

## 1. 文档目的

本文是 Garmin 健康数据改造的主说明书，统一回答四个问题：

1. 为什么现有 Home Assistant 实体不足以保存 Garmin 历史；
2. Garmin 原始响应中实际有哪些数据；
3. 当前插件如何归一化、去重并持久化这些数据；
4. 哪些数据已经确定处理方式，哪些留待后续讨论。

本文记录数据事实和设计约束，不负责长期健康分析。趋势判断、相关性分析和模型推断由
外部程序读取归档后的源记录完成。

更底层的 Recorder 实验、兼容性证据和复现步骤保留在
[Home Assistant 高分辨率历史专项研究](ha-high-resolution-history-research.md)；时区行为见
[HA 时区、存储与显示研究](ha-timezone-storage-display-research.md)。

## 2. 目标和约束

- Home Assistant 是主要查询入口和长期数据源，不额外要求用户维护数据库服务。
- 对支持的数据族保留 Garmin 返回的每个有效源记录，不故意降采样。
- 分钟级生理数据不能仅用小时 `min/max/mean` 代替。
- 缺失保持缺失，不补零、不插值，也不把缺失伪装成设备不支持。
- 自动归档从启用日期向后运行，不自动回填过去一年；手工修复范围仍受限制。
- 正常目标是数据上传 Garmin 后约十五分钟内进入 HA，不追求实时性。
- 当前集成调用未公开 Garmin Connect 端点。端点、字段、认证和限流没有稳定性承诺。
- 原始响应和健康值只允许保存在本机私有位置；文档和普通诊断不记录凭据或真实数值。

## 3. 数据为什么不能全部做成普通实体

当前值链路是：

```text
Garmin API -> ha-garmin -> coordinator -> HA entity -> Recorder states
```

普通实体只表示一个当前状态。Recorder 可以记录实体历次上报，但默认记录 HA 收到状态
的时间。Garmin 常在手表或手机同步后一次返回一批带过去时间戳的数据；逐点写普通实体会
把整批点压在同步时刻，还会改变当前状态并触发状态事件。

复杂数组放在实体属性中也不能解决问题：History 和 Statistics 不理解数组内部时间戳，
大属性还会放大 Recorder 负担。普通 `states` 和 `statistics_short_term` 也通常按 Recorder
保留期清理，不能承担多年分钟级数据。

当前数据形态因此分为四类：

| 数据形态 | 例子 | 当前合适的 HA 存储 |
| --- | --- | --- |
| 连续数值点 | 心率、压力、Body Battery、HRV、呼吸、SpO2、步数 | 带源时间的长期 Recorder statistics |
| 区间或会话 | 睡眠阶段、午睡、运动活动 | 插件私有 Store；必要时投影为 Calendar |
| 离散测量或事件 | 体重、血压、异常心率、Body Battery 事件 | 待按测量/事件语义确定 |
| 每日或周期快照 | HRV 状态、VO2 Max、训练状态、睡眠助手 | 年度状态 Store；数值另投影长期 statistics |

## 4. 当前存储架构

### 4.1 当前值实体

插件已有大量 Sensor，适合展示最近值、当日累计和 Garmin 计算摘要，例如当前 Body
Battery、静息心率、睡眠分数、HRV 周平均和训练状态。它们的普通 HA 历史只表示 HA
观察到这些值的时间，不替代 Garmin 源时间序列。

### 4.2 高分辨率长期 statistics

HA 2026.7.4 的公开 external statistics 导入接口要求整点时间。当前插件通过受版本检查
和测试保护的 Recorder 内部队列，把每个 Garmin 数值样本写入长期 `statistics` 表：

- `(statistic_id, start)` 提供幂等插入和修订；
- 写入不改变实体当前状态，也不触发实体自动化；
- 任意分钟时间戳跨重启保留；
- 同一时间戳的新值修订原记录，不产生重复行；
- HA 升级可能改变内部接口，因此启动前必须进行兼容性检查并失败关闭。

这条路径已在 HA 2026.7.3 和 2026.7.4 原型及 beta 现场验证。完整证据见
[高分辨率历史专项研究](ha-high-resolution-history-research.md)。

### 4.3 私有 Store、Calendar 和 FIT

- 睡眠会话、阶段、部分评分子项、事件和活动摘要按年保存在账号隔离的私有 Store。
- Sleep、Health events 和 Activities Calendar 是 Store 的只读投影，不负责原始落库。
- 活动 FIT 文件保留会话内传感器细节，不与全天 wellness 序列混合。
- 停用归档、重载、升级或回滚不删除已有 statistics、Store、Calendar 或 FIT 数据。

当前 Sleep Store 尚未保存完整 `sleepNeed`、`nextSleepNeed`、需求调整和三类睡眠反馈。
第 8 节定义的年度状态 Store 是目标设计，不应误写成当前实现。

## 5. 证据等级和状态词

每条数据结论必须标明依据，避免把一次现场响应误写成 Garmin 永久契约。

| 标记 | 含义 |
| --- | --- |
| 接口/schema | 官方资料、开源客户端或当前代码证明字段可能存在 |
| 捕获确认 | 本账号完整原始响应中观察到字段和数据形态 |
| beta.12 已归档 | 当前插件已经归一化并持久化 |
| 已讨论方向 | 当前讨论认可，但代码尚未按该方式统一 |
| 待讨论 | 尚未决定 HA 存储身份或查询表面 |

缺失状态必须区分：

- `0`：Garmin 明确返回真实零值；
- `null`：字段或数组明确为 null；
- `empty`：接口返回空数组；
- `missing`：已知字段不存在；
- `all-null`：数组存在但没有有效数值；
- `unsupported`：有明确能力或 schema 证据证明不支持；
- `failed`：请求、解析或持久化失败。

## 6. 跟踪级数据血缘目录

“跟踪级”指带原始采样时间的数值点，或带开始和结束时间的状态区间。每日总量、周平均
和当前值不属于本节。

### 6.1 Body Battery

| 项目 | 内容 |
| --- | --- |
| 逻辑 key | `body_battery` |
| 主要端点 | `bodyBattery/reports/daily` |
| 主要数组 | `bodyBatteryValuesArray` |
| 补充端点 | `bodyBattery/events/{date}` |
| 补充数组 | 事件中的 `bodyBatteryValuesArray` |
| 值字段 | `bodyBatteryValue`、`bodyBatteryLevel` 或描述符对应列 |
| 当前落库 | 账号级 `body_battery` statistic；睡眠副本另按会话写入 |

处理特点：

- 日报告与 Body Battery 事件按 UTC 源时刻合并；相同时间戳相同值去重，冲突值导致
  schema 失败，不能静默任选一个。
- 2026-08-01 现场主序列有 201 点，常见间隔约三分钟。
- 睡眠 Body Battery 的 199 点全部存在于主序列，时间和值完全相同。
- **已讨论方向**：账号级全天序列是规范曲线；睡眠副本保留原始来源，但不作为第二个
  逻辑指标。

### 6.2 心率

| 项目 | 内容 |
| --- | --- |
| 逻辑 key | `heart_rate` |
| 全天端点 | `dailyHeartRate/{displayName}?date=` |
| 全天数组 | `heartRateValues` |
| 睡眠来源 | `dailySleepData.sleepHeartRate` |
| 值字段 | `heartRate`、`heartRateValue` 或描述符对应列 |
| 当前落库 | 全天 `heart_rate` 与每个睡眠会话的 `sleep_heart_rate:<session>` 分开 |

处理特点：

- 全天与睡眠序列按绝对源时刻去重。
- 现场重合的 274 点时间和值完全相同；睡眠来源另补充午夜前 24 点。
- 两源合并后得到 423 个唯一点，23:12–13:16 最大间隔两分钟。
- **当前问题**：数据仍分散在全天和每会话 statistics 中。
- **已讨论方向**：规范心率曲线应合并两源；Store 继续保留睡眠会话归属。

### 6.3 压力

| 项目 | 内容 |
| --- | --- |
| 逻辑 key | `stress` |
| 主要端点 | `dailyStress/{date}` |
| 主要数组 | `stressValuesArray` |
| 补充端点 | `bodyBattery/events/{date}` |
| 补充数组 | 事件中的 `stressValuesArray` |
| 当前落库 | 账号级 `stress` statistic；睡眠副本另按会话写入 |

处理特点：

- 主端点和事件端点按源时刻合并；负值状态码不能伪装成生理压力值。
- 2026-08-01 当日从 00:00 到 13:15 保存 266 点，常见间隔三分钟。
- 睡眠压力的 199 点全部存在于全天序列，时间和值完全相同。
- **已讨论方向**：全天 `stress` 是规范曲线；真实缺口保持缺失。

### 6.4 夜间 HRV

| 项目 | 内容 |
| --- | --- |
| 逻辑 key | `nightly_hrv` |
| 端点 | `hrv-service/hrv/{date}` |
| 数组 | `hrvReadings` |
| 时间字段 | `readingTimeGMT`、`readingTimeGmt` 或 `readingTime` |
| 值字段 | `hrvValue` |
| 摘要 | `hrvSummary`：夜间平均、5 分钟最高、周平均、状态、baseline |
| 当前落库 | 原始点进入 `nightly_hrv`；摘要进入私有有界目录；睡眠 HRV 另按会话写入 |

处理特点：

- HRV 在本设备上只有夜间序列，不构造全天 HRV。
- GMT 字段按 UTC 解析；相同时间戳保留最后一个有效源记录。
- 现场 Nightly HRV 与 Sleep HRV 均为 119 点，时间和值完全相同，常见间隔五分钟。
- **已讨论方向**：`nightly_hrv` 是唯一规范原始曲线；HRV 状态等摘要按第 8 节的每日
  状态快照处理。

### 6.5 分段步数

| 项目 | 内容 |
| --- | --- |
| 逻辑 key | `steps` |
| 端点 | `dailySummaryChart/{displayName}?date=` |
| 数组别名 | `stepsValues`、`stepsValuesArray`、`chartData` 或 `data` |
| 值字段 | `steps`、`stepCount` 或 `value` |
| 每日摘要 | `totalSteps` 单独保存，不替代分段点 |
| 当前落库 | `steps` 与 `steps_daily_total` |

处理特点：

- 分段点和每日总量具有不同身份；不能用总量复制成全天曲线。
- 2026-08-01 现场保存 54 个分段点，常见间隔十五分钟。

### 6.6 呼吸

| 项目 | 全天来源 | 睡眠来源 |
| --- | --- | --- |
| 端点 | `daily/respiration/{date}` | `dailySleepData` |
| 数组 | `respirationValuesArray` | `wellnessEpochRespirationDataDTOList` |
| 时间字段 | 描述符 `timestamp` | `startTimeGMT` |
| 值字段 | 描述符 `respiration` | `respirationValue` |
| 时间语义 | 两分钟 epoch 结束 | 两分钟 epoch 开始 |
| 当前落库 | `respiration_raw` | `sleep_respiration:<session>` |

处理特点：

- 不能直接按相同时间戳比较两源。睡眠时间加 120 秒后，现场 273 个重合点的数值全部
  一致；此前看到的差异来自周期开始/结束锚点，不是两种呼吸测量。
- 全天端点另含 `respirationAveragesValuesArray`，但小时平均不进入规范原始曲线。
- **已讨论方向**：全天呼吸作为规范全天曲线；睡眠来源保留会话归属和跨午夜证据，
  不伪装成第二种呼吸指标。

### 6.7 SpO2

| 逻辑序列 | 原始来源 | 当前现场 |
| --- | --- | --- |
| `spo2_hourly` | `daily/spo2/{date}.spO2HourlyAverages` | 14 个小时点 |
| `spo2_continuous` | `continuousReadingDTOList` 等别名 | 0 点 |
| `spo2_single` | `spO2SingleValues` 等别名 | 0 点 |
| `sleep_spo2:<session>` | `dailySleepData.wellnessEpochSPO2DataDTOList` | 592 个分钟级点 |

处理特点：

- 小时全天趋势与分钟级睡眠 SpO2 分辨率和使用语义不同，必须保持两个逻辑序列。
- 空的 continuous/single 数组只表示本次没有返回点，不能写成零或永久 unsupported。

### 6.8 睡眠阶段和睡眠动作

| 数据 | 原始字段 | 当前存储 |
| --- | --- | --- |
| 睡眠阶段 | `sleepLevels[]` 的 `startGMT`、`endGMT`、`activityLevel` | 私有睡眠 Store |
| 睡眠动作 | `sleepMovement` | 每会话 `sleep_movement:<session>` 和 Store |

睡眠阶段是区间，不是数值测量。现场一晚保存 21 段，`activityLevel` 对应深睡、浅睡、
REM 和清醒，并可由区间总时长与现有摘要交叉校验。它不应编码成可以计算平均值的普通
statistic，也不应通过回放当前实体制造历史状态。

睡眠响应还保存每会话心率、HRV、Body Battery、压力、呼吸、SpO2 和动作。当前这些
statistics 的 key 包含睡眠会话 ID；长期运行会每晚产生新序列。该分裂问题已记录，
本轮文档整理不修改实现。

## 7. 当前跟踪级归档状态

2026-08-01 beta.12 现场只读盘点：

| 数据 | 状态 |
| --- | --- |
| Body Battery、心率、压力、夜间 HRV | 已有源时间原始点 |
| 分段步数 | 已有十五分钟点和每日总量 |
| 全天呼吸 | 已有两分钟点；小时平均另存 |
| 睡眠 SpO2、心率、HRV、Body Battery、压力、呼吸、动作 | 已按睡眠会话保存 |
| 睡眠阶段 | 已在私有 Store 保存完整区间 |
| SpO2 小时平均 | 已有点 |
| 全天连续/单次 SpO2 | 元数据存在，当前无点 |
| 分段楼层 | 已退役的固定桶来源，不再自动请求；每日楼层摘要仍保存 |
| 中等/高强度分钟分段 | 当前无有效点；每日总量已保存 |

现场点数只证明当前账号和日期的响应形态，不保证其他设备、固件、地区或日期相同。

## 8. 每日状态快照

每日状态快照是 Garmin 针对一个日历日、睡眠会话或训练周期计算的摘要。它与第 6 节的
连续曲线并存：原始曲线回答“当时测到了什么”，每日快照回答“Garmin 如何评价这一日”。

### 8.1 统一存储模型和保留期

每日状态不能只依赖普通实体。Recorder 的 `purge_keep_days` 默认是十天，普通状态和属性
历史会被清理；当前实体仍适合展示最新值，但不能承担长期归档。

```text
Garmin 每日响应
    -> 账号隔离、按年分区的每日状态 Store（规范记录）
       -> 数值字段：带源日期的长期 Recorder statistics
       -> 最新状态：Sensor、Enum Sensor 或 Binary Sensor
       -> 结构、枚举和文本：保留在 Store
```

统一规则：

- Store 记录身份是账号、数据族、Garmin `calendarDate` 和可选设备身份；缺少明确源时刻时，
  使用 UTC+8 的日历日起点作为数值 statistic 时间桶。
- 同一天相同内容幂等跳过；Garmin 后续返回修订值时更新该日记录；临时空响应不能删除
  已有正常数据。
- 数值、状态、范围、组成因素、调整和反馈均保留。`null`、`empty`、`missing`、
  `unsupported` 和 `failed` 继续保持不同语义。
- Store 和长期 statistics 不自动删除；禁用实体也不删除历史。
- 枚举不能伪装成可求平均的数字。字符串历史永久保存在 Store，实体只投影最新状态。
- 睡眠建议等自由文本随快照保存，但不为每段文字建立实体。
- 调试用完整请求/响应仍保存在临时捕获目录；长期 Store 保存经过 schema 校验的已知字段。
- 本轮不开放 REST、WebSocket、响应动作或自定义卡片。内部读取模块应支持按数据族和日期
  范围读取，为后续查询和外部分析保留稳定接缝。

### 8.2 HRV 每日状态

| 原始字段 | 数据语义 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `calendarDate`、`createTimeStamp` | 所属日期和 Garmin 生成时间 | Store 身份和源时间；不单独建实体 |
| `status` | HRV 状态；现场为 `Balanced` | Store 原始枚举；最新 Enum Sensor |
| `lastNightAvg` | 昨夜平均 HRV；现场 62 ms | Store 和长期 statistic；保留数值 Sensor |
| `lastNight5MinHigh` | 昨夜最高五分钟平均；现场 102 ms | Store 和长期 statistic；保留数值 Sensor |
| `weeklyAvg` | 七日平均；现场 50 ms | Store 和长期 statistic；保留数值 Sensor |
| `baseline.lowUpper` | 低状态上界 | Store 和长期 statistic |
| `baseline.balancedLow` | 平衡区间下界 | Store 和长期 statistic |
| `baseline.balancedUpper` | 平衡区间上界 | Store 和长期 statistic |
| `baseline.markerValue` | 当前值在 baseline 中的位置 | Store 和长期 statistic |
| `feedbackPhrase` | Garmin HRV 反馈 | Store；不单独建实体 |

`hrvReadings` 仍归 `nightly_hrv` 规范曲线。睡眠响应中的重复 HRV 保留会话来源，但不创建
第二套账号级历史。

### 8.3 训练状态、训练负荷和 VO2 Max

#### 训练状态

| 原始字段 | 数据语义 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `calendarDate`、`sinceDate` | 所属日期和当前状态起始日 | Store；`sinceDate` 可作为最新实体属性 |
| `trainingStatus` | 原始状态代码；现场为 4 | Store 原始代码；不写 statistic |
| 映射状态 | 现场为 `Unproductive` | Store 规范枚举；最新 Enum Sensor |
| `trainingStatusFeedbackPhrase` | 现场为 `MAINTAINING_1` | Store；不单独建实体 |
| `fitnessTrend` | 现场为 0 | Store 和长期 statistic；数值 Sensor |
| `trainingPaused` | 是否暂停训练状态计算 | Store；需要时投影默认禁用的 Binary Sensor |
| 设备身份 | 每个返回设备的状态归属 | Store 内部身份；不把真实设备 ID 暴露为健康值 |

#### 急慢性负荷和 ACWR

| 原始字段 | 数据语义 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `dailyTrainingLoadAcute` | 急性负荷；现场 94 | Store 和长期 statistic；数值 Sensor |
| `dailyTrainingLoadChronic` | 慢性负荷；现场 100 | Store 和长期 statistic；数值 Sensor |
| `dailyAcuteChronicWorkloadRatio` | ACWR；现场 0.9 | Store 和长期 statistic；数值 Sensor |
| `acwrPercent` | Garmin ACWR 百分比；现场 38 | Store 和长期 statistic；按需投影数值 Sensor |
| `acwrStatus` | 现场为 `OPTIMAL` | Store 原始枚举；最新 Enum Sensor |
| `acwrStatusFeedback` | 现场为 `FEEDBACK_2` | Store；不单独建实体 |
| 最低、最高慢性负荷目标 | 现场范围 80–150 | Store 和两条长期 statistics |

#### 训练负荷重点

| 原始数据 | 数据语义 | 规范保存和 HA 投影 |
| --- | --- | --- |
| 月度低有氧负荷 | 现场约 358.3 | Store 和长期 statistic；数值 Sensor |
| 低有氧目标上下限 | Garmin 推荐范围 | Store 和两条长期 statistics |
| 月度高有氧负荷 | 现场为 0 | Store 和长期 statistic；数值 Sensor |
| 高有氧目标上下限 | Garmin 推荐范围 | Store 和两条长期 statistics |
| 月度无氧负荷 | 现场约 8.41 | Store 和长期 statistic；数值 Sensor |
| 无氧目标上下限 | Garmin 推荐范围 | Store 和两条长期 statistics |
| 负荷重点反馈 | 现场为 `AEROBIC_HIGH_SHORTAGE` | Store；最新状态属性或后续查询 |

#### VO2 Max

| 原始字段 | 数据语义 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `calendarDate` | VO2 Max 所属日期 | Store 身份 |
| `vo2MaxValue` | 现场 37.0 | Store 和长期 statistic；当前 Sensor |
| `vo2MaxPreciseValue` | 现场 37.1 | Store 和长期 statistic；图表优先使用精确值 |
| `maxMetCategory` | Garmin 分类代码 | Store；语义确认前不投影 |
| 可选设备身份 | 跑步、骑行或设备归属 | Store 内部身份；有明确来源时拆分 statistic |

当前训练归档只保存急性负荷、慢性负荷、ACWR 和 fitness trend 等部分数值。状态、目标
范围、负荷重点和反馈尚未完整落库。通用 VO2 Max 缺少设备 ID 时也可能被跳过；目标实现
必须保存通用值，不能因无法关联设备而丢弃。

### 8.4 睡眠评分和睡眠助手

#### 睡眠总体评分和分项

| 原始字段或子项 | 可取得内容 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `sleepScores.overall` | 总分和 `qualifierKey`；现场 93、`EXCELLENT` | 数值进 Store/statistic/Sensor；评级进 Store/最新状态 |
| `deepPercentage` | 数值、评级、最佳范围、理想秒数；现场 `FAIR` | 数值和范围进 Store/statistics；评级进 Store |
| `lightPercentage` | 数值、评级、最佳范围、理想秒数；现场 `GOOD` | 同上 |
| `remPercentage` | 数值、评级、最佳范围、理想秒数；现场 `EXCELLENT` | 同上 |
| `awakeCount` | 清醒次数、评级和范围 | Store；数值存在时写 statistic |
| `restlessness` | 躁动评分、评级和范围 | Store；数值存在时写 statistic |
| `stress` | 睡眠压力评分、评级和范围 | Store；数值存在时写 statistic |
| `totalDuration` | 总时长评分、评级和范围 | Store；时长写 statistic |
| `sleepScoreFeedback` | 睡眠评分反馈 | Store；不单独建实体 |
| `sleepScoreInsight` | Garmin 睡眠洞察 | Store；不单独建实体 |
| `sleepScorePersonalizedInsight` | 个性化洞察 | Store；不单独建实体 |

睡眠阶段的实际起止区间仍归第 6.8 节。本节保存的是 Garmin 对阶段和整晚睡眠的每日
计算结果，不能替代睡眠时间轴。

#### 当日 `sleepNeed` 和下一晚 `nextSleepNeed`

| 原始字段 | 数据语义 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `actual` | 计算后的需求；现场当日 450、下一晚 440 分钟 | 两个快照分别进 Store/statistics；当前 Sensor 显示下一晚值 |
| `baseline` | 基础需求；现场 480 分钟 | Store 和长期 statistic |
| `hrvAdjustment` | HRV 对需求的调整 | Store 和长期 statistic |
| `napAdjustment` | 午睡对需求的调整 | Store 和长期 statistic |
| `sleepHistoryAdjustment` | 近期睡眠历史调整 | Store 和长期 statistic |
| 训练调整 | 训练对需求的影响 | Store；有数值时写长期 statistic |
| `feedback`、`trainingFeedback` | 睡眠需求和训练反馈 | Store；不单独建实体 |
| `displayedForTheDay` | Garmin 是否向用户展示 | Store；不建实体 |
| `calendarDate`、`timestampGmt` | 所属日期和生成时间 | Store 身份和源时间 |
| `preferredActivityTracker` | 计算所依据的首选设备 | Store 内部来源；不公开真实 ID |
| `recommendedBedtimeStartMins` 等推荐窗口 | 推荐入睡窗口 | Store；转换后的时间投影最佳入睡实体 |
| 推导的最佳起床时间 | 推荐入睡时间加睡眠需求 | Store 标记为插件推导值；投影时间实体 |

`sleepNeed` 与 `nextSleepNeed` 必须保留为两个不同快照，不能用一个最终值覆盖另一份结构。

#### 睡眠响应中的重复来源

| 睡眠响应字段 | 规范处理 |
| --- | --- |
| `avgOvernightHrv` | Store 保留来源值用于核对；账号级历史使用 HRV 每日摘要 |
| `hrvStatus` | Store 保留来源值；不创建第二套 HRV 状态历史 |
| `bodyBatteryChange` | Store 保留睡眠变化量；全天 Body Battery 仍使用规范曲线 |
| 睡眠心率、压力、呼吸、SpO2 | 继续保留睡眠会话来源；不重复写账号级规范曲线 |
| 睡眠阶段 | 继续使用 Sleep Store 的完整区间 |

当前 Sleep Store 保存 `sleepScores` 的部分结构和会话流，但没有完整保存两份睡眠需求、
调整因素及三类反馈。这是明确实现差距。

### 8.5 健身年龄和身体评估

| 原始字段或组成项 | 可取得内容 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `chronologicalAge` | 实际年龄；现场 31 | Store 和长期 statistic；当前 Sensor |
| `fitnessAge` | 健身年龄；现场约 27.86 | Store 和长期 statistic；Sensor 和曲线 |
| `achievableFitnessAge` | 可达年龄；现场约 24.26 | Store 和长期 statistic；Sensor 和曲线 |
| `previousFitnessAge` | 上次结果；现场约 27.86 | Store 和长期 statistic；Sensor 和曲线 |
| `metabolicAge` | Garmin 返回时的代谢年龄 | Store 和长期 statistic；当前 Sensor |
| `lastUpdated` | Garmin 最后更新时间 | Store 源时间；实体属性 |
| `bodyFat` 组成项 | 当前值、目标、潜在年龄、改善值、日期、优先级、`stale` | 完整 Store；数值和目标写 statistics |
| `rhr` 组成项 | 静息心率、日期和 `stale` | Store；数值与规范静息心率交叉核对 |
| `vigorousDaysAvg` | 均值、目标、潜在年龄、周数和优先级 | Store；数值和目标写 statistics |
| `vigorousMinutesAvg` | 均值、目标、潜在年龄、周数和优先级 | Store；数值和目标写 statistics |
| `physiqueRating` | 体型分类代码；现场 5 | Store；最新实体；不写可求平均的 statistic |
| `visceralFat` | 内脏脂肪值 | Store 和长期 statistic；Sensor 和曲线 |

健身年龄响应中的身体脂肪和静息心率是 Garmin 计算时采用的输入快照。Store 保留其来源
值用于复现，但如果账号已有规范身体成分或静息心率历史，不制造第二条重复曲线。

### 8.6 压力每日摘要

压力每日摘要不是“此刻压力状态”。`stressQualifier` 是 Garmin 根据一个监测周期内全部
压力测量给出的日级定性标签，已知可出现 `balanced_awake`、`stressful_awake` 或
`unknown`。

| 原始字段 | 数据语义 | 规范保存和 HA 投影 |
| --- | --- | --- |
| `calendarDate` | 摘要所属日期 | Store 身份 |
| `averageStressLevel` | 当日平均压力 | Store 和长期 statistic；Sensor 和曲线 |
| `maxStressLevel` | 当日最高压力 | Store 和长期 statistic；Sensor 和曲线 |
| `totalStressDuration`、`stressDuration` | 有效压力监测时长 | Store；统一为分钟写 statistic |
| `restStressDuration` | 休息状态时长 | Store 和长期 statistic |
| `activityStressDuration` | 活动状态时长 | Store 和长期 statistic |
| `lowStressDuration` | 低压力时长 | Store 和长期 statistic |
| `mediumStressDuration` | 中压力时长 | Store 和长期 statistic |
| `highStressDuration` | 高压力时长 | Store 和长期 statistic |
| `uncategorizedStressDuration` | 未分类时长 | Store 和长期 statistic |
| `stressQualifier` | 当日整体定性评价 | Store 原始枚举；最新“压力每日摘要”Enum Sensor |
| 低、中、高压力百分比 | 由各时长计算 | 查询时可推导；现有百分比 Sensor 可保留 |

全天压力采样点继续归第 6.3 节的规范 `stress` 曲线。每日摘要和连续曲线均保存，互不
替代。

### 8.7 当前设备确认不支持的数据

下列能力在当前账号接口中返回空值，并已在 Garmin App 确认不可见。它们是当前设备能力
限制，不是解析或持久化缺陷：

| 数据族 | 当前结论 | 处理 |
| --- | --- | --- |
| 训练准备度 | 接口为空，App 无 | 保留兼容代码；记录 `unsupported`；不创建历史 |
| 晨间训练准备度 | 接口为空，App 无 | 同上 |
| 恢复时间 | 当前设备和 App 无 | 同上 |
| 耐力分数 | 空对象，App 无 | 同上 |
| 坡度分数 | 空对象，App 无 | 同上 |
| 乳酸阈值 | 当前设备和 App 无 | 同上 |

其他设备未来返回有效结构时仍可启用对应兼容路径；不能把本设备结论推广成 Garmin 全局
不支持。

### 8.8 尚待单独设计的数据族

| 数据族 | 原始形态 | 当前插件状态 | 后续问题 |
| --- | --- | --- | --- |
| 活动会话 | 起止、类型、训练效果、GPS、FIT | Calendar、Store、FIT | 会话摘要与 FIT 传感器流的关系 |
| 体重和体成分 | 每次测量事件 | 主要暴露最近值 | 按每次测量长期保存 |
| 血压 | 每次测量事件 | 最近值实体 | 账号是否持续产生数据 |
| 饮水 | 手工记录和每日累计 | 当前实体与写入服务 | 事件和每日累计如何并存 |
| 热量、楼层、强度分钟 | 每日摘要，部分可有分段 | 当前实体；部分每日归档 | 稳定字段目录 |
| Move IQ、异常心率 | 区间或事件 | 已确认部分结构 | 真实事件 schema 和来源语义 |
| 女性健康、营养 | 敏感事件和摘要 | 默认禁用实体 | 仅在明确使用和授权后处理 |
| Health Snapshot | 两分钟会话 | 当前 GraphQL 认证返回 403 | 不重复撞接口，另找可靠入口 |
| ECG、皮温、血糖 | 设备、地区或订阅限制 | 当前设备多数不支持或未验证 | 不猜测端点，不作为当前目标 |

## 9. 已确认的数据能力和限制

### 9.1 低频能力审计

2026-07-25 使用现有账号完成 31 项低频只读检查：30 个普通 Connect 请求成功，未出现
429、401 或普通请求错误；Health Snapshot GraphQL 返回 403 后停止；ECG 未猜测 URL。

代表日确认过睡眠阶段、动作、心率、HRV、Body Battery、压力、呼吸、SpO2、分段步数、
训练状态、每日事件和 Body Battery 事件。较老代表日的详细数组多数为空，不能据此判断
设备永久不支持；Garmin 可能延迟上传或 offload 旧 wellness 数据。

### 9.2 设备和数据缺口

- 当前 Forerunner 255 支持主要健康和训练数据，但不支持 ECG；夜间皮温也不在其能力内。
- 睡眠、HRV、SpO2 和压力依赖佩戴、光学心率、Primary Wearable、同步和设备设置。
- `skinTempDataExists=false` 只说明指定夜晚没有皮温数据。
- `dailyEvents` 出现活动类型不等于每条事件都已确认为 Move IQ。
- Garmin Connect UI 有数据而接口暂时为空时，应优先判断同步、延迟或旧数据 offload，
  不能直接写成 `unsupported`。
- 第 8.7 节是上述规则的明确例外：接口为空且 App 也确认不存在，因此可将这些能力标记
  为当前设备 `unsupported`，但不能推广到其他设备。

设备和产品规则的详细交叉核对见
[Garmin 官方规则与用户反馈](garmin-forum-feedback.md)。

### 9.3 限流约束

- 持久化并复用 OAuth token，正常同步不能重复用户名/密码 SSO。
- 请求按账号串行门控并区分当前值与后台归档优先级。
- 429 立即停止归档并进入持久退避；不能把一次未限流审计当作安全配额。
- 401/403 不循环登录或换参数反复撞同一入口。
- 已成功且稳定的日期通过协调窗口结算，避免永久重复请求。

## 10. 当前决策与实现债务

### 已确定

- 数值原始点使用带源时间的长期 Recorder statistics。
- 结构化会话和事件使用账号隔离 Store，按需投影 Calendar。
- 活动 FIT 独立保存，不与全天 wellness 数据合并。
- 归档是前瞻性、十五分钟级、尽力完整；不自动全年回填。
- 原始数据不做健康解释，不提供自动归档删除。
- 每日状态快照使用账号隔离、按年分区的 Store 永久保存；数值字段另投影长期
  statistics，普通实体仅承担最新值展示和自动化入口。
- 状态枚举、评级、目标范围、组成因素、调整和反馈均保留；枚举不转换成可求平均的数字。
- 睡眠建议文本随快照保存但不单独建实体；临时捕获目录继续承担完整响应重放。
- 当前不开放每日状态的 REST、WebSocket、响应动作或自定义卡片查询入口。

### 已讨论但尚未实现统一

- Body Battery、压力和夜间 HRV 各自只保留一个用户逻辑主序列。
- 心率把全天与睡眠补充点合并成跨午夜连续序列。
- 全天呼吸作为规范曲线；睡眠呼吸保留会话和时间锚点来源。
- 睡眠 SpO2 与全天小时 SpO2 保持两个独立序列。
- 睡眠阶段保留区间语义，不转为可求平均的数值。
- 每日状态 Store 统一承载 HRV、训练、睡眠、健身年龄和压力摘要；同日完整响应可修订，
  临时空响应不能擦除正常数据。

### 实现债务

- 睡眠数值 statistic ID 目前包含会话 ID，长期会分裂成每晚一条序列。
- Recorder 非整点导入依赖内部接口，需要继续维持版本门和回归测试。
- 当前 HRV 摘要目录只覆盖部分字段，尚未实现统一的年度状态 Store。
- 训练归档缺少状态、目标区间、负荷重点、反馈和无设备 ID 的通用 VO2 Max。
- Sleep Store 尚未保存完整 `sleepNeed`、`nextSleepNeed`、调整因素和三类睡眠反馈。
- 健身年龄当前只暴露结果实体，组成因素刷新后不会形成永久历史。
- 压力每日摘要当前依赖普通实体，尚未形成按源日期的完整快照。

## 11. 下一轮讨论

跟踪级曲线和每日状态快照的目标模型已经确定。下一轮继续讨论尚未完成数据身份设计的
离散测量、事件和会话：

1. 体重和身体成分按每次测量保存，而不是只保留最近值；
2. 血压和饮水事件与每日摘要如何并存；
3. 活动会话摘要、Calendar 和 FIT 传感器流的关系；
4. Move IQ、异常心率、热量、楼层和强度分钟的稳定字段目录；
5. 数据积累后是否开放统一查询动作或为少量状态制作自定义卡片。

插件继续只负责采集、持久化和查询，不在 HA 内承担健康分析。外部分析程序未来应读取
同一规范归档；查询表面在数据开始稳定积累后再设计。

## 12. 参考资料

- [Garmin Health API](https://developer.garmin.com/gc-developer-program/health-api/)
- [Garmin Health SDK 能力比较](https://developer.garmin.com/health-sdk/overview/)
- [python-garminconnect](https://github.com/cyberjunky/python-garminconnect)
- [Home Assistant Recorder](https://www.home-assistant.io/integrations/recorder/)
- [Home Assistant Sensor 和长期统计](https://developers.home-assistant.io/docs/core/entity/sensor/)
- [Home Assistant state reported timestamp](https://developers.home-assistant.io/blog/2024/03/20/state_reported_timestamp/)
- [Recorder statistics API changes](https://developers.home-assistant.io/blog/2025/10/16/recorder-statistics-api-changes/)
- 本项目能力审计账本：`/home/js/garmin-capability-audit-ledger.json`
