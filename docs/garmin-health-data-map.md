# Garmin Connect 健康数据地图

更新时间：2026-07-25

## 结论

Garmin Connect 中适合长期分析的数据远不止当前验证过的心率、压力、Body
Battery 和 HRV。按数据形态，可以分成三层：

1. **日内时间序列**：全天心率、压力、Body Battery、HRV、步数活动片段、
   呼吸、Pulse Ox（血氧）。
2. **事件或会话**：睡眠及睡眠阶段、午睡、运动活动、Body Battery 事件、
   体重/体成分、血压、饮水、女性健康日志。
3. **每日或周期摘要**：步数、热量、楼层、强度分钟、睡眠评分、静息心率、
   HRV 状态、训练准备度、训练状态/负荷、恢复时间、身体能量充放电等。

官方 Garmin Health API 明确列出 Steps、Intensity Minutes、Sleep、Calories、
Heart Rate、Stress、Pulse Ox、Body Battery、Body Composition、Respiration 和
Blood Pressure，并说明会提供全天活动的详细 stress、pulse-ox 和 epoch
summary。[Garmin Health API](https://developer.garmin.com/gc-developer-program/health-api/)

不过，当前 HA 集成不是获批的 Garmin Health API 客户端，而是复用 Garmin
Connect 网页/移动端使用的 **未公开 Connect API**。开源客户端已经为很多项目
找到可用端点，但这些端点没有稳定性或兼容性承诺，字段还会因设备、固件、地区、
账号功能和订阅层级而不同。`python-garminconnect` 的类型层因此明确允许 Garmin
随固件或订阅增加未知字段。[类型模型说明](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/typed.py#L68-L84)

## 数据目录

“当前 HA 支持”指 `home-assistant-garmin_connect` 3.0.14 / `ha-garmin`
0.1.31 已生成实体或已有读取方法；不代表它已经保留了完整历史序列。

| 数据族 | 数据形态与可用细节 | Connect API / 当前代码证据 | 当前 HA 支持 | 结论 |
|---|---|---|---|---|
| 全天心率 | 日内序列；每日最低、最高、静息心率；运动会话内可有更高频心率、心率区间 | `dailyHeartRate/{displayName}?date=` 已由开源客户端实现；活动详情和原始 FIT 另有接口。[心率读取](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1020-L1047) [活动详情/FIT](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L2493-L2516) | 有每日摘要；本项目已验证全天序列 | **P0**：长期健康曲线的核心 |
| HRV | 夜间时间序列；每日夜间平均、5 分钟最高、7 日平均、状态和个人 baseline | `hrv-service/hrv/{date}`；模型含 `hrvReadings`、`hrvSummary`、baseline 和睡眠起止时间。[HRV 端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1722-L1728) [HRV schema](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/typed.py#L242-L287) | 有摘要实体；本项目已验证夜间序列 | **P0**：长期趋势应同时保留原始夜间点和 Garmin 计算的 baseline/status |
| 压力 | 全天日内序列；每日平均/最大；休息、活动、低/中/高压力时长；负数可能是非压力状态码 | `dailyStress/{date}`；开源客户端实现日数据与周聚合。[日内压力](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1560-L1566) [周压力](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L974-L993) | 有每日摘要；本项目已验证日内序列 | **P0**：适合与睡眠、HRV、活动交叉分析 |
| Body Battery / 身体能量 | 不规则日内变化点；每日 charged/drained/high/low；事件可关联睡眠、午睡、运动和自动检测活动 | `bodyBattery/reports/daily` 返回 `bodyBatteryValuesArray`；另有 `bodyBattery/events/{date}`。[序列与事件](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1253-L1279) [schema](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/typed.py#L290-L315) | 有每日摘要；本项目已验证变化点 | **P0**：序列与事件都值得保留 |
| 睡眠和午睡 | 每次睡眠会话；入睡/醒来时间；深睡、浅睡、REM、清醒时长；午睡；整体评分及 duration/stress/awake count/REM/restlessness/light/deep 子评分；睡眠中的 HRV、SpO2、呼吸 | `dailySleepData`；模型明确列出阶段时长、评分子项、睡眠 HRV/SpO2/呼吸，并说明 raw payload 含较大的 per-minute heart rate / movement / SpO2 arrays。[睡眠端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1684-L1691) [睡眠 schema](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/typed.py#L155-L238) | 有评分、时间和阶段总量实体；未保留阶段时间轴 | **P0**：优先确认并保留每晚阶段时间轴、评分子项和夜间生命体征 |
| 步数、活动强度和“热血时间” | 每日总步数；全天 epoch/时间片步数及活动级别；每日/每周 moderate、vigorous intensity minutes；久坐/活动/高度活动时长 | `dailySummaryChart` 返回分段 steps/activity level；另有日步数范围、周步数和周强度分钟接口。[分段步数与日/周统计](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L860-L1018) | 有当日总量和部分周平均；未保留完整分段 | **P0**：这里很可能就是用户所说的“热血/活跃时间” |
| 呼吸率 | 全天/睡眠日内序列与聚合；最低、最高、最新、睡眠平均 | `daily/respiration/{date}` 已实现；实际响应字段包括 `respirationValuesArray` 和 `respirationAveragesValuesArray`（是否有值依设备）。[呼吸端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1536-L1542) [捕获的响应 schema](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/tests/cassettes/test_respiration_data.yaml#L90-L98) | 目前主要是 min/max/latest 摘要实体 | **P1**：应确认用户设备是否返回全天数组和睡眠数组 |
| Pulse Ox / SpO2 | 单点、连续读取、小时平均及每日/睡眠平均、最低、最新；实际是否有连续序列取决于手表血氧设置、设备和耗电策略 | `daily/spo2/{date}` 已实现；响应 schema 暴露 `spO2SingleValues`、`continuousReadingDTOList`、`spO2HourlyAverages`。[血氧端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1544-L1550) [捕获的响应 schema](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/tests/cassettes/test_spo2_data.yaml#L90-L99) | 有 average/lowest/latest 摘要实体；未保留序列 | **P1**：健康意义高，但首先要确认账号实际返回非空序列 |
| 训练准备度 | 每日或醒来时快照；score/level；sleep、recovery time、acute/chronic workload ratio、HRV、近期压力等 factor 与反馈 | `trainingreadiness/{date}` 返回快照列表；类型模型列出 score、recovery time、ACWR、HRV 和 stress-history factor。[端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1730-L1736) [schema](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/typed.py#L318-L379) | 有当前/晨间准备度和恢复时间实体，详细 factors 在属性中 | **P1**：对长期恢复分析很有价值，应按日期保留完整快照而非只留最终 score |
| 训练状态、负荷、恢复、VO2 Max | 通常是每日状态/快照；training status、acute load/ACWR、恢复时间、VO2 Max；另有 endurance score、hill score、乳酸阈值、FTP/功重比 | 当前客户端提供 `trainingstatus/aggregated/{date}`，HA 集成将完整响应放在实体属性；训练准备度也携带恢复时间和负荷因素。[训练状态端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1895-L1901) [当前 HA 暴露字段](https://github.com/cyberjunky/home-assistant-garmin_connect/blob/cb2cfca39b4fe21f5178f89d4584c5d2f784f460/custom_components/garmin_connect/sensor.py#L790-L925) | 有当前实体/属性；没有可靠的逐日序列保留 | **P1**：先盘点真实 payload，再决定各负荷字段含义；不要仅凭字段名推断 |
| 运动活动 | 离散会话；活动类型、起止时间、距离、热量、心率、速度、配速、GPS、圈/分段、功率、训练负荷等；原始 FIT 可提供设备实际记录的高分辨率传感器点 | Garmin 官方 Activity API 定位为 30+ 活动类型的完整活动数据；FIT 官方说明 Activity 文件记录时间、运动类型、圈/分段、GPS、传感器和事件。[Developer Program](https://developer.garmin.com/gc-developer-program/) [FIT Activity](https://developer.garmin.com/fit/file-types/activity/) | 有最近活动和有限详情；没有完整长期活动仓库 | **P1**：与全天健康曲线分开看待，属于会话数据 |
| 热量 | 每日 total/active/BMR/wellness calories；活动会话也有 calories；官方 Health API 支持 calories | 每日 summary 模型列出四类 calories。[DailyStats schema](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/typed.py#L92-L137) | 有每日实体 | **P1**：每日趋势可靠；日内热量是否有有意义的粒度需另行确认 |
| 楼层/垂直活动 | 每日上升/下降楼层和距离；独立 `floorsChartData/daily/{date}` 可能提供时间片 | 客户端已有 floors chart 端点及 daily summary 字段。[楼层端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L877-L890) [摘要字段](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/typed.py#L124-L137) | 有每日上升/下降实体 | **P2**：设备支持时可纳入活动量分析 |
| 饮水/补水 | 手工或第三方记录事件汇总为每日 intake、goal、average、sweat loss、activity intake；不是手表自动测得的体内含水量 | `usersummary/hydration/daily/{date}`；当前集成也有写入服务。[读取端点](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1528-L1534) | 有每日实体和写入服务 | **P2**：只有持续记录时才有分析价值；不要与体成分中的 body water 混为一谈 |
| 体重和身体成分 | 每次称重事件；weight、BMI、body fat、body water、muscle mass、bone mass、visceral fat、physique rating、metabolic age 等（取决于秤/来源） | `weight/dateRange`；客户端写入模型列出这些字段。[体成分范围与字段](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1060-L1115) | 有最新值实体，当前读取逻辑偏向最近一次称重 | **P1/P2**：若账号有历史称重，应保留“每次测量”，不能只按每日最终值 |
| 血压 | 离散测量事件：收缩压、舒张压、脉搏、时间、来源/备注 | Garmin Health API 明确支持；私有接口支持日期范围和 `includeAll`。[血压范围接口](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L1314-L1329) | 有最近一次测量实体及写入服务 | **P2**：有 Garmin/兼容血压记录时再纳入 |
| 女性健康 | 每日/事件日志与周期：经期阶段、cycle day、flow、症状、情绪、排卵、预测窗口；另有 pregnancy snapshot | Garmin 官方 Women’s Health API 支持 menstrual cycle tracking/scheduling 和 pregnancy；私有接口已有 day view、calendar range 与 pregnancy snapshot。[官方 Women’s Health API](https://developer.garmin.com/gc-developer-program/womens-health-api-japanese/) [私有接口](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L2991-L3017) | 集成已有实体，默认禁用 | **按需**：高度敏感，只在用户实际使用且明确需要时处理 |

## 已确认、可确认和未验证候选

### 已在本账号确认有完整序列

- 全天心率
- 全天压力
- Body Battery 变化点
- 夜间 HRV

### 有明确端点/schema，值得在下一轮做只读能力确认

- 睡眠原始 payload：睡眠阶段时间轴、评分子项、睡眠 movement/HR/SpO2
- 分段步数及 activity level
- 呼吸日内数组和睡眠呼吸
- SpO2 单点/连续/小时数组
- Body Battery 事件
- 训练准备度完整 factors
- 训练状态 payload 中的负荷、状态和 VO2 Max 历史
- 每次称重/体成分历史
- 活动会话详情及原始 FIT

### 只能列为未验证候选

以下项目在官方产品能力或私有 payload 中可能出现，但尚无本账号真实响应证明，不能
先假定存在：

- 睡眠阶段边界的确切字段名、采样粒度和午睡阶段细节
- 全天 SpO2 连续序列（许多账号只返回摘要或空数组）
- 全天呼吸数组的实际覆盖率
- training load、acute load、chronic load、load focus、load ratio 等字段在本设备
  上的完整集合
- Health Snapshot 会话及其中的 HRV、SpO2、呼吸等短时测量
- Move IQ 自动识别事件的完整 schema
- 异常心率事件的逐条历史
- 女性健康/孕期数据（取决于账号是否启用）
- 营养、血糖、ECG、皮肤温度等较新、地区限制或订阅限制的数据

这些候选必须继续遵守“先看 payload keys/点数/时间范围，不打印完整隐私内容”的
只读检查原则。

## 当前集成与长期分析之间的差距

当前 HA 集成已经有 130+ 个实体，覆盖大量“今天/最近一次”的值，但它主要是状态
展示层：例如睡眠阶段目前是每晚总分钟数，训练状态是当前文本和属性，体成分倾向返回
最近一次测量。它没有把 Garmin 云端已有的全部历史序列自动变成 HA 内的历史。

需要特别区分：

- **每日摘要实体**不是日内曲线。
- **实体属性中的复杂 JSON**不会自然变成可长期统计的数值曲线。
- **运动 FIT**是会话传感器流，不应与全天 wellness epoch 混成同一类。
- Garmin 会 offload 较老数据；`python-garminconnect` 明确提供
  `wellness/epoch/request/{date}` 来请求重新加载旧日期。
  [旧数据 reload 说明](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L2735-L2743)

## 可用性、许可和隐私边界

1. **官方路径**：Garmin Health API 是经审批的正式路径，支持用户同意后的云端同步、
   JSON、push 或 ping/pull、backfill；商业使用需要许可费。
   [官方 Health API](https://developer.garmin.com/gc-developer-program/health-api/)
2. **当前路径**：`ha-garmin` 和 `python-garminconnect` 调用的是 Garmin Connect
   私有端点。它适合个人、自托管实验，但端点、字段、认证、限流可能随时变化。
3. **设备可用性**：Garmin 官方也明确说明某些指标/设备型号并不全部支持；云端只有在
   设备同步后才有数据。[Health SDK/Connect 数据能力比较](https://developer.garmin.com/health-sdk/overview/)
4. **品牌要求**：若以后形成对外展示或产品，Garmin 的品牌指南要求 Garmin
   设备来源数据在 dashboard/activity feed/summary 中带设备归属标识。
   [Garmin API Brand Guidelines](https://developer.garmin.com/downloads/brand/Garmin-Developer-API-Brand-Guidelines.pdf)
5. **健康数据敏感性**：睡眠、HRV、血氧、体重、女性健康等应默认最小暴露；日志、
   服务返回和诊断文件不应包含账号凭据，也不应无意打印完整日内健康数组。

## 优先级建议（仅数据范围，不是实施计划）

1. **第一优先级**：睡眠完整会话/阶段、心率、HRV、压力、Body Battery、步数和
   强度分钟。这组数据能形成“睡眠—恢复—压力—活动”的核心长期视图。
2. **第二优先级**：呼吸、SpO2、训练准备度/训练状态/负荷/恢复、运动会话详情。
   它们的健康价值高，但设备覆盖和 schema 差异更大。
3. **第三优先级**：热量、楼层、体重/体成分、饮水、血压。是否重要取决于用户是否
   持续产生相应记录。
4. **按需且单独授权**：女性健康、孕期、营养及其他特别敏感或订阅限制数据。

最关键的设计事实是：不能把所有内容都当作普通 HA“当前值实体”。完整目录里同时
存在高频点、睡眠/运动会话、离散测量事件和每日摘要；长期分析时必须保留它们原本的
数据形态和时间语义。

## 2026-07-25 第一轮低频只读能力审计

本轮使用现有 HA Garmin 登录，逐项调用一个端点；调用之间间隔 30 秒。每个
“端点＋日期”都写入本地完成账本，后续默认跳过。只保存字段结构、非空状态、
数组长度、白名单可用性标志和事件分类，不保存实际健康数值。

结果：

- 31 个审计项中，30 个普通 Connect API 请求成功。
- 没有出现 429 限流、401 或普通 Connect API 错误。
- Health Snapshot GraphQL 返回 403 后立即停止；没有重试，也没有继续调用同一
  GraphQL 入口的另一个查询。
- ECG 没有找到足够可靠的读取端点，因此没有猜测 URL 发请求。

### 近期账号中已确认存在的数据结构

2026-07-24 睡眠响应包含：

| 数组 | 点数 |
|---|---:|
| 睡眠阶段 `sleepLevels` | 27 |
| 睡眠动作 `sleepMovement` | 648 |
| 睡眠心率 `sleepHeartRate` | 264 |
| 夜间 HRV `hrvData` | 105 |
| 睡眠 Body Battery | 177 |
| 睡眠压力 | 177 |
| 睡眠呼吸 | 264 |
| 夜间 SpO2 | 526 |
| 不安时刻 | 35 |

同一天的全天/活动响应还确认：

- 分段步数 96 个时间片。
- 楼层 96 个时间片。
- 全天呼吸 720 点，呼吸平均值 24 点。
- SpO2 小时平均 19 点。
- Intensity Minutes 时间片数组存在。
- `dailyEvents` 4 项，Body Battery 事件 3 项。
- 训练状态响应非空，包含最近训练状态、训练负荷平衡和记录设备结构。
- 每日摘要中的 `abnormalHeartRateAlertsCount` 为非空数值字段；此前心率探针
  也确认响应含 `abnormalHRValuesArray` 字段。

### 皮肤温度、Move IQ 与 Health Snapshot

- 睡眠响应确实有 `skinTempDataExists` 标志，但 2026-07-22、2025-07-24 和
  2024-07-24 三个代表日均为 `false`。这表示接口正确且账号返回了明确标志，
  但这些日期没有皮温数据；可能与当晚佩戴设备或设备设置有关。
- `dailyEvents` 在多个代表日返回事件，并出现 `activityType=cycling`；每日摘要/
  Body Battery 事件还出现 `ACTIVITY`、`NAP`、`SLEEP`、`STRESS` 分类。这确认
  自动/日事件数据可以读取，但仅凭当前结构摘要还不能把每一项都断言为 Move IQ。
- Health Snapshot 有已知 GraphQL 查询，但当前 `ha-garmin` DI Bearer 认证访问
  GraphQL 得到 403。不能据此判断账号没有 Health Snapshot，只能判断当前认证
  路径无法读取。

### 较老代表日期的限制

2025-07-24 和 2024-07-24 的每日摘要、步数、事件与训练状态接口仍可返回结构，
但所选日期的睡眠、呼吸、SpO2 详细数组大多为空。这可能是当日没有设备数据，也
可能是 Garmin 将较老 epoch 数据离线，不能用两个日期推断旧设备整体不支持。

审计账本：

`/home/js/garmin-capability-audit-ledger.json`
